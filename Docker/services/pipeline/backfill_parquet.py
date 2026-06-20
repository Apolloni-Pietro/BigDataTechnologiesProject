"""PARQUET BACKFILL: ingest pre-downloaded monthly Parquet files straight into
silver, skipping bronze and JSON parsing entirely.

The monthly files are produced by the repo-root GHArchiveDownload.py (the rich
13-type schema). They are already typed and exploded, so this stage only has to
**re-project them into the exact silver schema** and write them hive-partitioned
into the silver bucket, where the normal silver->gold transform picks them up.

WHY this mirrors silver.py exactly
----------------------------------
gold reads ALL silver as one uniform dataset
(`read_parquet('events/**/*.parquet', hive_partitioning=true)`), and the hourly
scheduler keeps writing silver.build_hour files into the same `events/` prefix
afterwards. So the output here MUST be byte-compatible with silver.build_hour:
same columns, same struct field names/order/types, and `event_date` encoded ONLY
in the partition path (never a stored column). See Docker/PARQUET_BACKFILL.md.

This is the one HIGH-danger spot: if silver.py's schema changes, the projection
below must change in lockstep.

WHY per-day, not the whole month at once
----------------------------------------
A monthly file is multi-GB (~150M rows). Deduplicating it in one query
(`ROW_NUMBER() OVER (PARTITION BY event_id ...)`) is a global sort whose working
set cannot fully spill, so it OOMs even with a memory limit + spill dir. We
instead process **one calendar day at a time**: the dedup sort then runs over a
single day (~few M rows), which fits comfortably in memory. This is also exactly
faithful — GH Archive's rare duplicate events always carry the same timestamp, so
they fall in the same day; per-day dedup catches every duplicate a whole-month
dedup would. Each day is written as its own `event_date=YYYY-MM-DD/` partition.

We partition by event_date ONLY (via the explicit output key, not a stored
column). Live silver files encode hour in the *filename*
(`event_date=X/hour=H.parquet`), which DuckDB does NOT parse as a hive key, so the
only hive key live files expose is event_date. Matching that key set (event_date
only) keeps gold's combined `read_parquet(..., hive_partitioning=true)` consistent
across backfill and live files; gold never uses hour anyway.
"""

import logging

import config
import storage

log = logging.getLogger("pipeline.backfill_parquet")


# The 13 silver data columns, in silver.py's exact order (NO event_date — that is
# encoded in the output key's partition path). Re-projects native struct fields
# from the monthly file into silver's narrower struct shapes.
_PROJECTION = """
    event_id,
    event_type,
    event_timestamp,
    actor_id,
    actor_login,
    repo_id,
    repo_name,
    org_login,

    CASE WHEN event_type = 'PushEvent' THEN {
        'size': payload_push.size,
        'distinct_size': payload_push.distinct_size,
        'commits': list_transform(
            payload_push.commits,
            x -> {'sha': x.sha,
                  'author_name':  x.author_name,
                  'author_email': x.author_email}
        )
    } ELSE NULL END                   AS payload_push,

    CASE WHEN event_type = 'PullRequestEvent' THEN {
        'action': payload_pull_request.action,
        'pr_id': payload_pull_request.pr_id,
        'state': payload_pull_request.state,
        'merged': payload_pull_request.merged,
        'created_at': payload_pull_request.created_at,
        'closed_at': payload_pull_request.closed_at,
        'merged_at': payload_pull_request.merged_at
    } ELSE NULL END                   AS payload_pull_request,

    CASE WHEN event_type = 'IssuesEvent' THEN {
        'action': payload_issue.action,
        'issue_id': payload_issue.issue_id,
        'state': payload_issue.state,
        'created_at': payload_issue.created_at,
        'closed_at': payload_issue.closed_at
    } ELSE NULL END                   AS payload_issue,

    CASE WHEN event_type = 'IssueCommentEvent' THEN {
        'issue_number': payload_issue_comment.issue_number,
        'author_association': payload_issue_comment.author_association
    } ELSE NULL END                   AS payload_issue_comment,

    CASE WHEN event_type = 'ReleaseEvent' THEN {
        'action': payload_release.action,
        'tag_name': payload_release.tag_name,
        'prerelease': payload_release.prerelease
    } ELSE NULL END                   AS payload_release
"""


def build_month(local_path: str) -> int:
    """Re-project one monthly Parquet file into silver, day by day.

    The monthly file is read locally; each day's deduped, re-projected rows are
    written to `silver/events/event_date=YYYY-MM-DD/data.parquet` on MinIO.
    Returns total silver rows written for the month.
    """
    con = storage.duckdb_con()  # memory_limit + temp_directory are set here (see storage)
    con.execute("SET preserve_insertion_order = false;")

    tracked = tuple(config.TRACKED_EVENT_TYPES)

    # Distinct days present in the file (cheap streaming aggregate over a handful
    # of values). Drives the per-day loop below.
    days = [
        r[0] for r in con.execute(
            f"""
            SELECT DISTINCT event_date
            FROM read_parquet('{local_path}')
            WHERE event_type IN {tracked} AND repo_name IS NOT NULL
            ORDER BY 1
            """
        ).fetchall()
    ]
    log.info("backfill_parquet: %s -> %d day(s) to ingest", local_path, len(days))

    total = 0
    for day in days:
        key = f"events/event_date={day}/data.parquet"
        # Idempotency / resumability: a day already written (e.g. from an earlier
        # run that crashed or was restarted) is skipped, so a restart re-reads only
        # the cheap event_date column above and resumes where it left off instead of
        # re-ingesting the whole month. Matches bronze/silver's skip-existing contract.
        if storage.object_exists(config.SILVER_BUCKET, key):
            log.info("backfill_parquet:   %s already built, skipping", day)
            continue
        dst = storage.s3_uri(config.SILVER_BUCKET, key)
        query = f"""
        COPY (
            SELECT {_PROJECTION}
            FROM read_parquet('{local_path}')
            WHERE event_date = DATE '{day}'
              AND event_type IN {tracked}
              AND repo_name IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_timestamp) = 1  -- dedup
        ) TO '{dst}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
        """
        result = con.execute(query).fetchone()
        rows = int(result[0]) if result else 0
        total += rows
        log.info("backfill_parquet:   %s -> %d rows", day, rows)

    log.info("backfill_parquet: %s -> %d silver rows total", local_path, total)
    return total
