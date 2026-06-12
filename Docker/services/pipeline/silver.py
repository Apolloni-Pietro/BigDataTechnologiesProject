"""SILVER layer: clean, type, deduplicate and explode bronze JSON into a single
tabular event model, stored as partitioned Parquet on MinIO.

This is the medallion layer the original plan was missing. Metrics are NEVER
computed on raw JSON — they are computed on this conformed table. The schema
mirrors the one engineered in the top-level GHArchiveDownload.py.
"""

import logging

import config
import storage

log = logging.getLogger("pipeline.silver")


def silver_key(year: int, month: int, day: int, hour: int) -> str:
    """Hive-partitioned silver key: events/event_date=YYYY-MM-DD/hour=H.parquet."""
    return f"events/event_date={year:04d}-{month:02d}-{day:02d}/hour={hour}.parquet"


def build_hour(year: int, month: int, day: int, hour: int) -> str | None:
    """Transform one bronze hour into one silver Parquet file. Idempotent.

    Returns the silver key on success, None if the bronze input is missing.
    """
    from bronze import gharchive_key

    bkey = gharchive_key(year, month, day, hour)
    skey = silver_key(year, month, day, hour)

    if storage.object_exists(config.SILVER_BUCKET, skey):
        log.info("silver: %s already built, skipping", skey)
        return skey
    if not storage.object_exists(config.BRONZE_BUCKET, bkey):
        log.warning("silver: bronze input %s missing, cannot build %s", bkey, skey)
        return None

    src = storage.s3_uri(config.BRONZE_BUCKET, bkey)
    dst = storage.s3_uri(config.SILVER_BUCKET, skey)
    con = storage.duckdb_con()

    # One conformed event row per GH event, with type-specific payloads kept as
    # nested STRUCTs (cheap to skip, easy to query). Text/free-form fields are
    # dropped; identity & numeric/categorical signals are kept.
    query = f"""
    COPY (
        SELECT
            id::VARCHAR                       AS event_id,
            type                              AS event_type,
            -- NB: event_date is intentionally NOT a column here; it is encoded
            -- in the Hive partition path (event_date=YYYY-MM-DD/). Duplicating
            -- it would clash with the partition column on read.
            created_at::TIMESTAMP             AS event_timestamp,
            actor.id                          AS actor_id,
            actor.login                       AS actor_login,
            repo.id                           AS repo_id,
            repo.name                         AS repo_name,
            org.login                         AS org_login,

            CASE WHEN type = 'PushEvent' THEN {{
                'size': (payload->>'size')::INT,
                'distinct_size': (payload->>'distinct_size')::INT,
                'commits': list_transform(
                    CAST(payload->'commits' AS JSON[]),
                    x -> {{'sha': json_extract_string(x, '$.sha'),
                          'author_name':  json_extract_string(x, '$.author.name'),
                          'author_email': json_extract_string(x, '$.author.email')}}
                )
            }} ELSE NULL END                   AS payload_push,

            CASE WHEN type = 'PullRequestEvent' THEN {{
                'action': payload->>'action',
                'pr_id': (payload->'pull_request'->>'id')::BIGINT,
                'state': payload->'pull_request'->>'state',
                'merged': (payload->'pull_request'->>'merged')::BOOLEAN,
                'created_at': (payload->'pull_request'->>'created_at')::TIMESTAMP,
                'closed_at': (payload->'pull_request'->>'closed_at')::TIMESTAMP,
                'merged_at': (payload->'pull_request'->>'merged_at')::TIMESTAMP
            }} ELSE NULL END                   AS payload_pull_request,

            CASE WHEN type = 'IssuesEvent' THEN {{
                'action': payload->>'action',
                'issue_id': (payload->'issue'->>'id')::BIGINT,
                'state': payload->'issue'->>'state',
                'created_at': (payload->'issue'->>'created_at')::TIMESTAMP,
                'closed_at': (payload->'issue'->>'closed_at')::TIMESTAMP
            }} ELSE NULL END                   AS payload_issue,

            CASE WHEN type = 'IssueCommentEvent' THEN {{
                'issue_number': (payload->'issue'->>'number')::INT,
                'author_association': payload->'comment'->>'author_association'
            }} ELSE NULL END                   AS payload_issue_comment,

            CASE WHEN type = 'ReleaseEvent' THEN {{
                'action': payload->>'action',
                'tag_name': payload->'release'->>'tag_name',
                'prerelease': (payload->'release'->>'prerelease')::BOOLEAN
            }} ELSE NULL END                   AS payload_release

        FROM read_json('{src}',
            format='newline_delimited',
            columns={{
                'id': 'VARCHAR', 'type': 'VARCHAR', 'created_at': 'VARCHAR',
                'actor': 'STRUCT(id BIGINT, login VARCHAR)',
                'repo': 'STRUCT(id BIGINT, name VARCHAR)',
                'org': 'STRUCT(id BIGINT, login VARCHAR)',
                'payload': 'JSON'
            }}
        )
        WHERE type IN {tuple(config.TRACKED_EVENT_TYPES)}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at) = 1  -- dedup
    ) TO '{dst}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
    """
    con.execute(query)
    log.info("silver: built %s", skey)
    return skey
