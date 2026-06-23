"""GOLD layer: compute business-ready per-repo health metrics from silver,
attach the ML risk score, and serve them via TimescaleDB (history) + Redis (hot).

This is the silver -> gold transform. All heavy aggregation runs in DuckDB over
the partitioned silver Parquet on MinIO; only the small per-repo result set is
pulled into Python for bus-factor / risk computation and storage.
"""

import logging
import time
from datetime import datetime, timezone

import psycopg2
import redis as redis_lib

import config
import clock
import storage
import risk_model

log = logging.getLogger("pipeline.gold")

REDIS_RETENTION_MS = 7 * 24 * 60 * 60 * 1000


def _bus_factor(counts: list[int] | None) -> int | None:
    """Smallest N of top contributors who together own >80% of commits."""
    if not counts:
        return None
    counts = sorted(counts, reverse=True)
    total = sum(counts)
    if total == 0:
        return None
    cum = 0
    for n, c in enumerate(counts, start=1):
        cum += c
        if cum >= 0.8 * total:
            return n
    return len(counts)


def _compute_features() -> list[dict]:
    """Run the metric aggregation in DuckDB and return one dict per repo."""
    src = storage.s3_uri(config.SILVER_BUCKET, "events/**/*.parquet")
    con = storage.duckdb_con()

    cw = config.CONTRIBUTOR_WINDOW_DAYS
    fw = config.COMMIT_FREQ_WINDOW_DAYS

    # All "now"/"today" references below come from the (possibly replay-shifted)
    # clock as SQL literals, so the rolling windows and recency math are relative
    # to the logical now. In non-replay mode these equal real now/today.
    eff_now = clock.effective_now().strftime("%Y-%m-%d %H:%M:%S")
    eff_date = clock.effective_today().strftime("%Y-%m-%d")

    query = f"""
    WITH base AS (
        SELECT * FROM read_parquet('{src}', hive_partitioning=true)
        WHERE CAST(event_date AS DATE) >= DATE '{eff_date}' - {cw}
          AND repo_name IS NOT NULL
    ),
    -- Cardinality bound: the firehose has tens of millions of repos in the window.
    -- Restrict to the busiest GOLD_MAX_REPOS (with a GOLD_MIN_EVENTS floor) BEFORE
    -- the heavy per-repo aggregations, so they run over a small, meaningful set.
    eligible AS (
        SELECT repo_name FROM base
        GROUP BY repo_name
        -- "Importance" = distinct active actors (people), not raw event volume:
        -- a bot pushing 700K events is a single actor, so this resists the
        -- push/comment spam that dominated event_count. approx_count_distinct
        -- (HyperLogLog) makes ranking all ~22M repos cheap; the exact distinct
        -- count is computed only for the chosen few in `cnt` below.
        HAVING approx_count_distinct(actor_login) >= {config.GOLD_MIN_ACTORS}
        ORDER BY approx_count_distinct(actor_login) DESC
        LIMIT {config.GOLD_MAX_REPOS}
    ),
    ev AS (
        SELECT * FROM base WHERE repo_name IN (SELECT repo_name FROM eligible)
    ),
    commits AS (
        SELECT repo_name, event_timestamp,
               COALESCE(c.author_email, c.author_name) AS author
        FROM ev, UNNEST(ev.payload_push.commits) AS t(c)
        WHERE event_type = 'PushEvent'
    ),
    author_counts AS (
        SELECT repo_name, author, COUNT(*) AS n
        FROM commits GROUP BY 1, 2
    ),
    authors AS (
        SELECT repo_name,
               list(n ORDER BY n DESC)      AS author_commit_counts,
               COUNT(DISTINCT author)       AS active_contributors_90d
        FROM author_counts GROUP BY 1
    ),
    push AS (
        SELECT repo_name,
               SUM(CASE WHEN event_timestamp >= TIMESTAMP '{eff_now}' - INTERVAL '{fw} days'
                        THEN payload_push.size ELSE 0 END)         AS commits_window,
               MAX(event_timestamp)                                AS last_commit
        FROM ev WHERE event_type = 'PushEvent' GROUP BY 1
    ),
    pr AS (
        SELECT repo_name,
               COUNT(*) FILTER (WHERE payload_pull_request.action = 'closed')          AS pr_closed,
               COUNT(*) FILTER (WHERE payload_pull_request.action = 'closed'
                                  AND payload_pull_request.merged)                      AS pr_merged,
               quantile_cont(
                   CASE WHEN payload_pull_request.merged_at IS NOT NULL
                        THEN date_diff('minute', payload_pull_request.created_at,
                                       payload_pull_request.merged_at) / 60.0 END, 0.5) AS pr_latency_p50
        FROM ev WHERE event_type = 'PullRequestEvent' GROUP BY 1
    ),
    iss AS (
        SELECT repo_name,
               COUNT(*) FILTER (WHERE payload_issue.action = 'opened') AS issues_opened,
               COUNT(*) FILTER (WHERE payload_issue.action = 'closed') AS issues_closed
        FROM ev WHERE event_type = 'IssuesEvent' GROUP BY 1
    ),
    rel AS (
        SELECT repo_name, MAX(event_timestamp) AS last_release
        FROM ev WHERE event_type = 'ReleaseEvent' GROUP BY 1
    ),
    -- Per-repo counts over the (already small, eligible-only) ev set:
    --   active_actors = distinct people = the importance metric (exact here).
    --   event_count   = raw tracked volume, kept as a secondary signal (a high
    --                   event_count with a tiny active_actors flags bot activity).
    cnt AS (
        SELECT repo_name,
               COUNT(*)                     AS event_count,
               COUNT(DISTINCT actor_login)  AS active_actors
        FROM ev GROUP BY 1
    ),
    repos AS (SELECT repo_name FROM eligible)
    SELECT
        r.repo_name,
        COALESCE(cnt.active_actors, 0)                                 AS active_actors,
        COALESCE(cnt.event_count, 0)                                   AS event_count,
        COALESCE(push.commits_window, 0) / {fw}.0                       AS commit_freq_30d,
        COALESCE(authors.active_contributors_90d, 0)                    AS active_contributors_90d,
        authors.author_commit_counts                                   AS author_commit_counts,
        date_diff('day', push.last_commit, TIMESTAMP '{eff_now}')      AS days_since_last_commit,
        CASE WHEN COALESCE(pr.pr_closed, 0) > 0
             THEN 1.0 - (pr.pr_merged::DOUBLE / pr.pr_closed) END       AS pr_abandon_rate,
        pr.pr_latency_p50                                              AS pr_latency_p50,
        CASE WHEN COALESCE(iss.issues_opened, 0) > 0
             THEN 1.0 - LEAST(1.0, iss.issues_closed::DOUBLE / iss.issues_opened)
        END                                                            AS stale_issue_ratio,
        date_diff('day', rel.last_release, TIMESTAMP '{eff_now}')      AS days_since_last_release
    FROM repos r
    LEFT JOIN authors USING (repo_name)
    LEFT JOIN push    USING (repo_name)
    LEFT JOIN pr      USING (repo_name)
    LEFT JOIN iss     USING (repo_name)
    LEFT JOIN rel     USING (repo_name)
    LEFT JOIN cnt     USING (repo_name)
    """
    cols = [
        "repo_name", "active_actors", "event_count", "commit_freq_30d",
        "active_contributors_90d", "author_commit_counts", "days_since_last_commit",
        "pr_abandon_rate", "pr_latency_p50", "stale_issue_ratio",
        "days_since_last_release",
    ]
    rows = [dict(zip(cols, row)) for row in con.execute(query).fetchall()]

    # Derived-in-Python: bus factor from the author-count list.
    for r in rows:
        r["bus_factor"] = _bus_factor(r.pop("author_commit_counts"))
    return rows


def _write_timescale(conn, rows: list[dict]) -> None:
    with conn.cursor() as cur:
        for r in rows:
            cur.execute(
                """
                INSERT INTO repo_health_metrics
                    (time, repo_name, active_actors, event_count, commit_freq_30d,
                     active_contributors_90d, bus_factor, pr_latency_p50, pr_abandon_rate,
                     stale_issue_ratio, days_since_last_commit, days_since_last_release,
                     risk_score, health_score)
                VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (r["repo_name"], r["active_actors"], r["event_count"],
                 r["commit_freq_30d"], r["active_contributors_90d"],
                 r["bus_factor"], r["pr_latency_p50"], r["pr_abandon_rate"],
                 r["stale_issue_ratio"], r["days_since_last_commit"],
                 r["days_since_last_release"], r["risk_score"], 1.0 - r["risk_score"]),
            )
    conn.commit()


def _write_redis(r_client, rows: list[dict]) -> None:
    ts = r_client.ts()
    now_ms = int(time.time() * 1000)
    written = 0
    failed = 0
    for r in rows:
        # Per-repo guard: one malformed repo can't abort the whole batch, and a
        # wholesale Redis outage surfaces in the summary below instead of silently
        # bubbling up to hourly_job. (Redis is a hot cache; the API falls back to
        # TimescaleDB on a miss, so a failure here degrades latency, not data.)
        try:
            mapping = {k: ("" if v is None else str(v)) for k, v in r.items()}
            r_client.hset(f"latest:{r['repo_name']}", mapping=mapping)
            for metric in ("risk_score", "health_score", "commit_freq_30d", "bus_factor"):
                val = r.get(metric)
                if val is None:
                    continue
                key = f"ts:{r['repo_name']}:{metric}"
                try:
                    ts.create(key, retention_msecs=REDIS_RETENTION_MS,
                              labels={"repo": r["repo_name"], "metric": metric},
                              duplicate_policy="LAST")
                except redis_lib.ResponseError:
                    pass
                try:
                    ts.add(key, now_ms, float(val))
                except (redis_lib.ResponseError, ValueError):
                    pass
            written += 1
        except Exception:
            failed += 1
            if failed == 1:  # log the first failure with a traceback for diagnosis
                log.exception("gold: redis write failed for %s", r.get("repo_name"))

    log.info("gold: redis wrote %d/%d latest keys, %d failures",
             written, len(rows), failed)


def build() -> int:
    """Compute gold for all repos in the silver window. Returns repo count."""
    rows = _compute_features()
    if not rows:
        log.info("gold: no silver data in window yet")
        return 0

    conn = psycopg2.connect(config.POSTGRES_DSN)

    scores = risk_model.score_repos(rows)
    for r in rows:
        r["risk_score"] = scores.get(r["repo_name"], 0.5)
        r["health_score"] = round(1.0 - r["risk_score"], 4)

    _write_timescale(conn, rows)
    conn.close()

    # TimescaleDB (above) is the durable write; Redis is the hot cache. Never let
    # a Redis outage discard an already-committed gold cycle — log and move on.
    try:
        r_client = redis_lib.Redis.from_url(config.REDIS_URL, decode_responses=True)
        _write_redis(r_client, rows)
    except Exception:
        log.exception("gold: redis phase failed; timescale write is already committed")

    log.info("gold: wrote %d repos at %s", len(rows), datetime.now(timezone.utc).isoformat())
    return len(rows)
