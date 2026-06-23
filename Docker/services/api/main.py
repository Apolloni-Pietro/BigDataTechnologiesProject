import os
import time
import logging
from contextlib import asynccontextmanager

import redis
import psycopg2
from fastapi import FastAPI, HTTPException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://oss:changeme@timescaledb:5432/oss_health")
REDIS_URL    = os.getenv("REDIS_URL",    "redis://redis:6379")

# ── Module-level connections ──────────────────────────────────────────────
# We create one connection per worker process at startup and reuse it
# across all requests. Creating a new DB connection per HTTP request
# is expensive (~50-100ms) and would make the API very slow under load.
# These are set in the lifespan function below.

redis_client: redis.Redis | None = None
pg_conn:      psycopg2.extensions.connection | None = None


def get_connections():
    """Retry connecting to both stores on startup."""
    global redis_client, pg_conn

    for attempt in range(1, 20):
        try:
            redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
            redis_client.ping()
            log.info("Connected to Redis")
            break
        except Exception as e:
            log.warning(f"Waiting for Redis ({attempt}/20): {e}")
            time.sleep(3)

    for attempt in range(1, 20):
        try:
            pg_conn = psycopg2.connect(POSTGRES_DSN)
            # Autocommit so each request runs in its own implicit transaction and
            # always sees the latest committed gold snapshot. Without this the single
            # long-lived connection keeps one transaction open ("idle in
            # transaction") and can serve a stale, frozen view of the metrics.
            pg_conn.autocommit = True
            log.info("Connected to TimescaleDB")
            break
        except Exception as e:
            log.warning(f"Waiting for TimescaleDB ({attempt}/20): {e}")
            time.sleep(3)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Code before 'yield' runs on startup.
    Code after 'yield' runs on shutdown.
    This is FastAPI's recommended way to manage startup/shutdown logic.
    """
    log.info("API starting up — connecting to storage...")
    get_connections()
    log.info("API ready.")
    yield
    # Shutdown: close connections cleanly
    if pg_conn:
        pg_conn.close()
    log.info("API shut down cleanly.")


app = FastAPI(
    title       = "OSS Health Monitor API",
    description = "Health metrics for open-source repositories.",
    version     = "0.1.0",
    lifespan    = lifespan,
)


# ── Endpoints ─────────────────────────────────────────────────────────────

@app.get("/health")
def health_check():
    """
    Liveness check used by Docker's healthcheck config.
    Returns 200 if the server is running. Does not check DB connections
    (that would make the healthcheck expensive and flaky).
    """
    return {"status": "ok"}


# Whitelist of allowed sort keys -> the SQL ORDER BY clause applied to the
# latest-per-repo result set. Whitelisting (never interpolating the raw param)
# keeps this safe from SQL injection.
_SORT_CLAUSES = {
    "importance":   "active_actors DESC NULLS LAST",  # most distinct people first (bot-resistant)
    "health_score": "health_score DESC NULLS LAST",   # healthiest first
    "name":         "repo_name ASC",
}


@app.get("/repos")
def list_repos(
    limit: int = 50,
    min_score: float = 0.0,
    max_score: float = 1.0,
    sort: str = "importance",
):
    """
    List repositories with their latest health scores.
    Optionally filter by score range (e.g. max_score=0.35 for at-risk repos).
    `sort` selects the ordering: importance (distinct active actors), health_score, or name.
    Data comes from TimescaleDB — this is a historical/analytical query.
    """
    if not pg_conn:
        raise HTTPException(status_code=503, detail="Database not connected")

    order_by = _SORT_CLAUSES.get(sort)
    if order_by is None:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid sort '{sort}'. Allowed: {', '.join(_SORT_CLAUSES)}",
        )

    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                # Inner query: latest row per repo (DISTINCT ON needs repo_name to
                # lead its ORDER BY). Outer query: rank those latest rows.
                f"""
                SELECT * FROM (
                    SELECT DISTINCT ON (repo_name)
                        repo_name,
                        health_score,
                        commit_freq_30d,
                        bus_factor,
                        stale_issue_ratio,
                        active_actors,
                        event_count,
                        time
                    FROM repo_health_metrics
                    WHERE health_score BETWEEN %s AND %s
                    ORDER BY repo_name, time DESC
                ) latest
                ORDER BY {order_by}
                LIMIT %s
                """,
                (min_score, max_score, limit),
            )
            rows = cur.fetchall()
    except Exception as e:
        log.error(f"Database query failed: {e}")
        raise HTTPException(status_code=500, detail="Query failed")

    return {
        "repos": [
            {
                "repo_name":        row[0],
                "health_score":     row[1],
                "commit_freq_30d":  row[2],
                "bus_factor":       row[3],
                "stale_issue_ratio": row[4],
                "active_actors":    row[5],
                "event_count":      row[6],
                "last_updated":     row[7].isoformat() if row[7] else None,
            }
            for row in rows
        ],
        "count": len(rows),
        "sort":  sort,
    }


@app.get("/repos/{repo_owner}/{repo_name}/current")
def get_current_metrics(repo_owner: str, repo_name: str):
    """
    Return the most recent metric values for a single repo.
    Uses Redis for sub-millisecond response.
    The repo path is split into owner/name to handle slashes in URL routing.
    Example: GET /repos/facebook/react/current
    """
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis not connected")

    full_name = f"{repo_owner}/{repo_name}"
    data      = redis_client.hgetall(f"latest:{full_name}")

    if data:
        return {
            "repo":    full_name,
            "metrics": data,
            "source":  "redis",
        }

    # Redis is a hot cache, not durable storage: with `--save 60 1` +
    # `allkeys-lru` the `latest:` hash can legitimately be missing (fresh start,
    # un-saved window, eviction) even though the repo has metrics. Fall back to
    # the latest committed row in TimescaleDB — the durable source of truth — so
    # the endpoint stays correct whenever the cache is cold.
    metrics = _latest_from_timescale(full_name)
    if metrics:
        return {
            "repo":    full_name,
            "metrics": metrics,
            "source":  "timescaledb",
        }

    raise HTTPException(
        status_code=404,
        detail=f"No data found for {full_name}. "
               "It may not have been seen in the event stream yet."
    )


# Columns mirror gold._write_timescale's INSERT. Kept here so the Redis-miss
# fallback returns the same metric set the Redis `latest:` hash would.
_CURRENT_COLUMNS = (
    "active_actors", "event_count", "commit_freq_30d", "active_contributors_90d",
    "bus_factor", "pr_latency_p50", "pr_abandon_rate", "stale_issue_ratio",
    "days_since_last_commit", "days_since_last_release", "risk_score", "health_score",
)


def _latest_from_timescale(full_name: str) -> dict | None:
    """Most recent metric row for one repo, as a str-valued dict (Redis-shaped)."""
    if not pg_conn:
        return None
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT {', '.join(_CURRENT_COLUMNS)}
                FROM repo_health_metrics
                WHERE repo_name = %s
                ORDER BY time DESC
                LIMIT 1
                """,
                (full_name,),
            )
            row = cur.fetchone()
    except Exception as e:
        log.error(f"TimescaleDB current-metrics fallback failed: {e}")
        return None

    if not row:
        return None

    # Stringify to match the Redis hgetall shape the dashboard expects.
    return {
        col: ("" if val is None else str(val))
        for col, val in zip(_CURRENT_COLUMNS, row)
    }


@app.get("/repos/{repo_owner}/{repo_name}/history")
def get_metric_history(repo_owner: str, repo_name: str, days: int = 7):
    """
    Return historical health_score values for a repo over a time window.
    - days <= 7:  served from Redis TimeSeries (fast, recent data)
    - days >  7:  served from TimescaleDB (slower, but full history)
    """
    full_name = f"{repo_owner}/{repo_name}"

    if days <= 7:
        # ── Redis path ────────────────────────────────────────────
        if not redis_client:
            raise HTTPException(status_code=503, detail="Redis not connected")

        ts_key  = f"ts:{full_name}:health_score"
        now_ms  = int(time.time() * 1000)
        from_ms = now_ms - (days * 24 * 60 * 60 * 1000)

        try:
            points = redis_client.ts().range(ts_key, from_ms, now_ms)
            # Returns a list of (timestamp_ms, value) tuples.
        except Exception as e:
            log.warning(f"Redis TimeSeries query failed for {ts_key}: {e}")
            points = []

        return {
            "repo":   full_name,
            "days":   days,
            "source": "redis",
            "points": [
                {"timestamp_ms": ts, "value": val}
                for ts, val in points
            ],
        }

    else:
        # ── TimescaleDB path ──────────────────────────────────────
        if not pg_conn:
            raise HTTPException(status_code=503, detail="Database not connected")

        try:
            with pg_conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        day,
                        health_score_avg,
                        health_score_min
                    FROM repo_health_daily
                    WHERE repo_name = %s
                      AND day >= NOW() - (%s || ' days')::INTERVAL
                    ORDER BY day ASC
                    """,
                    (full_name, str(days)),
                )
                rows = cur.fetchall()

                # Continuous aggregate refreshes hourly; on a fresh stack it may not
                # have materialized yet even though raw rows exist. Fall back to the
                # hypertable with a manual daily bucket so data is visible immediately.
                if not rows:
                    log.info(
                        "repo_health_daily empty for %s (days=%d), falling back to raw hypertable",
                        full_name, days,
                    )
                    cur.execute(
                        """
                        SELECT
                            time_bucket('1 day', time) AS day,
                            AVG(health_score)           AS health_score_avg,
                            MIN(health_score)           AS health_score_min
                        FROM repo_health_metrics
                        WHERE repo_name = %s
                          AND time >= NOW() - (%s || ' days')::INTERVAL
                        GROUP BY 1
                        ORDER BY 1 ASC
                        """,
                        (full_name, str(days)),
                    )
                    rows = cur.fetchall()
        except Exception as e:
            log.error(f"TimescaleDB query failed: {e}")
            raise HTTPException(status_code=500, detail="Query failed")

        return {
            "repo":   full_name,
            "days":   days,
            "source": "timescaledb",
            "points": [
                {
                    "day":               row[0].isoformat(),
                    "health_score_avg":  row[1],
                    "health_score_min":  row[2],
                }
                for row in rows
            ],
        }


@app.get("/at-risk")
def get_at_risk_repos(threshold: float = 0.35, limit: int = 20):
    """
    Convenience endpoint: return repos whose health score is below
    the given threshold, sorted from worst to best.
    """
    return list_repos(limit=limit, min_score=0.0, max_score=threshold)