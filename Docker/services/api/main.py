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
    "importance":   "event_count DESC NULLS LAST",   # busiest repos first
    "health_score": "health_score DESC NULLS LAST",  # healthiest first
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
    `sort` selects the ordering: importance (event volume), health_score, or name.
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
                "event_count":      row[5],
                "last_updated":     row[6].isoformat() if row[6] else None,
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

    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for {full_name}. "
                   "It may not have been seen in the event stream yet."
        )

    return {
        "repo":    full_name,
        "metrics": data,
        "source":  "redis",
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