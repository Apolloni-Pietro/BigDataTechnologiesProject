"""The medallion DAG: bronze -> silver -> gold for a single hour, plus the
daily enrichment job. Kept deliberately small and explicit so it can be lifted
into Dagster/Airflow later without rewriting the stage logic.
"""

import logging
from datetime import datetime, timedelta, timezone

import bronze
import silver
import gold
import enrichment
import storage
import config

log = logging.getLogger("pipeline.dag")


def run_hour(dt: datetime) -> bool:
    """Process one GH Archive hour end-to-end (bronze -> silver -> gold)."""
    log.info("── DAG: processing hour %s ──", dt.strftime("%Y-%m-%d-%H"))

    bkey = bronze.ingest_hour(dt.year, dt.month, dt.day, dt.hour)
    if not bkey:
        log.warning("DAG: bronze unavailable for %s, aborting this hour", dt)
        return False

    skey = silver.build_hour(dt.year, dt.month, dt.day, dt.hour)
    if not skey:
        log.warning("DAG: silver build failed for %s", dt)
        return False

    # Gold recomputes over the rolling silver window, not just this hour, so a
    # single hour's events update every affected rolling metric.
    gold.build()
    return True


def latest_available_hour() -> datetime:
    """The most recent hour GH Archive should have published (accounting for lag)."""
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return now - timedelta(hours=config.PUBLISH_LAG_HOURS)


def distinct_silver_repos(limit: int) -> list[str]:
    """Repos seen in silver, busiest first — the worklist for enrichment."""
    src = storage.s3_uri(config.SILVER_BUCKET, "events/**/*.parquet")
    con = storage.duckdb_con()
    try:
        rows = con.execute(
            f"""
            SELECT repo_name, COUNT(*) c
            FROM read_parquet('{src}', hive_partitioning=true)
            WHERE repo_name IS NOT NULL
            GROUP BY repo_name ORDER BY c DESC LIMIT {limit}
            """
        ).fetchall()
        return [r[0] for r in rows]
    except Exception as e:
        log.warning("DAG: could not list silver repos (%s)", e)
        return []


def run_enrichment() -> None:
    """Daily supply-chain enrichment over the busiest silver repos."""
    log.info("── DAG: daily enrichment ──")
    repos = distinct_silver_repos(config.ENRICHMENT_MAX_REPOS)
    enrichment.run(repos)
