"""The medallion DAG: bronze -> silver -> gold for a single hour, plus the
retention job and the parquet backfill. Kept deliberately small and explicit so it
can be lifted into Dagster/Airflow later without rewriting the stage logic.
"""

import logging
from datetime import datetime

import glob
import os

import bronze
import silver
import gold
import retention
import config
import clock
import backfill_parquet

log = logging.getLogger("pipeline.dag")


def run_hour(dt: datetime, with_gold: bool = True) -> bool:
    """Process one GH Archive hour (bronze -> silver, then gold unless deferred).

    `with_gold=False` lets a multi-hour backfill ingest cheaply and rebuild gold
    once at the end instead of on every hour (gold is a ~minutes-long aggregation).
    """
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
    if with_gold:
        gold.build()
    return True


def latest_available_hour() -> datetime:
    """Most recent hour the feed should have published, on the (replay-aware) clock."""
    return clock.effective_latest_hour(config.PUBLISH_LAG_HOURS)


def run_retention() -> None:
    """Daily prune of aged bronze/silver objects in MinIO (gold is unaffected)."""
    retention.run()


def run_parquet_backfill(max_date: str | None = None) -> None:
    """Backfill silver+gold from pre-downloaded monthly Parquet files.

    Re-projects each monthly file (repo-root GHArchiveDownload.py output) straight
    into silver, then builds gold once. Bronze is intentionally bypassed (the speed
    win). `max_date` (YYYY-MM-DD) caps ingestion to days <= that date, so in a staged
    replay the parquet stage doesn't overlap the hourly/live stage that owns the
    recent days (overlap would double-count in gold). See Docker/PARQUET_BACKFILL.md.
    """
    pattern = os.path.join(config.BACKFILL_PARQUET_DIR, config.BACKFILL_PARQUET_GLOB)
    files = sorted(glob.glob(pattern))
    if not files:
        log.warning("parquet-backfill: no files match %s — nothing to do", pattern)
        return

    log.info("parquet-backfill: %d monthly file(s) to ingest%s", len(files),
             f" (up to {max_date})" if max_date else "")
    for path in files:
        try:
            backfill_parquet.build_month(path, max_date=max_date)
        except Exception:
            log.exception("parquet-backfill: failed on %s", path)

    gold.build()
    log.info("parquet-backfill complete.")
