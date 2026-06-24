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

    Source is determined by config (BACKFILL_PARQUET_BUCKET takes precedence):
    - BACKFILL_PARQUET_BUCKET: reads from a MinIO bucket via s3:// URIs (Mode B)
    - BACKFILL_PARQUET_DIR:    reads from a local filesystem path (Mode A)

    DuckDB's httpfs extension (loaded by storage.duckdb_con()) handles both local
    paths and s3:// URIs transparently in backfill_parquet.build_month(). Bronze is
    intentionally bypassed. `max_date` (YYYY-MM-DD) caps ingestion to avoid overlap
    with a subsequent hourly stage. See Docker/PARQUET_BACKFILL.md.
    """
    if config.BACKFILL_PARQUET_BUCKET:
        import fnmatch as _fnmatch
        import storage as _storage
        bucket = config.BACKFILL_PARQUET_BUCKET
        objects = list(_storage.minio_client().list_objects(bucket, recursive=True))
        paths = sorted(
            _storage.s3_uri(bucket, obj.object_name)
            for obj in objects
            if _fnmatch.fnmatch(os.path.basename(obj.object_name),
                                config.BACKFILL_PARQUET_GLOB)
        )
        source_desc = f"bucket {bucket}"
    else:
        pattern = os.path.join(config.BACKFILL_PARQUET_DIR, config.BACKFILL_PARQUET_GLOB)
        paths = sorted(glob.glob(pattern))
        source_desc = pattern

    if not paths:
        log.warning("parquet-backfill: no files found in %s — nothing to do", source_desc)
        return

    log.info("parquet-backfill: %d file(s) from %s%s",
             len(paths), source_desc, f" (up to {max_date})" if max_date else "")
    for path in paths:
        try:
            backfill_parquet.build_month(path, max_date=max_date)
        except Exception:
            log.exception("parquet-backfill: failed on %s", path)

    gold.build()
    log.info("parquet-backfill complete.")
