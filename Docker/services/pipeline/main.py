"""Pipeline entrypoint: scheduler + optional one-shot backfill.

Default behaviour:
  * every hour  -> process the latest available GH Archive hour (bronze->silver->gold)
  * every day   -> prune aged bronze/silver (retention)

Set BACKFILL_START / BACKFILL_END (format YYYY-MM-DD-H) to replay a historical
range once on startup before the schedule takes over. Set REPLAY_OFFSET_YEARS to
shift the whole pipeline's "now" back N years (see clock.py): startup then runs the
parquet bulk backfill, the hourly-download backfill, then goes live — all targeting
the feed from N years ago.
"""

import logging
import time
from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.background import BackgroundScheduler

import config
import storage
import pipeline
import gold

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pipeline.main")


def _parse_hour(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d-%H").replace(tzinfo=timezone.utc)


def backfill(start: datetime, end: datetime) -> None:
    """Download + process every hour in [start, end], building gold once at the end.

    Gold is deferred (run_hour(..., with_gold=False)) so a multi-hour range doesn't
    pay the ~minutes-long gold aggregation on every hour.
    """
    log.info("Backfill from %s to %s", start, end)
    cur = start
    while cur <= end:
        pipeline.run_hour(cur, with_gold=False)
        cur += timedelta(hours=1)
    gold.build()
    log.info("Backfill complete.")


def hourly_job() -> None:
    try:
        pipeline.run_hour(pipeline.latest_available_hour())
    except Exception:
        log.exception("hourly_job failed")


def retention_job() -> None:
    try:
        pipeline.run_retention()
    except Exception:
        log.exception("retention_job failed")


def wait_for_minio(retries: int = 30) -> None:
    for attempt in range(1, retries + 1):
        try:
            storage.ensure_buckets()
            log.info("MinIO ready, buckets ensured.")
            return
        except Exception as e:
            log.warning("Waiting for MinIO (%d/%d): %s", attempt, retries, e)
            time.sleep(3)
    raise RuntimeError("MinIO never became available.")


def _replay_startup() -> None:
    """Staged replay (REPLAY_OFFSET_YEARS > 0): parquet bulk -> hourly tail -> live.

    The parquet stage ingests complete history up to the day before BACKFILL_START;
    the hourly stage downloads BACKFILL_START .. the (shifted) latest hour; then the
    scheduler takes over live. BACKFILL_START is optional — if unset, only the
    parquet bulk runs (used by the single-file test).
    """
    if config.BACKFILL_PARQUET_BUCKET or config.BACKFILL_PARQUET_DIR:
        max_date = None
        if config.BACKFILL_START:
            start = _parse_hour(config.BACKFILL_START)
            max_date = (start.date() - timedelta(days=1)).isoformat()
        pipeline.run_parquet_backfill(max_date=max_date)
    if config.BACKFILL_START:
        backfill(_parse_hour(config.BACKFILL_START), pipeline.latest_available_hour())
    log.info("Replay backfill complete; going live (%d year(s) ago).",
             config.REPLAY_OFFSET_YEARS)


def _parquet_startup() -> None:
    """Parquet bulk-ingest (Mode A or B), then optional hourly-download chain.

    If BACKFILL_START is set, caps parquet at day_before(BACKFILL_START) and
    chains an hourly GH Archive download from BACKFILL_START to now.
    Without BACKFILL_START: parquet only, no chain (old bind-mount behaviour).
    """
    max_date = None
    if config.BACKFILL_START:
        start = _parse_hour(config.BACKFILL_START)
        max_date = (start.date() - timedelta(days=1)).isoformat()
    pipeline.run_parquet_backfill(max_date=max_date)
    if config.BACKFILL_START:
        backfill(_parse_hour(config.BACKFILL_START), pipeline.latest_available_hour())


def main() -> None:
    log.info("Pipeline service starting.")
    wait_for_minio()

    if config.BACKFILL_PARQUET_BUCKET and config.BACKFILL_PARQUET_DIR:
        log.warning(
            "Both BACKFILL_PARQUET_BUCKET and BACKFILL_PARQUET_DIR are set; "
            "BACKFILL_PARQUET_BUCKET (Mode B) takes precedence."
        )

    if config.REPLAY_OFFSET_YEARS > 0:
        _replay_startup()
    elif config.BACKFILL_PARQUET_BUCKET or config.BACKFILL_PARQUET_DIR:
        _parquet_startup()   # run_parquet_backfill() picks Mode A or B internally
    elif config.BACKFILL_START and config.BACKFILL_END:
        backfill(_parse_hour(config.BACKFILL_START), _parse_hour(config.BACKFILL_END))

    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(hourly_job, "cron", minute=15, id="hourly",
                      misfire_grace_time=3600, coalesce=True)
    scheduler.add_job(retention_job, "cron", hour=config.RETENTION_CRON_HOUR,
                      minute=45, id="retention", misfire_grace_time=7200, coalesce=True)
    scheduler.start()
    log.info("Scheduler started: hourly @ :15, retention daily @ %02d:45 UTC.",
             config.RETENTION_CRON_HOUR)

    # Run one cycle immediately so a fresh stack produces data without waiting.
    hourly_job()

    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        log.info("Shutting down scheduler.")
        scheduler.shutdown()


if __name__ == "__main__":
    main()
