"""Pipeline entrypoint: scheduler + optional one-shot backfill.

Default behaviour:
  * every hour  -> process the latest available GH Archive hour (bronze->silver->gold)
  * every day   -> run dependency-risk enrichment

Set BACKFILL_START / BACKFILL_END (format YYYY-MM-DD-H) to replay a historical
range once on startup before the schedule takes over.
"""

import logging
import time
from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.background import BackgroundScheduler

import config
import storage
import pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pipeline.main")


def _parse_hour(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d-%H").replace(tzinfo=timezone.utc)


def backfill() -> None:
    """Replay a fixed [start, end] hour range once."""
    start, end = _parse_hour(config.BACKFILL_START), _parse_hour(config.BACKFILL_END)
    log.info("Backfill from %s to %s", start, end)
    cur = start
    while cur <= end:
        pipeline.run_hour(cur)
        cur += timedelta(hours=1)
    pipeline.run_enrichment()
    log.info("Backfill complete.")


def hourly_job() -> None:
    try:
        pipeline.run_hour(pipeline.latest_available_hour())
    except Exception:
        log.exception("hourly_job failed")


def enrichment_job() -> None:
    try:
        pipeline.run_enrichment()
    except Exception:
        log.exception("enrichment_job failed")


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


def main() -> None:
    log.info("Pipeline service starting.")
    wait_for_minio()

    if config.BACKFILL_START and config.BACKFILL_END:
        backfill()

    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(hourly_job, "cron", minute=15, id="hourly",
                      misfire_grace_time=3600, coalesce=True)
    scheduler.add_job(enrichment_job, "cron", hour=config.ENRICHMENT_CRON_HOUR,
                      minute=30, id="enrichment", misfire_grace_time=7200, coalesce=True)
    scheduler.start()
    log.info("Scheduler started: hourly @ :15, enrichment daily @ %02d:30 UTC.",
             config.ENRICHMENT_CRON_HOUR)

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
