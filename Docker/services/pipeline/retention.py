"""RETENTION: prune old bronze/silver objects in MinIO so a 24/7 pipeline does
not fill the disk. Gold is already bounded (TimescaleDB retention/compression +
Redis maxmemory/LRU), so MinIO is the only unbounded store.

Two windows, on purpose:
  * bronze  -> a transient raw landing zone; safe to prune aggressively.
  * silver  -> the history gold aggregates over rolling windows, so it MUST
               outlive the largest window (config.CONTRIBUTOR_WINDOW_DAYS) or
               metrics degrade at the edge.

Object age is taken from the DATE ENCODED IN THE KEY when present (robust to
re-runs, which would reset last-modified); otherwise we fall back to the
object's last-modified time. Keys with neither are never deleted.
"""

import logging
import re
from datetime import datetime, timezone, timedelta

from minio.deleteobjects import DeleteObject

import config
import storage

log = logging.getLogger("pipeline.retention")

# bronze:  gharchive/YYYY/MM/DD/YYYY-MM-DD-H.json.gz
_GHARCHIVE_RE = re.compile(r"gharchive/(\d{4})/(\d{2})/(\d{2})/")
# silver:  events/event_date=YYYY-MM-DD/hour=H.parquet
_EVENT_DATE_RE = re.compile(r"event_date=(\d{4})-(\d{2})-(\d{2})")


def _key_date(key: str):
    """Date encoded in an object key, or None (then caller uses last-modified)."""
    m = _GHARCHIVE_RE.search(key) or _EVENT_DATE_RE.search(key)
    if m:
        y, mo, d = map(int, m.groups())
        return datetime(y, mo, d, tzinfo=timezone.utc)
    return None


def _purge(bucket: str, prefix: str, cutoff: datetime) -> tuple[int, int]:
    """Delete objects under prefix strictly older than cutoff. Returns (deleted, kept)."""
    client = storage.minio_client()
    to_delete = []
    kept = 0
    for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
        when = _key_date(obj.object_name) or obj.last_modified
        if when is None:           # no date anywhere -> never delete
            kept += 1
            continue
        if when < cutoff:
            to_delete.append(DeleteObject(obj.object_name))
        else:
            kept += 1

    deleted = 0
    if to_delete:
        # remove_objects returns a lazy iterator of errors; it must be consumed.
        errors = list(client.remove_objects(bucket, to_delete))
        for e in errors:
            log.warning("retention: failed to delete %s: %s", e.object_name, e)
        deleted = len(to_delete) - len(errors)
    return deleted, kept


def purge_bronze() -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(days=config.BRONZE_RETENTION_DAYS)
    deleted, kept = _purge(config.BRONZE_BUCKET, "", cutoff)
    log.info("retention bronze: deleted %d, kept %d (cutoff %d days)",
             deleted, kept, config.BRONZE_RETENTION_DAYS)
    return deleted


def purge_silver() -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(days=config.SILVER_RETENTION_DAYS)
    deleted, kept = _purge(config.SILVER_BUCKET, "events/", cutoff)
    log.info("retention silver: deleted %d, kept %d (cutoff %d days)",
             deleted, kept, config.SILVER_RETENTION_DAYS)
    return deleted


def run() -> None:
    """Prune bronze then silver. Never touches gold (TimescaleDB/Redis)."""
    log.info("── retention sweep ──")
    if config.SILVER_RETENTION_DAYS < config.CONTRIBUTOR_WINDOW_DAYS:
        log.warning(
            "SILVER_RETENTION_DAYS (%d) < CONTRIBUTOR_WINDOW_DAYS (%d): gold "
            "metrics will lose data at the edge of their rolling window.",
            config.SILVER_RETENTION_DAYS, config.CONTRIBUTOR_WINDOW_DAYS,
        )
    purge_bronze()
    purge_silver()
