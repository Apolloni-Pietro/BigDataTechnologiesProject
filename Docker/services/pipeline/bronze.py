"""BRONZE layer: land raw, untouched GH Archive hourly files in MinIO.

Bronze is immutable and schema-on-read. We change nothing about the bytes — we
just verify the gzip is complete and store it. This is what makes the whole
pipeline replayable: silver/gold can always be rebuilt from bronze.
"""

import gzip
import io
import logging

import requests

import config
import storage

log = logging.getLogger("pipeline.bronze")


def gharchive_key(year: int, month: int, day: int, hour: int) -> str:
    """Bronze object key for one GH Archive hour."""
    return f"gharchive/{year:04d}/{month:02d}/{day:02d}/{year:04d}-{month:02d}-{day:02d}-{hour}.json.gz"


def _is_valid_gzip(data: bytes) -> bool:
    """Fully decompress in memory; truncated downloads raise here."""
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(data)) as f:
            while f.read(1024 * 1024):
                pass
        return True
    except (OSError, EOFError):
        return False


def ingest_hour(year: int, month: int, day: int, hour: int, retries: int = 3) -> str | None:
    """Download one GH Archive hour and store it in bronze. Idempotent.

    Returns the bronze key on success (or if already present), None on failure.
    """
    key = gharchive_key(year, month, day, hour)

    if storage.object_exists(config.BRONZE_BUCKET, key):
        log.info("bronze: %s already present, skipping", key)
        return key

    url = f"{config.GHARCHIVE_BASE}/{year:04d}-{month:02d}-{day:02d}-{hour}.json.gz"
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=60)
            if resp.status_code == 200:
                if _is_valid_gzip(resp.content):
                    storage.put_bytes(config.BRONZE_BUCKET, key, resp.content, "application/gzip")
                    log.info("bronze: stored %s (%d bytes)", key, len(resp.content))
                    return key
                log.warning("bronze: %s download was truncated (attempt %d)", url, attempt)
            elif resp.status_code == 404:
                log.warning("bronze: %s not available yet (404)", url)
                return None
            else:
                log.warning("bronze: %s HTTP %d (attempt %d)", url, resp.status_code, attempt)
        except requests.RequestException as e:
            log.warning("bronze: %s error %s (attempt %d)", url, e, attempt)

    log.error("bronze: gave up on %s after %d attempts", url, retries)
    return None
