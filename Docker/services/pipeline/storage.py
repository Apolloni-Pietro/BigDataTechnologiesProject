"""Storage helpers: MinIO object I/O and a DuckDB connection wired to MinIO.

Two ways we touch object storage:
  * the `minio` SDK  -> small control-plane ops (put a file, check existence)
  * DuckDB httpfs     -> bulk analytical reads/writes of JSON & Parquet over S3
"""

import io
import logging

import duckdb
from minio import Minio
from minio.error import S3Error

import config

log = logging.getLogger("pipeline.storage")


# ── MinIO control-plane client ─────────────────────────────────────────────

def minio_client() -> Minio:
    """Return a configured MinIO client (S3-compatible)."""
    return Minio(
        config.MINIO_ENDPOINT,
        access_key=config.MINIO_ACCESS_KEY,
        secret_key=config.MINIO_SECRET_KEY,
        secure=config.MINIO_SECURE,
    )


def ensure_buckets() -> None:
    """Create the bronze/silver/gold buckets if they do not exist (idempotent)."""
    client = minio_client()
    for bucket in (config.BRONZE_BUCKET, config.SILVER_BUCKET, config.GOLD_BUCKET):
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            log.info("Created bucket %s", bucket)


def object_exists(bucket: str, key: str) -> bool:
    """True if an object already exists (used to make each stage idempotent)."""
    client = minio_client()
    try:
        client.stat_object(bucket, key)
        return True
    except S3Error:
        return False


def put_bytes(bucket: str, key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
    """Upload an in-memory buffer to object storage."""
    client = minio_client()
    client.put_object(bucket, key, io.BytesIO(data), length=len(data), content_type=content_type)


# ── DuckDB engine wired to MinIO ───────────────────────────────────────────

def duckdb_con() -> duckdb.DuckDBPyConnection:
    """A DuckDB connection that can read/write Parquet & JSON directly on MinIO.

    MinIO requires path-style addressing and (locally) plain HTTP, which is why
    we set s3_url_style='path' and s3_use_ssl explicitly.
    """
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{config.MINIO_ENDPOINT}';")
    con.execute(f"SET s3_access_key_id='{config.MINIO_ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{config.MINIO_SECRET_KEY}';")
    con.execute(f"SET s3_use_ssl={'true' if config.MINIO_SECURE else 'false'};")
    con.execute("SET s3_url_style='path';")
    return con


def s3_uri(bucket: str, key: str) -> str:
    """Build an s3:// URI that DuckDB understands."""
    return f"s3://{bucket}/{key}"
