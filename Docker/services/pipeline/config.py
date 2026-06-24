"""Central configuration for the medallion pipeline.

Every value is read from an environment variable (set in docker-compose.yml or
.env) with a sensible default, so the same code runs locally and in Docker.
"""

import os

# ── Object storage (MinIO / S3) — bronze & silver live here ────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "minio:9000")      # host:port, no scheme
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE     = os.getenv("MINIO_SECURE", "false").lower() == "true"  # TLS?

BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "silver")
GOLD_BUCKET   = os.getenv("GOLD_BUCKET",   "gold")

# ── Gold serving stores ────────────────────────────────────────────────────
POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    "postgresql://oss:changeme@timescaledb:5432/oss_health",
)
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")

# ── Sources ────────────────────────────────────────────────────────────────
GHARCHIVE_BASE = os.getenv("GHARCHIVE_BASE", "https://data.gharchive.org")

# ── Scheduling ─────────────────────────────────────────────────────────────
# How the scheduler decides what to process. The pipeline always targets the
# most recently *published* GH Archive hour (publication lags ~1h, so we look
# back PUBLISH_LAG_HOURS).
PUBLISH_LAG_HOURS    = int(os.getenv("PUBLISH_LAG_HOURS", "2"))

# ── Replay ─────────────────────────────────────────────────────────────────
# Shift the pipeline's notion of "now" back this many years, so it fetches and
# processes the feed from N years ago (e.g. 1 → exactly one year ago). 0 disables
# replay (process the real latest hour). The data keeps its true dates; only the
# reference clock shifts — see clock.py. Useful because older GH Archive data
# carries full PushEvent commit arrays (newer/synthetic data may not).
REPLAY_OFFSET_YEARS = int(os.getenv("REPLAY_OFFSET_YEARS", "0"))

# Optional one-shot backfill on startup: "YYYY-MM-DD-H" .. "YYYY-MM-DD-H".
# When BACKFILL_START is set the service processes that whole range once and
# then continues with the normal schedule.
BACKFILL_START = os.getenv("BACKFILL_START", "")
BACKFILL_END   = os.getenv("BACKFILL_END", "")

# Optional parquet backfill. Two mutually exclusive source modes:
#
#   Mode A — Bind-mount (BACKFILL_PARQUET_DIR):
#     Container path to a directory of monthly .parquet files mounted from the host.
#     e.g. /backfill  (the default bind-mount in docker-compose.yml).
#
#   Mode B — MinIO-upload (BACKFILL_PARQUET_BUCKET):
#     Name of a MinIO bucket where the user has already uploaded the files.
#     The pipeline reads them via s3:// URIs — no bind-mount needed.
#     BACKFILL_PARQUET_BUCKET takes precedence over BACKFILL_PARQUET_DIR.
#
# In both modes: if BACKFILL_START is set, parquet is capped at
# day_before(BACKFILL_START) and an hourly GH Archive download continues from
# BACKFILL_START to now. Leave BACKFILL_START empty for parquet-only (old behaviour).
# See Docker/PARQUET_BACKFILL.md.
BACKFILL_PARQUET_DIR    = os.getenv("BACKFILL_PARQUET_DIR", "")
BACKFILL_PARQUET_GLOB   = os.getenv("BACKFILL_PARQUET_GLOB", "gh_events_*.parquet")
BACKFILL_PARQUET_BUCKET = os.getenv("BACKFILL_PARQUET_BUCKET", "")

# ── DuckDB resource caps (applied to every storage.duckdb_con) ──────────────
# Heavy stages (the parquet backfill's per-day dedup, and especially gold's
# 90-day aggregation with UNNEST over hundreds of millions of rows) must spill to
# disk rather than grab all host RAM — otherwise DuckDB's default (~80% of RAM)
# trips the Docker VM's OOM killer and the container restarts mid-run. A memory
# limit + a temp/spill directory keeps every query within bounds.
DUCKDB_MEMORY_LIMIT = os.getenv("DUCKDB_MEMORY_LIMIT", "10GB")
DUCKDB_TEMP_DIR     = os.getenv("DUCKDB_TEMP_DIR", "/tmp/duckdb_spill")
# Cap parallelism too: gold's 90-day aggregation builds large per-thread hash/list
# partitions whose pinned (un-spillable) memory scales with thread count, so fewer
# threads lowers peak memory markedly (at some speed cost) and lets it finish.
DUCKDB_THREADS = os.getenv("DUCKDB_THREADS", "4")

# ── Gold / metrics windows ─────────────────────────────────────────────────
COMMIT_FREQ_WINDOW_DAYS  = int(os.getenv("COMMIT_FREQ_WINDOW_DAYS", "30"))
CONTRIBUTOR_WINDOW_DAYS  = int(os.getenv("CONTRIBUTOR_WINDOW_DAYS", "90"))
STALE_ISSUE_DAYS         = int(os.getenv("STALE_ISSUE_DAYS", "90"))

# Gold repo cardinality bound. GH Archive's full firehose has tens of millions of
# distinct repos per 90-day window (most with 1-2 events), which is neither
# computable on a single node nor meaningful to monitor. Gold therefore scores only
# the most important repos, where importance = distinct active actors (people) in
# the window — bot-resistant, unlike raw event volume. Keeps those with at least
# GOLD_MIN_ACTORS distinct actors, capped at the GOLD_MAX_REPOS most active. This is
# the "health monitor watches real projects" selection, and it bounds
# memory/IO/DB-write cost. Raise on bigger hardware.
GOLD_MAX_REPOS  = int(os.getenv("GOLD_MAX_REPOS", "5000"))
GOLD_MIN_ACTORS = int(os.getenv("GOLD_MIN_ACTORS", "3"))

# ── Retention (24/7 disk management for MinIO; gold has its own retention) ───
# Bronze is a transient landing zone -> short window. Silver is the history gold
# aggregates over rolling windows, so it MUST outlive the largest window
# (CONTRIBUTOR_WINDOW_DAYS) or metrics degrade at the edge.
BRONZE_RETENTION_DAYS = int(os.getenv("BRONZE_RETENTION_DAYS", "30"))
SILVER_RETENTION_DAYS = int(os.getenv("SILVER_RETENTION_DAYS", "120"))
RETENTION_CRON_HOUR   = int(os.getenv("RETENTION_CRON_HOUR", "4"))   # daily, 04:45 UTC

# ── MQTT alerts ───────────────────────────────────────────────────────────────
MQTT_BROKER_HOST    = os.getenv("MQTT_BROKER_HOST", "mqtt")
MQTT_BROKER_PORT    = int(os.getenv("MQTT_BROKER_PORT", "1883"))
# Alert fires when health_score drops BELOW this threshold (0.0–1.0).
MQTT_ALERT_THRESHOLD = float(os.getenv("MQTT_ALERT_THRESHOLD", "0.4"))

# Event types we keep in silver (everything else is noise for health metrics).
TRACKED_EVENT_TYPES = (
    "PushEvent", "PullRequestEvent", "IssuesEvent", "IssueCommentEvent",
    "PullRequestReviewEvent", "ReleaseEvent", "ForkEvent", "WatchEvent",
)
