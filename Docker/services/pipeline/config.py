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
GITHUB_TOKEN   = os.getenv("GITHUB_TOKEN", "")        # for the SBOM API (optional)

# ── Scheduling ─────────────────────────────────────────────────────────────
# How the scheduler decides what to process. The pipeline always targets the
# most recently *published* GH Archive hour (publication lags ~1h, so we look
# back PUBLISH_LAG_HOURS).
PUBLISH_LAG_HOURS    = int(os.getenv("PUBLISH_LAG_HOURS", "2"))
ENRICHMENT_CRON_HOUR = int(os.getenv("ENRICHMENT_CRON_HOUR", "3"))   # daily, 03:00 UTC
ENRICHMENT_MAX_REPOS = int(os.getenv("ENRICHMENT_MAX_REPOS", "100"))

# Optional one-shot backfill on startup: "YYYY-MM-DD-H" .. "YYYY-MM-DD-H".
# When BACKFILL_START is set the service processes that whole range once and
# then continues with the normal schedule.
BACKFILL_START = os.getenv("BACKFILL_START", "")
BACKFILL_END   = os.getenv("BACKFILL_END", "")

# ── Gold / metrics windows ─────────────────────────────────────────────────
COMMIT_FREQ_WINDOW_DAYS  = int(os.getenv("COMMIT_FREQ_WINDOW_DAYS", "30"))
CONTRIBUTOR_WINDOW_DAYS  = int(os.getenv("CONTRIBUTOR_WINDOW_DAYS", "90"))
STALE_ISSUE_DAYS         = int(os.getenv("STALE_ISSUE_DAYS", "90"))

# ── Retention (24/7 disk management for MinIO; gold has its own retention) ───
# Bronze is a transient landing zone -> short window. Silver is the history gold
# aggregates over rolling windows, so it MUST outlive the largest window
# (CONTRIBUTOR_WINDOW_DAYS) or metrics degrade at the edge.
BRONZE_RETENTION_DAYS = int(os.getenv("BRONZE_RETENTION_DAYS", "30"))
SILVER_RETENTION_DAYS = int(os.getenv("SILVER_RETENTION_DAYS", "120"))
RETENTION_CRON_HOUR   = int(os.getenv("RETENTION_CRON_HOUR", "4"))   # daily, 04:45 UTC

# Event types we keep in silver (everything else is noise for health metrics).
TRACKED_EVENT_TYPES = (
    "PushEvent", "PullRequestEvent", "IssuesEvent", "IssueCommentEvent",
    "PullRequestReviewEvent", "ReleaseEvent", "ForkEvent", "WatchEvent",
)
