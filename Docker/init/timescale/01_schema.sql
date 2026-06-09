-- Enable the TimescaleDB extension inside our database.
-- This must come before any CREATE TABLE that uses hypertable features.
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ── Main metrics table ─────────────────────────────────────────────────
-- This is a standard PostgreSQL table. The magic happens in the next step.

CREATE TABLE IF NOT EXISTS repo_health_metrics (
    time                  TIMESTAMPTZ    NOT NULL,
    repo_name             TEXT           NOT NULL,
    commit_freq_30d       DOUBLE PRECISION,
    bus_factor            INTEGER,
    pr_latency_p50        DOUBLE PRECISION,
    pr_abandon_rate       DOUBLE PRECISION,
    stale_issue_ratio     DOUBLE PRECISION,
    days_since_last_release INTEGER,
    health_score          DOUBLE PRECISION
);

-- Convert the table to a TimescaleDB hypertable.
-- A hypertable looks and behaves exactly like a normal PostgreSQL table,
-- but TimescaleDB automatically partitions it into time-based chunks on disk.
-- Queries that filter on `time` only scan the relevant chunks, not the
-- whole table — this is what makes range queries fast at scale.
SELECT create_hypertable(
    'repo_health_metrics',
    'time',
    if_not_exists => TRUE
);

-- Index for looking up a specific repo's history quickly.
-- The DESC order puts recent data first, which matches most query patterns.
CREATE INDEX IF NOT EXISTS idx_repo_time
    ON repo_health_metrics (repo_name, time DESC);

-- ── Compression ────────────────────────────────────────────────────────
-- Tell TimescaleDB how to compress chunks older than 7 days.
-- segment_by = 'repo_name' means all rows for a given repo are grouped
-- together in the compressed block, which makes repo-specific queries
-- faster even on compressed data.
ALTER TABLE repo_health_metrics
    SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'repo_name',
        timescaledb.compress_orderby   = 'time DESC'
    );

-- Automatically compress chunks once they are older than 7 days.
SELECT add_compression_policy(
    'repo_health_metrics',
    compress_after => INTERVAL '7 days',
    if_not_exists  => TRUE
);

-- ── Retention ──────────────────────────────────────────────────────────
-- Automatically delete data older than 2 years.
-- TimescaleDB drops entire chunks, which is much faster than DELETE.
SELECT add_retention_policy(
    'repo_health_metrics',
    drop_after    => INTERVAL '2 years',
    if_not_exists => TRUE
);

-- ── Continuous aggregate ───────────────────────────────────────────────
-- A materialised daily rollup that refreshes incrementally every hour.
-- Querying this view is much faster than aggregating the raw table
-- for long date ranges. The WITH (timescaledb.continuous) clause is
-- what makes it a "continuous" aggregate — TimescaleDB refreshes only
-- the time buckets that have changed, not the whole view.

CREATE MATERIALIZED VIEW IF NOT EXISTS repo_health_daily
WITH (timescaledb.continuous) AS
    SELECT
        time_bucket('1 day', time)  AS day,
        repo_name,
        AVG(health_score)           AS health_score_avg,
        MIN(health_score)           AS health_score_min,
        MAX(health_score)           AS health_score_max,
        AVG(bus_factor)             AS bus_factor_avg,
        AVG(commit_freq_30d)        AS commit_freq_avg
    FROM repo_health_metrics
    GROUP BY 1, 2
WITH NO DATA;

-- Tell TimescaleDB to refresh this view every hour,
-- covering data from the last 3 days (in case of late-arriving events).
SELECT add_continuous_aggregate_policy(
    'repo_health_daily',
    start_offset      => INTERVAL '3 days',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists     => TRUE
);