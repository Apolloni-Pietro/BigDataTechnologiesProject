-- Medallion gold-layer schema additions.
-- Runs after 01_schema.sql (init scripts execute in filename order, once, on an
-- empty data volume). Safe to re-run thanks to IF NOT EXISTS guards.

-- ── Extra metric columns produced by the batch pipeline ────────────────────
ALTER TABLE repo_health_metrics
    ADD COLUMN IF NOT EXISTS active_contributors_90d INTEGER,
    ADD COLUMN IF NOT EXISTS days_since_last_commit  INTEGER,
    ADD COLUMN IF NOT EXISTS risk_score              DOUBLE PRECISION;
    -- risk_score in [0,1] (1 = highest risk) from the IsolationForest model.
    -- health_score is kept as (1 - risk_score) for backward compatibility with
    -- the existing API/dashboard.

-- ── Dependency-risk dimension (supply-chain enrichment, daily cadence) ──────
CREATE TABLE IF NOT EXISTS repo_dependency_risk (
    time                       TIMESTAMPTZ NOT NULL,
    repo_name                  TEXT        NOT NULL,
    declared_dependency_count  INTEGER,
    outdated_dependency_ratio  DOUBLE PRECISION,
    open_advisory_count        INTEGER,
    enrichment_available       BOOLEAN
);

SELECT create_hypertable(
    'repo_dependency_risk', 'time', if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_deprisk_repo_time
    ON repo_dependency_risk (repo_name, time DESC);
