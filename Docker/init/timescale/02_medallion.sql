-- Medallion gold-layer schema additions.
-- Runs after 01_schema.sql (init scripts execute in filename order, once, on an
-- empty data volume). Safe to re-run thanks to IF NOT EXISTS guards.

-- ── Extra metric columns produced by the batch pipeline ────────────────────
ALTER TABLE repo_health_metrics
    ADD COLUMN IF NOT EXISTS active_contributors_90d INTEGER,
    ADD COLUMN IF NOT EXISTS days_since_last_commit  INTEGER,
    ADD COLUMN IF NOT EXISTS risk_score              DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS event_count             BIGINT,
    ADD COLUMN IF NOT EXISTS active_actors           BIGINT;
    -- risk_score in [0,1] (1 = highest risk) from the IsolationForest model.
    -- health_score is kept as (1 - risk_score) for backward compatibility with
    -- the existing API/dashboard.
    -- active_actors = distinct people (actor_login) active on the repo over the
    -- rolling window; THIS is the "importance" ranking metric (bot-resistant).
    -- event_count = total tracked events, kept as a secondary signal (high
    -- event_count with low active_actors flags bot/spam activity).
