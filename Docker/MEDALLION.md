# Medallion Architecture

The OSS Health Monitor is structured as a **bronze → silver → gold** medallion
lakehouse. Each layer has a distinct backend chosen for what that layer actually needs
to do — a different backend for each concern is deliberate, not accidental complexity.

---

## The three layers

```
GH Archive (hourly .json.gz)
  │  download + integrity check
  ▼
BRONZE  — MinIO       gharchive/YYYY/MM/DD/YYYY-MM-DD-H.json.gz
  │  DuckDB: decompress, type, dedup, explode payloads
  ▼
SILVER  — MinIO       events/event_date=YYYY-MM-DD/hour=H.parquet
  │  DuckDB: per-repo metric aggregation + IsolationForest risk
  ▼
GOLD    — TimescaleDB (metric history) + Redis (hot tier)
          MinIO gold/models/ (trained ML model)
  │  HTTP/JSON
  ▼
FastAPI  →  Streamlit dashboard
```

---

## Backend choices

| Layer                | Backend                                           | Why                                                                                                                                                                                                                        |
| -------------------- | ------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Bronze**           | **MinIO** (S3-compatible object store)            | Raw, immutable, schema-on-read landing zone. Object storage is cheap and append-only; the S3 API means the same code runs against AWS S3 without changes. DuckDB reads JSON directly from it.                              |
| **Silver**           | **Parquet on MinIO**, queried by **DuckDB**       | Columnar, ZSTD-compressed, partitioned by `event_date`. DuckDB gives zero-server SQL over Parquet at GB–TB scale. Storing silver as open Parquet (not in a DB) means any engine can read it — the lakehouse property is preserved. |
| **Gold (history)**   | **TimescaleDB**                                   | Metrics form a time series: one snapshot per repo per pipeline run. Hypertables auto-partition by time, continuous aggregates pre-roll daily summaries, and native compression + retention keep it lean.                    |
| **Gold (hot)**       | **Redis** (redis-stack with RedisTimeSeries)      | Sub-millisecond current-value lookups and short-window trend charts off the critical SQL path. Memory-capped with LRU eviction — safe because the full history is in TimescaleDB.                                           |
| **Gold (artifacts)** | **MinIO** `gold/` prefix                          | Trained IsolationForest model persisted as `gold/models/risk_isolation_forest.pkl` so it survives container restarts.                                                                                                      |
| **Orchestration**    | **APScheduler** inside the `pipeline` container   | Drives the bronze→silver→gold DAG hourly and the retention sweep daily. The stage functions in `pipeline.py` are plain Python — they can be lifted into Dagster or Airflow as-is.                                          |
| **ML risk**          | **scikit-learn IsolationForest**                  | Risk scoring is unsupervised (no labeled "failed repo" ground truth). IsolationForest learns the normal feature distribution and flags outliers. Falls back to a weighted composite when fewer than 30 repos are available. |

Bronze and silver are deliberately kept out of a row-store database: bronze is an
append-only raw landing zone (a DB would be expensive and would lose replayability),
and silver's aggregation workload is columnar/OLAP — exactly what DuckDB over Parquet
is built for.

---

## Data contracts

Object keys encode dates and are parsed by the retention job; changing them is a
breaking change.

| Layer  | Key pattern                                                    | Notes                                                         |
| ------ | -------------------------------------------------------------- | ------------------------------------------------------------- |
| Bronze | `gharchive/YYYY/MM/DD/YYYY-MM-DD-H.json.gz`                    | Raw, never modified after landing.                            |
| Silver | `events/event_date=YYYY-MM-DD/hour=H.parquet`                  | Hive-partitioned by `event_date` only; `hour` is the filename.|
| Silver | `events/event_date=YYYY-MM-DD/data.parquet`                    | Written by the Parquet backfill path (no `hour` partition).   |
| Gold   | `gold/models/risk_isolation_forest.pkl`                        | Trained model; rewritten each gold cycle.                     |

The **join key across all layers is `repo_name`** (`owner/repo`), exactly as produced
by GH Archive and `GHArchiveDownload.py`.

TimescaleDB gold schema:
- **`repo_health_metrics`** hypertable — one row per repo per pipeline run, with
  `active_actors`, `event_count`, `commit_freq_30d`, `active_contributors_90d`,
  `bus_factor`, `pr_latency_p50`, `pr_abandon_rate`, `stale_issue_ratio`,
  `days_since_last_commit`, `days_since_last_release`, `risk_score`, `health_score`.
- **`repo_health_daily`** continuous aggregate — daily rollup, refreshed hourly.
- Compression after 7 days; data older than 2 years is dropped.

Redis gold schema:
- `latest:{repo}` — hash of current metric values (O(1) lookup).
- `ts:{repo}:{metric}` — RedisTimeSeries key, 7-day window, used for trend charts.

---

## Scale and upgrade paths

The current design is intentionally single-node. DuckDB handles tens of GB of Parquet
without a cluster; APScheduler is enough to drive an hourly DAG; plain partitioned
Parquet needs no table-format overhead at this scale.

When scale demands it, each component has a natural upgrade path:

| Current                      | Upgrade to                                  | What stays the same                             |
| ---------------------------- | ------------------------------------------- | ----------------------------------------------- |
| APScheduler                  | Dagster or Airflow                          | `pipeline.py` stage functions, unchanged        |
| Plain Parquet on MinIO       | Apache Iceberg on the same MinIO bucket     | DuckDB and Spark both read Iceberg natively      |
| DuckDB (single-node)         | Trino or Spark                              | Silver Parquet layout, unchanged                |
| MinIO (local)                | AWS S3 / GCS / ADLS                         | Code uses the S3 API — change endpoint + creds  |
