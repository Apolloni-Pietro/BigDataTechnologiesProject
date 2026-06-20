# Pipeline service — the medallion orchestrator

This container runs the batch **bronze → silver → gold** pipeline that powers the
OSS Health Monitor. It is the _default_ ingestion path. For the architectural
reasoning (why MinIO, why DuckDB, why not Kafka for batch, etc.) read
[`../../MEDALLION.md`](../../MEDALLION.md).

## What it does

| When                  | Job              | Stages                                                                                                                              |
| --------------------- | ---------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| Every hour (at :15)   | `hourly_job`     | bronze (download GH Archive hour → MinIO) → silver (typed/deduped Parquet → MinIO) → gold (metrics + ML risk → TimescaleDB + Redis) |
| Daily (at 03:30 UTC)  | `enrichment_job` | dependency-risk enrichment (GitHub SBOM → deps.dev → OSV) → bronze + TimescaleDB                                                    |
| Daily (at 04:45 UTC)  | `retention_job`  | prune aged bronze/silver objects in MinIO so 24/7 operation doesn't fill disk                                                       |
| On startup (optional) | `backfill`       | replay a fixed historical hour range once                                                                                           |

## Files

```
main.py        scheduler entrypoint (APScheduler) + backfill
pipeline.py    the DAG: run_hour(), run_enrichment(), run_retention()  ← lift into Dagster/Airflow later
bronze.py      download + integrity-check + land raw GH Archive in MinIO
silver.py      DuckDB transform: raw JSON → typed, deduped, partitioned Parquet
gold.py        DuckDB metric aggregation + bus factor + write to TimescaleDB/Redis
backfill_parquet.py  re-project pre-downloaded monthly Parquet straight into silver
risk_model.py  unsupervised IsolationForest "risk factor" (composite fallback)
enrichment.py  deps.dev / OSV / GitHub SBOM dependency-risk dimension
retention.py   prune aged bronze/silver objects from MinIO (gold has its own retention)
storage.py     MinIO client + DuckDB-over-S3 connection helpers
config.py      all configuration, read from environment variables
```

## Retention (24/7 disk management)

MinIO is the only unbounded store (gold's TimescaleDB/Redis already self-retain), so
`retention_job` runs daily and deletes objects older than two separate windows:

- `BRONZE_RETENTION_DAYS` (default 30) — bronze is a transient raw landing zone.
- `SILVER_RETENTION_DAYS` (default 120) — silver is the history `gold.build()`
  aggregates over rolling windows, so it **must stay ≥ the largest window**
  (`CONTRIBUTOR_WINDOW_DAYS`, 90 days) or metrics degrade at the edge. The job logs a
  warning if this invariant is violated.

Object age comes from the date in the key (`gharchive/YYYY/MM/DD/…`,
`event_date=YYYY-MM-DD`), falling back to last-modified for dateless keys (e.g.
enrichment blobs). Gold is never touched.

## Run it

From the `Docker/` directory:

```bash
cp .env.example .env          # then edit if you want (GITHUB_TOKEN, MinIO keys…)
docker compose up --build     # default stack = medallion pipeline + serving
```

This starts: `minio`, `timescaledb`, `redis`, `pipeline`, `api`, `dashboard`.
The pipeline runs one cycle immediately, then on schedule.

- Dashboard: http://localhost:8501
- API docs: http://localhost:8080/docs
- MinIO console: http://localhost:9001 (login = MINIO_ACCESS_KEY / MINIO_SECRET_KEY)

### Backfill historical data

```bash
# in .env
BACKFILL_START=2024-01-01-0
BACKFILL_END=2024-01-01-23
docker compose up --build
```

The service replays those hours once, then continues with live hourly processing.

**Faster path — pre-downloaded Parquet.** Set `BACKFILL_PARQUET_DIR=/backfill` to ingest
monthly Parquet files (from the repo-root `GHArchiveDownload.py`) straight into silver,
skipping download + JSON parsing. Takes precedence over `BACKFILL_START/END`; run on a
fresh silver bucket. Implemented in `backfill_parquet.py` /
`pipeline.run_parquet_backfill()`. Full guide: [`../../PARQUET_BACKFILL.md`](../../PARQUET_BACKFILL.md).

## Configuration (environment variables)

All defaults live in [`config.py`](./config.py). The ones you are most likely to
change are in [`../../.env.example`](../../.env.example): `MINIO_ACCESS_KEY`,
`MINIO_SECRET_KEY`, `GITHUB_TOKEN`, `BACKFILL_START/END`, `ENRICHMENT_MAX_REPOS`.

## How to extend it

- **Add a metric**: add the aggregation to the SQL in `gold.py::_compute_features`,
  add the column to `init/timescale/02_medallion.sql`, and add it to the INSERT in
  `gold.py::_write_timescale`.
- **Add a payload field to silver**: edit the projection in `silver.py`. Because
  bronze is immutable, you can then _replay_ (backfill) to rebuild silver/gold with
  the new field — no re-download needed.
- **Swap the orchestrator**: the stage functions in `pipeline.py` are plain Python.
  Wrap them as Dagster assets or Airflow tasks with no change to bronze/silver/gold.
- **Tune the ML model**: edit `risk_model.py` (features in `FEATURE_COLUMNS`, model
  in `score_repos`). The trained model is persisted to `gold/models/` in MinIO.
