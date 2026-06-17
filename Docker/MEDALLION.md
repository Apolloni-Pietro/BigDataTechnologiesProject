# Medallion Architecture — Design, Reasoning & Backends

This document analyses the proposed storage plan, explains where it is right and
where it needs refinement, and describes the architecture that is actually
implemented in this repository. It is the "why" companion to
[`Docker.md`](./Docker.md) (the "what/how" of the containers).

---

## 1. The proposed plan, restated

> 1. Query GH Archive every hour for data
> 2. (If implemented) poll all linked services to calculate all metrics
> 3. Store these data in the **bronze** layer
> 4. Calculate metrics and begin a re-organization of the data
> 5. Use an ML model to understand the "risk factor" for any repo
> 6. Provide the final cleaned, aggregated, ready-for-use data in the **gold** layer

## 2. Verdict: the backbone is correct, three things need fixing

The medallion (bronze → silver → gold) backbone is the **right** choice for this
workload, and MinIO for bronze is a good call. But the plan as written has three
gaps that would bite in a real deployment. Each is addressed in the implementation.

### ✅ What is right

- **Medallion layering.** Raw-immutable → cleaned-conformed → business-ready is the
  industry-standard lakehouse pattern. It gives replayability (re-derive silver/gold
  from bronze after a bug fix without re-downloading), clear lineage, and a natural
  place for each concern.
- **MinIO for bronze.** Bronze is an immutable, append-only, schema-on-read landing
  zone. Object storage with S3 semantics is exactly what production lakehouses use
  (S3/GCS/ADLS); MinIO is a drop-in S3 you can run locally and in Docker. DuckDB and
  Polars both read Parquet/JSON straight from it. **Keep MinIO for bronze.**
- **An ML model for the risk factor** fits the project's Phase 2 (anomaly detection).

### ⚠️ Fix 1 — "silver" is missing from the plan, but it is the most important layer

The plan jumps from "store raw" (bronze) to "calculate metrics" to "gold". In a
correct medallion design the **silver** layer sits in between and does the
unglamorous but essential work: decompress, parse, **type**, **deduplicate**,
explode payloads into a tabular event model, and partition by date. Metrics must be
computed on _clean, conformed_ data, not on raw JSON. We therefore make silver an
explicit, materialised layer (typed Parquet on MinIO), and compute metrics as the
**silver → gold** transform.

### ⚠️ Fix 2 — do **not** route hourly GH Archive through the streaming bus (Redpanda)

GH Archive is **already an hourly batch** product: one `.json.gz` per hour, published
on a delay. Pushing a batch file through Kafka/Redpanda just to read it back adds a
moving part with no benefit — you would be using a real-time transport for data that
is intrinsically not real-time. The correct driver for an hourly batch pipeline is an
**orchestrator/scheduler**, not a message bus.

An earlier scaffold kept Redpanda plus an `ingestion-worker`/`consumer-worker` pair
behind a `streaming` profile. **That subsystem has been removed**: it had no real
data source — the `ingestion-worker` only ever emitted _synthetic_ demo events — so
it added moving parts and a second, conflicting writer to the gold tables while
providing nothing real. A genuine real-time path (consuming the live GitHub Events
API) would be a separate, honest implementation; the batch medallion pipeline is now
the single, real ingestion path.

### ⚠️ Fix 3 — enrichment cannot run "every hour" on the same clock as ingestion

deps.dev / OSV / the GitHub SBOM API are **external and rate-limited**, and a repo's
dependency risk barely changes hour to hour. Polling them on the hourly ingestion
clock would (a) blow through API quotas and (b) couple a fast, reliable job to a slow,
flaky one. Enrichment runs on its **own slower cadence** (default daily), lands its
raw responses in **bronze** too, and is joined into gold opportunistically. If
enrichment is unavailable, the activity metrics still flow (left join + an
`enrichment_available` flag).

---

## 3. The implemented architecture

```
                    ┌──────────────────────── ORCHESTRATION ────────────────────────┐
                    │            pipeline service (APScheduler, hourly + daily)     │
                    └───────────────┬──────────────────────────────────┬────────────┘
                                    │                                  │
            hourly                  ▼                 daily            ▼
  GH Archive  ──────►  ┌───────────────────-──┐    deps.dev/OSV ──►  (enrichment)
  data.gharchive.org   │      BRONZE          │    GitHub SBOM         │
                       │   MinIO (object)     │◄─────────────-─────────┘
                       │  raw .json.gz + raw  │
                       │  enrichment JSON     │
                       └──────────┬───────────┘
                                  │  DuckDB: decompress, type, dedup, explode payloads
                                  ▼
                       ┌─────────────────────┐
                       │      SILVER          │   typed, partitioned Parquet
                       │   MinIO (object)     │   events/event_date=YYYY-MM-DD/hour=H.parquet
                       │  one clean event tbl │   + dim_dependency_risk/
                       └──────────┬───────────┘
                                  │  DuckDB: per-repo metric aggregation  +  IsolationForest risk
                                  ▼
                       ┌─────────────────────────────────────────────┐
                       │                  GOLD                        │
                       │  TimescaleDB (hypertable: metric history)    │  ← analytical / history
                       │  Redis (latest hash + TimeSeries trends)     │  ← hot / low-latency
                       │  MinIO gold/ (model artifacts, snapshots)    │
                       └──────────────────────┬──────────────────────┘
                                              ▼
                                  FastAPI  ──►  Streamlit dashboard
```

### Layer-by-layer backend choices and the reasoning

| Layer                | Backend                                           | Why this backend                                                                                                                                                                                                                                                                                                     |
| -------------------- | ------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Bronze**           | **MinIO** (S3-compatible object store)            | Immutable raw landing zone. Object storage is cheap, append-only, infinitely replayable, and schema-on-read. S3 semantics mean the exact same code runs against AWS S3 in the cloud. DuckDB/Polars read it natively. This is the canonical bronze backend.                                                           |
| **Silver**           | **Parquet files on MinIO**, queried by **DuckDB** | Columnar, compressed (ZSTD), partitioned by `event_date`. DuckDB gives single-node, zero-server SQL over Parquet at GB–TB scale — no cluster to operate. Polars is interchangeable for dataframe work. Keeping silver as open Parquet (not locked in a DB) preserves the lakehouse property: any engine can read it. |
| **Gold (history)**   | **TimescaleDB**                                   | Metrics are a **time-series** (one snapshot per repo per run). Hypertables auto-partition by time, continuous aggregates pre-roll daily summaries, native compression + retention policies keep it lean. This is exactly the access pattern the API's historical endpoints need. **Kept from the existing stack.**   |
| **Gold (hot)**       | **Redis** (redis-stack: RedisTimeSeries)          | Sub-millisecond "what is this repo's score right now?" lookups and short-window trend charts, off the critical path of the SQL database. **Kept from the existing stack.**                                                                                                                                           |
| **Gold (artifacts)** | **MinIO** `gold/` prefix                          | Trained ML model + optional aggregated Parquet snapshots for sharing/reproducibility.                                                                                                                                                                                                                                |
| **Orchestration**    | **APScheduler** inside a `pipeline` container     | Triggers the bronze→silver→gold DAG hourly and enrichment daily. Lightweight, pure-Python, no extra infra. See the upgrade note below.                                                                                                                                                                               |
| **ML risk**          | **scikit-learn IsolationForest**                  | The "risk factor" is unsupervised (we have no labelled "this repo failed" ground truth), so an anomaly-detection model that learns the normal feature distribution and flags outliers is the honest choice. Falls back to a deterministic weighted composite when there is too little data to train.                 |

### Were TimescaleDB and Redis the right DBs? Yes — but only for _gold_

The `add_docker` branch already chose TimescaleDB + Redis, and they are correct **for
the serving (gold) layer**. The mistake would be to use them for bronze/silver:

- **Bronze is not a database problem.** Dumping raw hourly JSON into Postgres would be
  expensive, slow, and throw away the cheap-immutable-replayable property. MinIO is right.
- **Silver is not a row-store problem.** Analytical metric aggregation over millions of
  events is a columnar/OLAP job (DuckDB over Parquet), not an OLTP row-store job.
- **Gold _is_ a serving problem**, and that is exactly where TimescaleDB (history) and
  Redis (hot) shine. So we keep both, unchanged, at the right layer.

We did **not** adopt a table format (Apache Iceberg / Delta Lake) for silver/gold.
For this data volume and a single-node DuckDB engine it would be pure overhead. The
upgrade path is documented below — the Parquet-on-MinIO layout is deliberately
Iceberg-compatible so the migration is mechanical if scale ever demands it.

---

## 4. Why this is "optimal enough" and where it would scale

**Deliberate simplifications (correct for this project's scale):**

- **DuckDB instead of Spark/Trino.** Single-node DuckDB handles tens of GB of Parquet
  comfortably and needs no cluster. Spark would be operational overhead with no payoff
  here.
- **APScheduler instead of Airflow/Dagster.** A cron-like scheduler in one container is
  enough to run an hourly DAG. We keep the orchestration logic explicit and isolated in
  `pipeline.py` so swapping in a real orchestrator is a lift-and-shift of function calls.
- **Plain partitioned Parquet instead of Iceberg/Delta.**

**Documented upgrade paths (when scale demands):**

- Orchestration → **Dagster** (models bronze/silver/gold as software-defined _assets_,
  which maps 1:1 onto this design) or **Airflow**.
- Silver/gold table format → **Apache Iceberg** on the same MinIO bucket (gives ACID,
  time-travel, schema evolution). DuckDB and Spark both read Iceberg.
- Query engine → **Trino/Spark** if a single node stops coping.
- Bronze → swap MinIO for **AWS S3 / GCS** by changing only the endpoint + credentials
  (the code uses the S3 API, not MinIO-specific calls).

---

## 5. Data contracts (the glue between layers)

- **Bronze object keys**
  - GH Archive: `gharchive/YYYY/MM/DD/YYYY-MM-DD-H.json.gz` (raw, untouched)
  - Enrichment: `enrichment/sbom/<owner>__<repo>.json`, `enrichment/osv/<run_date>.json`
- **Silver object keys**
  - Events: `events/event_date=YYYY-MM-DD/hour=H.parquet`
  - Dependency-risk dimension: `dim_dependency_risk/run_date=YYYY-MM-DD/data.parquet`
- **Gold**
  - TimescaleDB `repo_health_metrics` (+ `risk_score`), continuous aggregate `repo_health_daily`
  - TimescaleDB `repo_dependency_risk`
  - Redis `latest:<repo>` (hash) and `ts:<repo>:<metric>` (TimeSeries)
  - MinIO `gold/models/risk_isolation_forest.pkl`

The **join key across everything is `repo_name`** (`owner/repo`), exactly as produced
by `GHArchiveDownload.py` and consumed by `DependencyRisk.py`.

See [`services/pipeline/README.md`](./services/pipeline/README.md) for how to run,
configure, backfill, and extend the pipeline.
