# OSS Health Monitor — Docker / container reference

The container and operations reference for the platform. This is the "what runs
and how to operate it" companion to:

- [`MEDALLION.md`](./MEDALLION.md) — the architecture and the reasoning behind it.
- [`services/pipeline/README.md`](./services/pipeline/README.md) — the bronze →
  silver → gold pipeline internals (run, configure, backfill, extend).

---

## Table of Contents

- [How it works](#how-it-works)
- [Architecture](#architecture)
- [Storage layers](#storage-layers)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Environment Variables](#environment-variables)
- [Services](#services)
- [Scheduled jobs](#scheduled-jobs)
- [Common Operations](#common-operations)
- [Accessing the UIs](#accessing-the-uis)
- [Data Flow](#data-flow)
- [Project Structure](#project-structure)
- [Troubleshooting](#troubleshooting)

---

## How it works

The platform is a **batch medallion lakehouse**. Once an hour, the `pipeline`
container downloads the latest [GH Archive](https://www.gharchive.org/) hourly file
(every public GitHub event since 2011) and runs it through three quality tiers —
**bronze** (raw), **silver** (cleaned), **gold** (business-ready metrics + an ML
risk score). A FastAPI layer serves the gold tier; a Streamlit dashboard reads the
API.

GH Archive is a batch product (one `.json.gz` per hour), so there is **no message
broker** — an in-container scheduler drives the pipeline. There is no synthetic
data and no streaming path; every number comes from real GitHub events.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────-─┐
│                          Docker Compose Network                           │
│                                                                           │
│  ── Object Storage ────────────────────────────────────────────────────   │
│  ┌──────────────────────────────────────────────────────────────────────┐ │
│  │ MinIO (S3-compatible)             :9000 (S3)    :9001 (Console)      │ │
│  │  bronze/ raw .json.gz       silver/ typed Parquet    gold/ models    │ │
│  └──────────────────────────────────────────────────────────────────────┘ │
│            ▲ write            ▲ read/write           ▲ read               │
│  ── Orchestrator ─────────────┼──────────────────────┼─────────────────   │
│  ┌────────────────────────────┴──────────────────────┴──────────────────┐ │
│  │ pipeline (APScheduler)                                               │ │
│  │  hourly: bronze → silver → gold     daily: enrichment + retention    │ │
│  └───────────────────────────────┬───────────────────┬──────────────────┘ │
│                                  │ write gold        │ write gold         │
│  ── Serving Stores ──────────────▼───────────────────▼─────────────────   │
│  ┌──────────────────────────┐        ┌──────────────────────────────────┐ │
│  │ TimescaleDB :5432        │        │ Redis Stack :6379  :8001         │ │
│  │ metric history (2y)      │        │ latest values + 7-day trends     │ │
│  └─────────────┬────────────┘        └────────────────┬─────────────────┘ │
│                └─────────────────┬────────────────────┘                   │
│  ── Serving ─────────────────────▼─────────────────────────────────────   │
│  ┌──────────────────────────────────────────────────────────────────────┐ │
│  │ api (FastAPI) :8080 ──► routes queries to Redis (≤7d) or TimescaleDB │ │
│  └──────────────────────────────────┬───────────────────────────────────┘ │
│                                     │ calls API only                      │
│  ┌──────────────────────────────────▼───────────────────────────────────┐ │
│  │ dashboard (Streamlit) :8501                                          │ │
│  └──────────────────────────────────────────────────────────────────────┘ │
│                                                                           │
│  Named volumes: minio-data  timescale-data  redis-data                    │
└──────────────────────────────────────────────────────────────────────────-┘
```

Six containers. Three run from **prebuilt public images** (`minio`, `timescaledb`,
`redis`); three are **built from Dockerfiles** (`pipeline`, `api`, `dashboard`).

---

## Storage layers

| Layer  | Backend                  | Holds                                              | Retention                     |
| ------ | ------------------------ | -------------------------------------------------- | ----------------------------- |
| Bronze | MinIO (object)           | raw GH Archive `.json.gz` + raw enrichment JSON    | `BRONZE_RETENTION_DAYS` (30)  |
| Silver | MinIO (Parquet) + DuckDB | cleaned, typed, deduplicated event table           | `SILVER_RETENTION_DAYS` (120) |
| Gold   | TimescaleDB + Redis      | per-repo metrics, ML risk score, served to the API | TimescaleDB 2y / Redis 7d     |

Gold is bounded by TimescaleDB's retention policy and Redis's LRU + 7-day series.
MinIO is the only store that would otherwise grow forever, so the `pipeline`'s
daily **retention job** prunes aged bronze/silver objects (see
[Scheduled jobs](#scheduled-jobs)).

---

## Prerequisites

- **Docker** ≥ 24.0 — [Install Docker](https://docs.docker.com/get-docker/)
- **Docker Compose** ≥ 2.20 (bundled with Docker Desktop; verify `docker compose version`)
- **RAM**: ≥ 6 GB available to Docker (8 GB recommended)
- **Disk**: ≥ 20 GB free for a first run; more for long backfills (each GH Archive
  month is ~15–25 GB of bronze before retention prunes it)
- **A GitHub token is _optional_** — only the daily dependency-risk enrichment
  (GitHub SBOM API) uses it. Hourly GH Archive ingestion needs no token.

---

## Quick Start

**1. Configure**

```bash
cd Docker
cp .env.example .env        # defaults work out of the box; edit if you like
```

**2. Build and start**

```bash
docker compose up --build -d
```

First build takes 3–5 minutes (base images + Python deps). The `pipeline` runs one
cycle immediately, then on schedule.

**3. Verify**

```bash
docker compose ps
```

All containers should be `running`/`healthy`. TimescaleDB needs ~20–30 s to
initialise its schema on first boot; dependents wait for it automatically.

> GH Archive publishes each hour's file on a ~1–2 h delay, so the very first live
> cycle processes an hour from a couple of hours ago. To populate history
> immediately, use a [backfill](#scheduled-jobs).

Then open:

| Interface           | URL                          | What it is                            |
| ------------------- | ---------------------------- | ------------------------------------- |
| Streamlit dashboard | `http://localhost:8501`      | main user-facing dashboard            |
| FastAPI docs        | `http://localhost:8080/docs` | interactive REST API explorer         |
| MinIO console       | `http://localhost:9001`      | browse bronze/silver/gold objects     |
| RedisInsight        | `http://localhost:8001`      | browse Redis keys and TimeSeries data |

---

## Environment Variables

Copy `.env.example` to `.env` and edit. Never commit `.env` — it is in `.gitignore`.

| Variable                | Default      | Description                                                                                                     |
| ----------------------- | ------------ | --------------------------------------------------------------------------------------------------------------- |
| `GITHUB_TOKEN`          | —            | Optional. Only for the daily dependency-risk enrichment (GitHub SBOM API). GH Archive needs no token.           |
| `MINIO_ACCESS_KEY`      | `minioadmin` | MinIO root user / S3 access key.                                                                                |
| `MINIO_SECRET_KEY`      | `minioadmin` | MinIO root password / S3 secret key. Change if exposed to a network.                                            |
| `BACKFILL_START`        | —            | Optional one-shot replay start, format `YYYY-MM-DD-H`. Empty = process new hours only.                          |
| `BACKFILL_END`          | —            | Optional one-shot replay end, format `YYYY-MM-DD-H`.                                                            |
| `ENRICHMENT_MAX_REPOS`  | `100`        | Cap on repos analysed per daily enrichment run (free-API rate-limit guard).                                     |
| `BRONZE_RETENTION_DAYS` | `30`         | Delete bronze objects older than this. Bronze is a transient raw landing zone.                                  |
| `SILVER_RETENTION_DAYS` | `120`        | Delete silver objects older than this. **Must stay ≥ 90** (the largest gold rolling window) or metrics degrade. |
| `RETENTION_CRON_HOUR`   | `4`          | UTC hour for the daily retention sweep (runs at `:45`).                                                         |
| `POSTGRES_USER`         | `oss`        | TimescaleDB username.                                                                                           |
| `POSTGRES_PASSWORD`     | `changeme`   | TimescaleDB password. Change if the port is exposed.                                                            |
| `POSTGRES_DB`           | `oss_health` | TimescaleDB database name.                                                                                      |
| `REDIS_MAX_MEMORY`      | `2gb`        | Redis memory cap before LRU eviction. Reduce to `1gb` on < 8 GB machines.                                       |

---

## Services

### MinIO

S3-compatible object store; the **bronze + silver** backend, plus `gold/models/`
for the trained ML model. Buckets (`bronze`, `silver`, `gold`) are created
automatically by the pipeline on startup.

**Ports:** `9000` (S3 API), `9001` (web console; log in with `MINIO_ACCESS_KEY` /
`MINIO_SECRET_KEY`).

### pipeline

The orchestrator (built from `services/pipeline`). Runs an APScheduler that drives
the whole medallion DAG — download, clean, aggregate, score, enrich, prune. It has
no exposed port. See [Scheduled jobs](#scheduled-jobs) and the
[pipeline README](./services/pipeline/README.md).

### TimescaleDB

PostgreSQL + time-series extension; the **gold history** store. The schema is
created automatically on first boot from `./init/timescale/01_schema.sql` and
`02_medallion.sql`. Key objects:

- **`repo_health_metrics`** hypertable — one row per repo per pipeline run
  (`commit_freq_30d`, `active_contributors_90d`, `bus_factor`, `pr_latency_p50`,
  `pr_abandon_rate`, `stale_issue_ratio`, `days_since_last_commit`,
  `days_since_last_release`, `risk_score`, `health_score`).
- **`repo_dependency_risk`** hypertable — supply-chain dimension from enrichment
  (`declared_dependency_count`, `outdated_dependency_ratio`, `open_advisory_count`).
- **`repo_health_daily`** continuous aggregate — daily rollup, refreshed hourly.
- Compression after 7 days; retention drops data older than 2 years.

**Port:** `5432`.

### Redis Stack

The **gold hot tier** (RedisTimeSeries module). Holds `latest:{repo}` (a hash of
current values for O(1) lookups) and `ts:{repo}:{metric}` (7-day series for trend
charts). Memory-capped with LRU eviction — safe because the full history lives in
TimescaleDB.

**Ports:** `6379` (Redis), `8001` (RedisInsight UI).

### FastAPI (`api`)

The REST layer every client talks to — never the databases directly. Routes by
time window: recent reads come from Redis, longer ranges from TimescaleDB.

Endpoints: `GET /health`, `GET /repos`, `GET /repos/{owner}/{name}/current`
(Redis), `GET /repos/{owner}/{name}/history` (≤7d Redis, >7d TimescaleDB),
`GET /at-risk`. Swagger UI at `http://localhost:8080/docs`.

**Port:** `8080`.

### Streamlit (`dashboard`)

The user-facing UI. Calls **FastAPI exclusively** — no database credentials. All
data access, caching, and routing live in the API.

**Port:** `8501`.

---

## Scheduled jobs

The `pipeline` container runs these on an in-process scheduler (UTC):

| When                  | Job              | What it does                                                                           |
| --------------------- | ---------------- | -------------------------------------------------------------------------------------- |
| Every hour at `:15`   | `hourly_job`     | process the latest available GH Archive hour: bronze → silver → gold                   |
| On startup, once      | immediate run    | one `hourly_job` so a fresh stack isn't empty                                          |
| Daily at `03:30`      | `enrichment_job` | dependency-risk enrichment (GitHub SBOM → deps.dev → OSV) → bronze + TimescaleDB       |
| Daily at `04:45`      | `retention_job`  | prune bronze (>`BRONZE_RETENTION_DAYS`) and silver (>`SILVER_RETENTION_DAYS`) in MinIO |
| On startup (optional) | `backfill`       | if `BACKFILL_START`/`BACKFILL_END` are set, replay that hour range once                |

**Backfill** (populate history fast): set the range in `.env`, then start the stack.

```dotenv
BACKFILL_START=2024-01-01-0
BACKFILL_END=2024-01-01-23
```

Each stage is idempotent (bronze/silver skip objects already in MinIO), so backfills
are resumable and safe to re-run.

---

## Common Operations

```bash
docker compose up -d              # start (detached)
docker compose down               # stop, keep data
docker compose down -v            # full reset — delete all volumes
docker compose ps                 # status / health
docker compose logs -f pipeline   # follow the pipeline (also: api, dashboard)
docker compose up --build -d api  # rebuild one service after code changes
```

**Run the retention sweep on demand** (instead of waiting for 04:45):

```bash
docker compose exec pipeline python -c "import retention; retention.run()"
```

**Connect to TimescaleDB:**

```bash
docker compose exec timescaledb psql -U oss -d oss_health \
  -c "SELECT repo_name, health_score, risk_score FROM repo_health_metrics ORDER BY time DESC LIMIT 10;"
```

**Inspect a Redis series:**

```bash
docker compose exec redis redis-cli TS.RANGE ts:facebook/react:health_score - +
```

---

## Accessing the UIs

- **Streamlit dashboard** — `http://localhost:8501` — health scores, trends, at-risk repos.
- **FastAPI docs** — `http://localhost:8080/docs` — test every endpoint in the browser.
- **MinIO console** — `http://localhost:9001` — browse the `bronze`, `silver`, `gold` buckets fill up.
- **RedisInsight** — `http://localhost:8001` — browse keys and TimeSeries visually.

---

## Data Flow

```
GH Archive (hourly .json.gz)
  │  download + integrity check
  ▼
BRONZE  — MinIO  gharchive/YYYY/MM/DD/…json.gz   (raw, immutable)
  │  DuckDB: decompress, type, dedup, project
  ▼
SILVER  — MinIO  events/event_date=…/hour=…parquet   (clean event table)
  │  DuckDB: per-repo metric aggregation + IsolationForest risk
  ▼
GOLD    — TimescaleDB (history) + Redis (hot) + MinIO gold/models
  │  HTTP JSON
  ▼
api (FastAPI)  →  dashboard (Streamlit)
```

Daily, enrichment (GitHub SBOM → deps.dev → OSV) lands raw responses in bronze and
writes the `repo_dependency_risk` dimension to TimescaleDB, which gold joins in.

---

## Project Structure

```
Docker/
├── docker-compose.yml           # all services, volumes, the default stack
├── .env.example                 # template — copy to .env
├── .env                         # local config — not committed
├── MEDALLION.md                 # architecture + reasoning
├── Docker.md                    # this file — container/ops reference
│
├── init/
│   └── timescale/
│       ├── 01_schema.sql        # base gold schema (auto-run on first boot)
│       └── 02_medallion.sql     # risk_score column + repo_dependency_risk table
│
└── services/
    ├── pipeline/                # the medallion orchestrator (built)
    │   ├── Dockerfile  requirements.txt  README.md
    │   ├── main.py              # scheduler + backfill
    │   ├── pipeline.py          # the DAG: run_hour / run_enrichment / run_retention
    │   ├── bronze.py silver.py gold.py
    │   ├── risk_model.py        # IsolationForest risk score
    │   ├── enrichment.py        # deps.dev / OSV / GitHub SBOM
    │   ├── retention.py         # prune aged bronze/silver in MinIO
    │   ├── storage.py config.py
    ├── api/                     # FastAPI (built)
    └── dashboard/               # Streamlit (built)
```

---

## Troubleshooting

**A container is stuck `starting` / `unhealthy`**
TimescaleDB initialises its schema on first boot (~20–30 s); `pipeline`/`api`/`dashboard`
wait for it via `condition: service_healthy`/`service_started`. Re-run `docker compose ps`.

**The dashboard is empty**
Likely no gold data yet. GH Archive lags ~1–2 h, so the first live hour is a couple
of hours old; either wait, or run a [backfill](#scheduled-jobs). Check `docker compose
logs -f pipeline` for `gold: wrote N repos`.

**Out of disk space**
The daily retention job prunes MinIO automatically; to reclaim sooner, lower
`BRONZE_RETENTION_DAYS` / `SILVER_RETENTION_DAYS` in `.env` (keep silver ≥ 90) and run
the on-demand sweep above. TimescaleDB self-manages via its 2-year retention; check
size with:

```bash
docker compose exec timescaledb psql -U oss -d oss_health \
  -c "SELECT pg_size_pretty(pg_database_size('oss_health'));"
```

**Enrichment finds nothing / 401**
Enrichment needs `GITHUB_TOKEN` for the SBOM API; without it, repos report
`enrichment_available = false` and the rest of the pipeline still runs. Check
`docker compose logs pipeline` around the daily run.

**Port already in use**
Find the conflict (`lsof -i :8501`, etc.) and change the host-side number in the
`"HOST:CONTAINER"` mapping in `docker-compose.yml`.

**Wipe one store without a full reset**

```bash
docker compose stop minio
docker volume rm docker_minio-data     # or timescale-data / redis-data
docker compose up -d minio
```
