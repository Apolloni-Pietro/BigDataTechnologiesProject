# Big Data Technologies — OSS Health Monitor

A platform that continuously measures the **health of open-source software** on
GitHub. It ingests public GitHub events from [GH Archive](https://www.gharchive.org/),
organises them as a **bronze → silver → gold** medallion lakehouse, computes per-repo
health metrics (commit cadence, bus factor, PR/issue responsiveness, release cadence,
supply-chain risk), attaches an unsupervised **ML risk score**, and serves the result
through a REST API and a live dashboard.

All data is **real** — there is no synthetic/streaming demo path. GH Archive is an
hourly batch product, so an in-container scheduler drives the pipeline (no message
broker).

## Two ways to use it

1. **The full platform (recommended)** — a Dockerised medallion lakehouse + serving
   stack, run with one `docker compose up`. This is the project.
2. **The standalone ingestion script** — [`GHArchiveDownload.py`](GHArchiveDownload.py)
   downloads GH Archive and converts it to typed Parquet (the same bronze→silver logic,
   without the platform). Handy for research/EDA on a laptop.

---

## Quickstart (the platform)

Prerequisites: Docker ≥ 24 and Docker Compose ≥ 2.20, ~6–8 GB RAM and ≥ 20 GB free disk.

```bash
cd Docker
cp .env.example .env          # defaults work out of the box
docker compose up --build -d
```

The stack is six containers: `minio`, `timescaledb`, `redis`, `pipeline`, `api`,
`dashboard`. The pipeline runs one cycle immediately, then on schedule.

| Interface             | URL                          |
| --------------------- | ---------------------------- |
| Dashboard (Streamlit) | `http://localhost:8501`      |
| API docs (FastAPI)    | `http://localhost:8080/docs` |
| MinIO console         | `http://localhost:9001`      |
| RedisInsight          | `http://localhost:8001`      |

> GH Archive publishes each hour on a ~1–2 h delay, so the first live cycle is a
> couple of hours old. To populate history immediately, run a [backfill](#backfilling-history).

**Deeper docs:** [`Docker/Docker.md`](Docker/Docker.md) (container/ops reference),
[`Docker/MEDALLION.md`](Docker/MEDALLION.md) (architecture + reasoning),
[`Docker/services/pipeline/README.md`](Docker/services/pipeline/README.md) (pipeline internals).

---

## Architecture in brief

| Layer  | Backend                   | Role                                               |
| ------ | ------------------------- | -------------------------------------------------- |
| Bronze | MinIO (object store)      | raw, immutable GH Archive + enrichment JSON        |
| Silver | Parquet on MinIO + DuckDB | cleaned, typed, deduplicated event table           |
| Gold   | TimescaleDB + Redis       | per-repo metrics, ML risk score, served to the API |

The `pipeline` container orchestrates everything on an in-process scheduler (UTC):

| When                  | Job              | What it does                                                            |
| --------------------- | ---------------- | ---------------------------------------------------------------------- |
| Hourly at `:15`       | `hourly_job`     | latest GH Archive hour → bronze → silver → gold (metrics + ML risk)    |
| Daily at `03:30`      | `enrichment_job` | dependency-risk enrichment (GitHub SBOM → deps.dev → OSV)              |
| Daily at `04:45`      | `retention_job`  | prune aged bronze/silver in MinIO so 24/7 operation doesn't fill disk  |
| On startup (optional) | `backfill`       | replay a fixed historical hour range once                              |

---

## Configuration

Everything is configured via `Docker/.env` (copied from `.env.example`). The full
table lives in [`Docker/Docker.md`](Docker/Docker.md#environment-variables); the most
relevant settings:

- `GITHUB_TOKEN` — **optional**, only for the daily dependency-risk enrichment
  (GitHub SBOM API). Hourly GH Archive ingestion needs no token.
- `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` — object-store credentials.
- `BACKFILL_START` / `BACKFILL_END` — optional one-shot history replay (`YYYY-MM-DD-H`).
- `BRONZE_RETENTION_DAYS` (30) / `SILVER_RETENTION_DAYS` (120) — see [Retention](#retention).
- `POSTGRES_*`, `REDIS_MAX_MEMORY` — database tuning.

### Backfilling history

To populate metrics with history (rolling windows up to 90 days look empty on a fresh
start), set a range and bring the stack up:

```dotenv
# Docker/.env
BACKFILL_START=2024-01-01-0
BACKFILL_END=2024-01-31-23
```

Each stage is idempotent (bronze/silver skip objects already in MinIO), so backfills
are resumable and safe to re-run.

### Retention

Gold self-retains (TimescaleDB 2-year policy + Redis LRU/7-day series), but MinIO would
grow forever under 24/7 ingestion. The daily retention job prunes it with **two
windows**:

- **Bronze** is a transient raw landing zone → pruned at `BRONZE_RETENTION_DAYS` (30).
- **Silver** is the history gold aggregates over rolling windows, so it **must stay
  ≥ 90 days** (the largest window) or metrics degrade at the edge → `SILVER_RETENTION_DAYS`
  (120). The pipeline warns if you set it below 90.

---

## Deploying 24/7 (free)

The stack is stateful and accumulates storage, so the realistic free options are:

- **Oracle Cloud Always Free** (Ampere A1: up to 4 OCPU / 24 GB RAM / 200 GB) — the best
  free cloud fit; runs the whole stack 24/7 (arm64 images, all supported).
- **A local always-on machine** (spare laptop, mini-PC, or Raspberry Pi 5 8 GB) with
  Docker; add a free Cloudflare Tunnel for a public URL. Lower `REDIS_MAX_MEMORY` to
  `1gb` on 8 GB hosts.

The retention job above is what makes either viable long-term.

---

## Standalone ingestion script

[`GHArchiveDownload.py`](GHArchiveDownload.py) downloads GH Archive month-by-month and
writes one typed Parquet per month — useful for offline analysis.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install duckdb requests
python3 GHArchiveDownload.py     # edit START_DATE / END_DATE near the top first
```

It creates `raw_json/` (raw `.json.gz`, emptied as each month finishes) and
`processed_parquet/` (the kept output). See `DependencyRisk.py` for the standalone
deps.dev/OSV supply-chain enrichment that joins on `repo_name`.

---

## Repository map

```
GHArchiveDownload.py     standalone GH Archive → typed Parquet (bronze/silver logic)
DependencyRisk.py        standalone supply-chain risk (deps.dev + OSV), keyed on repo_name
Docker/                  the full Dockerised platform
  docker-compose.yml     the 6-container stack
  MEDALLION.md           architecture + design reasoning
  Docker.md              container/operations reference
  init/timescale/        gold schema (auto-applied on first boot)
  services/pipeline/     the medallion orchestrator (bronze→silver→gold, ML, retention)
  services/api/          FastAPI serving layer
  services/dashboard/    Streamlit dashboard
ProjectPlan.md           research plan   ·   ProjectDiary.md  development notes
```
