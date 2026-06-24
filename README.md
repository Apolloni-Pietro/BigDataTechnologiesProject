# Big Data Technologies — OSS Health Monitor

A platform that continuously measures the **health of open-source software** on
GitHub. It ingests public GitHub events from [GH Archive](https://www.gharchive.org/),
organizes them as a **bronze → silver → gold** medallion lakehouse, computes per-repo
health metrics (commit cadence, bus factor, PR/issue responsiveness, release cadence,
maintenance gaps), attaches an unsupervised **ML risk score**, and serves the result
through a REST API and a live dashboard.

All data is **real** — there is no synthetic/streaming demo path. GH Archive is an
hourly batch product, so an in-container scheduler drives the pipeline. An MQTT broker
(Mosquitto) is included for real-time health-score alerts — see [MQTT alerts](#mqtt-alerts).

---

## Architecture in brief

| Layer  | Backend                   | Role                                               |
| ------ | ------------------------- | -------------------------------------------------- |
| Bronze | MinIO (object store)      | raw, immutable GH Archive `.json.gz`               |
| Silver | Parquet on MinIO + DuckDB | cleaned, typed, deduplicated event table           |
| Gold   | TimescaleDB + Redis       | per-repo metrics, ML risk score, served to the API |

The `pipeline` container orchestrates everything on an in-process scheduler (UTC):

| When                  | Job             | What it does                                                           |
| --------------------- | --------------- | ---------------------------------------------------------------------- |
| Hourly at `:15`       | `hourly_job`    | latest GH Archive hour → bronze → silver → gold (metrics + ML risk)    |
| Daily at `04:45`      | `retention_job` | prune aged bronze/silver in MinIO so 24/7 operation doesn't fill disk  |
| On startup (optional) | `backfill`      | replay a fixed historical hour range, or ingest pre-downloaded Parquet |

---

## Getting Started (recommended path)

The stack is **most useful with historical data** — rolling-window metrics (30/90 days)
are nearly empty on a fresh start and the dashboard looks sparse. The recommended setup
pre-loads a month (or more) of history using pre-downloaded Parquet files before the
live pipeline takes over.

**Prerequisites:** Docker ≥ 24 and Docker Compose ≥ 2.20, ~8 GB RAM and ≥ 30 GB free disk.

---

### Step 1 — Download history with `GHArchiveDownload.py`

The standalone script at the repo root downloads GH Archive data month-by-month and
writes one typed Parquet file per month. These files are what the platform ingests as
history.

```bash
# From the repo root
python3 -m venv .python_env && source .python_env/bin/activate
pip install duckdb requests

# Edit START_DATE / END_DATE / MAX_WORKERS near the top of the file, then:
python3 GHArchiveDownload.py
```

Output lands in `processed_parquet/gh_events_YYYY-MM.parquet` (one file per month).
Raw `.json.gz` files are cleaned up after each successful conversion.

> **Tip:** each month of GH Archive is roughly 13-15 GB of Parquet. Adjust `MAX_WORKERS` to your bandwidth (default 35).

---

### Step 2 — Configure the stack

```bash
cd Docker
cp .env.example .env
```

Open `.env` and set the backfill source to point at the Parquet files you just
generated. The simplest option (**Mode A — bind-mount**) requires no upload: Docker
mounts `../processed_parquet` read-only at `/backfill` inside the pipeline container.

```dotenv
# Docker/.env  — enable Mode A parquet backfill
BACKFILL_PARQUET_DIR=/backfill
```

To also chain a GH Archive hourly download after the Parquet phase (so history is
contiguous to the present), also set the seam date:

```dotenv
BACKFILL_PARQUET_DIR=/backfill
BACKFILL_START=2025-05-01-0    # hourly download picks up here; parquet covers everything before
```

All other defaults in `.env.example` work out of the box.

> **Alternative — Mode B (upload to MinIO):** if you want the Parquet files stored in
> MinIO (portable, no bind-mount dependency), see
> [`Docker/PARQUET_BACKFILL.md`](Docker/PARQUET_BACKFILL.md#mode-b--minio-upload).

---

### Step 3 — Start the stack

```bash
# From the Docker/ directory
docker compose down -v          # start clean (important on first run or after config changes)
docker compose up --build -d
```

The first build takes 3–5 minutes (base images + Python deps). Then watch the pipeline:

```bash
docker compose logs -f pipeline
```

You will see lines like:

```
parquet-backfill: ingesting gh_events_2025-04.parquet → silver …
parquet-backfill: ingesting gh_events_2025-05.parquet → silver …
gold: wrote 3847 repos → TimescaleDB + Redis
hourly_job: bronze → silver → gold complete
```

The backfill runs once at startup and is **idempotent** — safe to re-run. The
live hourly scheduler takes over automatically when it finishes.

---

### Step 4 — Explore the dashboard

| Interface             | URL                          |
| --------------------- | ---------------------------- |
| Dashboard (Streamlit) | `http://localhost:8501`      |
| API docs (FastAPI)    | `http://localhost:8080/docs` |
| MinIO console         | `http://localhost:9001`      |

MinIO console login: `minioadmin` / `minioadmin` (the defaults from `.env.example`).

> **GH Archive lag:** each hourly file is published ~1–2 h after the fact, so the
> latest live hour is always a couple of hours behind real time. This is normal.

---

## Quick start (cold, no history)

If you just want to try the stack without pre-downloading data, skip Steps 1–2 and
run directly:

```bash
cd Docker
cp .env.example .env
docker compose up --build -d
```

The pipeline will start processing the current hour immediately and populate data over
time. The dashboard will be sparse until enough hourly cycles accumulate.

---

## Configuration

### Environment Variables

Copy `.env.example` to `.env` and edit.

| Variable                  | Default                                                 | Description                                                                                                             |
| ------------------------- | ------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `MINIO_ENDPOINT`          | `minio:9000`                                            | Hostname and port of the MinIO S3-compatible service.                                                                   |
| `MINIO_ACCESS_KEY`        | `minioadmin`                                            | Root username / access key credentials for the MinIO object store.                                                      |
| `MINIO_SECRET_KEY`        | `minioadmin`                                            | Root password / secret key credentials for the MinIO object store.                                                      |
| `MINIO_SECURE`            | `false`                                                 | Boolean indicating whether to connect to MinIO via SSL/TLS (HTTPS).                                                     |
| `BRONZE_BUCKET`           | `bronze`                                                | Name of the MinIO bucket holding raw GH Archive JSON files.                                                             |
| `SILVER_BUCKET`           | `silver`                                                | Name of the MinIO bucket holding cleaned and typed Parquet partition files.                                             |
| `GOLD_BUCKET`             | `gold`                                                  | Name of the MinIO bucket storing gold layer snapshot outputs.                                                           |
| `POSTGRES_USER`           | `oss`                                                   | Username for connecting to the PostgreSQL/TimescaleDB database.                                                         |
| `POSTGRES_PASSWORD`       | `changeme`                                              | Password for connecting to the PostgreSQL/TimescaleDB database.                                                         |
| `POSTGRES_DB`             | `oss_health`                                            | Target database name in PostgreSQL/TimescaleDB.                                                                         |
| `POSTGRES_DSN`            | `postgresql://oss:changeme@timescaledb:5432/oss_health` | Fully qualified PostgreSQL connection DSN URL.                                                                          |
| `REDIS_URL`               | `redis://redis:6379`                                    | Connection string URL for the Redis server instance.                                                                    |
| `REDIS_MAX_MEMORY`        | `2gb`                                                   | RAM limit cap for the Redis server before evicting keys via LRU policies.                                               |
| `GHARCHIVE_BASE`          | `https://data.gharchive.org`                            | Base URL endpoint used to retrieve hourly JSON logs from GH Archive.                                                    |
| `PUBLISH_LAG_HOURS`       | `2`                                                     | Hour lag offset between real-time and the most recently published GH Archive hourly dump.                               |
| `REPLAY_OFFSET_YEARS`     | `0`                                                     | Time shift offset in years to process historical logs as if they were current events.                                   |
| `BACKFILL_START`          | _(empty)_                                               | Timestamp (format `YYYY-MM-DD-H`) to initiate one-shot historical hourly backfilling or Parquet transition seam.        |
| `BACKFILL_END`            | _(empty)_                                               | End hour boundary (format `YYYY-MM-DD-H`) up to which the one-shot download ingestion backfill will run.                |
| `BACKFILL_PARQUET_DIR`    | _(empty)_                                               | Container path holding monthly Parquet files for fast-path ingestion (Mode A).                                          |
| `BACKFILL_PARQUET_GLOB`   | `gh_events_*.parquet`                                   | Filename glob filters matching specific month files to process in Parquet backfill.                                     |
| `BACKFILL_PARQUET_BUCKET` | _(empty)_                                               | MinIO bucket name containing uploaded Parquet files to download/ingest directly from object storage (Mode B).           |
| `DUCKDB_MEMORY_LIMIT`     | `8GB`                                                   | Max memory limit allocated for DuckDB connections to prevent container OOMs during heavy aggregations.                  |
| `DUCKDB_TEMP_DIR`         | `/tmp/duckdb_spill`                                     | Temp spill directory on disk for DuckDB when operations exceed memory cap.                                              |
| `DUCKDB_THREADS`          | `4`                                                     | Max parallel CPU worker threads allocated for DuckDB computations.                                                      |
| `COMMIT_FREQ_WINDOW_DAYS` | `30`                                                    | Size in days of rolling window timeline used for measuring commit frequency metrics in the Gold layer.                  |
| `CONTRIBUTOR_WINDOW_DAYS` | `90`                                                    | Size in days of rolling query window used for counting active actors/contributors in the Gold layer.                    |
| `STALE_ISSUE_DAYS`        | `90`                                                    | Standard duration of inactivity in days used to label open issues as stale.                                             |
| `GOLD_MAX_REPOS`          | `5000`                                                  | Upper bound limit on top tracked repositories by actor volume scored in Gold layer database tables.                     |
| `GOLD_MIN_ACTORS`         | `3`                                                     | Minimum unique active human actors needed over the contributor window for a repository to be tracked in the Gold layer. |
| `BRONZE_RETENTION_DAYS`   | `30`                                                    | Lifespan duration threshold in days before deleting transient raw files from the bronze object store.                   |
| `SILVER_RETENTION_DAYS`   | `120`                                                   | Lifespan duration threshold in days before deleting processed partition Parquet files from the silver store.            |
| `RETENTION_CRON_HOUR`     | `4`                                                     | Hour of day (UTC format) when the periodic retention cleanup task runs.                                                 |
| `MQTT_BROKER_HOST`        | `mqtt`                                                  | Hostname connection address for the Mosquitto MQTT message broker instance.                                             |
| `MQTT_BROKER_PORT`        | `1883`                                                  | TCP port configuration for target MQTT connection.                                                                      |
| `MQTT_ALERT_THRESHOLD`    | `0.35`                                                  | The health score threshold below which repository alerts are published to the MQTT broker alerts topic.                 |
| `API_URL`                 | `http://api:8080`                                       | FastAPI endpoint URL used by the Streamlit dashboard component.                                                         |
| `BUCKET`                  | `parquet-backfill`                                      | Target upload bucket name used inside the helper script for the parquet upload service.                                 |

---

### Replaying historical data (`REPLAY_OFFSET_YEARS`)

Set `REPLAY_OFFSET_YEARS=1` to shift the pipeline's entire notion of "now" back one year,
so it fetches and processes the feed from exactly a year ago and then runs "live" on
year-old events. A single shifted clock ([`clock.py`](Docker/services/pipeline/clock.py))
drives the scheduler, gold's rolling windows, and retention together — the data keeps its
true dates. Startup runs parquet bulk → hourly tail → live, all year-shifted. This is
necessary because **older GH Archive data carries full PushEvent commit arrays**, so the
commit-based metrics (bus factor, commit frequency, contributor count) populate —
newer data omit them.

### Retention

Gold self-retains (TimescaleDB 2-year policy + Redis LRU/7-day series), but MinIO would
grow forever under 24/7 ingestion. The daily retention job prunes it with **two windows**:

- **Bronze** is a transient raw landing zone → pruned at `BRONZE_RETENTION_DAYS` (30).
- **Silver** is the history gold aggregates over rolling windows, so it **must stay
  ≥ 90 days** (the largest window) or metrics degrade at the edge → `SILVER_RETENTION_DAYS`
  (120). The pipeline warns if you set it below 90.

---

## Standalone ingestion script

[`GHArchiveDownload.py`](GHArchiveDownload.py) downloads GH Archive month-by-month and
writes one typed Parquet per month — useful for offline analysis or to pre-populate the
platform (see [Getting Started](#getting-started-recommended-path)).

```bash
python3 -m venv .python_env && source .python_env/bin/activate
pip install duckdb requests
python3 GHArchiveDownload.py     # edit START_DATE / END_DATE near the top first
```

It creates `raw_json/` (raw `.json.gz`, emptied as each month finishes) and
`processed_parquet/` (the kept output, which the platform's `BACKFILL_PARQUET_DIR`
path can ingest directly).

---

## MQTT alerts

After each gold cycle the pipeline publishes an alert to the Mosquitto broker whenever a
repo's `health_score` **drops below a configurable threshold** for the first time.
Alerts are **edge-triggered**: they fire once when the score crosses the threshold, not
on every subsequent cycle where it stays low.

### Topic structure

```
repos/{owner}/{repo}/alerts
```

### Alert payload (JSON)

```json
{
  "repo": "owner/repo",
  "health_score": 0.31,
  "previous_health_score": 0.76,
  "threshold": 0.35,
  "ts": 1782228400
}
```

`previous_health_score` is `null` the first time a repo is scored (no prior data in
Redis). On all subsequent cycles it reflects the score from the previous gold run.

### Ports

| Port   | Protocol             |
| ------ | -------------------- |
| `1883` | MQTT (TCP)           |
| `9883` | MQTT over WebSockets |

Both are exposed on `localhost` when the stack is running.

### Configuration

Set in `Docker/.env`:

```dotenv
MQTT_ALERT_THRESHOLD=0.35   # alert when health_score drops below this (0.0–1.0)
```

### Subscribing to alerts

Any MQTT client works. With `mosquitto_sub`:

```bash
# All repos
mosquitto_sub -h localhost -p 1883 -t "repos/#" -v

# Single repo
mosquitto_sub -h localhost -p 1883 -t "repos/owner/repo/alerts" -v
```

### Live demo

A demo script injects a fake health-score drop and fires a real MQTT alert without
waiting for the next hourly gold cycle.

Open two terminals:

```bash
# Terminal 1 — subscribe
mosquitto_sub -h localhost -p 1883 -t "repos/#" -v

# Terminal 2 — fire a default alert (demo-org/demo-repo, score 0.85 → 0.25)
docker compose exec pipeline python demo_alert.py

# Custom repo and scores
docker compose exec pipeline python demo_alert.py \
  --repo torvalds/linux --score 0.31 --prev 0.76
```

Terminal 1 will immediately print:

```
repos/demo-org/demo-repo/alerts {"repo": "demo-org/demo-repo", "health_score": 0.25, "previous_health_score": 0.85, "threshold": 0.35, "ts": ...}
```

The script seeds Redis with the "previous" score, publishes the alert, then removes the
fake key — no side-effects on the running pipeline.

---

## Repository map

```
GHArchiveDownload.py     standalone GH Archive → typed monthly Parquet (bronze/silver logic)
Docker/                  the full Dockerized platform
  docker-compose.yml     the 7-container stack
  MEDALLION.md           architecture + design reasoning
  PARQUET_BACKFILL.md    fast history ingestion from pre-downloaded Parquet
  init/timescale/        gold schema (auto-applied on first boot)
  services/pipeline/     the medallion orchestrator (bronze→silver→gold, ML, retention)
  services/api/          FastAPI serving layer
  services/dashboard/    Streamlit dashboard
```
