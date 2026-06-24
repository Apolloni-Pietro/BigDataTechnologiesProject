# Big Data Technologies — OSS Health Monitor

A platform that continuously measures the **health of open-source software** on
GitHub. It ingests public GitHub events from [GH Archive](https://www.gharchive.org/),
organises them as a **bronze → silver → gold** medallion lakehouse, computes per-repo
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

| When                  | Job             | What it does                                                         |
| --------------------- | --------------- | -------------------------------------------------------------------- |
| Hourly at `:15`       | `hourly_job`    | latest GH Archive hour → bronze → silver → gold (metrics + ML risk)  |
| Daily at `04:45`      | `retention_job` | prune aged bronze/silver in MinIO so 24/7 operation doesn't fill disk |
| On startup (optional) | `backfill`      | replay a fixed historical hour range, or ingest pre-downloaded Parquet |

**Deeper docs:** [`Docker/Docker.md`](Docker/Docker.md) (container/ops reference),
[`Docker/MEDALLION.md`](Docker/MEDALLION.md) (architecture + reasoning),
[`Docker/services/pipeline/README.md`](Docker/services/pipeline/README.md) (pipeline internals).

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

> **Tip:** each month of GH Archive is roughly 2–4 GB of Parquet. One month is enough
> to get meaningful metrics. Adjust `MAX_WORKERS` to your bandwidth (default 35).

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
| RedisInsight          | `http://localhost:8001`      |

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

Everything is configured via `Docker/.env` (copied from `.env.example`). The full
table lives in [`Docker/Docker.md`](Docker/Docker.md#environment-variables); the most
relevant settings:

- `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` — object-store credentials.
- `BACKFILL_PARQUET_DIR` — set to `/backfill` to activate Mode A Parquet backfill.
- `BACKFILL_START` / `BACKFILL_END` — optional one-shot GH Archive hour-range replay (`YYYY-MM-DD-H`).
- `REPLAY_OFFSET_YEARS` — process the feed from N years ago (0 = off; see below).
- `BRONZE_RETENTION_DAYS` (30) / `SILVER_RETENTION_DAYS` (120) — see [Retention](#retention).
- `POSTGRES_*`, `REDIS_MAX_MEMORY` — database tuning.

### Replaying historical data (`REPLAY_OFFSET_YEARS`)

Set `REPLAY_OFFSET_YEARS=1` to shift the pipeline's entire notion of "now" back one year,
so it fetches and processes the feed from exactly a year ago and then runs "live" on
year-old events. A single shifted clock ([`clock.py`](Docker/services/pipeline/clock.py))
drives the scheduler, gold's rolling windows, and retention together — the data keeps its
true dates. Startup runs parquet bulk → hourly tail → live, all year-shifted. This is
useful because **older GH Archive data carries full PushEvent commit arrays**, so the
commit-based metrics (bus factor, commit frequency, contributor count) populate — some
newer data omits them.

### Retention

Gold self-retains (TimescaleDB 2-year policy + Redis LRU/7-day series), but MinIO would
grow forever under 24/7 ingestion. The daily retention job prunes it with **two windows**:

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
  "threshold": 0.4,
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
MQTT_ALERT_THRESHOLD=0.4   # alert when health_score drops below this (0.0–1.0)
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
repos/demo-org/demo-repo/alerts {"repo": "demo-org/demo-repo", "health_score": 0.25, "previous_health_score": 0.85, "threshold": 0.4, "ts": ...}
```

The script seeds Redis with the "previous" score, publishes the alert, then removes the
fake key — no side-effects on the running pipeline.

---

## Repository map

```
GHArchiveDownload.py     standalone GH Archive → typed monthly Parquet (bronze/silver logic)
Docker/                  the full Dockerised platform
  docker-compose.yml     the 7-container stack
  MEDALLION.md           architecture + design reasoning
  Docker.md              container/operations reference
  PARQUET_BACKFILL.md    fast history ingestion from pre-downloaded Parquet
  init/timescale/        gold schema (auto-applied on first boot)
  services/pipeline/     the medallion orchestrator (bronze→silver→gold, ML, retention)
  services/api/          FastAPI serving layer
  services/dashboard/    Streamlit dashboard
```
