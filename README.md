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
| Bronze | MinIO (object store)      | raw, immutable GH Archive `.json.gz`               |
| Silver | Parquet on MinIO + DuckDB | cleaned, typed, deduplicated event table           |
| Gold   | TimescaleDB + Redis       | per-repo metrics, ML risk score, served to the API |

The `pipeline` container orchestrates everything on an in-process scheduler (UTC):

| When                  | Job              | What it does                                                            |
| --------------------- | ---------------- | ---------------------------------------------------------------------- |
| Hourly at `:15`       | `hourly_job`     | latest GH Archive hour → bronze → silver → gold (metrics + ML risk)    |
| Daily at `04:45`      | `retention_job`  | prune aged bronze/silver in MinIO so 24/7 operation doesn't fill disk  |
| On startup (optional) | `backfill`       | replay a fixed historical hour range, or a year-shifted replay         |

---

## Configuration

Everything is configured via `Docker/.env` (copied from `.env.example`). The full
table lives in [`Docker/Docker.md`](Docker/Docker.md#environment-variables); the most
relevant settings:

- `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` — object-store credentials.
- `BACKFILL_START` / `BACKFILL_END` — optional one-shot history replay (`YYYY-MM-DD-H`).
- `REPLAY_OFFSET_YEARS` — process the feed from N years ago (0 = off; see below).
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

**Faster: backfill from pre-downloaded Parquet.** Re-downloading months of hourly data
is slow. If you already have monthly Parquet from the standalone
[`GHArchiveDownload.py`](GHArchiveDownload.py), set `BACKFILL_PARQUET_DIR=/backfill` (its
files are bind-mounted there) to ingest them straight into silver — ~6–10× faster, no
re-download. It takes precedence over `BACKFILL_START/END`; run it on a fresh silver
bucket. See [`Docker/PARQUET_BACKFILL.md`](Docker/PARQUET_BACKFILL.md).

### Replaying historical data (`REPLAY_OFFSET_YEARS`)

Set `REPLAY_OFFSET_YEARS=1` to shift the pipeline's entire notion of "now" back one year,
so it fetches and processes the feed from exactly a year ago and then runs "live" on
year-old events. A single shifted clock ([`clock.py`](Docker/services/pipeline/clock.py))
drives the scheduler, gold's rolling windows, and retention together — the data keeps its
true dates. Startup runs parquet bulk → hourly tail → live, all year-shifted. This is
useful because **older GH Archive data carries full PushEvent commit arrays**, so the
commit-based metrics (bus factor, commit frequency, contributor count) populate — some
newer/synthetic data omits them.

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

| Port   | Protocol           |
| ------ | ------------------ |
| `1883` | MQTT (TCP)         |
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
  docker-compose.yml     the 6-container stack
  MEDALLION.md           architecture + design reasoning
  Docker.md              container/operations reference
  init/timescale/        gold schema (auto-applied on first boot)
  services/pipeline/     the medallion orchestrator (bronze→silver→gold, ML, retention)
  services/api/          FastAPI serving layer
  services/dashboard/    Streamlit dashboard
ProjectPlan.md           research plan   ·   ProjectDiary.md  development notes
```
