# Big Data Technologies Course - OSS Health Monitor Project

Docker implementation for the entire project.

---

## Table of Contents

- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Environment Variables](#environment-variables)
- [Services](#services)
- [Common Operations](#common-operations)
- [Accessing the UIs](#accessing-the-uis)
- [Data Flow](#data-flow)
- [Project Structure](#project-structure)
- [Troubleshooting](#troubleshooting)

---

## How it works

The platform ingests public GitHub events from [GH Archive](https://www.gharchive.org/) (a free archive of every public GitHub event since 2011) and supplements this with live data from the GitHub REST API. Events flow through a message broker (Redpanda) into a consumer that computes health metrics and stores them in two complementary databases: Redis for real-time access, and TimescaleDB for long-term historical analysis.

A FastAPI layer sits in front of both databases and exposes a unified REST API. A Streamlit dashboard reads from that API to present live charts, trend analysis, and risk alerts.

---

## Architecture

```

┌────────────────────────────────────────────────────────────────────────┐
│                         Docker Compose Network                         │
│                                                                        │
│  ── Infrastructure ──────────────────────────────────────────────────  │
│                                                                        │
│  ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐    │
│  │     Redpanda     │   │   TimescaleDB    │   │   Redis Stack    │    │
│  │  Kafka-compat    │   │  PostgreSQL +    │   │  TimeSeries +    │    │
│  │  broker          │   │  time-series     │   │  pub/sub         │    │
│  │  :9092  :9644    │   │  :5432           │   │  :6379  :8001    │    │
│  └──────────────────┘   └──────────────────┘   └──────────────────┘    │
│         ▲      │                 ▲                       ▲             │
│         │      │                 │                       │             │
│  ── Workers ───┼─────────────────┼───────────────────────┼───────────  │
│         │      │                 │                       │             │
│  ┌──────┴──────▼───────┐   ┌─────┴───────────────────────┴────────┐    │
│  │   Ingestion Worker  │   │           Consumer Worker            │    │
│  │  GH Archive + API   │   │  Redpanda → metrics → dual-write     │    │
│  │  → Redpanda producer│   │  to Redis (hot) + TimescaleDB (cold) │    │
│  └─────────────────────┘   └──────────────────────┬───────────────┘    │
│                                                   │                    │
│  ── Serving ──────────────────────────────────────┼──────────────────  │
│                                                   ▼                    │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │                         FastAPI  :8080                         │    │
│  │       REST API — routes queries to Redis or TimescaleDB        │    │
│  └────────────────────────────────┬───────────────────────────────┘    │
│                                   │                                    │
│                                   ▼                                    │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │                        Streamlit  :8501                        │    │
│  │            Dashboard — health charts, trend analysis           │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                                                                        │
│  Named volumes: redpanda-data  timescale-data  redis-data              │
│  Bind mount:    ./data/parquet  (Parquet files persist on host)        │
└────────────────────────────────────────────────────────────────────────┘
```

### Storage tiers

The platform uses a **hot/cold** two-tier storage strategy.

| Tier | Store                    | Retention | Latency | Purpose                                                  |
| ---- | ------------------------ | --------- | ------- | -------------------------------------------------------- |
| Hot  | Redis Stack (TimeSeries) | 7 days    | < 1 ms  | Live dashboard, real-time alerting, current scores       |
| Cold | TimescaleDB              | 2 years   | 5–50 ms | Historical trends, ML feature computation, SQL analytics |

Every metric is written to **both** stores simultaneously by the consumer worker. The FastAPI layer automatically routes queries to the appropriate tier based on the requested time window: ≤ 7 days goes to Redis, anything longer goes to TimescaleDB.

---

## Prerequisites

- **Docker** ≥ 24.0 — [Install Docker](https://docs.docker.com/get-docker/)
- **Docker Compose** ≥ 2.20 (included in Docker Desktop; verify with `docker compose version`)
- **Git**
- **A GitHub personal access token** — needed by the ingestion worker to call the GitHub REST API without hitting the 60 req/hr unauthenticated rate limit. A token bumps this to 5,000 req/hr.
  - Generate one at: GitHub → Settings → Developer settings → Personal access tokens → Tokens (classic)
  - Required scopes: `public_repo` (read-only access to public repositories)
- **Disk space**: at least 20 GB free for a first run (Parquet data, Docker images, database volumes)
- **RAM**: at least 6 GB available to Docker (8 GB recommended)

---

## Quick Start

Five steps from a fresh clone to a running platform.

**1. Clone the repository**

```bash
git clone https://github.com/your-org/oss-health-monitor.git
cd oss-health-monitor
```

**2. Create your environment file**

```bash
cp .env.example .env
```

Open `.env` and fill in your GitHub token (the only required change):

```bash
GITHUB_TOKEN=ghp_your_token_here
```

**3. Create the local data directory**

```bash
mkdir -p data/parquet
```

**4. Build images and start all containers**

```bash
docker compose up --build -d
```

The first build downloads base images and installs Python dependencies. This takes 3–5 minutes. Subsequent starts take a few seconds.

**5. Verify everything is healthy**

```bash
docker compose ps
```

All containers should show `healthy` or `running`. If any show `starting`, wait 30 seconds and run the command again — TimescaleDB needs a moment to initialise its schema on the first boot.

You should now be able to open:

| Interface           | URL                        | What it is                                               |
| ------------------- | -------------------------- | -------------------------------------------------------- |
| Streamlit dashboard | http://localhost:8501      | Main user-facing dashboard                               |
| FastAPI docs        | http://localhost:8080/docs | Interactive REST API explorer                            |
| Redpanda Console    | http://localhost:9644      | Browse topics, inspect events, monitor consumer lag      |
| RedisInsight        | http://localhost:8001      | Browse Redis keys, run commands, inspect TimeSeries data |

---

## Environment Variables

Copy `.env.example` to `.env` and edit the values. Never commit `.env` to version control — it is listed in `.gitignore`.

| Variable               | Required | Default      | Description                                                                                                                     |
| ---------------------- | -------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------- |
| `GITHUB_TOKEN`         | **Yes**  | —            | GitHub personal access token. Increases API rate limit from 60 to 5,000 req/hr.                                                 |
| `INGESTION_START_DATE` | No       | `2023-01-01` | Earliest date to backfill from GH Archive. Format: `YYYY-MM-DD`. Use a recent date to keep the initial data volume manageable.  |
| `POSTGRES_USER`        | No       | `oss`        | TimescaleDB username.                                                                                                           |
| `POSTGRES_PASSWORD`    | No       | `changeme`   | TimescaleDB password. Change this if the port is exposed to a network.                                                          |
| `POSTGRES_DB`          | No       | `oss_health` | TimescaleDB database name.                                                                                                      |
| `REDIS_MAX_MEMORY`     | No       | `2gb`        | Maximum RAM Redis may use before evicting old keys (LRU policy). Reduce if your machine has less than 8 GB available to Docker. |

**`.env.example`**

```dotenv
# Required
GITHUB_TOKEN=

# Ingestion
INGESTION_START_DATE=2023-01-01

# TimescaleDB
POSTGRES_USER=oss
POSTGRES_PASSWORD=changeme
POSTGRES_DB=oss_health

# Redis
REDIS_MAX_MEMORY=2gb
```

---

## Services

### Redpanda

Redpanda is a Kafka-compatible message broker that acts as the central nervous system of the platform. Every GitHub event enters the system here. The ingestion worker appends events as they arrive; the consumer worker reads from its current offset at its own pace. If the consumer crashes, it resumes from exactly where it left off — no events are lost or duplicated.

**Topics created on startup:**

| Topic                 | Partitions | Retention | Key         | Purpose                                  |
| --------------------- | ---------- | --------- | ----------- | ---------------------------------------- |
| `gh-events`           | 6          | 7 days    | `repo_name` | Raw GitHub events                        |
| `repo-health-metrics` | 6          | 30 days   | `repo_name` | Computed metric snapshots                |
| `repo-latest-state`   | 6          | Compacted | `repo_name` | Latest metadata per repo (log-compacted) |
| `health-alerts`       | 3          | 90 days   | `repo_name` | Health score drop notifications          |

Partitioning by `repo_name` ensures all events for a given repository land in the same partition, preserving per-repo ordering.

**Ports:** `9092` (Kafka protocol), `9644` (Admin API + Redpanda Console UI)

---

### TimescaleDB

TimescaleDB is PostgreSQL with a time-series extension. It stores all computed health metrics going back up to 2 years. Data is automatically organised into time-based chunks on disk, which makes range queries (e.g., "last 90 days for 10,000 repos") fast without manual indexing work.

Key features enabled in the schema:

- **Hypertable** on `repo_health_metrics` — automatic time-based chunk management
- **Compression policy** — metrics older than 7 days are compressed (typically 90%+ reduction)
- **Retention policy** — data older than 2 years is automatically dropped
- **Continuous aggregate** `repo_health_daily` — a materialised daily rollup that updates incrementally every hour

The schema is initialised automatically from `./init/timescale/01_schema.sql` the first time the container starts with an empty volume.

**Port:** `5432`

---

### Redis Stack

Redis Stack extends standard Redis with several modules. This project uses **RedisTimeSeries** for storing recent metric history and **pub/sub** for real-time alerting.

RedisTimeSeries stores the last 7 days of every metric for every tracked repository. Each metric lives in a key formatted as `ts:{repo_name}:{metric_name}`. A plain Redis hash at `latest:{repo_name}` holds only the most recent values for each metric, making current-value lookups O(1).

When the consumer detects a health score drop, it publishes to the `alerts` Redis channel. Any subscriber (alerting service, Slack notifier, webhook handler) receives it immediately without polling.

The **RedisInsight** web interface is available at port `8001` for browsing keys and running commands.

**Ports:** `6379` (Redis protocol), `8001` (RedisInsight UI)

---

### Ingestion Worker

A continuously running Python service responsible for one task: getting data from the internet into Redpanda.

It runs two concurrent loops:

- **GH Archive loop** — downloads hourly `.json.gz` event files from `gharchive.org`, parses each JSON line, and produces events to the `gh-events` Redpanda topic. Each event is keyed by repository name. After producing, each hourly file is converted to Parquet format and saved to `./data/parquet/` for long-term local storage, then the raw `.json.gz` is discarded.

- **GitHub API loop** — periodically fetches repository metadata (topics, language, star count, open issue count, branch protection status) using the GitHub REST API. Uses conditional `If-Modified-Since` requests to avoid burning rate limit on unchanged repos.

Rate limiting is handled with exponential backoff via the `tenacity` library. The worker respects `X-RateLimit-Remaining` headers and sleeps accordingly before each request.

---

### Consumer Worker

A continuously running Python service responsible for turning raw events into stored metrics.

For each event it reads from Redpanda, it identifies the repository and updates in-memory rolling statistics: commit counters, contributor sets, PR open/close timestamps, issue response times, and release dates. Periodically it flushes these statistics as computed health metrics, writing to both Redis and TimescaleDB in a single dual-write operation.

Health metrics computed per repository:

| Metric                    | Description                                                                |
| ------------------------- | -------------------------------------------------------------------------- |
| `commit_freq_30d`         | Average daily commits over the last 30 days                                |
| `bus_factor`              | Minimum number of contributors who collectively own > 80% of commits       |
| `pr_latency_p50`          | Median hours from PR opened to first maintainer response                   |
| `pr_abandon_rate`         | Fraction of PRs closed without merging                                     |
| `stale_issue_ratio`       | Fraction of open issues with no activity in > 90 days                      |
| `days_since_last_release` | Days elapsed since the most recent tagged release                          |
| `outdated_dep_ratio`      | Fraction of declared dependencies > 2 major versions behind (via deps.dev) |
| `health_score`            | Composite weighted score in [0, 1]. Values below 0.35 trigger an alert.    |

When `health_score` drops below the alert threshold, the worker publishes to the Redis `alerts` channel.

---

### FastAPI

The REST API layer that all external clients talk to. Streamlit, any future frontend, CLI tools, and webhooks all go through here — never directly to the databases.

FastAPI is responsible for routing queries to the correct storage tier:

- Requests for recent data (≤ 7 days) are served from **Redis** — sub-millisecond response.
- Requests for historical data (> 7 days) are served from **TimescaleDB** — handles any time range.
- Current-value lookups (`/health/current`) always hit the Redis `latest:{repo}` hash.

Interactive API documentation (Swagger UI) is available at `http://localhost:8080/docs` while the container is running.

**Port:** `8080`

---

### Streamlit Dashboard

The user-facing web interface. Streamlit is a Python-native tool for building data applications — it renders sliders, charts, and tables from plain Python code, with no frontend development required.

The dashboard queries **FastAPI exclusively** — it has no database credentials and no direct connection to Redis or TimescaleDB. This separation means all data access logic, caching, and routing lives in one place. If the backend changes, only FastAPI needs to be updated.

**Port:** `8501`

---

## Common Operations

**Start all containers (detached)**

```bash
docker compose up -d
```

**Stop all containers (preserves data)**

```bash
docker compose down
```

**Full reset — delete all data and start fresh**

```bash
docker compose down -v
```

**View logs for a specific service**

```bash
docker compose logs -f consumer-worker
docker compose logs -f ingestion-worker
docker compose logs -f api
```

**Rebuild a single service after code changes**

```bash
docker compose up --build -d api
docker compose up --build -d consumer-worker
```

**Check container health and status**

```bash
docker compose ps
```

**Scale the consumer worker to 3 parallel instances**

```bash
docker compose up -d --scale consumer-worker=3
```

Redpanda will automatically distribute topic partitions across all three instances. Useful when consumer lag is growing faster than a single instance can process.

**Check consumer lag (how far behind the consumer is)**

```bash
docker compose exec redpanda rpk group describe health-metric-worker
```

**Connect to TimescaleDB with psql**

```bash
docker compose exec timescaledb psql -U oss -d oss_health
```

**Run a Redis command**

```bash
docker compose exec redis redis-cli TS.RANGE ts:redis/redis:health_score - +
```

---

## Accessing the UIs

All interfaces are accessible from your browser once the containers are running.

**Streamlit dashboard** — `http://localhost:8501`
The main interface for browsing health scores, trends, and alerts.

**FastAPI interactive docs** — `http://localhost:8080/docs`
Swagger UI for exploring and testing every API endpoint directly in the browser.

**Redpanda Console** — `http://localhost:9644`
Browse topics, inspect individual messages, view consumer group lag, and monitor throughput. The "Consumer groups" section is the most useful for diagnosing processing slowdowns.

**RedisInsight** — `http://localhost:8001`
Browse all Redis keys, run commands, and inspect TimeSeries data visually. Use the TimeSeries chart view to see raw metric history without writing any code.

---

## Data Flow

The complete journey of a single event through the system:

```
GH Archive (external)
  │  HTTP download — hourly .json.gz file
  ▼
Ingestion Worker
  │  produce(key=repo_name, value=event_json)
  ▼
Redpanda — topic: gh-events
  │  consumer reads at its own offset
  ▼
Consumer Worker — compute health metrics
  ├─── TS.MADD + HSET ──────────────────▶ Redis Stack (hot, 7 days)
  │                                              │
  └─── INSERT INTO repo_health_metrics ──▶ TimescaleDB (cold, 2 years)
                                                 │
                                    ┌────────────┴────────────┐
                                    │ FastAPI — routes by     │
                                    │ time window requested   │
                                    └────────────┬────────────┘
                                                 │  HTTP JSON
                                                 ▼
                                          Streamlit Dashboard
```

---

## Project Structure

```
oss-health-monitor/
├── docker-compose.yml           # Defines all services, networks, volumes
├── .env.example                 # Template — copy to .env and fill in values
├── .env                         # Your local secrets — never commit this
│
├── data/
│   └── parquet/                 # Bind-mounted — Parquet files persist here on the host
│
├── init/
│   └── timescale/
│       └── 01_schema.sql        # Auto-executed by TimescaleDB on first boot
│
└── services/
    ├── ingestion/
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── main.py              # GH Archive download loop + GitHub API crawler
    │
    ├── consumer/
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── main.py              # Redpanda consumer + metric computation + dual-write
    │
    ├── api/
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── main.py              # FastAPI endpoints — routes queries to Redis or TimescaleDB
    │
    └── dashboard/
        ├── Dockerfile
        ├── requirements.txt
        └── app.py               # Streamlit app — calls FastAPI, renders charts
```

---

## Troubleshooting

**Container stuck in `starting` or `unhealthy`**

TimescaleDB takes 20–30 seconds to initialise its schema on the very first boot. The containers that depend on it (`consumer-worker`, `api`) will wait for it automatically via the `condition: service_healthy` dependency. Give it a minute and run `docker compose ps` again.

**`ingestion-worker` exits immediately**

The most common cause is a missing or invalid `GITHUB_TOKEN` in your `.env`. Check with:

```bash
docker compose logs ingestion-worker
```

If you see a 401 or 403 error, regenerate your token and restart the container:

```bash
docker compose up -d ingestion-worker
```

**Consumer lag keeps growing**

The consumer is processing events more slowly than the ingestion worker produces them. Scale it up:

```bash
docker compose up -d --scale consumer-worker=3
```

You can also reduce the `INGESTION_START_DATE` in `.env` to a more recent date to limit the volume of historical backfill.

**Out of disk space**

Parquet files accumulate in `./data/parquet/`. To clear old data:

```bash
# Delete Parquet files older than 90 days
find ./data/parquet -name "*.parquet" -mtime +90 -delete
```

TimescaleDB manages its own disk usage via the 2-year retention policy set in the schema. Check current database size with:

```bash
docker compose exec timescaledb psql -U oss -d oss_health \
  -c "SELECT pg_size_pretty(pg_database_size('oss_health'));"
```

**Redis running out of memory**

Redis is configured with a 2 GB cap and an LRU eviction policy — when it reaches the limit, it automatically evicts the least recently used keys. If you are seeing important data being dropped, increase `REDIS_MAX_MEMORY` in `.env` and restart the Redis container:

```bash
docker compose up -d redis
```

Note that Redis only stores the last 7 days of data per series by design. This is intentional — older data lives in TimescaleDB.

**Can't connect to an API or UI in the browser**

Verify the container is running and healthy:

```bash
docker compose ps
```

If the container is healthy but the UI is unreachable, check whether another process on your machine is already using the port:

```bash
lsof -i :8501   # or 8080, 9644, 8001, etc.
```

Change the conflicting port in `docker-compose.yml` (the left-hand number in `"HOST:CONTAINER"` port mappings) and restart.

**Wiping a single service's data without a full reset**

```bash
# Remove only the Redis volume and restart
docker compose stop redis
docker volume rm oss-health-monitor_redis-data
docker compose up -d redis
```

Replace `redis-data` with `timescale-data` or `redpanda-data` as needed.
