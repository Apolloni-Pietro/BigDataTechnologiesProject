# Parquet backfill — fast history from pre-downloaded monthly files

On a fresh stack the rolling-window metrics (30/90 days) are nearly empty and big
repos rarely show up, so the dashboard looks "strange". The cure is a **backfill**.
The default backfill (`BACKFILL_START`/`BACKFILL_END`) re-**downloads** every hour
from GH Archive — correct but slow and flaky over months.

**Parquet backfill** is the fast alternative: it ingests **pre-downloaded monthly
`.parquet` files** straight into the silver layer, skipping the download + JSON-parse
work entirely. Expect roughly **6–10× faster** for a year-scale backfill.

Both modes can optionally chain a GH Archive hourly download after the parquet phase
using `BACKFILL_START` as the seam — giving you a complete history without redundancy.

---

## How it works

```
monthly gh_events_YYYY-MM.parquet  (local or uploaded to MinIO)
        │
        ▼
backfill_parquet.build_month()  ── DuckDB: re-project to the EXACT silver schema,
        │                                   partition by event_date
        ▼
SILVER (MinIO)  events/event_date=YYYY-MM-DD/…parquet
        │   normal silver → gold transform
        ▼
GOLD (TimescaleDB + Redis)  ──►  API ──► dashboard
```

- **Bronze is bypassed.** The monthly file is already typed/exploded, so there is no
  raw landing stage. Consequence: backfilled months have **no bronze**, so you cannot
  "replay silver from bronze" for them — keep the monthly Parquet files as the
  re-derivation source of truth. Live hours (the hourly scheduler) are unaffected.
- **Output is byte-compatible with `silver.build_hour`.** `gold` reads all silver as a
  single uniform dataset (`read_parquet('events/**/*.parquet', hive_partitioning=true)`),
  and the hourly scheduler keeps writing into the same `events/` prefix afterwards, so
  the re-projection in [`services/pipeline/backfill_parquet.py`](services/pipeline/backfill_parquet.py)
  must mirror [`silver.py`](services/pipeline/silver.py) exactly (same columns, struct
  field names/order/types; `event_date` lives only in the partition path, never as a
  stored column). **If you change silver's schema, change `backfill_parquet.py` in lockstep.**
  Note it partitions by **`event_date` only** (not hour): live silver encodes hour in
  the _filename_, which DuckDB doesn't treat as a hive key, so matching the key set
  (`event_date` alone) avoids a "Hive partition mismatch" error on gold's combined read.
- After all months are ingested, gold is built **once** over the busiest repos.

---

## Step 1 — generate the monthly Parquet files

Use the repo-root [`GHArchiveDownload.py`](../GHArchiveDownload.py) (the rich 13-type
schema — this is the schema the backfill reader expects). From the repo root:

```bash
python3 -m venv .python_env && source .python_env/bin/activate
pip install duckdb requests
# Edit START_DATE / END_DATE / MAX_WORKERS near the top of the file, then:
python3 GHArchiveDownload.py
```

This writes `processed_parquet/gh_events_YYYY-MM.parquet` (one file per month). It
downloads to `raw_json/` and deletes each month's raw files after a successful convert.

> ⚠️ The files **must** come from this rich-schema script. A simpler projection that
> omits the `payload_*` structs will produce empty/degenerate metrics.

---

## Step 2 — choose a source mode

### Mode A — bind-mount (files stay on your host machine)

The simplest option: Docker mounts `../processed_parquet` read-only at `/backfill`
inside the pipeline container. No upload step needed.

```bash
cd Docker

# Start clean so backfill output is the only silver data (recommended):
docker compose down -v

# Enable Mode A in .env:
echo "BACKFILL_PARQUET_DIR=/backfill" >> .env

# Optional: chain a GH Archive hourly download after the parquet phase.
# Parquet will stop at 2025-06-22; hourly will cover 2025-06-23 → now.
# echo "BACKFILL_START=2025-06-23-0" >> .env

docker compose up --build -d
docker compose logs -f pipeline      # watch "parquet-backfill" + "gold:" lines
```

The pipeline reads every `gh_events_*.parquet` from the bind-mounted directory,
builds silver, builds gold once, then continues with the normal hourly schedule.

---

### Mode B — MinIO-upload (files go into object storage first)

Upload the parquet files into a MinIO bucket **before** starting the full pipeline.
The pipeline reads them over S3 — no bind-mount is needed or used.

```bash
cd Docker

# Start clean:
docker compose down -v

# Set Mode B in .env:
echo "BACKFILL_PARQUET_BUCKET=parquet-backfill" >> .env

# Optional seam: parquet covers up to 2025-06-22; hourly starts here.
# echo "BACKFILL_START=2025-06-23-0" >> .env

# 1. Start MinIO alone:
docker compose up -d minio

# 2. Upload parquet files (adjust the host path as needed):
docker run --rm \
  --network docker_default \
  -v /path/to/processed_parquet:/data:ro \
  minio/mc:latest /bin/sh -c "
    mc alias set local http://minio:9000 minioadmin minioadmin &&
    mc mb --ignore-existing local/parquet-backfill &&
    mc cp /data/*.parquet local/parquet-backfill/
  "

# 3. Start everything:
docker compose up --build -d
docker compose logs -f pipeline
```

Alternatively, uncomment the `mc-upload` service in `docker-compose.yml` and run:
```bash
docker compose up -d minio
docker compose --profile upload run --rm mc-upload
docker compose up -d
```

---

## Choosing between Mode A and Mode B

| | Mode A (bind-mount) | Mode B (MinIO-upload) |
|---|---|---|
| **Extra steps** | None — files stay local | Upload to MinIO first |
| **Bind-mount required** | Yes (`../processed_parquet` must exist) | No |
| **Files accessible after stack restart** | Only if host dir still present | Yes — in MinIO named volume |
| **Best for** | Local dev / one-off backfills | Clean environments, shared setups |

---

## The seam: chaining parquet + hourly download

Set `BACKFILL_START` alongside either mode to split history into two phases:

```
parquet phase  → covers everything up to day_before(BACKFILL_START)
hourly phase   → covers BACKFILL_START to latest_available_hour()
scheduler      → takes over from there
```

Example for Mode B:
```dotenv
BACKFILL_PARQUET_BUCKET=parquet-backfill
BACKFILL_START=2025-06-23-0
```

The parquet files will stop at 2025-06-22 (inclusive); the pipeline then downloads
every hour from 2025-06-23-0 onward before the scheduler takes over. No overlap,
no gap, no double-counting in gold.

Without `BACKFILL_START`: the parquet phase ingests all available files and the
scheduler starts immediately (no hourly chain — original behaviour).

---

## Verify

- MinIO console (http://localhost:9001) → `silver/events/event_date=…/…parquet`.
- Dashboard (http://localhost:8501) → Overview sorted by **Importance** shows big repos
  with sensible (non-null) metrics and multi-day history.
- Schema parity (the key check): in the pipeline container, `DESCRIBE` a backfill file
  vs a live `silver.build_hour` file — columns/types/struct shapes must be identical.

---

## Configuration reference

| Variable                  | Default               | Meaning |
| ------------------------- | --------------------- | ------- |
| `BACKFILL_PARQUET_DIR`    | _(empty)_             | **Mode A**: container path holding the monthly files. Set to `/backfill`. |
| `BACKFILL_PARQUET_BUCKET` | _(empty)_             | **Mode B**: MinIO bucket name where files were uploaded. Takes precedence over Mode A. |
| `BACKFILL_PARQUET_GLOB`   | `gh_events_*.parquet` | Filename glob within the source (applied in both modes). |
| `BACKFILL_START`          | _(empty)_             | Seam hour (`YYYY-MM-DD-H`). When set alongside a parquet mode, caps parquet at `day_before(BACKFILL_START)` and chains an hourly download from here to now. |
| `BACKFILL_END`            | _(empty)_             | End hour for the hourly-only backfill (no parquet). Unused when a parquet mode is active. |

The bind-mount lives in [`docker-compose.yml`](docker-compose.yml) under the `pipeline`
service (`../processed_parquet:/backfill:ro`). It is harmless when Mode B is active or
when no parquet backfill is configured.
