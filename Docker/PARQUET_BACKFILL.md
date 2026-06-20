# Parquet backfill — fast history from pre-downloaded monthly files

On a fresh stack the rolling-window metrics (30/90 days) are nearly empty and big
repos rarely show up, so the dashboard looks "strange". The cure is a **backfill**.
The default backfill (`BACKFILL_START`/`BACKFILL_END`) re-**downloads** every hour
from GH Archive — correct but slow and flaky over months.

**Parquet backfill** is the fast alternative: it ingests **pre-downloaded monthly
`.parquet` files** straight into the silver layer, skipping the download + JSON-parse
work entirely. Expect roughly **6–10× faster** for a year-scale backfill.

---

## How it works

```
processed_parquet/gh_events_YYYY-MM.parquet   (host, produced by GHArchiveDownload.py)
        │   bind-mounted read-only into the pipeline container at /backfill
        ▼
backfill_parquet.build_month()   ── DuckDB: re-project to the EXACT silver schema,
        │                             partition by event_date
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
  the *filename*, which DuckDB doesn't treat as a hive key, so matching the key set
  (`event_date` alone) avoids a "Hive partition mismatch" error on gold's combined read.
- After all months are ingested, gold is built **once** and enrichment runs over the
  busiest repos (mirroring the default backfill's tail).

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

## Step 2 — run the backfill

From `Docker/`:

```bash
# Start clean so backfill output is the only silver data (recommended):
docker compose down -v

# Enable parquet backfill in .env:
echo "BACKFILL_PARQUET_DIR=/backfill" >> .env

docker compose up --build -d
docker compose logs -f pipeline      # watch "parquet-backfill" + "gold:" lines
```

The pipeline bind-mounts `../processed_parquet` → `/backfill` (read-only), ingests every
`gh_events_*.parquet`, builds gold, then continues with the normal hourly schedule.
`BACKFILL_PARQUET_DIR` takes **precedence** over `BACKFILL_START`/`BACKFILL_END`.

### Verify
- MinIO console (http://localhost:9001) → `silver/events/event_date=…/…parquet`.
- Dashboard (http://localhost:8501) → Overview sorted by **Importance** shows big repos
  with sensible (non-null) metrics and multi-day history.
- Schema parity (the key check): in the pipeline container, `DESCRIBE` a backfill file
  vs a live `silver.build_hour` file — columns/types/struct shapes must be identical.

---

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `BACKFILL_PARQUET_DIR` | _(empty)_ | Container path holding the monthly files. Set to `/backfill` to enable. Empty = disabled. |
| `BACKFILL_PARQUET_GLOB` | `gh_events_*.parquet` | Filename glob within that directory. |

The bind mount lives in [`docker-compose.yml`](docker-compose.yml) under the `pipeline`
service (`../processed_parquet:/backfill:ro`). It is harmless when the feature is off.

---

## Alternative: pre-upload to MinIO instead of bind-mounting

The bind-mount path above keeps the monthly files on the host. For a more cloud-native
setup (e.g. running the stack on a remote VM where the files live in object storage,
or to avoid a host bind mount entirely) you can instead **upload the monthly Parquet to
MinIO** and have the backfill read them over S3. This is **not implemented** today; the
following is a complete design so it can be added without re-deriving anything.

**Why it's cheap to add:** `storage.duckdb_con()` is already wired for S3/httpfs against
MinIO, and `backfill_parquet.build_month()` already takes a single path argument and
passes it to `read_parquet(...)`. DuckDB's `read_parquet` accepts an `s3://` URI
transparently, so the projection query needs **no change** — only the source path and
the file-discovery step change.

**Changes required:**

1. **A new bucket/prefix** for the raw monthly files, e.g. `backfill/gh_events_*.parquet`
   (either a new bucket `backfill` added to `storage.ensure_buckets()`, or a `backfill/`
   prefix inside the existing `bronze` bucket). Keep it separate from `silver/events/`.

2. **Upload step (operator action).** Either:
   - `mc cp processed_parquet/*.parquet myminio/backfill/` with the MinIO client, or
   - a tiny helper using the existing `storage.minio_client()` /
     `fput_object` to push each local file. (One-time, run from the host or a job.)

3. **Config:** add `BACKFILL_PARQUET_S3 = os.getenv("BACKFILL_PARQUET_S3", "")` (e.g.
   `s3://backfill/`) alongside `BACKFILL_PARQUET_DIR`. Decide precedence: if the S3 var
   is set, use S3 discovery; else fall back to the local-dir discovery.

4. **Discovery over S3** in `pipeline.run_parquet_backfill()`: instead of
   `glob.glob(local_pattern)`, list objects via
   `storage.minio_client().list_objects(bucket, prefix="...", recursive=True)`, filter by
   the `gh_events_*.parquet` suffix, and build `s3://bucket/key` URIs. Sort them. Pass
   each URI straight to `backfill_parquet.build_month()` — the DuckDB query is unchanged
   because it already reads whatever path it's handed.

5. **Compose:** drop the `../processed_parquet:/backfill:ro` bind mount; nothing else in
   the service definition changes (the pipeline already has MinIO credentials/endpoint).

**Trade-offs vs. bind-mount:**

| | Bind-mount (implemented) | MinIO upload (this design) |
| --- | --- | --- |
| Setup | Zero — files just sit on the host | Extra one-time upload step |
| Storage | Single copy on host | Duplicated into MinIO (until deleted) |
| Remote/cloud hosts | Awkward (need files on the VM's disk) | Natural (object storage is the transport) |
| Read speed | Local disk (fastest) | Network to MinIO (still fast, same box) |
| Code touched | none | config + discovery (~30 lines), no query change |

For local/laptop demos the bind-mount is simpler and faster. The MinIO path is worth it
only when the stack runs somewhere the host filesystem isn't a convenient place for
multi-GB inputs.
