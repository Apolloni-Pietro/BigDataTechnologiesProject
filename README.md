# Big Data Technologies Course - OSS Health Monitor Project

## Project Setup

### GH Archive Download

This script will download `JSON` files from [GH Archive](https://www.gharchive.org/).

Create a Python environment:

```
# Using venv
python3 -m venv .env
source .env/bin/activate
```

Install required dependencies:

```
pip install duckdb requests
```

Run the code:

```
python3 GHArchiveDownload.py
```

The script will create two directories:

- `raw_json`, where it will store the raw `JSON` files downloaded from GH Archive
- `processed_parquet`, where it will store the processed Parquet files month-by-month

NOTE: the `raw_json` directory will be empty at the end of the execution.

## Medallion Architecture (Dockerized platform)

The full platform is organised as a **bronze → silver → gold** lakehouse and runs
under Docker Compose from the [`Docker/`](Docker/) directory.

- **Design & reasoning:** [`Docker/MEDALLION.md`](Docker/MEDALLION.md) — why MinIO for
  bronze, DuckDB for silver, TimescaleDB + Redis for gold, why the hourly GH Archive
  ingest is batch (not Kafka), and how the ML risk score is computed.
- **Pipeline how-to:** [`Docker/services/pipeline/README.md`](Docker/services/pipeline/README.md)
  — run, configure, backfill, and extend the bronze/silver/gold pipeline.
- **Container details:** [`Docker/Docker.md`](Docker/Docker.md).

### Quickstart

```
cd Docker
cp .env.example .env          # edit MinIO keys / GITHUB_TOKEN if desired
docker compose up --build     # default stack = medallion pipeline + serving
```

| Service | URL |
|---|---|
| Dashboard (Streamlit) | http://localhost:8501 |
| API (FastAPI docs)    | http://localhost:8080/docs |
| MinIO console         | http://localhost:9001 |

The optional real-time streaming path (Redpanda) is off by default; enable it with
`docker compose --profile streaming up`.

| Layer | Backend | Role |
|---|---|---|
| Bronze | MinIO (object store) | raw, immutable GH Archive + enrichment JSON |
| Silver | Parquet on MinIO + DuckDB | cleaned, typed, deduplicated event model |
| Gold | TimescaleDB + Redis | per-repo metrics, ML risk score, served to the API |
