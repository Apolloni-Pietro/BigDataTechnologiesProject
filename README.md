# Big Data Technologies Course - Project

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
