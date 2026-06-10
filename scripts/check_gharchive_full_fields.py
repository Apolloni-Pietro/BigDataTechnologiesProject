import os
import requests
import duckdb

RAW_DIR = "data/raw_check"
OUT_DIR = "data/check_output"

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)

file_name = "2023-01-01-0.json.gz"
url = f"https://data.gharchive.org/{file_name}"
raw_path = os.path.join(RAW_DIR, file_name)

reduced_parquet = os.path.join(OUT_DIR, "reduced_sample.parquet")
full_parquet = os.path.join(OUT_DIR, "full_sample.parquet")

print("Downloading one GH Archive hourly file...")

if not os.path.exists(raw_path):
    r = requests.get(url, stream=True)
    r.raise_for_status()

    with open(raw_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)

print(f"Downloaded: {raw_path}")

con = duckdb.connect()

print("\nCreating reduced parquet, similar to current conversion...")

con.execute(f"""
COPY (
    SELECT
        id,
        type AS event_type,
        actor.login AS contributor,
        repo.name AS repository,
        created_at::TIMESTAMP AS event_timestamp
    FROM read_json(
        '{raw_path}',
        format='newline_delimited',
        columns={{
            'id': 'VARCHAR',
            'type': 'VARCHAR',
            'actor': 'STRUCT(login VARCHAR)',
            'repo': 'STRUCT(name VARCHAR)',
            'created_at': 'VARCHAR'
        }}
    )
    WHERE type IN ('PushEvent', 'PullRequestEvent', 'IssuesEvent', 'IssueCommentEvent')
) TO '{reduced_parquet}' (FORMAT PARQUET);
""")

print("\nCreating fuller parquet with payload included...")

con.execute(f"""
COPY (
    SELECT
        id,
        type AS event_type,
        actor.login AS contributor,
        repo.name AS repository,
        created_at::TIMESTAMP AS event_timestamp,
        payload
    FROM read_json(
        '{raw_path}',
        format='newline_delimited'
    )
    WHERE type IN ('PushEvent', 'PullRequestEvent', 'IssuesEvent', 'IssueCommentEvent', 'ReleaseEvent')
) TO '{full_parquet}' (FORMAT PARQUET);
""")

print("\nReduced parquet columns:")
print(con.execute(f"DESCRIBE SELECT * FROM read_parquet('{reduced_parquet}')").fetchall())

print("\nFuller parquet columns:")
print(con.execute(f"DESCRIBE SELECT * FROM read_parquet('{full_parquet}')").fetchall())

print("\nExample PullRequestEvent payload:")
rows = con.execute(f"""
SELECT payload
FROM read_parquet('{full_parquet}')
WHERE event_type = 'PullRequestEvent'
LIMIT 1
""").fetchall()

for row in rows:
    print(row[0])

print("\nDone.")