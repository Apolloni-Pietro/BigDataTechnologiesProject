import duckdb

con = duckdb.connect()

reduced = "data/check_output/reduced_sample.parquet"
full = "data/check_output/full_sample.parquet"

print("=== REDUCED PARQUET COLUMNS ===")
print(con.execute(f"""
DESCRIBE SELECT * FROM read_parquet('{reduced}')
""").fetchall())

print("\n=== FULLER PARQUET COLUMNS ===")
print(con.execute(f"""
DESCRIBE SELECT * FROM read_parquet('{full}')
""").fetchall())

print("\n=== SAMPLE PULL REQUEST EVENT FROM FULLER PARQUET ===")
rows = con.execute(f"""
SELECT
    id,
    event_type,
    contributor,
    repository,
    event_timestamp,
    payload
FROM read_parquet('{full}')
WHERE event_type = 'PullRequestEvent'
LIMIT 1
""").fetchall()

for row in rows:
    print(row)

print("\n=== SAMPLE RELEASE EVENT FROM FULLER PARQUET ===")
rows = con.execute(f"""
SELECT
    id,
    event_type,
    contributor,
    repository,
    event_timestamp,
    payload
FROM read_parquet('{full}')
WHERE event_type = 'ReleaseEvent'
LIMIT 1
""").fetchall()

for row in rows:
    print(row)