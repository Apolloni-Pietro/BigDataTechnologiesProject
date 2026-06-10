import polars as pl

path = "data/gh_events_2023-01.parquet"

df = pl.scan_parquet(path)

print("=== SCHEMA ===")
print(df.collect_schema())

print("\n=== FIRST 5 ROWS ===")
print(df.head(20).collect())

print("\n=== TOTAL ROW COUNT ===")
print(
    df.select(
        pl.len().alias("row_count")
    ).collect()
)

# print("\n=== EVENT TYPE DISTRIBUTION ===")
# print(
#     df.group_by("event_type")
#       .len()
#       .sort("len", descending=True)
#       .collect()
# )

# print("\n=== TOP 20 REPOSITORIES BY EVENT COUNT ===")
# print(
#     df.group_by("repository")
#       .len()
#       .sort("len", descending=True)
#       .head(20)
#       .collect()
# )

# print("\n=== TOP 20 CONTRIBUTORS BY EVENT COUNT ===")
# print(
#     df.group_by("contributor")
#       .len()
#       .sort("len", descending=True)
#       .head(20)
#       .collect()
# )

# print("\n=== UNIQUE REPOSITORIES ===")
# print(
#     df.select(
#         pl.col("repository")
#           .n_unique()
#           .alias("unique_repositories")
#     ).collect()
# )

# print("\n=== UNIQUE CONTRIBUTORS ===")
# print(
#     df.select(
#         pl.col("contributor")
#           .n_unique()
#           .alias("unique_contributors")
#     ).collect()
# )

# print("\n=== DATE RANGE ===")
# print(
#     df.select(
#         pl.col("event_timestamp").min().alias("first_event"),
#         pl.col("event_timestamp").max().alias("last_event")
#     ).collect()
# )