import duckdb

INPUT_PATH = "data/gh_events_2023-01.parquet"
OUTPUT_PATH = "data/activity_trend_v2_jan2023.csv"

REPOSITORIES = [
    "pytorch/pytorch",
    "kubernetes/kubernetes",
    "microsoft/vscode",
    "rust-lang/rust",
    "flutter/flutter",
    "grafana/grafana",
    "elastic/kibana",
    "home-assistant/core",
    "NixOS/nixpkgs",
    "dotnet/runtime",
    "llvm/llvm-project",
    "godotengine/godot",
    "sourcegraph/sourcegraph",
    "airbytehq/airbyte",
    "cockroachdb/cockroach",
]

print("Script started")

con = duckdb.connect()

repo_list_sql = ", ".join([f"'{repo}'" for repo in REPOSITORIES])

query = f"""
COPY (

    WITH repo_events AS (

        SELECT
            repository,
            event_timestamp

        FROM read_parquet('{INPUT_PATH}')

        WHERE repository IN ({repo_list_sql})

    )

    SELECT

        repository,

        SUM(
            CASE
                WHEN event_timestamp >= TIMESTAMP '2023-01-25'
                 AND event_timestamp < TIMESTAMP '2023-02-01'
                THEN 1
                ELSE 0
            END
        ) AS last_7_days,

        SUM(
            CASE
                WHEN event_timestamp >= TIMESTAMP '2023-01-18'
                 AND event_timestamp < TIMESTAMP '2023-01-25'
                THEN 1
                ELSE 0
            END
        ) AS previous_7_days,

        CASE
            WHEN SUM(
                CASE
                    WHEN event_timestamp >= TIMESTAMP '2023-01-18'
                     AND event_timestamp < TIMESTAMP '2023-01-25'
                    THEN 1
                    ELSE 0
                END
            ) = 0
            THEN NULL

            ELSE

            (
                SUM(
                    CASE
                        WHEN event_timestamp >= TIMESTAMP '2023-01-25'
                         AND event_timestamp < TIMESTAMP '2023-02-01'
                        THEN 1
                        ELSE 0
                    END
                )

                -

                SUM(
                    CASE
                        WHEN event_timestamp >= TIMESTAMP '2023-01-18'
                         AND event_timestamp < TIMESTAMP '2023-01-25'
                        THEN 1
                        ELSE 0
                    END
                )

            ) * 1.0

            /

            SUM(
                CASE
                    WHEN event_timestamp >= TIMESTAMP '2023-01-18'
                     AND event_timestamp < TIMESTAMP '2023-01-25'
                    THEN 1
                    ELSE 0
                END
            )

        END AS activity_trend

    FROM repo_events

    GROUP BY repository

    ORDER BY repository

) TO '{OUTPUT_PATH}' (HEADER, DELIMITER ',');

"""

print("Running query...")
con.execute(query)

print(f"Saved output to: {OUTPUT_PATH}")

rows = con.execute(f"""
SELECT *
FROM read_csv_auto('{OUTPUT_PATH}')
""").fetchall()

print("\n=== Activity Trend V2 ===")

for row in rows:
    print(row)