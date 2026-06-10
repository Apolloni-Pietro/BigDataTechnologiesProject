import duckdb

INPUT_PATH = "data/gh_events_2023-01.parquet"
OUTPUT_PATH = "data/repository_health_metrics_jan2023.csv"

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
con.execute("SET memory_limit='4GB'")
con.execute("SET threads=4")

repo_list_sql = ", ".join([f"'{repo}'" for repo in REPOSITORIES])

query = f"""
COPY (
    WITH filtered_events AS (
        SELECT
            id,
            event_type,
            contributor,
            repository,
            event_timestamp,
            CASE
                WHEN lower(contributor) LIKE '%bot%'
                  OR lower(contributor) LIKE '%dependabot%'
                  OR lower(contributor) LIKE '%renovate%'
                  OR lower(contributor) LIKE '%github-actions%'
                THEN 1 ELSE 0
            END AS is_bot
        FROM read_parquet('{INPUT_PATH}')
        WHERE repository IN ({repo_list_sql})
    ),

    repo_base_metrics AS (
        SELECT
            repository,
            COUNT(*) AS total_events,
            COUNT(DISTINCT contributor) AS active_contributors,
            SUM(is_bot) AS bot_events,
            SUM(CASE WHEN event_type = 'PushEvent' THEN 1 ELSE 0 END) AS push_events,
            SUM(CASE WHEN event_type = 'PullRequestEvent' THEN 1 ELSE 0 END) AS pr_events,
            SUM(CASE WHEN event_type = 'IssuesEvent' THEN 1 ELSE 0 END) AS issue_events,
            SUM(CASE WHEN event_type = 'IssueCommentEvent' THEN 1 ELSE 0 END) AS issue_comment_events,
            MIN(event_timestamp) AS first_event,
            MAX(event_timestamp) AS last_event
        FROM filtered_events
        GROUP BY repository
    ),

    contributor_events AS (
        SELECT
            repository,
            contributor,
            COUNT(*) AS contributor_events
        FROM filtered_events
        GROUP BY repository, contributor
    ),

    top_contributor AS (
        SELECT
            repository,
            contributor AS top_contributor,
            contributor_events AS top_contributor_events
        FROM (
            SELECT
                repository,
                contributor,
                contributor_events,
                ROW_NUMBER() OVER (
                    PARTITION BY repository
                    ORDER BY contributor_events DESC
                ) AS rn
            FROM contributor_events
        )
        WHERE rn = 1
    )

    SELECT
        b.repository,
        b.total_events,
        b.active_contributors,
        b.bot_events,
        b.bot_events * 1.0 / b.total_events AS bot_ratio,
        b.push_events,
        b.pr_events,
        b.issue_events,
        b.issue_comment_events,
        b.first_event,
        b.last_event,
        DATE_DIFF('day', b.last_event, TIMESTAMP '2023-01-31 23:59:59') AS maintenance_gap_days,
        t.top_contributor,
        t.top_contributor_events,
        t.top_contributor_events * 1.0 / b.total_events AS top_contributor_share
    FROM repo_base_metrics b
    LEFT JOIN top_contributor t
        ON b.repository = t.repository
    ORDER BY b.total_events DESC
) TO '{OUTPUT_PATH}' (HEADER, DELIMITER ',');
"""

print("Running query...")
con.execute(query)

print(f"Saved output to: {OUTPUT_PATH}")

preview = con.execute(f"""
    SELECT *
    FROM read_csv_auto('{OUTPUT_PATH}')
""").fetchall()

print("\n=== Repository health metrics ===")
for row in preview:
    print(row)