import duckdb

INPUT_PATH = "data/gh_events_2023-01.parquet"
OUTPUT_PATH = "data/candidate_repositories_jan2023.csv"

print("Script started")

con = duckdb.connect()

con.execute("SET memory_limit='4GB'")
con.execute("SET threads=4")

query = f"""
COPY (
    WITH repo_metrics AS (
        SELECT
            repository,
            COUNT(*) AS total_events,
            COUNT(DISTINCT contributor) AS active_contributors,
            SUM(
                CASE
                    WHEN lower(contributor) LIKE '%bot%'
                      OR lower(contributor) LIKE '%dependabot%'
                      OR lower(contributor) LIKE '%renovate%'
                      OR lower(contributor) LIKE '%github-actions%'
                    THEN 1 ELSE 0
                END
            ) AS bot_events,
            MIN(event_timestamp) AS first_event,
            MAX(event_timestamp) AS last_event,
            SUM(CASE WHEN event_type = 'PullRequestEvent' THEN 1 ELSE 0 END) AS pr_events,
            SUM(CASE WHEN event_type IN ('IssuesEvent', 'IssueCommentEvent') THEN 1 ELSE 0 END) AS issue_events
        FROM read_parquet('{INPUT_PATH}')
        GROUP BY repository
    )
    SELECT
        *,
        bot_events * 1.0 / total_events AS bot_ratio
    FROM repo_metrics
    WHERE total_events >= 1000
      AND active_contributors >= 5
      AND bot_events * 1.0 / total_events < 0.8
    ORDER BY total_events DESC
) TO '{OUTPUT_PATH}' (HEADER, DELIMITER ',');
"""

print("Running query...")
con.execute(query)

print(f"Saved output to: {OUTPUT_PATH}")

preview = con.execute(f"""
    SELECT *
    FROM read_csv_auto('{OUTPUT_PATH}')
    LIMIT 20
""").fetchall()

for row in preview:
    print(row)