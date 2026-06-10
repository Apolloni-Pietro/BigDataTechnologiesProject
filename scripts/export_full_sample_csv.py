import duckdb

con = duckdb.connect()

query = """
COPY (
    SELECT
        id,
        event_type,
        contributor,
        repository,
        event_timestamp,

        payload.action AS action,

        payload.issue.created_at AS issue_created_at,
        payload.issue.closed_at AS issue_closed_at,

        payload.pull_request.created_at AS pr_created_at,
        payload.pull_request.closed_at AS pr_closed_at,
        payload.pull_request.merged_at AS pr_merged_at,

        payload.release.published_at AS release_published_at

    FROM 'data/check_output/full_sample.parquet'
    LIMIT 100
)
TO 'data/check_output/full_sample_flat.csv'
WITH (HEADER, DELIMITER ',');
"""

con.execute(query)

print("CSV exported:")
print("data/check_output/full_sample_flat.csv")