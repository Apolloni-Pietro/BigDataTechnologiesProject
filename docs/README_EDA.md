# GHArchive Exploration

Initial data exploration and metric engineering for the **Open-Source Ecosystem Health Monitor** project.

## Goal

The purpose of this folder is to:

1. Explore GH Archive data.
2. Identify candidate repositories.
3. Define and validate health metrics.
4. Compute an initial repository health score (V0).

---

# Dataset

Input dataset:

```
data/gh_events_2023-01.parquet
```

Contains the following fields:

- id
- event_type
- contributor
- repository
- event_timestamp

Filtered event types:

- PushEvent
- PullRequestEvent
- IssuesEvent
- IssueCommentEvent

---

# Scripts

## inspect_parquet.py

Purpose:

- Inspect dataset schema.
- Count rows.
- Explore event distribution.
- Identify top repositories and contributors.

Output:

- Printed statistics in terminal.

---

## find_candidate_repositories_duckdb.py

Purpose:

- Identify repositories suitable for monitoring.
- Remove mostly automated repositories.
- Select active repositories with multiple contributors.

Output:

```
data/candidate_repositories_jan2023.csv
```

---

## repository_health_metrics_duckdb.py

Purpose:

Compute initial repository-level metrics:

- total_events
- active_contributors
- bot_ratio
- push_events
- pr_events
- issue_events
- issue_comment_events
- maintenance_gap_days
- top_contributor_share

Output:

```
data/repository_health_metrics_jan2023.csv
```

---

## activity_trend_analysis_duckdb.py

Purpose:

First attempt at repository activity trend analysis.

Status:

Superseded by V2.

---

## activity_trend_v2_duckdb.py

Purpose:

Compute repository activity trend using:

```
last_7_days
vs
previous_7_days
```

Metric:

```
activity_trend =
(last_7_days - previous_7_days)
/
previous_7_days
```

Output:

```
data/activity_trend_v2_jan2023.csv
```

---

## health_score_v0.py

Purpose:

Combine all repository metrics into an initial health score.

Current score components:

- contributor_score
- activity_score
- maintenance_score
- bot_score
- pr_issue_score

Output:

```
data/health_score_v0_jan2023.csv
```

---

# GH Archive Full Dataset Investigation

These scripts were used to compare the reduced parquet dataset with the original GH Archive structure.

---

## check_gharchive_full_fields.py

Purpose:

Download a single GH Archive hourly file and create:

- reduced_sample.parquet
- full_sample.parquet

Output:

```
data/check_output/
```

---

## compare_sample_parquets.py

Purpose:

Compare schemas between:

- reduced parquet
- full parquet

Used to verify which fields were removed during conversion.

---

## export_full_sample_csv.py

Purpose:

Export selected fields from the original GH Archive payload.

Generated CSV includes:

- action
- issue_created_at
- issue_closed_at
- pr_created_at
- pr_closed_at
- pr_merged_at
- release_published_at

Output:

```
data/check_output/full_sample_flat.csv
```

This investigation confirmed that the current parquet dataset is a reduced version of the original GH Archive events.

---

# Current Status

Implemented:

- Candidate repository selection
- Contributor metrics
- Activity trend
- Maintenance gap
- Bot ratio
- Health Score V0

Next steps:

- Multi-month analysis
- Redis (or others) integration
- Redpanda replay pipeline
- Streamlit dashboard