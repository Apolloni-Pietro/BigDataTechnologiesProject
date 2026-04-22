# Big Data Technologies Project

## Overview

The goal of this project is to build a platform that constantly tracks the health of Open Source Software on [Github](https://github.com/).

The planned tech stack to use comprises:

- DuckDB on Python (with Parquet files), for the initial data ingestion and handling
- Polars on Python
- Redpanda, a lightweigth Apache Kafka-compatible streaming data platform
- Streamlit, for displaying a web dashboard of "Project Health"
- Docker may be used for the project deployment phase

## Research Plan

### Phase 1: Data Ingestion

Using Python, we download historic data from [GH Archive](https://www.gharchive.org/) and store them in Parquet files. These will then be used to establish "baseline" metrics (like how often commits happen, or how many people maintain a repo). Basically, a static database of "how things have been" for the last X amount of time.

### Phase 2: Anomaly Detection

We train Machine Learning models to understand what "normal" looks like for different projects. The models flag projects that suddenly deviate from their historical norms (e.g., a massive drop in commits, or a spike in unmerged PRs).

### Phase 3: Data Streaming

We set up Redpanda to ingest GitHub events as they happen. The system now updates project health scores in real-time and serves them via an API and a web dashboard (possibly using Streamlit).

### Phase 4: Validation

TBD

## Project Implementation

First draft:

1. GH Archive dump
2. GitHub API Crawler
3. ML / Anomaly Detection
4. Redpanda (?)
5. Cloud (?)
6. Dashboard
