import os
import gzip
import requests
import duckdb
from datetime import datetime, timedelta
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration ---
START_DATE = "2026-01-01" 
END_DATE = "2026-01-31" 
RAW_DIR = "./raw_json"
PARQUET_DIR = "./processed_parquet"
MAX_WORKERS = 35 # Edit according to available internet bandwidth
MAX_RETRIES = 3  # Download attempts per file before giving up

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PARQUET_DIR, exist_ok=True)

def is_valid_gzip(file_path):
    """Return True only if the file decompresses fully without error.

    A truncated download (the usual cause of 'unexpected end of data' during
    conversion) raises EOFError / BadGzipFile here, letting us catch the
    corruption before DuckDB ever touches the file.
    """
    try:
        with gzip.open(file_path, 'rb') as f:
            while f.read(1024 * 1024):
                pass
        return True
    except (OSError, EOFError):
        return False

def download_single_file(file_name):
    """Download one file atomically, verifying gzip integrity before keeping it.

    Guarantees that a file present in RAW_DIR is always a complete, valid
    archive, so the month-wide conversion never trips over a partial download.
    """
    file_path = os.path.join(RAW_DIR, file_name)
    tmp_path = file_path + ".part"
    url = f"https://data.gharchive.org/{file_name}"

    # Trust an existing file only if it is actually intact; otherwise re-fetch.
    if os.path.exists(file_path):
        if is_valid_gzip(file_path):
            return f"{file_name} already exists. Skipped."
        os.remove(file_path)
        # fall through and re-download the corrupted file

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, stream=True, timeout=60)
            if response.status_code == 200:
                # Write to a temp file first so a partial write never lands as .json.gz
                with open(tmp_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                if is_valid_gzip(tmp_path):
                    os.replace(tmp_path, file_path)  # atomic rename
                    return f"Downloaded {file_name}"
                last_error = "incomplete/corrupted archive"
            elif response.status_code == 404:
                # Some hours are genuinely absent on GH Archive; don't retry.
                return f"[!] {file_name} not available (404)."
            else:
                last_error = f"HTTP {response.status_code}"
        except Exception as e:
            last_error = str(e)
        finally:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

    return f"[!] Failed to download {file_name} after {MAX_RETRIES} attempts ({last_error})."

def convert_month_to_parquet(month_key):
    """Converts a single month's JSON files into the Optimized Hybrid Parquet schema."""
    print(f"  Converting JSON to Parquet for {month_key}...")

    # All hours of the month are collapsed into a single Parquet file:
    #   ./processed_parquet/gh_events_2026-01.parquet
    glob_pattern = os.path.join(RAW_DIR, f"{month_key}-*.json.gz")
    output_path = os.path.join(PARQUET_DIR, f"gh_events_{month_key}.parquet")

    con = duckdb.connect()
    
    query = f"""
    COPY (
        SELECT 
            -- 1. Global Metadata
            id::VARCHAR AS event_id,
            type AS event_type,
            (created_at::TIMESTAMP)::DATE AS event_date,
            created_at::TIMESTAMP AS event_timestamp,
            public AS is_public,
            actor.id AS actor_id,
            actor.login AS actor_login,
            repo.id AS repo_id,
            repo.name AS repo_name,
            org.id AS org_id,
            org.login AS org_login,

            -- 2. PushEvent
            CASE WHEN type = 'PushEvent' THEN {{
                'push_id': (payload->>'push_id')::BIGINT,
                'ref': payload->>'ref',
                'head': payload->>'head',
                'before': payload->>'before',
                'size': (payload->>'size')::INT,
                'distinct_size': (payload->>'distinct_size')::INT,
                'commits': list_transform(
                    CAST(payload->'commits' AS JSON[]),
                    x -> {{'sha': json_extract_string(x, '$.sha'), 'author_name': json_extract_string(x, '$.author.name'), 'author_email': json_extract_string(x, '$.author.email'), 'is_distinct': (json_extract_string(x, '$.distinct'))::BOOLEAN}}
                )
            }} ELSE NULL END AS payload_push,

            -- 3. PullRequestEvent
            CASE WHEN type = 'PullRequestEvent' THEN {{
                'action': payload->>'action',
                'number': (payload->>'number')::INT,
                'pr_id': (payload->'pull_request'->>'id')::BIGINT,
                'state': payload->'pull_request'->>'state',
                'draft': (payload->'pull_request'->>'draft')::BOOLEAN,
                'merged': (payload->'pull_request'->>'merged')::BOOLEAN,
                'author_association': payload->'pull_request'->>'author_association',
                'created_at': (payload->'pull_request'->>'created_at')::TIMESTAMP,
                'closed_at': (payload->'pull_request'->>'closed_at')::TIMESTAMP,
                'merged_at': (payload->'pull_request'->>'merged_at')::TIMESTAMP,
                'merged_by_login': payload->'pull_request'->'merged_by'->>'login',
                'commits': (payload->'pull_request'->>'commits')::INT,
                'additions': (payload->'pull_request'->>'additions')::INT,
                'deletions': (payload->'pull_request'->>'deletions')::INT,
                'changed_files': (payload->'pull_request'->>'changed_files')::INT,
                'comments': (payload->'pull_request'->>'comments')::INT,
                'review_comments': (payload->'pull_request'->>'review_comments')::INT,
                'head_ref': payload->'pull_request'->'head'->>'ref',
                'base_ref': payload->'pull_request'->'base'->>'ref'
            }} ELSE NULL END AS payload_pull_request,

            -- 4. IssuesEvent
            CASE WHEN type = 'IssuesEvent' THEN {{
                'action': payload->>'action',
                'issue_id': (payload->'issue'->>'id')::BIGINT,
                'number': (payload->'issue'->>'number')::INT,
                'state': payload->'issue'->>'state',
                'state_reason': payload->'issue'->>'state_reason',
                'author_association': payload->'issue'->>'author_association',
                'created_at': (payload->'issue'->>'created_at')::TIMESTAMP,
                'closed_at': (payload->'issue'->>'closed_at')::TIMESTAMP,
                'comments': (payload->'issue'->>'comments')::INT,
                'reactions_total': (payload->'issue'->'reactions'->>'total_count')::INT,
                'labels': list_transform(
                    CAST(payload->'issue'->'labels' AS JSON[]),
                    x -> json_extract_string(x, '$.name')
                )
            }} ELSE NULL END AS payload_issue,

            -- 5. IssueCommentEvent
            CASE WHEN type = 'IssueCommentEvent' THEN {{
                'action': payload->>'action',
                'issue_number': (payload->'issue'->>'number')::INT,
                'comment_id': (payload->'comment'->>'id')::BIGINT,
                'author_association': payload->'comment'->>'author_association',
                'reactions_total': (payload->'comment'->'reactions'->>'total_count')::INT
            }} ELSE NULL END AS payload_issue_comment,

            -- 6. PullRequestReviewEvent
            CASE WHEN type = 'PullRequestReviewEvent' THEN {{
                'action': payload->>'action',
                'pull_request_number': (payload->'pull_request'->>'number')::INT,
                'review_id': (payload->'review'->>'id')::BIGINT,
                'state': payload->'review'->>'state',
                'author_association': payload->'review'->>'author_association'
            }} ELSE NULL END AS payload_pr_review,

            -- 7. PullRequestReviewCommentEvent
            CASE WHEN type = 'PullRequestReviewCommentEvent' THEN {{
                'action': payload->>'action',
                'pull_request_number': (payload->'pull_request'->>'number')::INT,
                'review_id': (payload->'comment'->>'pull_request_review_id')::BIGINT,
                'comment_id': (payload->'comment'->>'id')::BIGINT,
                'author_association': payload->'comment'->>'author_association',
                'path': payload->'comment'->>'path',
                'line': (payload->'comment'->>'line')::INT,
                'reactions_total': (payload->'comment'->'reactions'->>'total_count')::INT
            }} ELSE NULL END AS payload_pr_review_comment,

            -- 8. CommitCommentEvent
            CASE WHEN type = 'CommitCommentEvent' THEN {{
                'comment_id': (payload->'comment'->>'id')::BIGINT,
                'commit_id': payload->'comment'->>'commit_id',
                'author_association': payload->'comment'->>'author_association',
                'path': payload->'comment'->>'path',
                'line': (payload->'comment'->>'line')::INT,
                'reactions_total': (payload->'comment'->'reactions'->>'total_count')::INT
            }} ELSE NULL END AS payload_commit_comment,

            -- 9. CreateEvent & DeleteEvent
            CASE WHEN type IN ('CreateEvent', 'DeleteEvent') THEN {{
                'ref': payload->>'ref',
                'ref_type': payload->>'ref_type',
                'pusher_type': payload->>'pusher_type'
            }} ELSE NULL END AS payload_lifecycle,

            -- 10. ForkEvent
            CASE WHEN type = 'ForkEvent' THEN {{
                'forkee_id': (payload->'forkee'->>'id')::BIGINT,
                'full_name': payload->'forkee'->>'full_name',
                'owner_login': payload->'forkee'->'owner'->>'login',
                'is_private': (payload->'forkee'->>'private')::BOOLEAN
            }} ELSE NULL END AS payload_fork,

            -- 11. ReleaseEvent
            CASE WHEN type = 'ReleaseEvent' THEN {{
                'action': payload->>'action',
                'release_id': (payload->'release'->>'id')::BIGINT,
                'tag_name': payload->'release'->>'tag_name',
                'draft': (payload->'release'->>'draft')::BOOLEAN,
                'prerelease': (payload->'release'->>'prerelease')::BOOLEAN
            }} ELSE NULL END AS payload_release,

            -- 12. MemberEvent
            CASE WHEN type = 'MemberEvent' THEN {{
                'action': payload->>'action',
                'member_id': (payload->'member'->>'id')::BIGINT,
                'member_login': payload->'member'->>'login',
                'member_type': payload->'member'->>'type'
            }} ELSE NULL END AS payload_member,

            -- 13. GollumEvent (Wiki)
            CASE WHEN type = 'GollumEvent' THEN {{
                'pages': list_transform(
                    CAST(payload->'pages' AS JSON[]),
                    x -> {{'action': json_extract_string(x, '$.action'), 'page_name': json_extract_string(x, '$.page_name'), 'sha': json_extract_string(x, '$.sha')}}
                )
            }} ELSE NULL END AS payload_gollum

        FROM read_json('{glob_pattern}',
            format='newline_delimited',
            columns={{
                'id': 'VARCHAR',
                'type': 'VARCHAR',
                'created_at': 'VARCHAR',
                'public': 'BOOLEAN',
                'actor': 'STRUCT(id BIGINT, login VARCHAR)',
                'repo': 'STRUCT(id BIGINT, name VARCHAR)',
                'org': 'STRUCT(id BIGINT, login VARCHAR)',
                'payload': 'JSON'
            }}
        )
    ) TO '{output_path}' (
        FORMAT PARQUET,
        COMPRESSION 'ZSTD'
    );
    """

    try:
        con.execute(query)
        print(f"  Successfully converted {month_key} into {output_path}.")
        return True
    except Exception as e:
        print(f"  [!] Failed to convert {month_key}: {e}")
        return False

def cleanup_raw_files(month_key):
    """Deletes the raw JSON files for a specific month to free up disk space."""
    print(f"  Cleaning up raw files for {month_key}...")
    count = 0
    for filename in os.listdir(RAW_DIR):
        if filename.startswith(month_key) and filename.endswith(".json.gz"):
            file_path = os.path.join(RAW_DIR, filename)
            try:
                os.remove(file_path)
                count += 1
            except Exception as e:
                print(f"  [!] Failed to delete {filename}: {e}")
    print(f"  Deleted {count} files for {month_key}.")

def main():
    print("--- Starting Monthly Batch Ingestion ---")
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")
    
    tasks_by_month = defaultdict(list)
    current = start
    while current <= end:
        month_key = current.strftime("%Y-%m")
        date_str = current.strftime("%Y-%m-%d")
        for hour in range(24):
            tasks_by_month[month_key].append(f"{date_str}-{hour}.json.gz")
        current += timedelta(days=1)
        
    for month_key, files_to_download in tasks_by_month.items():
        print(f"\n=== Processing Month: {month_key} ===")
        
        final_parquet_path = os.path.join(PARQUET_DIR, f"gh_events_{month_key}.parquet")
        if os.path.exists(final_parquet_path):
            print(f"  {final_parquet_path} already exists. Skipping entire month!")
            continue
            
        print(f"  Downloading missing files for {month_key}...")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(download_single_file, fname): fname for fname in files_to_download}
            
            completed = 0
            for future in as_completed(futures):
                completed += 1
                future.result()
                if completed % 100 == 0 or completed == len(files_to_download):
                    print(f"    Progress: {completed}/{len(files_to_download)} files checked/downloaded.")
        
        success = convert_month_to_parquet(month_key)
        
        if success:
            cleanup_raw_files(month_key)
            
    print("\n--- All operations complete! ---")

if __name__ == "__main__":
    main()