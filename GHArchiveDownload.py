import os
import requests
import duckdb
from datetime import datetime, timedelta
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration ---
START_DATE = "2023-01-01" 
END_DATE = "2024-01-31" 
RAW_DIR = "./raw_json"
PARQUET_DIR = "./processed_parquet"
MAX_WORKERS = 35 # Edit according to available internet bandwidth

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PARQUET_DIR, exist_ok=True)

def download_single_file(file_name):
    """Worker function to download exactly one file if it doesn't exist."""
    file_path = os.path.join(RAW_DIR, file_name)
    url = f"https://data.gharchive.org/{file_name}"
    
    if os.path.exists(file_path):
        return f"{file_name} already exists. Skipped."
        
    try:
        response = requests.get(url, stream=True, timeout=30)
        if response.status_code == 200:
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return f"Downloaded {file_name}"
        else:
            return f"[!] Failed to download {file_name} (Status: {response.status_code})"
    except Exception as e:
        return f"[!] Error downloading {file_name}: {e}"

def convert_month_to_parquet(month_key):
    """Converts a single month's JSON files into an optimized Hybrid Parquet schema."""
    print(f"  Converting JSON to Parquet for {month_key}...")
    
    output_parquet = os.path.join(PARQUET_DIR, f"gh_events_{month_key}.parquet")
    glob_pattern = os.path.join(RAW_DIR, f"{month_key}-*.json.gz")
    
    con = duckdb.connect()
    
    query = f"""
    COPY (
        SELECT 
            -- 1. Global Metadata
            id,
            type AS event_type,
            (created_at::TIMESTAMP) AS event_timestamp,
            public AS is_public,
            actor.id AS actor_id,
            actor.login AS actor_login,
            repo.id AS repo_id,
            repo.name AS repo_name,
            org.id AS org_id,
            org.login AS org_login,

            -- 2. Ordered Push payload
            CASE WHEN type = 'PushEvent' THEN {{
                'push_id': (payload->>'push_id')::BIGINT,
                'ref': payload->>'ref',
                'size': (payload->>'size')::INT,
                'distinct_size': (payload->>'distinct_size')::INT,
                'commits': list_transform(
                    from_json(payload->>'commits', 'STRUCT(sha VARCHAR, author STRUCT(name VARCHAR), message VARCHAR)[]'),
                    x -> {{'sha': x.sha, 'author_name': x.author.name, 'message': x.message}}
                )
            }} ELSE NULL END AS payload_push,

            -- 3. Ordered Pull Request payload
            CASE WHEN type = 'PullRequestEvent' THEN {{
                'action': payload->>'action',
                'number': (payload->>'number')::INT,
                'title': payload->'pull_request'->>'title',
                'state': payload->'pull_request'->>'state',
                'is_draft': (payload->'pull_request'->>'draft')::BOOLEAN,
                'additions': (payload->'pull_request'->>'additions')::INT,
                'deletions': (payload->'pull_request'->>'deletions')::INT,
                'changed_files': (payload->'pull_request'->>'changed_files')::INT,
                'head_sha': payload->'pull_request'->'head'->>'sha',
                'base_sha': payload->'pull_request'->'base'->>'sha'
            }} ELSE NULL END AS payload_pull_request,

            -- 4. Ordered Issue Comment payload
            CASE WHEN type = 'IssueCommentEvent' THEN {{
                'action': payload->>'action',
                'issue_number': (payload->'issue'->>'number')::INT,
                'issue_title': payload->'issue'->>'title',
                'comment_id': (payload->'comment'->>'id')::BIGINT,
                'comment_body': payload->'comment'->>'body'
            }} ELSE NULL END AS payload_issue_comment,

            -- 5. Ordered Issues lifecycle payload
            CASE WHEN type = 'IssuesEvent' THEN {{
                'action': payload->>'action',
                'number': (payload->'issue'->>'number')::INT,
                'title': payload->'issue'->>'title',
                'state': payload->'issue'->>'state',
                'labels': list_transform(
                    from_json(payload->'issue'->>'labels', 'STRUCT(name VARCHAR)[]'),
                    x -> x.name
                )
            }} ELSE NULL END AS payload_issue,

            -- 6. Shared Create/Delete payload
            CASE WHEN type IN ('CreateEvent', 'DeleteEvent') THEN {{
                'ref': payload->>'ref',
                'ref_type': payload->>'ref_type',
                'master_branch': payload->>'master_branch'
            }} ELSE NULL END AS payload_lifecycle

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
    ) TO '{output_parquet}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
    """
    
    try:
        con.execute(query)
        print(f"  Successfully created {output_parquet}")
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