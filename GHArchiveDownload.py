import os
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
    """Converts a single month's JSON files into the Optimized Hybrid Parquet schema."""
    print(f"  Converting JSON to Parquet for {month_key}...")
    
    # We use Hive partitioning to prevent massive >30GB single files.
    # Output will be written to directories like: ./processed_parquet/event_date=2023-01-01/
    glob_pattern = os.path.join(RAW_DIR, f"{month_key}-*.json.gz")
    
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
                    from_json(payload->>'commits', 'STRUCT(sha VARCHAR, author STRUCT(name VARCHAR), message VARCHAR, "distinct" BOOLEAN)[]'),
                    x -> {{'sha': x.sha, 'author_name': x.author.name, 'message': x.message, 'is_distinct': x."distinct"}}
                )
            }} ELSE NULL END AS payload_push,

            -- 3. PullRequestEvent
            CASE WHEN type = 'PullRequestEvent' THEN {{
                'action': payload->>'action',
                'number': (payload->>'number')::INT,
                'pr_id': (payload->'pull_request'->>'id')::BIGINT,
                'title': payload->'pull_request'->>'title',
                'state': payload->'pull_request'->>'state',
                'draft': (payload->'pull_request'->>'draft')::BOOLEAN,
                'merged': (payload->'pull_request'->>'merged')::BOOLEAN,
                'additions': (payload->'pull_request'->>'additions')::INT,
                'deletions': (payload->'pull_request'->>'deletions')::INT,
                'changed_files': (payload->'pull_request'->>'changed_files')::INT,
                'head_ref': payload->'pull_request'->'head'->>'ref',
                'base_ref': payload->'pull_request'->'base'->>'ref',
                'body': payload->'pull_request'->>'body'
            }} ELSE NULL END AS payload_pull_request,

            -- 4. IssuesEvent
            CASE WHEN type = 'IssuesEvent' THEN {{
                'action': payload->>'action',
                'issue_id': (payload->'issue'->>'id')::BIGINT,
                'number': (payload->'issue'->>'number')::INT,
                'title': payload->'issue'->>'title',
                'state': payload->'issue'->>'state',
                'state_reason': payload->'issue'->>'state_reason',
                'comments': (payload->'issue'->>'comments')::INT,
                'body': payload->'issue'->>'body',
                'labels': list_transform(
                    from_json(payload->'issue'->>'labels', 'STRUCT(name VARCHAR)[]'),
                    x -> x.name
                )
            }} ELSE NULL END AS payload_issue,

            -- 5. IssueCommentEvent
            CASE WHEN type = 'IssueCommentEvent' THEN {{
                'action': payload->>'action',
                'issue_number': (payload->'issue'->>'number')::INT,
                'issue_title': payload->'issue'->>'title',
                'comment_id': (payload->'comment'->>'id')::BIGINT,
                'author_association': payload->'comment'->>'author_association',
                'body': payload->'comment'->>'body'
            }} ELSE NULL END AS payload_issue_comment,

            -- 6. PullRequestReviewEvent
            CASE WHEN type = 'PullRequestReviewEvent' THEN {{
                'action': payload->>'action',
                'pull_request_number': (payload->'pull_request'->>'number')::INT,
                'review_id': (payload->'review'->>'id')::BIGINT,
                'state': payload->'review'->>'state',
                'body': payload->'review'->>'body'
            }} ELSE NULL END AS payload_pr_review,

            -- 7. PullRequestReviewCommentEvent
            CASE WHEN type = 'PullRequestReviewCommentEvent' THEN {{
                'action': payload->>'action',
                'pull_request_number': (payload->'pull_request'->>'number')::INT,
                'review_id': (payload->'comment'->>'pull_request_review_id')::BIGINT,
                'comment_id': (payload->'comment'->>'id')::BIGINT,
                'path': payload->'comment'->>'path',
                'line': (payload->'comment'->>'line')::INT,
                'body': payload->'comment'->>'body'
            }} ELSE NULL END AS payload_pr_review_comment,

            -- 8. CommitCommentEvent
            CASE WHEN type = 'CommitCommentEvent' THEN {{
                'comment_id': (payload->'comment'->>'id')::BIGINT,
                'commit_id': payload->'comment'->>'commit_id',
                'author_association': payload->'comment'->>'author_association',
                'path': payload->'comment'->>'path',
                'line': (payload->'comment'->>'line')::INT,
                'position': (payload->'comment'->>'position')::INT,
                'body': payload->'comment'->>'body'
            }} ELSE NULL END AS payload_commit_comment,

            -- 9. CreateEvent & DeleteEvent
            CASE WHEN type IN ('CreateEvent', 'DeleteEvent') THEN {{
                'ref': payload->>'ref',
                'ref_type': payload->>'ref_type',
                'pusher_type': payload->>'pusher_type',
                'master_branch': payload->>'master_branch',
                'description': payload->>'description'
            }} ELSE NULL END AS payload_lifecycle,

            -- 10. ForkEvent
            CASE WHEN type = 'ForkEvent' THEN {{
                'forkee_id': (payload->'forkee'->>'id')::BIGINT,
                'name': payload->'forkee'->>'name',
                'full_name': payload->'forkee'->>'full_name',
                'is_private': (payload->'forkee'->>'private')::BOOLEAN,
                'owner_login': payload->'forkee'->'owner'->>'login'
            }} ELSE NULL END AS payload_fork,

            -- 11. ReleaseEvent
            CASE WHEN type = 'ReleaseEvent' THEN {{
                'action': payload->>'action',
                'release_id': (payload->'release'->>'id')::BIGINT,
                'name': payload->'release'->>'name',
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
                    from_json(payload->>'pages', 'STRUCT(action VARCHAR, page_name VARCHAR, sha VARCHAR, title VARCHAR)[]'),
                    x -> {{'action': x.action, 'page_name': x.page_name, 'sha': x.sha, 'title': x.title}}
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
    ) TO '{PARQUET_DIR}' (
        FORMAT PARQUET, 
        COMPRESSION 'ZSTD', 
        PARTITION_BY (event_date), 
        OVERWRITE_OR_IGNORE 1
    );
    """
    
    try:
        con.execute(query)
        print(f"  Successfully converted {month_key} into partitioned Parquet files.")
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