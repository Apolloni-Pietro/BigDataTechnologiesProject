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
    
    # [EDIT] Skip download if the file is already on disk
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
    """Converts a single month's JSON files into a Parquet file."""
    print(f"  Converting JSON to Parquet for {month_key}...")
    
    output_parquet = os.path.join(PARQUET_DIR, f"gh_events_{month_key}.parquet")
    
    # [EDIT] Glob pattern targets only files for this specific month (e.g., 2024-01-*.json.gz)
    glob_pattern = os.path.join(RAW_DIR, f"{month_key}-*.json.gz")
    
    con = duckdb.connect()
    
    query = f"""
    COPY (
        SELECT 
            id,
            type as event_type,
            actor.login as contributor,
            repo.name as repository,
            created_at::TIMESTAMP as event_timestamp
        FROM read_json('{glob_pattern}',
            format='newline_delimited',
            columns={{
                'id': 'VARCHAR',
                'type': 'VARCHAR',
                'actor': 'STRUCT(login VARCHAR)',
                'repo': 'STRUCT(name VARCHAR)',
                'created_at': 'VARCHAR'
            }}
        )
        WHERE type IN ('PushEvent', 'PullRequestEvent', 'IssuesEvent', 'IssueCommentEvent')
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
    
    # 1. Group all required files by month (YYYY-MM)
    tasks_by_month = defaultdict(list)
    current = start
    while current <= end:
        month_key = current.strftime("%Y-%m")
        date_str = current.strftime("%Y-%m-%d")
        for hour in range(24):
            tasks_by_month[month_key].append(f"{date_str}-{hour}.json.gz")
        current += timedelta(days=1)
        
    # 2. Process each month one at a time
    for month_key, files_to_download in tasks_by_month.items():
        print(f"\n=== Processing Month: {month_key} ===")
        
        # Check if we already finished this month completely
        final_parquet_path = os.path.join(PARQUET_DIR, f"gh_events_{month_key}.parquet")
        if os.path.exists(final_parquet_path):
            print(f"  {final_parquet_path} already exists. Skipping entire month!")
            continue
            
        print(f"  Downloading missing files for {month_key}...")
        
        # Download files in parallel
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(download_single_file, fname): fname for fname in files_to_download}
            
            completed = 0
            for future in as_completed(futures):
                completed += 1
                result = future.result()
                if completed % 100 == 0 or completed == len(files_to_download):
                    print(f"    Progress: {completed}/{len(files_to_download)} files checked/downloaded.")
        
        # Convert to Parquet
        success = convert_month_to_parquet(month_key)
        
        # Cleanup only if conversion was successful
        if success:
            cleanup_raw_files(month_key)
            
    print("\n--- All operations complete! ---")

if __name__ == "__main__":
    main()