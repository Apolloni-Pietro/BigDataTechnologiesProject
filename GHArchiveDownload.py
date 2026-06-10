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
    """Converts a single month's JSON files into a Parquet file."""
    print(f"  Converting JSON to Parquet for {month_key}...")
    
    output_parquet = os.path.join(PARQUET_DIR, f"gh_events_{month_key}.parquet")
    glob_pattern = os.path.join(RAW_DIR, f"{month_key}-*.json.gz")
    
    con = duckdb.connect()
    
    # [EDIT] Updated schema extraction to pull all relevant fields, ignore fluff, 
    # and handle the polymorphic payload correctly.
    query = f"""
    COPY (
        SELECT 
            id,
            type AS event_type,
            actor.id AS actor_id,
            actor.login AS actor_login,
            repo.id AS repo_id,
            repo.name AS repo_name,
            org.id AS org_id,
            org.login AS org_login,
            public AS is_public,
            created_at::TIMESTAMP AS event_timestamp,
            payload::JSON AS payload
        FROM read_json('{glob_pattern}',
            format='newline_delimited',
            columns={{
                'id': 'VARCHAR',
                'type': 'VARCHAR',
                'actor': 'STRUCT(id BIGINT, login VARCHAR)',
                'repo': 'STRUCT(id BIGINT, name VARCHAR)',
                'org': 'STRUCT(id BIGINT, login VARCHAR)',
                'public': 'BOOLEAN',
                'created_at': 'VARCHAR',
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