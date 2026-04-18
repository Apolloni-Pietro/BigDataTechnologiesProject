import os
import requests
import duckdb
from datetime import datetime, timedelta

# --- Configuration ---
START_DATE = "2024-01-01"
END_DATE = "2024-01-31" # Keeping it to one day for the initial test
RAW_DIR = "./raw_json"
PARQUET_DIR = "./processed_parquet"

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PARQUET_DIR, exist_ok=True)

def download_gh_archive(date_str):
    """Downloads 24 hours of GH Archive data for a given date."""
    downloaded_files = []
    print(f"Starting downloads for {date_str}...")
    
    for hour in range(24):
        file_name = f"{date_str}-{hour}.json.gz"
        file_path = os.path.join(RAW_DIR, file_name)
        url = f"https://data.gharchive.org/{file_name}"
        
        if not os.path.exists(file_path):
            print(f"  Downloading {file_name}...")
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                downloaded_files.append(file_path)
            else:
                print(f"  [!] Failed to download {file_name} (Status: {response.status_code})")
        else:
            print(f"  {file_name} already exists. Skipping download.")
            downloaded_files.append(file_path)
            
    return downloaded_files

def convert_to_parquet(date_str):
    """
    Uses DuckDB to read the raw JSON and stream it directly to a Parquet file.
    We explicitly define the schema to avoid memory crashes during inference.
    """
    print(f"\nConverting JSON to Parquet for {date_str} using DuckDB...")
    
    output_parquet = os.path.join(PARQUET_DIR, f"gh_events_{date_str}.parquet")
    glob_pattern = os.path.join(RAW_DIR, f"{date_str}-*.json.gz")
    
    # Initialize DuckDB
    con = duckdb.connect()
    
    # We define only the columns we actually need for the ecosystem monitor.
    # Ignoring the massive 'payload' blob unless specifically needed saves massive amounts of memory.
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
        -- Optional: Filter out trivial events immediately to save disk space
        WHERE type IN ('PushEvent', 'PullRequestEvent', 'IssuesEvent', 'IssueCommentEvent')
    ) TO '{output_parquet}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
    """
    
    try:
        con.execute(query)
        print(f"Successfully created {output_parquet}")
        
        # Optional: Delete the raw JSON to free up disk space
        # for f in os.listdir(RAW_DIR):
        #     if f.startswith(date_str):
        #         os.remove(os.path.join(RAW_DIR, f))
                
    except Exception as e:
        print(f"Failed to convert {date_str}: {e}")

def main():
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")
    
    current = start
    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        download_gh_archive(date_str)
        convert_to_parquet(date_str)
        current += timedelta(days=1)
        
    print("\nBatch ingestion complete.")

if __name__ == "__main__":
    main()