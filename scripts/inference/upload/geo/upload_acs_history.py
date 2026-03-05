import os
import io
import time
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from google.cloud import storage

BUCKET_NAME = "properlytic-raw-data"
PREFIX = "inference/jurisdiction=acs_nationwide/"
SCHEMA = "forecast_20260220_7f31c6e4"

DB_DSN = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"

def ts():
    return time.strftime("%H:%M:%S")

def main():
    print(f"[{ts()}] Connecting to GCS...")
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    
    blobs = list(bucket.list_blobs(prefix=PREFIX))
    hist_blobs = [b for b in blobs if "history" in b.name and b.name.endswith(".parquet")]
    print(f"[{ts()}] Found {len(hist_blobs)} history parquet chunks.")
    
    if not hist_blobs:
        print(f"[{ts()}] No history data found. Exiting.")
        return

    print(f"[{ts()}] Connecting to Supabase...")
    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    
    total_rows = 0
    for idx, b in enumerate(hist_blobs):
        print(f"[{ts()}] [{idx+1}/{len(hist_blobs)}] Processing {b.name}...")
        
        data = b.download_as_bytes()
        df = pd.read_parquet(io.BytesIO(data))
        
        if "acct" not in df.columns or "year" not in df.columns:
            print(f"  Skipping... malformed chunk.")
            continue
            
        print(f"   Schema: {df.columns.tolist()}")
        print(df.head(2))
        break

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
