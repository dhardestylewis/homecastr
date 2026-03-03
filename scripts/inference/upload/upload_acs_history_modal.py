import modal
import os
import time

app = modal.App("upload-acs-history-supabase")
vol = modal.Volume.from_name("inference-outputs")
image = modal.Image.debian_slim(python_version="3.11").pip_install(["psycopg2-binary", "pandas", "pyarrow"])
supabase_secret = modal.Secret.from_name("supabase-creds", required_keys=["SUPABASE_DB_URL"])

def ts():
    from datetime import datetime
    return datetime.now().strftime("%H:%M:%S")

@app.function(image=image, volumes={"/data": vol}, secrets=[supabase_secret], timeout=3600)
def upload_history():
    import psycopg2
    from psycopg2.extras import execute_values
    import pandas as pd
    
    print(f"[{ts()}] Connecting to Supabase...")
    db_url = os.environ.get("SUPABASE_DB_URL", "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require")
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    
    base_dir = "/data"
        
    hist_files = []
    for root, _, files in os.walk(base_dir):
        for f in files:
            if "history" in f and f.endswith(".parquet"):
                hist_files.append(os.path.join(root, f))
                
    print(f"[{ts()}] Found {len(hist_files)} history chunks.")
    if not hist_files:
        return
        
    cur = conn.cursor()
    total_tract_rows = 0
    total_block_rows = 0
    total_zcta_rows = 0
    
    for idx, fp in enumerate(hist_files):
        print(f"[{ts()}] ({idx+1}/{len(hist_files)}) Reading {fp}")
        df = pd.read_parquet(fp)
        
        if "acct" not in df.columns or "year" not in df.columns or "p50" not in df.columns:
            continue
            
        df['acct_len'] = df['acct'].str.len()
            
        # 1. Tracts (length 11) -> metrics_tract_history
        df_tract = df[df['acct_len'] == 11].copy()
        if not df_tract.empty:
            records = df_tract[['acct', 'year', 'value', 'p50']].assign(jurisdiction='acs_nationwide').values.tolist()
            query = """
                INSERT INTO forecast_20260220_7f31c6e4.metrics_tract_history 
                (tract_geoid20, year, value, p50, jurisdiction)
                VALUES %s
                ON CONFLICT DO NOTHING
            """
            execute_values(cur, query, records, page_size=1000)
            total_tract_rows += len(records)
            
        # 2. Blocks (length 15) -> metrics_tabblock_history
        df_block = df[df['acct_len'] == 15].copy()
        if not df_block.empty:
            records_block = df_block[['acct', 'year', 'value', 'p50']].assign(jurisdiction='acs_nationwide').values.tolist()
            query_block = """
                INSERT INTO forecast_20260220_7f31c6e4.metrics_tabblock_history 
                (tabblock_geoid20, year, value, p50, jurisdiction)
                VALUES %s
                ON CONFLICT DO NOTHING
            """
            execute_values(cur, query_block, records_block, page_size=1000)
            total_block_rows += len(records_block)
            
        # 3. ZCTA (length 5) -> metrics_zcta_history
        df_zcta = df[df['acct_len'] == 5].copy()
        if not df_zcta.empty:
            records_zcta = df_zcta[['acct', 'year', 'value', 'p50']].assign(jurisdiction='acs_nationwide').values.tolist()
            query_zcta = """
                INSERT INTO forecast_20260220_7f31c6e4.metrics_zcta_history 
                (zcta5, year, value, p50, jurisdiction)
                VALUES %s
                ON CONFLICT DO NOTHING
            """
            execute_values(cur, query_zcta, records_zcta, page_size=1000)
            total_zcta_rows += len(records_zcta)
            
    print(f"[{ts()}] Upload complete. {total_tract_rows} tracts, {total_block_rows} blocks, {total_zcta_rows} zcta rows pushed.")
    cur.close()
    conn.close()

@app.local_entrypoint()
def main():
    upload_history.remote()
