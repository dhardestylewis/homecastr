import os
import psycopg2, psycopg2.extras
from psycopg2.extensions import register_adapter, AsIs
from dotenv import load_dotenv
import pyarrow.parquet as pq
import urllib.request
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

def addapt_numpy_float64(numpy_float64):
    import numpy as np
    if np.isnan(numpy_float64):
        return AsIs('NULL')
    return AsIs(numpy_float64)

def get_db_connection():
    load_dotenv('.env.local')
    db_url = ""
    for key in ["SUPABASE_DB_URL", "POSTGRES_URL_NON_POOLING", "POSTGRES_URL"]:
        raw = os.environ.get(key, "").strip()
        if raw:
            parts = urlsplit(raw)
            q = dict(parse_qsl(parts.query, keep_blank_values=True))
            allowed = {"sslmode": q.get("sslmode")} if "sslmode" in q else {}
            db_url = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(allowed), parts.fragment)).strip()
            break
    if not db_url:
        raise RuntimeError("No database URL found in environment")
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    return conn

def main():
    try:
        import numpy as np
        register_adapter(np.float64, addapt_numpy_float64)
    except Exception:
        pass

    conn = get_db_connection()
    local_parquet = "florida_dor_panel_tmp.parquet"
    if not os.path.exists(local_parquet):
        print("Downloading florida_dor_panel.parquet...")
        from google.cloud import storage
        client = storage.Client()
        bucket = client.bucket("properlytic-raw-data")
        blob = bucket.blob("panels/florida_dor_panel.parquet")
        blob.download_to_filename(local_parquet)
        print(f"Downloaded {os.path.getsize(local_parquet)/1e6:.1f} MB.")

    print("Streaming parquet batches to Supabase...")
    buffer_deg = 0.00015
    parquet_file = pq.ParquetFile(local_parquet)
    
    inserted = 0
    total_processed = 0

    with conn.cursor() as cur:
        for batch in parquet_file.iter_batches(batch_size=200000, columns=["global_parcel_id", "latitude", "longitude"]):
            df = batch.to_pandas().dropna(subset=["latitude", "longitude"]).drop_duplicates(subset=["global_parcel_id"])
            total_processed += len(df)
            
            db_batch = []
            for row in df.itertuples(index=False):
                acct = str(row.global_parcel_id)
                lat = row.latitude
                lon = row.longitude
                db_batch.append((
                    acct, 
                    lon - buffer_deg, 
                    lat - buffer_deg, 
                    lon + buffer_deg, 
                    lat + buffer_deg
                ))
                
            if db_batch:
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO public.geo_parcel_poly (acct, geom) VALUES %s ON CONFLICT (acct) DO NOTHING",
                    db_batch,
                    template="(%s, ST_Multi(ST_MakeEnvelope(%s, %s, %s, %s, 4326)))",
                    page_size=5000
                )
                inserted += len(db_batch)
                
            print(f"Processed valid rows: {total_processed:,} | Sent to DB: {inserted:,} (including dupes ignored by Postgres)")

    print(f"✅ Successfully finished stream processing. Final insertions attempted: {inserted:,}")

if __name__ == "__main__":
    main()
