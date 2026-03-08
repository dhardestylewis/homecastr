"""Find where the '1147360110035' format accts came from in the forecast table."""
import psycopg2

CONN = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"
SCHEMA = "forecast_20260220_7f31c6e4"

conn = psycopg2.connect(CONN)
cur = conn.cursor()
cur.execute("SET statement_timeout='15000'")

# Get a sample of HCAD forecast accts
print("=== Sample HCAD forecast accts ===")
cur.execute(f"SELECT acct, origin_year, forecast_year, run_id, as_of_date FROM {SCHEMA}.metrics_parcel_forecast WHERE jurisdiction='hcad' LIMIT 10")
for r in cur.fetchall():
    print(f"  acct={r[0]} origin={r[1]} fy={r[2]} run_id={r[3]} date={r[4]}")

# Also check 'hcad_houston' 
print("\n=== Sample hcad_houston forecast accts ===")
cur.execute(f"SELECT acct, origin_year, forecast_year, run_id, as_of_date FROM {SCHEMA}.metrics_parcel_forecast WHERE jurisdiction='hcad_houston' LIMIT 10")
rows = cur.fetchall()
if rows:
    for r in rows:
        print(f"  acct={r[0]} origin={r[1]} fy={r[2]} run_id={r[3]} date={r[4]}")
else:
    print("  No 'hcad_houston' rows found!")

# Check run_ids for hcad
print("\n=== Distinct run_ids for HCAD ===")
try:
    cur.execute(f"SELECT DISTINCT run_id FROM {SCHEMA}.metrics_parcel_forecast WHERE jurisdiction='hcad' AND run_id IS NOT NULL LIMIT 10")
    for r in cur.fetchall():
        print(f"  {r[0]}")
except:
    print("  Query timed out")
    conn.rollback()

# Check panel parquet for 1147360110035
print("\n=== Checking panel for acct '1147360110035' ===")
import pandas as pd
from google.cloud import storage
import json, io

c = storage.Client.from_service_account_info(json.load(open("scripts/.gcs-key.json")))
b = c.bucket("properlytic-raw-data")
blob = b.blob("panel/jurisdiction=hcad_houston/part.parquet")
buf = io.BytesIO()
blob.download_to_file(buf)
buf.seek(0)
panel = pd.read_parquet(buf, columns=['acct'])
has_it = '1147360110035' in panel['acct'].astype(str).values
print(f"  '1147360110035' in panel: {has_it}")
print(f"  Panel sample accts: {panel['acct'].astype(str).head(5).tolist()}")
print(f"  Panel acct lengths: {panel['acct'].astype(str).str.len().value_counts().head()}")

conn.close()
