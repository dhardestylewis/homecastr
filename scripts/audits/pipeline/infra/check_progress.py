"""Check TxGIO transfer progress and parcel_ladder for Seattle."""
import psycopg2
from google.cloud import storage

# === TxGIO GCS Progress ===
c = storage.Client()
bucket = c.bucket('properlytic-raw-data')

# Expected file counts per year (from --list-only runs)
# 2019: ~2372 files, 27.1 GB / 2024: ~2017 files, 30 GB (approx similar for others)
expected = {2019: 2372, 2021: 2400, 2022: 2400, 2023: 2400, 2024: 2017, 2025: 2400}

print("=== TxGIO GCS Progress ===")
for year in [2019, 2021, 2022, 2023, 2024, 2025]:
    blobs = list(bucket.list_blobs(prefix=f'geo/txgio_land_parcels/{year}/'))
    total = sum(b.size for b in blobs)
    exp = expected.get(year, 2400)
    pct = 100.0 * len(blobs) / exp if exp else 0
    print(f"  {year}: {len(blobs):>5}/{exp} files ({pct:.1f}%), {total/1e9:.2f} GB")

# === Check inference output in GCS ===
print("\n=== Inference Output in GCS ===")
for prefix in ['output/', 'inference_output/']:
    blobs = list(bucket.list_blobs(prefix=prefix, max_results=5))
    if blobs:
        print(f"  {prefix}: found files")
        for b in blobs[:5]:
            print(f"    {b.name} ({b.size/1e6:.1f}MB)")

# === Check Seattle panel + checkpoints ===
print("\n=== Seattle Assets in GCS ===")
for prefix in ['checkpoints/seattle_wa/', 'panel/jurisdiction=seattle_wa/']:
    blobs = list(bucket.list_blobs(prefix=prefix))
    total = sum(b.size for b in blobs)
    print(f"  {prefix}: {len(blobs)} files, {total/1e6:.1f} MB")
    for b in blobs:
        print(f"    {b.name} ({b.size/1e6:.1f}MB)")

# === Check parcel_ladder_v1 for WA ===
print("\n=== Parcel Ladder for WA ===")
conn = psycopg2.connect('postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require')
cur = conn.cursor()
cur.execute("SET statement_timeout = 15000")
cur.execute("SELECT count(1) FROM forecast_20260220_7f31c6e4.parcel_ladder_v1 WHERE geoid_tract20 LIKE '53%' LIMIT 1")
wa_count = cur.fetchone()[0]
print(f"  WA tracts in parcel_ladder_v1: {wa_count}")
cur.execute("SELECT DISTINCT substring(geoid_tract20 from 1 for 2) as st FROM forecast_20260220_7f31c6e4.parcel_ladder_v1 LIMIT 10")
states = [r[0] for r in cur.fetchall()]
print(f"  State prefixes in parcel_ladder_v1: {states}")
conn.close()
