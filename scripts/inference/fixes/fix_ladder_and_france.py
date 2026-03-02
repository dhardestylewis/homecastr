"""Populate France parcel_ladder in batches to avoid timeout."""
import psycopg2

DB_URL = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"
SCHEMA = "forecast_20260220_7f31c6e4"

conn = psycopg2.connect(DB_URL)
conn.autocommit = True
cur = conn.cursor()
cur.execute("SET statement_timeout = '600000'")  # 10 minutes

# Get distinct France accounts 
print("=== Getting France accounts ===")
cur.execute(f"SELECT DISTINCT acct FROM {SCHEMA}.metrics_parcel_forecast WHERE jurisdiction = 'france_dvf'")
accts = [r[0] for r in cur.fetchall()]
print(f"  {len(accts):,} unique France accounts")

# Insert in batches
batch_size = 500
total = 0
for i in range(0, len(accts), batch_size):
    batch = accts[i:i+batch_size]
    values = []
    for acct in batch:
        commune = acct[:5] if len(acct) >= 5 else acct
        dept = acct[:2] if len(acct) >= 2 else acct
        values.append(cur.mogrify(
            "(%s, %s, %s, %s, %s)",
            (acct, commune, dept, 'france_dvf', 2024)
        ).decode())
    
    sql = f"""INSERT INTO public.parcel_ladder_v1 (acct, tract_geoid20, zcta5, jurisdiction, gis_year)
              VALUES {','.join(values)}
              ON CONFLICT (acct) DO UPDATE SET 
                tract_geoid20 = EXCLUDED.tract_geoid20,
                zcta5 = EXCLUDED.zcta5,
                jurisdiction = EXCLUDED.jurisdiction"""
    cur.execute(sql)
    total += len(batch)
    if total % 5000 == 0 or total == len(accts):
        print(f"  {total:,}/{len(accts):,} ({100*total/len(accts):.0f}%)")

print(f"\n✅ {total:,} France accounts in parcel_ladder_v1")

# Verify
cur.execute("SELECT jurisdiction, count(1) FROM public.parcel_ladder_v1 GROUP BY jurisdiction")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]:,}")
conn.close()
