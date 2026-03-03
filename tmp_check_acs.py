import psycopg2

db_url = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"
conn = psycopg2.connect(db_url)
conn.autocommit = True
cur = conn.cursor()
cur.execute("SET statement_timeout = 0")

SCHEMA = "forecast_20260220_7f31c6e4"

# 1. Check what jurisdictions/run_ids are in parcel_forecast
print("=== distinct jurisdictions in parcel_forecast ===")
cur.execute(f"""SELECT COALESCE(jurisdiction, 'NULL'), count(*) 
    FROM {SCHEMA}.metrics_parcel_forecast GROUP BY 1 ORDER BY 2 DESC LIMIT 10""")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]:,}")

# 2. Sample ACS accounts
print("\n=== sample ACS accts in parcel_forecast ===")
cur.execute(f"""SELECT DISTINCT acct FROM {SCHEMA}.metrics_parcel_forecast 
    WHERE jurisdiction = 'acs_nationwide' LIMIT 10""")
acs_accts = [r[0] for r in cur.fetchall()]
print(f"  sample: {acs_accts}")

# 3. Check if they exist in parcel_ladder_v1
if acs_accts:
    print("\n=== ACS accts in parcel_ladder_v1 ===")
    cur.execute("SELECT acct FROM public.parcel_ladder_v1 WHERE acct = ANY(%s)", (acs_accts,))
    found = [r[0] for r in cur.fetchall()]
    print(f"  found {len(found)}/{len(acs_accts)} in ladder: {found}")
    
    # Total ACS accounts in parcel_forecast
    cur.execute(f"SELECT count(DISTINCT acct) FROM {SCHEMA}.metrics_parcel_forecast WHERE jurisdiction = 'acs_nationwide'")
    print(f"\n  total ACS accounts in parcel_forecast: {cur.fetchone()[0]:,}")
    
    # How many of those are in parcel_ladder_v1?
    cur.execute(f"""SELECT count(DISTINCT mp.acct) 
        FROM {SCHEMA}.metrics_parcel_forecast mp 
        JOIN public.parcel_ladder_v1 pl ON pl.acct = mp.acct 
        WHERE mp.jurisdiction = 'acs_nationwide'""")
    print(f"  ACS accounts matched in parcel_ladder_v1: {cur.fetchone()[0]:,}")

# 4. Check total parcel_ladder_v1 row count
print("\n=== parcel_ladder_v1 total ===")
cur.execute("SELECT count(*) FROM public.parcel_ladder_v1")
print(f"  total rows: {cur.fetchone()[0]:,}")

# 5. Check HCAD parcel_forecast counts too
print("\n=== HCAD in parcel_forecast ===")
cur.execute(f"SELECT count(DISTINCT acct) FROM {SCHEMA}.metrics_parcel_forecast WHERE jurisdiction IN ('hcad', 'hcad_houston')")
print(f"  distinct accts: {cur.fetchone()[0]:,}")

conn.close()
