"""Check parcel_ladder_v1 schema and HCAD sample for structure."""
import psycopg2
CONN = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"
conn = psycopg2.connect(CONN)
cur = conn.cursor()
cur.execute("SET statement_timeout = 15000")

# Schema
cur.execute("""SELECT column_name, data_type FROM information_schema.columns 
    WHERE table_schema = 'public' AND table_name = 'parcel_ladder_v1' ORDER BY ordinal_position""")
print("=== parcel_ladder_v1 columns ===")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]}")

# Sample row
cur.execute("SELECT * FROM public.parcel_ladder_v1 LIMIT 3")
cols = [d[0] for d in cur.description]
rows = cur.fetchall()
print(f"\n=== Sample rows ({cols}) ===")
for r in rows:
    print(f"  {r}")

# France accounts sample from parcel data
print("\n=== France account sample ===")
cur.execute(f"SELECT DISTINCT acct FROM forecast_20260220_7f31c6e4.metrics_parcel_forecast WHERE jurisdiction = 'france_dvf' LIMIT 5")
fr_accts = [r[0] for r in cur.fetchall()]
print(f"  France accounts: {fr_accts}")

# WA/Seattle accounts (from parcel history - check if any exist)
print("\n=== Seattle account check ===")
cur.execute(f"SELECT DISTINCT acct FROM forecast_20260220_7f31c6e4.metrics_parcel_forecast WHERE jurisdiction = 'seattle_wa' LIMIT 5")
wa_accts = [r[0] for r in cur.fetchall()]
print(f"  Seattle accounts: {wa_accts}")

conn.close()
