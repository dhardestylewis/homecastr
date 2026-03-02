"""Quick check Houston data — fast queries only."""
import psycopg2

CONN = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"
SCHEMA = "forecast_20260220_7f31c6e4"

conn = psycopg2.connect(CONN)
cur = conn.cursor()
cur.execute("SET statement_timeout='15000'")

# List all tables in the schema
print(f"=== Tables in {SCHEMA} ===")
cur.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{SCHEMA}'")
for r in cur.fetchall():
    print(f"  {r[0]}")

# Approximate counts
print("\n=== Approx counts (pg_class) ===")
for tbl in ['metrics_parcel_forecast', 'metrics_tract_forecast', 'metrics_tabblock_forecast']:
    cur.execute(f"SELECT reltuples::bigint FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid WHERE c.relname='{tbl}' AND n.nspname='{SCHEMA}'")
    r = cur.fetchone()
    print(f"  {tbl}: ~{r[0]:,}" if r else f"  {tbl}: not found")

# Public tables
print("\n=== Public tables with 'ladder' or 'parcel' ===")
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE '%parcel%' OR table_name LIKE '%ladder%'")
for r in cur.fetchall():
    print(f"  {r[0]}")

# Parcel ladder counts by jurisdiction
print("\n=== Parcel ladder v1 ===")
try:
    cur.execute("SELECT jurisdiction, COUNT(*) FROM public.parcel_ladder_v1 GROUP BY jurisdiction ORDER BY 2 DESC")
    for r in cur.fetchall():
        print(f"  {r[0]}: {r[1]:,}")
except Exception as e:
    print(f"  Error: {str(e)[:100]}")
    conn.rollback()

# Sample HCAD
print("\n=== Sample HCAD parcel forecasts ===")
try:
    cur.execute(f"SELECT acct, origin_year, forecast_year, series_kind, p50 FROM {SCHEMA}.metrics_parcel_forecast WHERE jurisdiction='hcad_houston' LIMIT 5")
    for r in cur.fetchall():
        print(f"  acct={r[0]} origin={r[1]} fyr={r[2]} kind={r[3]} p50={r[4]}")
    if cur.rowcount == 0:
        print("  (no rows)")
except Exception as e:
    print(f"  Error: {str(e)[:100]}")
    conn.rollback()

# Tract forecast sample
print("\n=== Sample tract forecasts ===")
try:
    cur.execute(f"SELECT jurisdiction, COUNT(*) FROM {SCHEMA}.metrics_tract_forecast GROUP BY jurisdiction ORDER BY 2 DESC LIMIT 10")
    for r in cur.fetchall():
        print(f"  {r[0]}: {r[1]:,}")
    if cur.rowcount == 0:
        print("  (empty)")
except Exception as e:
    print(f"  Error: {str(e)[:100]}")
    conn.rollback()

conn.close()
