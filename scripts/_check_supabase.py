"""Create jurisdiction index on metrics_parcel_forecast. 
Uses direct connection (not pooler) with long timeout."""
import psycopg2

# Use direct connection URL for DDL operations (not the pooler)
CONN = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"
SCHEMA = "forecast_20260220_7f31c6e4"

conn = psycopg2.connect(CONN)
conn.autocommit = True
cur = conn.cursor()

# Set long timeout for index creation (10 min)
cur.execute("SET statement_timeout = '600000'")

# Check existing indexes first
print("=== Existing indexes ===")
cur.execute("""
    SELECT indexname, indexdef
    FROM pg_indexes
    WHERE schemaname = %s AND tablename = 'metrics_parcel_forecast'
""", (SCHEMA,))
for name, defn in cur.fetchall():
    print(f"  {name}")

# Check table size
cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.metrics_parcel_forecast")
total = cur.fetchone()[0]
print(f"\n  Total rows: {total:,}")

# Create the index (non-concurrently since we need it NOW)
print("\n=== Creating jurisdiction index (may take a few minutes) ===")
try:
    cur.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_mpf_jurisdiction
        ON {SCHEMA}.metrics_parcel_forecast (jurisdiction)
    """)
    print("  ✅ Index created!")
except Exception as e:
    print(f"  ❌ Error: {e}")

# Test the query speed after index
print("\n=== Testing query speed ===")
import time
t0 = time.time()
cur.execute(f"""
    SELECT COUNT(*) FROM {SCHEMA}.metrics_parcel_forecast
    WHERE jurisdiction = 'hcad'
""")
cnt = cur.fetchone()[0]
print(f"  HCAD count: {cnt:,} in {time.time()-t0:.1f}s")

t0 = time.time()
cur.execute(f"""
    SELECT COUNT(*) FROM {SCHEMA}.metrics_parcel_forecast
    WHERE jurisdiction = 'seattle_wa'
""")
cnt = cur.fetchone()[0]
print(f"  Seattle count: {cnt:,} in {time.time()-t0:.1f}s")

t0 = time.time()
cur.execute(f"""
    SELECT COUNT(*) FROM {SCHEMA}.metrics_parcel_forecast
    WHERE jurisdiction = 'france_dvf'
""")
cnt = cur.fetchone()[0]
print(f"  France count: {cnt:,} in {time.time()-t0:.1f}s")

conn.close()
print("\nDone!")
