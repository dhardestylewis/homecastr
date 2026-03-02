"""Check exactly what's in Supabase for HCAD backtest origin years."""
import psycopg2

DB_URL = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"
SCHEMA = "forecast_20260220_7f31c6e4"

conn = psycopg2.connect(DB_URL, connect_timeout=30)
conn.autocommit = True
cur = conn.cursor()
cur.execute("SET statement_timeout = 30000")

# Get all distinct combos for 2021-2023
cur.execute(f"""
    SELECT DISTINCT variant_id, series_kind, origin_year, forecast_year
    FROM "{SCHEMA}"."metrics_parcel_forecast"
    WHERE origin_year IN (2021, 2022, 2023)
    ORDER BY origin_year, variant_id, forecast_year
    LIMIT 50
""")
rows = cur.fetchall()
print(f"Found {len(rows)} distinct combos:")
for r in rows:
    print(f"  variant_id={r[0]!r:30s}  series_kind={r[1]!r:12s}  origin={r[2]}  forecast_year={r[3]}")

# Also sample a few actual rows to check acct format
cur.execute(f"""
    SELECT acct, origin_year, forecast_year, p50, variant_id, series_kind
    FROM "{SCHEMA}"."metrics_parcel_forecast"
    WHERE origin_year = 2021
    LIMIT 5
""")
sample = cur.fetchall()
print(f"\nSample rows (origin=2021):")
for r in sample:
    print(f"  acct={r[0]!r}  origin={r[1]}  forecast_year={r[2]}  p50={r[3]:.0f}  variant={r[4]!r}  kind={r[5]!r}")

conn.close()
print("Done.")
