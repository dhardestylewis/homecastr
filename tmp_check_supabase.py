"""Check acct format overlap — sample from Supabase forecast rows vs HCAD panel."""
import psycopg2

DB_URL = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"
SCHEMA = "forecast_20260220_7f31c6e4"

conn = psycopg2.connect(DB_URL, connect_timeout=30)
conn.autocommit = True
cur = conn.cursor()
cur.execute("SET statement_timeout = 30000")

# Sample some acct values from the backtest rows
cur.execute(f"""
    SELECT acct, origin_year, forecast_year, variant_id, series_kind
    FROM "{SCHEMA}"."metrics_parcel_forecast"
    WHERE series_kind = 'backtest'
    LIMIT 10
""")
rows = cur.fetchall()
print("Sample backtest rows from Supabase:")
for r in rows:
    print(f"  acct={r[0]!r:20s}  origin={r[1]}  forecast_year={r[2]}  variant={r[3]!r:40s}  kind={r[4]!r}")

# Also sample forecast rows
cur.execute(f"""
    SELECT acct, origin_year, forecast_year, variant_id, series_kind
    FROM "{SCHEMA}"."metrics_parcel_forecast"
    WHERE series_kind = 'forecast'
    LIMIT 10
""")
rows2 = cur.fetchall()
print("\nSample forecast rows from Supabase:")
for r in rows2:
    print(f"  acct={r[0]!r:20s}  origin={r[1]}  forecast_year={r[2]}  variant={r[3]!r:40s}  kind={r[4]!r}")

# Check a generic LIMIT to see all kinds
cur.execute(f"""
    SELECT acct, origin_year, forecast_year, variant_id, series_kind
    FROM "{SCHEMA}"."metrics_parcel_forecast"
    LIMIT 10
""")
rows3 = cur.fetchall()
print("\nGeneric LIMIT 10:")
for r in rows3:
    print(f"  acct={r[0]!r:20s}  origin={r[1]}  forecast_year={r[2]}  variant={r[3]!r:40s}  kind={r[4]!r}")

conn.close()
print("Done.")
