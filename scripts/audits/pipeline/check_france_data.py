"""Quick check for france_dvf data in Supabase."""
import psycopg2

CONN = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"
SCHEMA = "forecast_20260220_7f31c6e4"

conn = psycopg2.connect(CONN)
cur = conn.cursor()
cur.execute("SET statement_timeout = 30000")

# Check for france_dvf in forecast
cur.execute(f"SELECT acct, jurisdiction, origin_year, horizon_m, value, p50 FROM {SCHEMA}.metrics_parcel_forecast WHERE jurisdiction = 'france_dvf' LIMIT 5")
rows = cur.fetchall()
print(f"france_dvf forecast rows (sample): {len(rows)}")
for r in rows:
    print(f"  {r}")

# Check for france_dvf in history
cur.execute(f"SELECT acct, jurisdiction, year, value, p50 FROM {SCHEMA}.metrics_parcel_history WHERE jurisdiction = 'france_dvf' LIMIT 5")
rows2 = cur.fetchall()
print(f"\nfrance_dvf history rows (sample): {len(rows2)}")
for r in rows2:
    print(f"  {r}")

# Check inference_runs for france
cur.execute(f"SELECT run_id, level_name, mode, status, jurisdiction FROM {SCHEMA}.inference_runs WHERE jurisdiction = 'france_dvf' OR run_id LIKE '%france%' LIMIT 10")
rows3 = cur.fetchall()
print(f"\nfrance_dvf inference_runs: {len(rows3)}")
for r in rows3:
    print(f"  {r}")

# Also check distinct jurisdictions (quick - just from a small sample)
cur.execute(f"SELECT DISTINCT jurisdiction FROM {SCHEMA}.metrics_parcel_forecast LIMIT 20")
rows4 = cur.fetchall()
print(f"\nDistinct jurisdictions in forecast (sample): {[r[0] for r in rows4]}")

conn.close()
