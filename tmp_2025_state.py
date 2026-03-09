import psycopg2
DST = "forecast_20260220_7f31c6e4"
DB_URL = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"
conn = psycopg2.connect(DB_URL, connect_timeout=30)
cur = conn.cursor()
cur.execute("SET statement_timeout = 30000")
cur.execute(f"SELECT state_fips, year, p50 FROM {DST}.metrics_state_history WHERE year = 2025")
print("History 2025:", cur.fetchall())
cur.execute(f"SELECT DISTINCT state_fips FROM {DST}.metrics_state_forecast WHERE forecast_year = 2025 LIMIT 5")
print("Forecast 2025 states:", cur.fetchall())
conn.close()
