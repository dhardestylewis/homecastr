import psycopg2

DST = "forecast_20260220_7f31c6e4"
DB_URL = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"
conn = psycopg2.connect(DB_URL, connect_timeout=30)
cur = conn.cursor()
cur.execute("SET statement_timeout = 60000")

# Check when these 2025 TX rows were actually inserted
cur.execute(f"""
    SELECT min(inserted_at), max(inserted_at), count(*)
    FROM {DST}.metrics_state_forecast 
    WHERE forecast_year = 2025 AND state_fips = '48'
""")
print('State Forecast Insert Window:', cur.fetchone())

cur.execute(f"""
    SELECT min(inserted_at), max(inserted_at), count(*)
    FROM {DST}.metrics_state_history 
    WHERE year = 2025 AND state_fips = '48'
""")
print('State History Insert Window:', cur.fetchone())

# Just to be absolutely sure - are there *any* tracts with origin_year=2025 that were inserted recently?
try:
    cur.execute(f"""
        SELECT min(inserted_at), max(inserted_at), count(*)
        FROM {DST}.metrics_tract_forecast
        WHERE origin_year = 2025 AND LEFT(tract_geoid20, 2) = '48'
    """)
    print('Tract Forecast (TX, 2025) Insert Window:', cur.fetchone())
except Exception as e:
    print(e)
    conn.rollback()
    
# Are any TX inference rows being inserted *right now*?
try:
    cur.execute(f"""
        SELECT min(inserted_at), max(inserted_at), count(*)
        FROM {DST}.metrics_tract_forecast
        WHERE inserted_at > NOW() - INTERVAL '4 hours'
    """)
    print('Recent Tract Forecast Inserts (Last 4h):', cur.fetchone())
except Exception as e:
    print(e)

conn.close()
