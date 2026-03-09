import psycopg2

DB_URL = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"
conn = psycopg2.connect(DB_URL, connect_timeout=30)
cur = conn.cursor()

schemas = ["forecast_queue", "forecast_20260220_7f31c6e4"]

for s in schemas:
    print(f"\n=== Schema: {s} ===")
    
    # TX tracts origin_year=2025 h12
    try:
        cur.execute(f"""
            SELECT COUNT(*), ROUND(AVG(p50)::numeric, 0), ROUND(MAX(p50)::numeric, 0)
            FROM {s}.metrics_tract_forecast
            WHERE origin_year = 2025 AND LEFT(tract_geoid20, 2) = '48' AND horizon_m = 12
        """)
        print("  TX tracts o2025 h12:", cur.fetchone())
    except Exception as e:
        print("  TX tracts o2025 h12 ERROR:", e)
        conn.rollback()

    # Total tracts origin_year=2025
    try:
        cur.execute(f"SELECT COUNT(*) FROM {s}.metrics_tract_forecast WHERE origin_year = 2025")
        print("  Total tracts o2025:", cur.fetchone())
    except Exception as e:
        print("  Total tracts o2025 ERROR:", e)
        conn.rollback()

    # Total tracts origin_year=2024
    try:
        cur.execute(f"SELECT COUNT(*) FROM {s}.metrics_tract_forecast WHERE origin_year = 2024")
        print("  Total tracts o2024:", cur.fetchone())
    except Exception as e:
        print("  Total tracts o2024 ERROR:", e)
        conn.rollback()

    # Check if state tables exist
    try:
        cur.execute(f"SELECT COUNT(*) FROM {s}.metrics_state_forecast WHERE state_fips = '48' AND forecast_year = 2025")
        print("  State forecast TX 2025:", cur.fetchone())
    except Exception as e:
        print("  State forecast TX 2025 ERROR:", e)
        conn.rollback()

conn.close()
