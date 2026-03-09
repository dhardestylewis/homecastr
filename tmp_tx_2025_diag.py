"""
Diagnose the 2025 TX state-level outlier:
- What does the full row look like (jurisdiction, variant_id, series_kind)?
- What do the underlying ZCTA/tract rows look like for TX in 2024 vs 2025?
- How does the state agg compare to what a fresh avg would give us?
"""
import psycopg2

DST = "forecast_20260220_7f31c6e4"
DB_URL = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"
conn = psycopg2.connect(DB_URL, connect_timeout=30)
conn.autocommit = True
cur = conn.cursor()
cur.execute("SET statement_timeout = 60000")

# 1. Full content of 2025 TX history row
print("=== 1. metrics_state_history 2025 TX full row ===")
cur.execute(f"SELECT * FROM {DST}.metrics_state_history WHERE state_fips = '48' ORDER BY year DESC LIMIT 5")
cols = [d[0] for d in cur.description]
print("  cols:", cols)
for r in cur.fetchall():
    print("  ", dict(zip(cols, r)))

# 2. Full content of 2025 TX forecast rows
print("\n=== 2. metrics_state_forecast 2025 TX rows ===")
cur.execute(f"SELECT * FROM {DST}.metrics_state_forecast WHERE state_fips = '48' ORDER BY forecast_year, horizon_m")
cols = [d[0] for d in cur.description]
print("  cols:", cols)
for r in cur.fetchall():
    print("  ", dict(zip(cols, r)))

# 3. What do the underlying ZCTA rows look like for TX-2024 vs 2025?
print("\n=== 3. metrics_zcta_history: TX zctas, recent years ===")
cur.execute(f"""
    SELECT year, COUNT(*) as n_zcta,
           ROUND(AVG(p50)::numeric, 2) as avg_p50,
           ROUND(MIN(p50)::numeric, 2) as min_p50,
           ROUND(MAX(p50)::numeric, 2) as max_p50
    FROM {DST}.metrics_zcta_history z
    JOIN public.xwalk_tract_zcta xw ON xw.zcta5 = z.zcta5
    WHERE LEFT(xw.tract_geoid20, 2) = '48'
      AND (z.series_kind = 'history' OR z.series_kind IS NULL)
    GROUP BY year
    ORDER BY year DESC
    LIMIT 8
""")
for r in cur.fetchall():
    print(f"  year={r[0]}  n_zcta={r[1]}  avg_p50={r[2]}  min={r[3]}  max={r[4]}")

# 4. What do the underlying ZCTA forecast rows look like for TX?
print("\n=== 4. metrics_zcta_forecast: TX zctas, by forecast_year + horizon_m ===")
cur.execute(f"""
    SELECT z.forecast_year, z.horizon_m, COUNT(*) as n_zcta,
           ROUND(AVG(z.p50)::numeric, 2) as avg_p50,
           ROUND(MIN(z.p50)::numeric, 2) as min_p50,
           ROUND(MAX(z.p50)::numeric, 2) as max_p50
    FROM {DST}.metrics_zcta_forecast z
    JOIN public.xwalk_tract_zcta xw ON xw.zcta5 = z.zcta5
    WHERE LEFT(xw.tract_geoid20, 2) = '48'
      AND (z.series_kind = 'forecast' OR z.series_kind IS NULL)
    GROUP BY z.forecast_year, z.horizon_m
    ORDER BY z.forecast_year, z.horizon_m
""")
rows = cur.fetchall()
if rows:
    for r in rows:
        print(f"  fc_year={r[0]}  hz={r[1]}  n_zcta={r[2]}  avg_p50={r[3]}  min={r[4]}  max={r[5]}")
else:
    print("  (no zcta forecast rows for TX)")

# 5. Check if there are any tract forecast rows for TX with 2025 origin
print("\n=== 5. metrics_tract_forecast: TX tracts, origin_year=2025 sample ===")
try:
    cur.execute(f"""
        SELECT origin_year, horizon_m, COUNT(*) as n,
               ROUND(AVG(p50)::numeric, 2) as avg_p50
        FROM {DST}.metrics_tract_forecast
        WHERE LEFT(tract_geoid20, 2) = '48'
          AND origin_year = 2025
        GROUP BY origin_year, horizon_m
        ORDER BY horizon_m
        LIMIT 10
    """)
    rows = cur.fetchall()
    if rows:
        for r in rows:
            print(f"  origin={r[0]}  hz={r[1]}  n={r[2]}  avg_p50={r[3]}")
    else:
        print("  (no 2025 origin tract forecast rows for TX)")
except Exception as e:
    print(f"  Error (origin_year): {e}")
    conn.rollback()
    # try forecast_year
    cur.execute(f"""
        SELECT forecast_year, horizon_m, COUNT(*) as n,
               ROUND(AVG(p50)::numeric, 2) as avg_p50
        FROM {DST}.metrics_tract_forecast
        WHERE LEFT(tract_geoid20, 2) = '48'
          AND forecast_year = 2025
        GROUP BY forecast_year, horizon_m
        ORDER BY horizon_m
        LIMIT 10
    """)
    rows = cur.fetchall()
    for r in rows:
        print(f"  fc_year={r[0]}  hz={r[1]}  n={r[2]}  avg_p50={r[3]}")

conn.close()
print("\nDone.")
