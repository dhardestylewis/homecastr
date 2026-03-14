"""
Build ZCTA (ZIP5) and ZIP3 aggregates from tract-level ACS forecast/history data.

Steps:
1. Download Census tract→ZCTA crosswalk
2. Upload crosswalk to Supabase
3. Aggregate tract forecast → ZCTA forecast
4. Aggregate tract history → ZCTA history
5. Create ZIP3 GIS geometries (dissolve ZIP5 by 3-digit prefix)
6. Aggregate ZCTA → ZIP3
"""
import os
import psycopg2
import urllib.request
import csv
import io
import time

CONN_STR = os.environ["SUPABASE_DB_URL"]
SCHEMA = "forecast_20260220_7f31c6e4"

def ts():
    return time.strftime("%H:%M:%S")

def main():
    conn = psycopg2.connect(CONN_STR)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SET statement_timeout = '600000'")

    # ==========================================
    # STEP 1: Download and upload tract→ZCTA crosswalk
    # ==========================================
    print(f"[{ts()}] Downloading Census tract-to-ZCTA crosswalk...")
    crosswalk_url = "https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/tab20_zcta520_tract20_natl.txt"
    response = urllib.request.urlopen(crosswalk_url)
    content = response.read().decode('utf-8')
    
    reader = csv.DictReader(io.StringIO(content), delimiter='|')
    rows = []
    for row in reader:
        tract_geoid = row.get('GEOID_TRACT_20', '').strip()
        zcta5 = row.get('GEOID_ZCTA5_20', '').strip()
        arealand_pct = row.get('AREALAND_PART', '0').strip()
        if tract_geoid and zcta5:
            rows.append((tract_geoid, zcta5, float(arealand_pct) if arealand_pct else 0))
    
    print(f"[{ts()}] Parsed {len(rows)} crosswalk entries")
    
    # Create crosswalk table
    cur.execute("DROP TABLE IF EXISTS public.xwalk_tract_zcta")
    cur.execute("""
        CREATE TABLE public.xwalk_tract_zcta (
            tract_geoid20 TEXT NOT NULL,
            zcta5 TEXT NOT NULL,
            arealand_part DOUBLE PRECISION DEFAULT 0
        )
    """)
    
    # Bulk insert
    from psycopg2.extras import execute_values
    execute_values(cur, "INSERT INTO public.xwalk_tract_zcta (tract_geoid20, zcta5, arealand_part) VALUES %s", rows, page_size=5000)
    cur.execute("CREATE INDEX idx_xwalk_tract ON public.xwalk_tract_zcta(tract_geoid20)")
    cur.execute("CREATE INDEX idx_xwalk_zcta ON public.xwalk_tract_zcta(zcta5)")
    print(f"[{ts()}] Crosswalk table created with {len(rows)} rows")

    # ==========================================
    # STEP 2: Aggregate tract forecast → ZCTA forecast
    # ==========================================
    print(f"\n[{ts()}] Aggregating tract forecast → ZCTA forecast...")
    
    # For each tract that maps to a ZCTA, take the area-weighted average
    # But many tracts map 1:1 to a ZCTA, so we use the dominant ZCTA (largest area overlap)
    cur.execute(f"""
        INSERT INTO {SCHEMA}.metrics_zcta_forecast (zcta5, origin_year, horizon_m, p10, p25, p50, p75, p90, jurisdiction)
        SELECT 
            xw.zcta5,
            tf.origin_year,
            tf.horizon_m,
            AVG(tf.p10) as p10,
            AVG(tf.p25) as p25,
            AVG(tf.p50) as p50,
            AVG(tf.p75) as p75,
            AVG(tf.p90) as p90,
            'hcad' as jurisdiction
        FROM {SCHEMA}.metrics_tract_forecast tf
        JOIN (
            SELECT DISTINCT ON (tract_geoid20) tract_geoid20, zcta5
            FROM public.xwalk_tract_zcta
            ORDER BY tract_geoid20, arealand_part DESC
        ) xw ON xw.tract_geoid20 = tf.tract_geoid20
        GROUP BY xw.zcta5, tf.origin_year, tf.horizon_m
        ON CONFLICT DO NOTHING
    """)
    zcta_forecast_rows = cur.rowcount
    print(f"[{ts()}] Inserted {zcta_forecast_rows} ZCTA forecast rows")

    # ==========================================
    # STEP 3: Aggregate tract history → ZCTA history
    # ==========================================
    print(f"\n[{ts()}] Aggregating tract history → ZCTA history...")
    cur.execute(f"""
        INSERT INTO {SCHEMA}.metrics_zcta_history (zcta5, year, value, p50, jurisdiction)
        SELECT 
            xw.zcta5,
            th.year,
            AVG(th.value) as value,
            AVG(th.p50) as p50,
            'hcad' as jurisdiction
        FROM {SCHEMA}.metrics_tract_history th
        JOIN (
            SELECT DISTINCT ON (tract_geoid20) tract_geoid20, zcta5
            FROM public.xwalk_tract_zcta
            ORDER BY tract_geoid20, arealand_part DESC
        ) xw ON xw.tract_geoid20 = th.tract_geoid20
        GROUP BY xw.zcta5, th.year
        ON CONFLICT DO NOTHING
    """)
    zcta_history_rows = cur.rowcount
    print(f"[{ts()}] Inserted {zcta_history_rows} ZCTA history rows")

    # ==========================================
    # STEP 4: Create ZIP3 GIS (dissolve ZIP5 by 3-digit prefix)
    # ==========================================
    print(f"\n[{ts()}] Creating ZIP3 GIS table...")
    cur.execute("DROP TABLE IF EXISTS public.geo_zip3_us")
    cur.execute("""
        CREATE TABLE public.geo_zip3_us AS
        SELECT 
            LEFT(zcta5, 3) as zip3,
            ST_Union(geom) as geom
        FROM public.geo_zcta20_us
        GROUP BY LEFT(zcta5, 3)
    """)
    cur.execute("SELECT count(1) FROM public.geo_zip3_us")
    zip3_count = cur.fetchone()[0]
    cur.execute("CREATE INDEX idx_geo_zip3_geom ON public.geo_zip3_us USING GIST(geom)")
    cur.execute("CREATE INDEX idx_geo_zip3_zip3 ON public.geo_zip3_us(zip3)")
    print(f"[{ts()}] Created {zip3_count} ZIP3 geometries")

    # ==========================================
    # STEP 5: Create ZIP3 forecast table and aggregate
    # ==========================================
    print(f"\n[{ts()}] Creating ZIP3 forecast/history tables and aggregating...")
    
    # Check if metrics tables for zip3 exist, create if not
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.metrics_zip3_forecast (
            zip3 TEXT NOT NULL,
            origin_year INTEGER NOT NULL,
            horizon_m INTEGER NOT NULL,
            p10 DOUBLE PRECISION,
            p25 DOUBLE PRECISION,
            p50 DOUBLE PRECISION,
            p75 DOUBLE PRECISION,
            p90 DOUBLE PRECISION,
            jurisdiction TEXT DEFAULT 'hcad',
            PRIMARY KEY (zip3, origin_year, horizon_m, jurisdiction)
        )
    """)
    
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.metrics_zip3_history (
            zip3 TEXT NOT NULL,
            year INTEGER NOT NULL,
            value DOUBLE PRECISION,
            p50 DOUBLE PRECISION,
            jurisdiction TEXT DEFAULT 'hcad',
            PRIMARY KEY (zip3, year, jurisdiction)
        )
    """)

    # Aggregate ZCTA forecast → ZIP3 forecast
    cur.execute(f"""
        INSERT INTO {SCHEMA}.metrics_zip3_forecast (zip3, origin_year, horizon_m, p10, p25, p50, p75, p90, jurisdiction)
        SELECT 
            LEFT(zcta5, 3) as zip3,
            origin_year,
            horizon_m,
            AVG(p10), AVG(p25), AVG(p50), AVG(p75), AVG(p90),
            'hcad'
        FROM {SCHEMA}.metrics_zcta_forecast
        GROUP BY LEFT(zcta5, 3), origin_year, horizon_m
        ON CONFLICT DO NOTHING
    """)
    zip3_forecast_rows = cur.rowcount
    print(f"[{ts()}] Inserted {zip3_forecast_rows} ZIP3 forecast rows")

    # Aggregate ZCTA history → ZIP3 history
    cur.execute(f"""
        INSERT INTO {SCHEMA}.metrics_zip3_history (zip3, year, value, p50, jurisdiction)
        SELECT 
            LEFT(zcta5, 3) as zip3,
            year,
            AVG(value), AVG(p50),
            'hcad'
        FROM {SCHEMA}.metrics_zcta_history
        GROUP BY LEFT(zcta5, 3), year
        ON CONFLICT DO NOTHING
    """)
    zip3_history_rows = cur.rowcount
    print(f"[{ts()}] Inserted {zip3_history_rows} ZIP3 history rows")

    # ==========================================
    # FINAL: Verify counts
    # ==========================================
    print(f"\n[{ts()}] === FINAL COUNTS ===")
    for tbl in ['metrics_zcta_forecast', 'metrics_zcta_history', 'metrics_zip3_forecast', 'metrics_zip3_history']:
        try:
            cur.execute(f"SELECT count(1) FROM {SCHEMA}.{tbl}")
            print(f"  {tbl}: {cur.fetchone()[0]:,}")
        except Exception as e:
            print(f"  {tbl}: {e}")
            conn.rollback()
    
    cur.execute("SELECT count(1) FROM public.geo_zip3_us")
    print(f"  geo_zip3_us: {cur.fetchone()[0]:,}")
    cur.execute("SELECT count(1) FROM public.geo_zcta20_us")
    print(f"  geo_zcta20_us: {cur.fetchone()[0]:,}")

    conn.close()
    print(f"\n[{ts()}] Done!")

if __name__ == "__main__":
    main()
