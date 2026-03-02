"""
Fix HCAD aggregation gaps + schema fixes:
1. Add jurisdiction column to metrics_tabblock_forecast (missing)
2. Rebuild metrics_zcta_history for HCAD (empty — was never aggregated)
"""
import psycopg2

CONN_STR = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require&options=-c%20statement_timeout%3D600000"
SCHEMA = "forecast_20260220_7f31c6e4"

def main():
    conn = psycopg2.connect(CONN_STR)
    conn.autocommit = True
    cur = conn.cursor()

    try:
        cur.execute("SET statement_timeout = '600000'")  # 10 min
        print("Set statement_timeout = 600s")
    except:
        print("Could not set statement_timeout")

    # ── 1. Add jurisdiction column to tabblock_forecast ──
    print("\n=== Adding jurisdiction column to metrics_tabblock_forecast ===")
    try:
        cur.execute(f"ALTER TABLE {SCHEMA}.metrics_tabblock_forecast ADD COLUMN IF NOT EXISTS jurisdiction TEXT DEFAULT 'hcad'")
        print("  Done: jurisdiction column added (default='hcad')")
    except Exception as e:
        print(f"  ERROR: {e}")

    # ── 2. Rebuild metrics_zcta_history for HCAD ──
    print("\n=== Rebuilding metrics_zcta_history ===")

    # Get years from parcel history
    cur.execute(f"""
        SELECT year, COUNT(1) as cnt
        FROM {SCHEMA}.metrics_parcel_history 
        WHERE jurisdiction = 'hcad'
        GROUP BY year ORDER BY year
    """)
    year_counts = cur.fetchall()
    print(f"Found {len(year_counts)} years in parcel history")
    for yr, cnt in year_counts:
        print(f"  year={yr}: {cnt:,} parcels")

    total = 0
    for yr, cnt in year_counts:
        try:
            cur.execute(f"""
                INSERT INTO {SCHEMA}.metrics_zcta_history (zcta5, year, value, p50, series_kind, variant_id, jurisdiction)
                SELECT
                    l.zcta5, ph.year,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ph.value),
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ph.value),
                    ph.series_kind, ph.variant_id, 'hcad'
                FROM {SCHEMA}.metrics_parcel_history ph
                JOIN public.parcel_ladder_v1 l ON l.acct = ph.acct
                WHERE l.zcta5 IS NOT NULL AND ph.year = {yr} AND ph.jurisdiction = 'hcad'
                GROUP BY l.zcta5, ph.year, ph.series_kind, ph.variant_id
                ON CONFLICT (zcta5, year, series_kind, variant_id)
                DO UPDATE SET value = EXCLUDED.value, p50 = EXCLUDED.p50, jurisdiction = 'hcad'
            """)
            total += cur.rowcount
            print(f"  year={yr}: {cur.rowcount} rows (total: {total})")
        except Exception as e:
            print(f"  year={yr}: ERROR - {e}")

    print(f"\n  DONE: {total} total zcta_history rows")

    # ── 3. Verify final counts ──
    print("\n=== Final Counts ===")
    for tbl in ['metrics_zcta_history', 'metrics_zcta_forecast', 
                'metrics_tract_history', 'metrics_tract_forecast',
                'metrics_tabblock_history', 'metrics_tabblock_forecast']:
        try:
            cur.execute(f'SELECT COUNT(1) FROM {SCHEMA}.{tbl}')
            print(f"  {tbl}: {cur.fetchone()[0]:,}")
        except Exception as e:
            print(f"  {tbl}: ERROR - {e}")

    conn.close()
    print("\nAll fixes applied!")

if __name__ == "__main__":
    main()
