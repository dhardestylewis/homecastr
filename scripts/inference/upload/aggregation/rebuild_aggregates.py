"""Rebuild tract and tabblock history — use direct DB endpoint to avoid pooler timeout."""
import psycopg2

# Direct connection (port 5432, no pooler restrictions)
CONN_STR = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require&options=-c%20statement_timeout%3D600000"
SCHEMA = "forecast_20260220_7f31c6e4"

def main():
    conn = psycopg2.connect(CONN_STR)
    conn.autocommit = True
    cur = conn.cursor()

    # Try to extend timeout
    try:
        cur.execute("SET statement_timeout = '600000'")  # 10 min in ms
        print("Set statement_timeout = 600s")
    except:
        print("Could not set statement_timeout")

    # Get distinct years with count
    cur.execute(f"""
        SELECT year, COUNT(*) as cnt
        FROM {SCHEMA}.metrics_parcel_history 
        GROUP BY year ORDER BY year
    """)
    year_counts = cur.fetchall()
    print(f"Processing {len(year_counts)} years")
    for yr, cnt in year_counts:
        print(f"  year={yr}: {cnt:,} parcels")

    # Split big years into sub-batches by acct prefix
    for tbl, geo_col in [
        ("metrics_tract_history", "tract_geoid20"),
        ("metrics_tabblock_history", "tabblock_geoid20"),
    ]:
        print(f"\n=== Rebuilding {tbl} ===")
        total = 0
        for yr, cnt in year_counts:
            if cnt < 100000:
                # Small year — do in one query
                try:
                    cur.execute(f"""
                        INSERT INTO {SCHEMA}.{tbl} ({geo_col}, year, value, p50, series_kind, variant_id)
                        SELECT
                            l.{geo_col}, ph.year,
                            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ph.value),
                            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ph.value),
                            ph.series_kind, ph.variant_id
                        FROM {SCHEMA}.metrics_parcel_history ph
                        JOIN public.parcel_ladder_v1 l ON l.acct = ph.acct
                        WHERE l.{geo_col} IS NOT NULL AND ph.year = {yr}
                        GROUP BY l.{geo_col}, ph.year, ph.series_kind, ph.variant_id
                        ON CONFLICT ({geo_col}, year, series_kind, variant_id)
                        DO UPDATE SET value = EXCLUDED.value, p50 = EXCLUDED.p50
                    """)
                    total += cur.rowcount
                    print(f"  year={yr}: {cur.rowcount} rows (total: {total})")
                except Exception as e:
                    print(f"  year={yr}: ERROR - {e}")
            else:
                # Big year — batch by acct ranges using OFFSET/LIMIT via subquery
                batch_size = 200000
                offset = 0
                yr_total = 0
                while True:
                    try:
                        cur.execute(f"""
                            INSERT INTO {SCHEMA}.{tbl} ({geo_col}, year, value, p50, series_kind, variant_id)
                            SELECT
                                l.{geo_col}, sub.year,
                                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sub.value),
                                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sub.value),
                                sub.series_kind, sub.variant_id
                            FROM (
                                SELECT acct, year, value, series_kind, variant_id
                                FROM {SCHEMA}.metrics_parcel_history
                                WHERE year = {yr}
                                ORDER BY acct
                                OFFSET {offset} LIMIT {batch_size}
                            ) sub
                            JOIN public.parcel_ladder_v1 l ON l.acct = sub.acct
                            WHERE l.{geo_col} IS NOT NULL
                            GROUP BY l.{geo_col}, sub.year, sub.series_kind, sub.variant_id
                            ON CONFLICT ({geo_col}, year, series_kind, variant_id)
                            DO UPDATE SET value = EXCLUDED.value, p50 = EXCLUDED.p50
                        """)
                        batch_rows = cur.rowcount
                        yr_total += batch_rows
                        total += batch_rows
                        print(f"  year={yr} batch@{offset}: {batch_rows} rows (yr_total: {yr_total}, total: {total})")
                        if batch_rows == 0 or offset + batch_size >= cnt + batch_size:
                            break
                        offset += batch_size
                    except Exception as e:
                        print(f"  year={yr} batch@{offset}: ERROR - {e}")
                        break

        print(f"  DONE: {total} total rows")

    # Verify
    print("\n=== Final counts ===")
    for tbl in ['metrics_zcta_history', 'metrics_tract_history', 'metrics_tabblock_history']:
        cur.execute(f'SELECT COUNT(*) FROM {SCHEMA}.{tbl}')
        print(f"  {tbl}: {cur.fetchone()[0]:,} rows")

    conn.close()
    print("\nDone!")

if __name__ == "__main__":
    main()
