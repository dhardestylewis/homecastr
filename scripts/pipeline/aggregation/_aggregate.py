"""Aggregation with hardcoded jurisdictions + progress tracking."""
import os
import psycopg2, time

CONN = os.environ["SUPABASE_DB_URL"]
SCHEMA = "forecast_20260220_7f31c6e4"

# Hardcoded based on known data
JURISDICTIONS = ["hcad", "seattle_wa"]
ORIGIN_YEARS = [2019, 2020, 2021, 2022, 2023, 2024, 2025]

conn = psycopg2.connect(CONN)
conn.autocommit = True
cur = conn.cursor()

# Ensure index exists
print("=== Ensuring index ===")
cur.execute("SET statement_timeout = '300000'")
try:
    cur.execute("CREATE INDEX IF NOT EXISTS idx_parcel_ladder_v1_acct ON public.parcel_ladder_v1 (acct)")
    print("  ✅ Index OK")
except Exception as e:
    print(f"  Index: {str(e)[:100]}")

combos = [(j, o) for j in JURISDICTIONS for o in ORIGIN_YEARS]
total = len(combos)
done = 0
total_rows = 0
skipped = 0
t_start = time.time()

print(f"\n=== Aggregating {total} jurisdiction × origin_year combos ===")

for jurisdiction, origin_year in combos:
    t0 = time.time()
    cur.execute("SET statement_timeout = '120000'")  # 2 min per combo

    try:
        cur.execute(f"""
            INSERT INTO {SCHEMA}.metrics_tract_forecast
                (tract_geoid20, origin_year, forecast_year, horizon_m, series_kind, variant_id,
                 jurisdiction, p10, p25, p50, p75, p90, n, run_id, as_of_date, updated_at)
            SELECT
                l.tract_geoid20,
                f.origin_year, f.forecast_year, f.horizon_m, f.series_kind, f.variant_id,
                f.jurisdiction,
                PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY f.p50),
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY f.p50),
                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY f.p50),
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY f.p50),
                PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY f.p50),
                COUNT(*)::int,
                MAX(f.run_id), MAX(f.as_of_date), NOW()
            FROM {SCHEMA}.metrics_parcel_forecast f
            JOIN public.parcel_ladder_v1 l ON f.acct = l.acct
            WHERE f.jurisdiction = %s AND f.origin_year = %s
              AND f.p50 IS NOT NULL
              AND l.tract_geoid20 IS NOT NULL AND l.tract_geoid20 != ''
            GROUP BY l.tract_geoid20, f.origin_year, f.forecast_year, f.horizon_m,
                     f.series_kind, f.variant_id, f.jurisdiction
            ON CONFLICT (tract_geoid20, origin_year, horizon_m, series_kind, variant_id)
            DO UPDATE SET
                p10=EXCLUDED.p10, p25=EXCLUDED.p25, p50=EXCLUDED.p50,
                p75=EXCLUDED.p75, p90=EXCLUDED.p90, n=EXCLUDED.n,
                run_id=EXCLUDED.run_id, updated_at=EXCLUDED.updated_at
        """, (jurisdiction, origin_year))
        rows = cur.rowcount
        total_rows += rows
    except Exception as e:
        rows = 0
        err = str(e)[:80]
        if "canceling" in err.lower():
            skipped += 1
            print(f"  [{done+1}/{total}] {jurisdiction} o={origin_year}: TIMEOUT (no data?)")
        else:
            print(f"  [{done+1}/{total}] {jurisdiction} o={origin_year}: ERROR {err}")
        conn = psycopg2.connect(CONN)
        conn.autocommit = True
        cur = conn.cursor()
        done += 1
        continue

    done += 1
    elapsed = time.time() - t_start
    rate = done / elapsed if elapsed > 0 else 0
    eta = (total - done) / rate if rate > 0 else 0
    dt = time.time() - t0
    if rows > 0:
        print(f"  [{done}/{total}] {jurisdiction} o={origin_year}: {rows:,} rows in {dt:.1f}s | total={total_rows:,} | ETA {eta/60:.1f}min")
    else:
        skipped += 1
        print(f"  [{done}/{total}] {jurisdiction} o={origin_year}: 0 rows (no data) in {dt:.1f}s")

elapsed = time.time() - t_start
print(f"\n✅ Done: {total_rows:,} tract rows in {elapsed:.0f}s ({elapsed/60:.1f}min), {skipped} skipped")
conn.close()
