"""
Identify outlier years at the state aggregation level.
"""
import psycopg2
import statistics
from collections import defaultdict

DST = "forecast_20260220_7f31c6e4"
DB_URL = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"

conn = psycopg2.connect(DB_URL, connect_timeout=30)
conn.autocommit = True
cur = conn.cursor()
cur.execute("SET statement_timeout = 120000")

# ── 1. History: year-level summary across all states ──
print("\n=== STATE HISTORY: p50 by year ===")
cur.execute(f"""
    SELECT year,
           COUNT(DISTINCT state_fips) AS n_states,
           ROUND(AVG(p50)::numeric, 4) AS avg_p50,
           ROUND(PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY p50)::numeric, 4) AS p10,
           ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY p50)::numeric, 4) AS med,
           ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY p50)::numeric, 4) AS p90,
           ROUND(MIN(p50)::numeric, 4) AS mn,
           ROUND(MAX(p50)::numeric, 4) AS mx
    FROM {DST}.metrics_state_history
    WHERE series_kind = 'history' OR series_kind IS NULL
    GROUP BY year
    ORDER BY year
""")
rows = cur.fetchall()
print(f"{'year':>6} {'n_st':>5} {'avg_p50':>12} {'p10':>12} {'median':>12} {'p90':>12} {'min':>12} {'max':>12}")
print("-" * 90)
for r in rows:
    print(f"{r[0]:>6} {r[1]:>5} {r[2]:>12} {r[3]:>12} {r[4]:>12} {r[5]:>12} {r[6]:>12} {r[7]:>12}")

if rows:
    medians = [float(r[4]) for r in rows if r[4] is not None]
    if len(medians) >= 4:
        q1 = statistics.quantiles(medians, n=4)[0]
        q3 = statistics.quantiles(medians, n=4)[2]
        iqr = q3 - q1
        lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        print(f"\nIQR bounds (cross-year): [{lo:.4f}, {hi:.4f}]")
        print("OUTLIER YEARS (history cross-state median outside IQR):")
        any_out = False
        for r in rows:
            if r[4] is not None and (float(r[4]) < lo or float(r[4]) > hi):
                print(f"  *** year={r[0]}  median_p50={r[4]}  n_states={r[1]}")
                any_out = True
        if not any_out:
            print("  (none)")

# ── 2. Forecast: columns then query ──
print("\n=== STATE FORECAST: p50 by year + horizon_m ===")
cur.execute(f"""
    SELECT column_name FROM information_schema.columns
    WHERE table_schema = '{DST}' AND table_name = 'metrics_state_forecast'
    ORDER BY ordinal_position
""")
fc_cols = [r[0] for r in cur.fetchall()]
print(f"  columns: {fc_cols}")

year_col = next((c for c in fc_cols if c in ('origin_year', 'forecast_year')), None)
horizon_col = next((c for c in fc_cols if c in ('horizon_m', 'horizon_months', 'forecast_horizon_months')), None)
print(f"  year_col={year_col}  horizon_col={horizon_col}")

if year_col and horizon_col:
    cur.execute(f"""
        SELECT {year_col},
               {horizon_col},
               COUNT(DISTINCT state_fips) AS n_states,
               ROUND(AVG(p50)::numeric, 4) AS avg_p50,
               ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY p50)::numeric, 4) AS med_p50,
               ROUND(MIN(p50)::numeric, 4) AS mn,
               ROUND(MAX(p50)::numeric, 4) AS mx
        FROM {DST}.metrics_state_forecast
        WHERE series_kind = 'forecast' OR series_kind IS NULL
        GROUP BY {year_col}, {horizon_col}
        ORDER BY {year_col}, {horizon_col}
    """)
    rows_fc = cur.fetchall()
    print(f"{'year':>8} {'horizon_m':>10} {'n_st':>5} {'avg_p50':>12} {'med_p50':>12} {'min':>12} {'max':>12}")
    print("-" * 80)
    for r in rows_fc:
        print(f"{r[0]:>8} {r[1]:>10} {r[2]:>5} {r[3]:>12} {r[4]:>12} {r[5]:>12} {r[6]:>12}")

    if rows_fc:
        meds = [float(r[4]) for r in rows_fc if r[4] is not None]
        if len(meds) >= 4:
            q1 = statistics.quantiles(meds, n=4)[0]
            q3 = statistics.quantiles(meds, n=4)[2]
            iqr = q3 - q1
            lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
            print(f"\nIQR bounds: [{lo:.4f}, {hi:.4f}]")
            print("OUTLIER YEAR+HORIZON combos (med_p50 outside IQR):")
            any_out = False
            for r in rows_fc:
                if r[4] is not None and (float(r[4]) < lo or float(r[4]) > hi):
                    print(f"  *** year={r[0]}  horizon_m={r[1]}  med_p50={r[4]}  n_states={r[2]}")
                    any_out = True
            if not any_out:
                print("  (none)")

# ── 3. Per-state per-year: flag outlier states within each year ──
print("\n=== PER-YEAR OUTLIER STATES (history IQR) ===")
cur.execute(f"""
    SELECT state_fips, year, ROUND(p50::numeric, 4) as p50
    FROM {DST}.metrics_state_history
    WHERE (series_kind = 'history' OR series_kind IS NULL)
      AND p50 IS NOT NULL
    ORDER BY year, state_fips
""")
state_rows = cur.fetchall()

year_vals = defaultdict(list)
for state, yr, p50 in state_rows:
    year_vals[yr].append((state, float(p50)))

any_outlier = False
for yr in sorted(year_vals.keys()):
    vals = [v for _, v in year_vals[yr]]
    if len(vals) < 4:
        continue
    q1 = statistics.quantiles(vals, n=4)[0]
    q3 = statistics.quantiles(vals, n=4)[2]
    iqr = q3 - q1
    lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    outliers = [(s, v) for s, v in year_vals[yr] if v < lo or v > hi]
    if outliers:
        print(f"  year={yr}  IQR=[{lo:.0f},{hi:.0f}]  outliers: {outliers}")
        any_outlier = True

if not any_outlier:
    print("  (none)")

conn.close()
print("\nDone.")
