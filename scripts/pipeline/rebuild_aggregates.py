"""
Rebuild forecast AND history aggregates from parcel-level rows.

Connects directly to Postgres via SUPABASE_DB_URL (bypasses Supabase SQL Editor
gateway timeout). Runs each geography × horizon combination as a separate
transaction.

Usage:
    export SUPABASE_DB_URL="postgresql://postgres.xxx:password@host:port/postgres"
    python scripts/pipeline/rebuild_aggregates.py

    # Or pass inline:
    SUPABASE_DB_URL="postgresql://..." python scripts/pipeline/rebuild_aggregates.py

    # Forecast only (skip history):
    python scripts/pipeline/rebuild_aggregates.py --skip-history

    # History only (skip forecast):
    python scripts/pipeline/rebuild_aggregates.py --skip-forecast
"""

import os
import sys
import time
import psycopg2

SCHEMA = "forecast_20260220_7f31c6e4"
HORIZONS = [12, 24, 36, 48, 60]

# (display_name, geo_id_column, forecast_table, history_table)
AGG_LEVELS = [
    ("neighborhood", "neighborhood_id",  "metrics_neighborhood_forecast", "metrics_neighborhood_history"),
    ("unsd",         "unsd_geoid",       "metrics_unsd_forecast",         "metrics_unsd_history"),
    ("zcta",         "zcta5",            "metrics_zcta_forecast",         "metrics_zcta_history"),
    ("tract",        "tract_geoid20",    "metrics_tract_forecast",        "metrics_tract_history"),
    ("tabblock",     "tabblock_geoid20", "metrics_tabblock_forecast",     "metrics_tabblock_history"),
]


def _table_exists(cur, schema: str, table: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
        (schema, table),
    )
    return cur.fetchone() is not None


def rebuild_forecast(cur, has_outlier_col: bool):
    outlier_filter = "AND coalesce(mp.is_outlier, false) = false" if has_outlier_col else ""

    for level_name, geoid_col, table_name, _ in AGG_LEVELS:
        if not _table_exists(cur, SCHEMA, table_name):
            print(f"  ⚠ {table_name} does not exist — skipping")
            continue

        print(f"\n{'='*60}")
        print(f"  FORECAST {level_name.upper()} — {table_name}")
        print(f"{'='*60}")

        cur.execute(f"""
            DELETE FROM {SCHEMA}.{table_name}
            WHERE series_kind = 'forecast' AND variant_id = '__forecast__'
        """)
        print(f"  DELETE complete ({cur.rowcount} rows removed)")

        for h in HORIZONS:
            t0 = time.time()
            cur.execute(f"""
                INSERT INTO {SCHEMA}.{table_name}
                ({geoid_col}, origin_year, horizon_m, forecast_year,
                 value, p10, p25, p50, p75, p90, n,
                 run_id, backtest_id, variant_id, model_version,
                 as_of_date, n_scenarios, is_backtest, series_kind,
                 inserted_at, updated_at)
                SELECT
                    pl.{geoid_col}, mp.origin_year, mp.horizon_m, mp.forecast_year,
                    AVG(mp.value)::float8, AVG(mp.p10)::float8, AVG(mp.p25)::float8,
                    AVG(mp.p50)::float8, AVG(mp.p75)::float8, AVG(mp.p90)::float8,
                    COUNT(*)::int, MAX(mp.run_id), MAX(mp.backtest_id),
                    '__forecast__', MAX(mp.model_version), MAX(mp.as_of_date),
                    MAX(mp.n_scenarios)::int, false, 'forecast', now(), now()
                FROM {SCHEMA}.metrics_parcel_forecast mp
                JOIN public.parcel_ladder_v1 pl USING (acct)
                WHERE mp.series_kind = 'forecast'
                  AND mp.variant_id = '__forecast__'
                  AND mp.horizon_m = {h}
                  {outlier_filter}
                  AND pl.{geoid_col} IS NOT NULL
                GROUP BY pl.{geoid_col}, mp.origin_year, mp.horizon_m, mp.forecast_year
            """)
            elapsed = time.time() - t0
            print(f"  horizon {h:>2}m → {cur.rowcount:>6} rows  ({elapsed:.1f}s)")


def rebuild_history(cur):
    """
    Rebuild history aggregates from metrics_parcel_history.
    History rows are keyed by (acct, year, series_kind, variant_id).
    We rebuild for series_kind='actual', variant_id='__history__'.
    """
    for level_name, geoid_col, _, table_name in AGG_LEVELS:
        if not _table_exists(cur, SCHEMA, table_name):
            print(f"  ⚠ {table_name} does not exist — skipping")
            continue

        print(f"\n{'='*60}")
        print(f"  HISTORY {level_name.upper()} — {table_name}")
        print(f"{'='*60}")

        # Check which series_kind / variant_id combos exist in parcel history
        cur.execute(f"""
            SELECT DISTINCT series_kind, variant_id
            FROM {SCHEMA}.metrics_parcel_history
            LIMIT 20
        """)
        combos = cur.fetchall()
        if not combos:
            print("  ⚠ metrics_parcel_history is empty — skipping")
            continue

        print(f"  Found history combos: {combos}")

        for series_kind, variant_id in combos:
            t0 = time.time()

            cur.execute(f"""
                DELETE FROM {SCHEMA}.{table_name}
                WHERE series_kind = %s AND variant_id = %s
            """, (series_kind, variant_id))
            deleted = cur.rowcount
            print(f"  DELETE {series_kind}/{variant_id}: {deleted} rows removed")

            cur.execute(f"""
                INSERT INTO {SCHEMA}.{table_name}
                ({geoid_col}, year,
                 value, p50, n,
                 run_id, backtest_id, variant_id, model_version,
                 as_of_date, series_kind,
                 inserted_at, updated_at)
                SELECT
                    pl.{geoid_col},
                    mh.year,
                    AVG(mh.value)::float8,
                    AVG(mh.p50)::float8,
                    COUNT(*)::int,
                    MAX(mh.run_id),
                    MAX(mh.backtest_id),
                    %s,
                    MAX(mh.model_version),
                    MAX(mh.as_of_date),
                    %s,
                    now(), now()
                FROM {SCHEMA}.metrics_parcel_history mh
                JOIN public.parcel_ladder_v1 pl USING (acct)
                WHERE mh.series_kind = %s
                  AND mh.variant_id = %s
                  AND pl.{geoid_col} IS NOT NULL
                GROUP BY pl.{geoid_col}, mh.year
            """, (variant_id, series_kind, series_kind, variant_id))
            elapsed = time.time() - t0
            print(f"  INSERT {series_kind}/{variant_id}: {cur.rowcount} rows  ({elapsed:.1f}s)")


def run():
    skip_history  = "--skip-history"  in sys.argv
    skip_forecast = "--skip-forecast" in sys.argv

    db_url = os.environ.get("SUPABASE_DB_URL", "")
    if not db_url:
        print("ERROR: Set SUPABASE_DB_URL env var first.")
        sys.exit(1)

    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    cur = conn.cursor()

    # Check outlier column
    cur.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = '{SCHEMA}'
          AND table_name = 'metrics_parcel_forecast'
          AND column_name = 'is_outlier'
    """)
    has_outlier_col = cur.fetchone() is not None
    if has_outlier_col:
        print("✓ is_outlier column found — filtering outliers from forecast agg")
    else:
        print("⚠ is_outlier column not found — building WITHOUT outlier filter")

    if not skip_forecast:
        print("\n" + "━"*60)
        print("REBUILDING FORECAST AGGREGATES")
        print("━"*60)
        rebuild_forecast(cur, has_outlier_col)
    else:
        print("⏭ Skipping forecast aggregates (--skip-forecast)")

    if not skip_history:
        print("\n" + "━"*60)
        print("REBUILDING HISTORY AGGREGATES")
        print("━"*60)
        rebuild_history(cur)
    else:
        print("⏭ Skipping history aggregates (--skip-history)")

    # Verification
    print(f"\n{'='*60}")
    print("  VERIFICATION")
    print(f"{'='*60}")
    for level_name, _, fc_table, hist_table in AGG_LEVELS:
        fc_rows = hist_rows = "N/A"
        if not skip_forecast and _table_exists(cur, SCHEMA, fc_table):
            cur.execute(f"SELECT count(*) FROM {SCHEMA}.{fc_table} WHERE series_kind = 'forecast'")
            fc_rows = cur.fetchone()[0]
        if not skip_history and _table_exists(cur, SCHEMA, hist_table):
            cur.execute(f"SELECT count(*) FROM {SCHEMA}.{hist_table}")
            hist_rows = cur.fetchone()[0]
        print(f"  {level_name:<15} forecast={fc_rows!s:>8}  history={hist_rows!s:>8}")

    cur.close()
    conn.close()
    print("\n✓ Done!")


if __name__ == "__main__":
    run()
