"""
Rebuild forecast AND history aggregates from parcel-level rows.

Parallelized across geo levels × horizons for maximum speed.

Usage:
    export SUPABASE_DB_URL="postgresql://postgres.xxx:password@host:port/postgres"
    python scripts/pipeline/rebuild_aggregates.py

    # Flags:
    python scripts/pipeline/rebuild_aggregates.py --skip-history
    python scripts/pipeline/rebuild_aggregates.py --skip-forecast
    python scripts/pipeline/rebuild_aggregates.py --workers 10  # default: 25
"""

import os
import sys
import time
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed

SCHEMA    = "forecast_20260220_7f31c6e4"
HORIZONS  = [12, 24, 36, 48, 60, 72]

# (display_name, geo_id_column, forecast_table, history_table)
AGG_LEVELS = [
    ("neighborhood", "neighborhood_id",  "metrics_neighborhood_forecast", "metrics_neighborhood_history"),
    ("unsd",         "unsd_geoid",       "metrics_unsd_forecast",         "metrics_unsd_history"),
    ("zcta",         "zcta5",            "metrics_zcta_forecast",         "metrics_zcta_history"),
    ("tract",        "tract_geoid20",    "metrics_tract_forecast",        "metrics_tract_history"),
    ("tabblock",     "tabblock_geoid20", "metrics_tabblock_forecast",     "metrics_tabblock_history"),
]


def _connect(db_url: str) -> psycopg2.extensions.connection:
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    # Raise statement_timeout to 10 min per query
    with conn.cursor() as c:
        c.execute("SET statement_timeout = 600000")
    return conn


def _table_exists(db_url: str, table: str) -> bool:
    conn = _connect(db_url)
    with conn.cursor() as c:
        c.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
            (SCHEMA, table),
        )
        exists = c.fetchone() is not None
    conn.close()
    return exists


# ─── Forecast ────────────────────────────────────────────────────────────────

def _delete_forecast_level(db_url: str, table: str) -> int:
    """One DELETE per geo level — clears all horizons at once."""
    conn = _connect(db_url)
    with conn.cursor() as c:
        c.execute(
            f"DELETE FROM {SCHEMA}.{table} WHERE series_kind = 'forecast' AND variant_id = '__forecast__'"
        )
        n = c.rowcount
    conn.close()
    return n


def _insert_forecast_horizon(db_url: str, geoid_col: str, table: str, horizon: int,
                              outlier_filter: str) -> tuple:
    """One INSERT per (geo_level, horizon) — fully parallelizable."""
    t0 = time.time()
    conn = _connect(db_url)
    with conn.cursor() as c:
        c.execute(f"""
            INSERT INTO {SCHEMA}.{table}
            ({geoid_col}, origin_year, horizon_m, forecast_year,
             value, p10, p25, p50, p75, p90, n,
             run_id, backtest_id, variant_id, model_version,
             as_of_date, n_scenarios, is_backtest, series_kind,
             inserted_at, updated_at)
            SELECT
                pl.{geoid_col}, mp.origin_year, mp.horizon_m, mp.forecast_year,
                AVG(mp.value)::float8, AVG(mp.p10)::float8, AVG(mp.p25)::float8,
                AVG(mp.p50)::float8,   AVG(mp.p75)::float8, AVG(mp.p90)::float8,
                COUNT(*)::int, MAX(mp.run_id), MAX(mp.backtest_id),
                '__forecast__', MAX(mp.model_version), MAX(mp.as_of_date),
                MAX(mp.n_scenarios)::int, false, 'forecast', now(), now()
            FROM {SCHEMA}.metrics_parcel_forecast mp
            JOIN public.parcel_ladder_v1 pl USING (acct)
            WHERE mp.series_kind = 'forecast'
              AND mp.variant_id  = '__forecast__'
              AND mp.horizon_m   = {horizon}
              {outlier_filter}
              AND pl.{geoid_col} IS NOT NULL
            GROUP BY pl.{geoid_col}, mp.origin_year, mp.horizon_m, mp.forecast_year
        """)
        rows = c.rowcount
    conn.close()
    return table, horizon, rows, time.time() - t0


def rebuild_forecast(db_url: str, has_outlier_col: bool, max_workers: int):
    outlier_filter = "AND coalesce(mp.is_outlier, false) = false" if has_outlier_col else ""

    # Step 1: DELETE each level serially (fast; avoids lock races)
    print("\n  Clearing old forecast aggregate rows...")
    for level_name, geoid_col, table, _ in AGG_LEVELS:
        if not _table_exists(db_url, table):
            print(f"  ⚠ {table} not found — skip")
            continue
        n = _delete_forecast_level(db_url, table)
        print(f"  DELETE {level_name:<15} {n:>7} rows")

    # Step 2: INSERT all (geo × horizon) in parallel
    print(f"\n  Inserting forecast aggregates ({len(AGG_LEVELS)} levels × {len(HORIZONS)} horizons = "
          f"{len(AGG_LEVELS)*len(HORIZONS)} tasks, {max_workers} workers)...")

    tasks = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for level_name, geoid_col, table, _ in AGG_LEVELS:
            if not _table_exists(db_url, table):
                continue
            for h in HORIZONS:
                tasks.append(pool.submit(_insert_forecast_horizon, db_url, geoid_col, table, h, outlier_filter))

        for fut in as_completed(tasks):
            tbl, h, rows, elapsed = fut.result()
            level = next(n for n, _, t, _ in AGG_LEVELS if t == tbl)
            print(f"  ✓ {level:<15} h={h:>2}m  {rows:>7} rows  ({elapsed:.1f}s)")


# ─── History ─────────────────────────────────────────────────────────────────

def _get_history_combos(db_url: str) -> list:
    conn = _connect(db_url)
    with conn.cursor() as c:
        c.execute(f"SELECT DISTINCT series_kind, variant_id FROM {SCHEMA}.metrics_parcel_history LIMIT 20")
        combos = c.fetchall()
    conn.close()
    return combos


def _rebuild_history_level_combo(db_url: str, geoid_col: str, table: str,
                                  series_kind: str, variant_id: str) -> tuple:
    """DELETE + INSERT for one (geo_level, series_kind, variant_id) — parallelizable."""
    t0 = time.time()
    conn = _connect(db_url)
    with conn.cursor() as c:
        c.execute(
            f"DELETE FROM {SCHEMA}.{table} WHERE series_kind = %s AND variant_id = %s",
            (series_kind, variant_id),
        )
        deleted = c.rowcount

        c.execute(f"""
            INSERT INTO {SCHEMA}.{table}
            ({geoid_col}, year,
             value, p50, n,
             run_id, backtest_id, variant_id, model_version,
             as_of_date, series_kind,
             inserted_at, updated_at)
            SELECT
                pl.{geoid_col}, mh.year,
                AVG(mh.value)::float8, AVG(mh.p50)::float8, COUNT(*)::int,
                MAX(mh.run_id), MAX(mh.backtest_id),
                %s, MAX(mh.model_version), MAX(mh.as_of_date),
                %s, now(), now()
            FROM {SCHEMA}.metrics_parcel_history mh
            JOIN public.parcel_ladder_v1 pl USING (acct)
            WHERE mh.series_kind = %s
              AND mh.variant_id  = %s
              AND pl.{geoid_col} IS NOT NULL
            GROUP BY pl.{geoid_col}, mh.year
        """, (variant_id, series_kind, series_kind, variant_id))
        inserted = c.rowcount
    conn.close()
    return table, series_kind, variant_id, deleted, inserted, time.time() - t0


def rebuild_history(db_url: str, max_workers: int):
    combos = _get_history_combos(db_url)
    if not combos:
        print("  ⚠ metrics_parcel_history is empty — nothing to rebuild")
        return

    print(f"\n  History combos: {combos}")
    n_tasks = len(AGG_LEVELS) * len(combos)
    print(f"  Rebuilding history ({len(AGG_LEVELS)} levels × {len(combos)} combos = {n_tasks} tasks, {max_workers} workers)...")

    tasks = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for _, geoid_col, _, hist_table in AGG_LEVELS:
            if not _table_exists(db_url, hist_table):
                continue
            for series_kind, variant_id in combos:
                tasks.append(pool.submit(
                    _rebuild_history_level_combo, db_url, geoid_col, hist_table, series_kind, variant_id
                ))

        for fut in as_completed(tasks):
            tbl, sk, vid, deleted, inserted, elapsed = fut.result()
            level = next(n for n, _, _, t in AGG_LEVELS if t == tbl)
            print(f"  ✓ {level:<15} {sk}/{vid}  del={deleted} ins={inserted}  ({elapsed:.1f}s)")


# ─── Main ─────────────────────────────────────────────────────────────────────

def run():
    skip_history  = "--skip-history"  in sys.argv
    skip_forecast = "--skip-forecast" in sys.argv
    max_workers   = 25
    for arg in sys.argv:
        if arg.startswith("--workers="):
            max_workers = int(arg.split("=")[1])

    db_url = os.environ.get("SUPABASE_DB_URL", "")
    if not db_url:
        print("ERROR: Set SUPABASE_DB_URL env var first.")
        sys.exit(1)

    # Check outlier column
    conn = _connect(db_url)
    with conn.cursor() as c:
        c.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = '{SCHEMA}'
              AND table_name = 'metrics_parcel_forecast'
              AND column_name = 'is_outlier'
        """)
        has_outlier_col = c.fetchone() is not None
    conn.close()

    print(f"✓ is_outlier filter: {'ON' if has_outlier_col else 'OFF (column missing)'}")

    t_start = time.time()

    if not skip_forecast:
        print("\n" + "━"*60)
        print("REBUILDING FORECAST AGGREGATES")
        print("━"*60)
        rebuild_forecast(db_url, has_outlier_col, max_workers)
    else:
        print("⏭ Skipping forecast aggregates (--skip-forecast)")

    if not skip_history:
        print("\n" + "━"*60)
        print("REBUILDING HISTORY AGGREGATES")
        print("━"*60)
        rebuild_history(db_url, max_workers)
    else:
        print("⏭ Skipping history aggregates (--skip-history)")

    # Verification
    print(f"\n{'='*60}")
    print("  VERIFICATION")
    print(f"{'='*60}")
    conn = _connect(db_url)
    with conn.cursor() as c:
        for level_name, _, fc_table, hist_table in AGG_LEVELS:
            fc_rows = hist_rows = "—"
            if not skip_forecast and _table_exists(db_url, fc_table):
                c.execute(f"SELECT count(*) FROM {SCHEMA}.{fc_table} WHERE series_kind = 'forecast'")
                fc_rows = c.fetchone()[0]
            if not skip_history and _table_exists(db_url, hist_table):
                c.execute(f"SELECT count(*) FROM {SCHEMA}.{hist_table}")
                hist_rows = c.fetchone()[0]
            print(f"  {level_name:<15} forecast={fc_rows!s:>8}  history={hist_rows!s:>8}")
    conn.close()

    print(f"\n✓ Done in {time.time()-t_start:.1f}s")


if __name__ == "__main__":
    run()
