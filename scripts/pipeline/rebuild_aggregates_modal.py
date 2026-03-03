"""
Rebuild forecast AND history aggregates via Modal (stable same-region connection).

Each (geo_level × series_type) pair runs as its own Modal function — all in parallel.
Uses upsert (ON CONFLICT DO UPDATE) so each horizon is atomic and resumable.
On resume, completed (level, horizon) pairs are detected and skipped.

Usage:
    modal run scripts/pipeline/rebuild_aggregates_modal.py
    modal run scripts/pipeline/rebuild_aggregates_modal.py --skip-history
    modal run scripts/pipeline/rebuild_aggregates_modal.py --skip-forecast
    modal run scripts/pipeline/rebuild_aggregates_modal.py --force   # redo even completed horizons
"""
import modal, os, sys

app = modal.App("rebuild-aggregates")

image = modal.Image.debian_slim(python_version="3.11").pip_install("psycopg2-binary")
supabase_secret = modal.Secret.from_name("supabase-creds", required_keys=["SUPABASE_DB_URL"])

SCHEMA = "forecast_20260220_7f31c6e4"
HORIZONS = [12, 24, 36, 48, 60, 72]

# (display_name, geo_id_column, forecast_table, history_table)
AGG_LEVELS = [
    ("neighborhood", "neighborhood_id",  "metrics_neighborhood_forecast", "metrics_neighborhood_history"),
    ("unsd",         "unsd_geoid",       "metrics_unsd_forecast",         "metrics_unsd_history"),
    ("zcta",         "zcta5",            "metrics_zcta_forecast",         "metrics_zcta_history"),
    ("tract",        "tract_geoid20",    "metrics_tract_forecast",        "metrics_tract_history"),
    ("tabblock",     "tabblock_geoid20", "metrics_tabblock_forecast",     "metrics_tabblock_history"),
]


def _connect():
    import psycopg2
    conn = psycopg2.connect(os.environ["SUPABASE_DB_URL"])
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SET statement_timeout = 0")
    return conn, cur


def _table_exists(cur, schema: str, table: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
        (schema, table),
    )
    return cur.fetchone() is not None


def _horizon_done(cur, schema: str, table: str, horizon: int) -> bool:
    """Check if this horizon already has rows (for resume)."""
    cur.execute(
        f"SELECT 1 FROM {schema}.{table} WHERE horizon_m=%s AND series_kind='forecast' AND variant_id='__forecast__' LIMIT 1",
        (horizon,),
    )
    return cur.fetchone() is not None


@app.function(image=image, secrets=[supabase_secret], timeout=3600, memory=2048)
def rebuild_forecast_level_horizon(
    level_name: str, geoid_col: str, table_name: str,
    horizon: int, outlier_filter: str, force: bool = False,
):
    """Rebuild forecast aggregates for ONE (geo level, horizon). Upsert = resumable."""
    import time
    conn, cur = _connect()

    if not _table_exists(cur, SCHEMA, table_name):
        print(f"[{level_name}] ⚠ {table_name} does not exist — skipping")
        return {"level": level_name, "type": f"forecast_{horizon}m", "rows": 0, "status": "skipped"}

    # Resume: skip if already done
    if not force and _horizon_done(cur, SCHEMA, table_name, horizon):
        print(f"  [{level_name}] horizon {horizon:>2}m — already done, skipping")
        return {"level": level_name, "type": f"forecast_{horizon}m", "rows": 0, "status": "already_done"}

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
          AND mp.horizon_m = {horizon}
          {outlier_filter}
          AND pl.{geoid_col} IS NOT NULL
        GROUP BY pl.{geoid_col}, mp.origin_year, mp.horizon_m, mp.forecast_year
        ON CONFLICT ({geoid_col}, origin_year, horizon_m, series_kind, variant_id)
        DO UPDATE SET
            forecast_year  = EXCLUDED.forecast_year,
            value          = EXCLUDED.value,
            p10            = EXCLUDED.p10,
            p25            = EXCLUDED.p25,
            p50            = EXCLUDED.p50,
            p75            = EXCLUDED.p75,
            p90            = EXCLUDED.p90,
            n              = EXCLUDED.n,
            run_id         = EXCLUDED.run_id,
            model_version  = EXCLUDED.model_version,
            as_of_date     = EXCLUDED.as_of_date,
            n_scenarios    = EXCLUDED.n_scenarios,
            updated_at     = now()
    """)
    elapsed = time.time() - t0
    
    conn.close()
    print(f"  ✅ [{level_name}] horizon {horizon:>2}m done: {cur.rowcount} rows ({elapsed:.1f}s)")
    return {"level": level_name, "type": f"forecast_{horizon}m", "rows": cur.rowcount, "status": "done"}


@app.function(image=image, secrets=[supabase_secret], timeout=3600, memory=2048)
def rebuild_history_level(level_name: str, geoid_col: str, table_name: str, force: bool = False):
    """Rebuild history aggregates for one geo level. DELETE+INSERT per combo."""
    import time
    conn, cur = _connect()

    if not _table_exists(cur, SCHEMA, table_name):
        print(f"[{level_name}] ⚠ {table_name} does not exist — skipping")
        return {"level": level_name, "type": "history", "rows": 0, "status": "skipped"}

    print(f"\n{'='*60}")
    print(f"  HISTORY {level_name.upper()} — {table_name}")
    print(f"{'='*60}")

    # Check which combos exist
    cur.execute(f"""
        SELECT DISTINCT series_kind, variant_id
        FROM {SCHEMA}.metrics_parcel_history
        LIMIT 20
    """)
    combos = cur.fetchall()
    if not combos:
        print(f"  [{level_name}] ⚠ metrics_parcel_history is empty — skipping")
        return {"level": level_name, "type": "history", "rows": 0, "status": "empty"}

    print(f"  [{level_name}] Found history combos: {combos}")

    # Resume check: if already has rows for a combo, skip it
    total = 0
    for series_kind, variant_id in combos:
        if not force:
            cur.execute(f"""
                SELECT 1 FROM {SCHEMA}.{table_name}
                WHERE series_kind=%s AND variant_id=%s LIMIT 1
            """, (series_kind, variant_id))
            if cur.fetchone() is not None:
                print(f"  [{level_name}] {series_kind}/{variant_id} — already done, skipping")
                continue

        t0 = time.time()

        cur.execute(f"""
            DELETE FROM {SCHEMA}.{table_name}
            WHERE series_kind = %s AND variant_id = %s
        """, (series_kind, variant_id))
        print(f"  [{level_name}] DELETE {series_kind}/{variant_id}: {cur.rowcount} rows removed")

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
        total += cur.rowcount
        print(f"  [{level_name}] INSERT {series_kind}/{variant_id}: {cur.rowcount} rows  ({elapsed:.1f}s)")

    conn.close()
    print(f"  ✅ {level_name} history done: {total} total rows")
    return {"level": level_name, "type": "history", "rows": total, "status": "done"}


@app.local_entrypoint()
def main(
    skip_history: bool = False,
    skip_forecast: bool = False,
    force: bool = False,
):
    """Launch all geo-level aggregations in parallel via Modal."""

    # Quick outlier column check
    import psycopg2
    db_url = os.environ.get("SUPABASE_DB_URL", "")
    if db_url:
        conn = psycopg2.connect(db_url)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = '{SCHEMA}'
              AND table_name = 'metrics_parcel_forecast'
              AND column_name = 'is_outlier'
        """)
        has_outlier = cur.fetchone() is not None
        conn.close()
    else:
        has_outlier = False

    outlier_filter = "AND coalesce(mp.is_outlier, false) = false" if has_outlier else ""
    print(f"✓ Outlier filter: {'ON' if has_outlier else 'OFF'}")
    print(f"✓ Force (redo completed): {force}")
    print(f"✓ Skip forecast: {skip_forecast}, Skip history: {skip_history}")

    # Fan out ALL levels in parallel — each gets its own Modal container + DB connection
    futures = []

    if not skip_forecast:
        for ln, gc, ft, _ in AGG_LEVELS:
            for h in HORIZONS:
                futures.append(rebuild_forecast_level_horizon.spawn(ln, gc, ft, h, outlier_filter, force))

    if not skip_history:
        for ln, gc, _, ht in AGG_LEVELS:
            futures.append(rebuild_history_level.spawn(ln, gc, ht, force))

    print(f"\n🚀 Dispatched {len(futures)} parallel aggregation tasks")

    # Collect results as they complete
    results = [f.get() for f in futures]

    print(f"\n{'='*60}")
    print("  RESULTS")
    print(f"{'='*60}")
    for r in sorted(results, key=lambda x: (x["type"], x["level"])):
        print(f"  {r['type']:<10} {r['level']:<15} {r['rows']:>8} rows  [{r['status']}]")

    total = sum(r["rows"] for r in results)
    print(f"\n✅ All done! Total rows written: {total:,}")
