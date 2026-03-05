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

SCHEMA = "forecast_queue"
HORIZONS = [12, 24, 36, 48, 60, 72]

# (display_name, geo_id_column, forecast_table, history_table)
AGG_LEVELS = [
    ("neighborhood", "neighborhood_id",  "metrics_neighborhood_forecast", "metrics_neighborhood_history"),
    ("unsd",         "unsd_geoid",       "metrics_unsd_forecast",         "metrics_unsd_history"),
    ("zcta",         "zcta5",            "metrics_zcta_forecast",         "metrics_zcta_history"),
    ("tract",        "tract_geoid20",    "metrics_tract_forecast",        "metrics_tract_history"),
    ("tabblock",     "tabblock_geoid20", "metrics_tabblock_forecast",     "metrics_tabblock_history"),
    ("zip3",         "zip3",             "metrics_zip3_forecast",         "metrics_zip3_history"),
]


def _connect(max_retries=100, initial_backoff=5.0):
    import psycopg2
    import time
    
    # Try direct port 5432 if it's the pooler URL
    url = os.environ["SUPABASE_DB_URL"]
    if "pooler.supabase.com" in url and "6543" in url:
        url = url.replace(":6543/", ":5432/")
    
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(url)
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("SET statement_timeout = 0")
            return conn, cur
        except psycopg2.OperationalError as e:
            if attempt == max_retries - 1:
                raise
            
            # If we hit max connections, just wait and retry
            msg = str(e).lower()
            if "max client connections" in msg or "too many clients" in msg or "connection to server" in msg:
                wait_time = min(60.0, initial_backoff * (1.5 ** attempt))
                print(f"  [DB Wait] Pool full, waiting {wait_time:.1f}s before retry {attempt+1}/{max_retries}")
                time.sleep(wait_time)
            else:
                raise


def _table_exists(cur, schema: str, table: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
        (schema, table),
    )
    return cur.fetchone() is not None


def _horizon_done(cur, schema: str, table: str, horizon: int, has_sk: bool) -> bool:
    """Check if this horizon already has rows (for resume)."""
    if has_sk:
        cur.execute(
            f"SELECT 1 FROM {schema}.{table} WHERE horizon_m=%s AND series_kind='forecast' AND variant_id='__forecast__' LIMIT 1",
            (horizon,),
        )
    else:
        cur.execute(
            f"SELECT 1 FROM {schema}.{table} WHERE horizon_m=%s LIMIT 1",
            (horizon,),
        )
    return cur.fetchone() is not None


@app.function(image=image, secrets=[supabase_secret], timeout=3600, memory=2048, max_containers=5)
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

    cur.execute("SELECT 1 FROM information_schema.columns WHERE table_schema=%s AND table_name=%s AND column_name='series_kind'", (SCHEMA, table_name))
    has_sk = cur.fetchone() is not None

    # Resume: skip if already done
    if not force and _horizon_done(cur, SCHEMA, table_name, horizon, has_sk):
        print(f"  [{level_name}] horizon {horizon:>2}m — already done, skipping")
        return {"level": level_name, "type": f"forecast_{horizon}m", "rows": 0, "status": "already_done"}

    pl_geo_col = f"pl.{geoid_col}"
    if geoid_col == "zip3":
        pl_geo_col = "LEFT(pl.zcta5, 3)"

    t0 = time.time()
    
    if has_sk:
        cols_extra = "variant_id, model_version, as_of_date, n_scenarios, is_backtest, series_kind"
        vals_extra = "'__forecast__', MAX(mp.model_version), MAX(mp.as_of_date), MAX(mp.n_scenarios)::int, false, 'forecast'"
        conflict_extra = ", series_kind, variant_id"
    else:
        cols_extra = "model_version, as_of_date, n_scenarios"
        vals_extra = "MAX(mp.model_version), MAX(mp.as_of_date), MAX(mp.n_scenarios)::int"
        conflict_extra = ""

    cur.execute(f"""
        INSERT INTO {SCHEMA}.{table_name}
        ({geoid_col}, origin_year, horizon_m, forecast_year,
         value, p10, p25, p50, p75, p90, n,
         run_id, backtest_id, {cols_extra},
         jurisdiction, inserted_at, updated_at)
        SELECT
            {pl_geo_col}, mp.origin_year, mp.horizon_m, mp.forecast_year,
            AVG(mp.value)::float8, AVG(mp.p10)::float8, AVG(mp.p25)::float8,
            AVG(mp.p50)::float8, AVG(mp.p75)::float8, AVG(mp.p90)::float8,
            COUNT(*)::int, MAX(mp.run_id), MAX(mp.backtest_id),
            {vals_extra}, MIN(pl.jurisdiction), now(), now()
        FROM {SCHEMA}.metrics_parcel_forecast mp
        JOIN public.parcel_ladder_v1 pl USING (acct)
        WHERE mp.series_kind = 'forecast'
          AND mp.variant_id = '__forecast__'
          AND mp.horizon_m = {horizon}
          {outlier_filter}
          AND {pl_geo_col} IS NOT NULL
        GROUP BY {pl_geo_col}, mp.origin_year, mp.horizon_m, mp.forecast_year
        ON CONFLICT ({geoid_col}, origin_year, horizon_m{conflict_extra})
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
            jurisdiction   = EXCLUDED.jurisdiction,
            updated_at     = now()
    """)
    elapsed = time.time() - t0
    
    conn.close()
    print(f"  ✅ [{level_name}] horizon {horizon:>2}m done: {cur.rowcount} rows ({elapsed:.1f}s)")
    return {"level": level_name, "type": f"forecast_{horizon}m", "rows": cur.rowcount, "status": "done"}


@app.function(image=image, secrets=[supabase_secret], timeout=3600, memory=2048, max_containers=5)
def rebuild_history_level_combo_year(level_name: str, geoid_col: str, table_name: str, 
                                     series_kind: str, variant_id: str, year: int, force: bool = False):
    """Rebuild history aggregates for one geo level, one combo, and one year. Resumable via checking existence."""
    import time
    conn, cur = _connect()

    if not _table_exists(cur, SCHEMA, table_name):
        print(f"[{level_name}] ⚠ {table_name} does not exist — skipping")
        return {"level": level_name, "type": "history", "rows": 0, "status": "skipped"}

    cur.execute("SELECT 1 FROM information_schema.columns WHERE table_schema=%s AND table_name=%s AND column_name='series_kind'", (SCHEMA, table_name))
    has_sk = cur.fetchone() is not None

    # Resume check: if already has rows for this geo, combo, and year, skip it
    if not force:
        if has_sk:
            cur.execute(f"SELECT 1 FROM {SCHEMA}.{table_name} WHERE series_kind=%s AND variant_id=%s AND year=%s LIMIT 1", (series_kind, variant_id, year))
        else:
            cur.execute(f"SELECT 1 FROM {SCHEMA}.{table_name} WHERE year=%s LIMIT 1", (year,))
        
        if cur.fetchone() is not None:
            print(f"  [{level_name}] {series_kind}/{variant_id} yr {year} — already done, skipping")
            return {"level": level_name, "type": f"hist_{year}", "rows": 0, "status": "already_done"}

    # If the target table lacks series_kind and we're looking at a backtest variant instead of history, we should skip
    if not has_sk and variant_id != "__history__":
        print(f"  [{level_name}] ⚠ skipping non-history variant {variant_id} because table lacks series_kind")
        return {"level": level_name, "type": f"hist_{year}", "rows": 0, "status": "skipped_schema"}

    t0 = time.time()

    pl_geo_col = f"pl.{geoid_col}"
    if geoid_col == "zip3":
        pl_geo_col = "LEFT(pl.zcta5, 3)"

    if has_sk:
        cur.execute(f"DELETE FROM {SCHEMA}.{table_name} WHERE series_kind = %s AND variant_id = %s AND year = %s", (series_kind, variant_id, year))
    else:
        cur.execute(f"DELETE FROM {SCHEMA}.{table_name} WHERE year = %s", (year,))
    deleted = cur.rowcount

    if has_sk:
        cols_extra = "variant_id, model_version, as_of_date, series_kind"
        vals_extra = "%s, MAX(mh.model_version), MAX(mh.as_of_date), %s"
        sql_args = (variant_id, series_kind, series_kind, variant_id, year)
    else:
        cols_extra = "model_version, as_of_date"
        vals_extra = "MAX(mh.model_version), MAX(mh.as_of_date)"
        sql_args = (series_kind, variant_id, year)

    cur.execute(f"""
        INSERT INTO {SCHEMA}.{table_name}
        ({geoid_col}, year,
         value, p50, n,
         run_id, backtest_id, {cols_extra},
         jurisdiction, inserted_at, updated_at)
        SELECT
            {pl_geo_col},
            mh.year,
            AVG(mh.value)::float8,
            AVG(mh.p50)::float8,
            COUNT(*)::int,
            MAX(mh.run_id),
            MAX(mh.backtest_id),
            {vals_extra},
            MIN(pl.jurisdiction), now(), now()
        FROM {SCHEMA}.metrics_parcel_history mh
        JOIN public.parcel_ladder_v1 pl USING (acct)
        WHERE mh.series_kind = %s
          AND mh.variant_id = %s
          AND mh.year = %s
          AND {pl_geo_col} IS NOT NULL
        GROUP BY {pl_geo_col}, mh.year
    """, sql_args)
    elapsed = time.time() - t0
    rows = cur.rowcount
    
    conn.close()
    print(f"  ✅ [{level_name}] {series_kind}/{variant_id} yr {year} done: del={deleted} ins={rows} ({elapsed:.1f}s)")
    return {"level": level_name, "type": f"hist_{year}", "rows": rows, "status": "done"}


@app.function(image=image, secrets=[supabase_secret], timeout=900, memory=512)
def _discover_db_metadata(need_history: bool):
    """Run on Modal to discover outlier column and history combos (needs DB access)."""
    conn, cur = _connect()
    cur.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = '{SCHEMA}'
          AND table_name = 'metrics_parcel_forecast'
          AND column_name = 'is_outlier'
    """)
    has_outlier = cur.fetchone() is not None

    hist_combos = []
    if need_history:
        cur.execute(f"SELECT DISTINCT series_kind, variant_id, year FROM {SCHEMA}.metrics_parcel_history")
        hist_combos = [list(row) for row in cur.fetchall()]

    conn.close()
    return {"has_outlier": has_outlier, "hist_combos": hist_combos}


@app.local_entrypoint()
def main(
    skip_history: bool = False,
    skip_forecast: bool = False,
    force: bool = False,
    acs_mode: bool = False,   # ACS data: acct = zcta5, no parcel_ladder join needed
):
    """Launch all geo-level aggregations in parallel via Modal."""

    # Run outlier + history combo discovery remotely (needs SUPABASE_DB_URL from Modal secret)
    print("  Discovering DB metadata via Modal...")
    meta = _discover_db_metadata.remote(not skip_history)
    has_outlier = meta["has_outlier"]
    hist_combos = meta["hist_combos"]

    outlier_filter = "AND mp.is_outlier IS NOT TRUE" if has_outlier else ""

    print(f"✓ Outlier filter: {'ON' if has_outlier else 'OFF'}")
    print(f"✓ Force (redo completed): {force}")
    print(f"✓ ACS mode (acct=zcta5): {acs_mode}")
    print(f"✓ Skip forecast: {skip_forecast}, Skip history: {skip_history}")

    # Fan out ALL levels in parallel — each gets its own Modal container + DB connection
    futures = []

    if acs_mode:
        # ACS path: acct IS zcta5; no parcel_ladder join — directly copy to zcta and zip3
        print("\n  [ACS MODE] Dispatching zcta + zip3 direct-copy tasks...")
        if not skip_forecast:
            for h in HORIZONS:
                futures.append(rebuild_acs_zcta_forecast.spawn(h, force))
                futures.append(rebuild_acs_zip3_forecast.spawn(h, force))
        if not skip_history:
            print(f"  [ACS MODE] {len(hist_combos)} history combos")
            for sk, vid, yr in hist_combos:
                futures.append(rebuild_acs_zcta_history.spawn(sk, vid, yr, force))
                futures.append(rebuild_acs_zip3_history.spawn(sk, vid, yr, force))
    else:
        if not skip_forecast:
            for ln, gc, ft, _ in AGG_LEVELS:
                for h in HORIZONS:
                    futures.append(rebuild_forecast_level_horizon.spawn(ln, gc, ft, h, outlier_filter, force))

        if not skip_history:
            print(f"  Found {len(hist_combos)} history combos to process across all {len(AGG_LEVELS)} levels.")
            for ln, gc, _, ht in AGG_LEVELS:
                for sk, vid, yr in hist_combos:
                    futures.append(rebuild_history_level_combo_year.spawn(ln, gc, ht, sk, vid, yr, force))

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


# ─────────────────────────────────────────────────────────────────────────────
# ACS-mode helpers: acct = census tract GEOID (11 chars), no parcel_ladder join
# ─────────────────────────────────────────────────────────────────────────────

@app.function(image=image, secrets=[supabase_secret], timeout=3600, memory=2048, max_containers=5)
def rebuild_acs_zcta_forecast(horizon: int, force: bool = False):
    """ACS: copy metrics_parcel_forecast (acct=tract_geoid20, len=11) → metrics_tract_forecast."""
    import time
    conn, cur = _connect()
    level, table, geo_col, acct_len = "tract", "metrics_tract_forecast", "tract_geoid20", 11

    if not _table_exists(cur, SCHEMA, table):
        print(f"[{level}] ⚠ {table} does not exist — skipping")
        return {"level": level, "type": f"forecast_{horizon}m", "rows": 0, "status": "skipped"}

    cur.execute("SELECT 1 FROM information_schema.columns WHERE table_schema=%s AND table_name=%s AND column_name='series_kind'", (SCHEMA, table))
    has_sk = cur.fetchone() is not None

    if not force and _horizon_done(cur, SCHEMA, table, horizon, has_sk):
        print(f"  [{level}] ACS horizon {horizon}m — already done, skipping")
        return {"level": level, "type": f"forecast_{horizon}m", "rows": 0, "status": "already_done"}

    t0 = time.time()
    if has_sk:
        sk_cols = ", series_kind, variant_id"
        sk_vals = ", mp.series_kind, mp.variant_id"
        sk_conflict = ", series_kind, variant_id"
    else:
        sk_cols = sk_vals = sk_conflict = ""

    cur.execute(f"""
        INSERT INTO {SCHEMA}.{table}
            ({geo_col}, origin_year, horizon_m, forecast_year,
             value, p10, p25, p50, p75, p90, n,
             run_id, backtest_id, model_version, as_of_date, n_scenarios,
             is_backtest{sk_cols}, jurisdiction, inserted_at, updated_at)
        SELECT
            mp.acct,
            mp.origin_year, mp.horizon_m, mp.forecast_year,
            AVG(mp.value), AVG(mp.p10), AVG(mp.p25), AVG(mp.p50),
            AVG(mp.p75), AVG(mp.p90), COUNT(*),
            MAX(mp.run_id), MAX(mp.backtest_id), MAX(mp.model_version),
            MAX(mp.as_of_date), MAX(mp.n_scenarios)::int,
            false{sk_vals}, MAX(mp.jurisdiction), now(), now()
        FROM {SCHEMA}.metrics_parcel_forecast mp
        WHERE mp.series_kind = 'forecast'
          AND mp.variant_id = '__forecast__'
          AND mp.horizon_m = {horizon}
          AND LENGTH(mp.acct) = {acct_len}
        GROUP BY mp.acct, mp.origin_year, mp.horizon_m, mp.forecast_year{sk_vals}
        ON CONFLICT ({geo_col}, origin_year, horizon_m{sk_conflict})
        DO UPDATE SET
            forecast_year = EXCLUDED.forecast_year,
            value = EXCLUDED.value, p10 = EXCLUDED.p10, p25 = EXCLUDED.p25,
            p50 = EXCLUDED.p50, p75 = EXCLUDED.p75, p90 = EXCLUDED.p90,
            n = EXCLUDED.n, run_id = EXCLUDED.run_id,
            model_version = EXCLUDED.model_version,
            as_of_date = EXCLUDED.as_of_date,
            n_scenarios = EXCLUDED.n_scenarios,
            jurisdiction = EXCLUDED.jurisdiction,
            updated_at = now()
    """)
    elapsed = time.time() - t0
    conn.close()
    print(f"  ✅ [{level}] ACS horizon {horizon}m: {cur.rowcount} rows ({elapsed:.1f}s)")
    return {"level": level, "type": f"forecast_{horizon}m", "rows": cur.rowcount, "status": "done"}


@app.function(image=image, secrets=[supabase_secret], timeout=3600, memory=2048, max_containers=5)
def rebuild_acs_zip3_forecast(horizon: int, force: bool = False):
    """ACS: aggregate tract (len=11) → zip3 using LEFT(acct, 3) for forecast."""
    import time
    conn, cur = _connect()
    level, table, geo_col, acct_len = "zip3", "metrics_zip3_forecast", "zip3", 11

    if not _table_exists(cur, SCHEMA, table):
        print(f"[{level}] ⚠ {table} does not exist — skipping")
        return {"level": level, "type": f"forecast_{horizon}m", "rows": 0, "status": "skipped"}

    cur.execute("SELECT 1 FROM information_schema.columns WHERE table_schema=%s AND table_name=%s AND column_name='series_kind'", (SCHEMA, table))
    has_sk = cur.fetchone() is not None

    if not force and _horizon_done(cur, SCHEMA, table, horizon, has_sk):
        print(f"  [{level}] ACS horizon {horizon}m — already done, skipping")
        return {"level": level, "type": f"forecast_{horizon}m", "rows": 0, "status": "already_done"}

    t0 = time.time()
    if has_sk:
        sk_cols = ", series_kind, variant_id"
        sk_vals = ", mp.series_kind, mp.variant_id"
        sk_conflict = ", series_kind, variant_id"
    else:
        sk_cols = sk_vals = sk_conflict = ""

    # zip3 = first 5 chars of tract GEOID = state(2) + county(3), NOT a zip code
    # Use LEFT(acct, 5) as ZCTA5 approximation when ACS data is at tract level
    # (tract geoid: SS CCC TTTTTT where SS=state, CCC=county, TTTTTT=tract)
    # There is no reliable tract→ZIP3 crosswalk here, so we use county FIPS (5 chars)
    # as the grouping key and store it in the zip3 column as a best-effort proxy.
    cur.execute(f"""
        INSERT INTO {SCHEMA}.{table}
            ({geo_col}, origin_year, horizon_m, forecast_year,
             value, p10, p25, p50, p75, p90, n,
             run_id, backtest_id, model_version, as_of_date, n_scenarios,
             is_backtest{sk_cols}, jurisdiction, inserted_at, updated_at)
        SELECT
            LEFT(mp.acct, 5),
            mp.origin_year, mp.horizon_m, mp.forecast_year,
            AVG(mp.value), AVG(mp.p10), AVG(mp.p25), AVG(mp.p50),
            AVG(mp.p75), AVG(mp.p90), COUNT(*),
            MAX(mp.run_id), MAX(mp.backtest_id), MAX(mp.model_version),
            MAX(mp.as_of_date), MAX(mp.n_scenarios)::int,
            false{sk_vals}, MAX(mp.jurisdiction), now(), now()
        FROM {SCHEMA}.metrics_parcel_forecast mp
        WHERE mp.series_kind = 'forecast'
          AND mp.variant_id = '__forecast__'
          AND mp.horizon_m = {horizon}
          AND LENGTH(mp.acct) = {acct_len}
        GROUP BY LEFT(mp.acct, 5), mp.origin_year, mp.horizon_m, mp.forecast_year{sk_vals}
        ON CONFLICT ({geo_col}, origin_year, horizon_m{sk_conflict})
        DO UPDATE SET
            forecast_year = EXCLUDED.forecast_year,
            value = EXCLUDED.value, p10 = EXCLUDED.p10, p25 = EXCLUDED.p25,
            p50 = EXCLUDED.p50, p75 = EXCLUDED.p75, p90 = EXCLUDED.p90,
            n = EXCLUDED.n, run_id = EXCLUDED.run_id,
            model_version = EXCLUDED.model_version,
            as_of_date = EXCLUDED.as_of_date,
            n_scenarios = EXCLUDED.n_scenarios,
            jurisdiction = EXCLUDED.jurisdiction,
            updated_at = now()
    """)
    elapsed = time.time() - t0
    conn.close()
    print(f"  ✅ [{level}] ACS horizon {horizon}m: {cur.rowcount} rows ({elapsed:.1f}s)")
    return {"level": level, "type": f"forecast_{horizon}m", "rows": cur.rowcount, "status": "done"}


@app.function(image=image, secrets=[supabase_secret], timeout=3600, memory=2048, max_containers=5)
def rebuild_acs_zcta_history(series_kind: str, variant_id: str, year: int, force: bool = False):
    """ACS: copy metrics_parcel_history (acct=tract_geoid20, len=11) → metrics_tract_history."""
    import time
    conn, cur = _connect()
    level, table, geo_col, acct_len = "tract", "metrics_tract_history", "tract_geoid20", 11

    if not _table_exists(cur, SCHEMA, table):
        return {"level": level, "type": f"hist_{year}", "rows": 0, "status": "skipped"}

    cur.execute("SELECT 1 FROM information_schema.columns WHERE table_schema=%s AND table_name=%s AND column_name='series_kind'", (SCHEMA, table))
    has_sk = cur.fetchone() is not None

    if not force:
        if has_sk:
            cur.execute(f"SELECT 1 FROM {SCHEMA}.{table} WHERE series_kind=%s AND variant_id=%s AND year=%s LIMIT 1", (series_kind, variant_id, year))
        else:
            cur.execute(f"SELECT 1 FROM {SCHEMA}.{table} WHERE year=%s LIMIT 1", (year,))
        if cur.fetchone():
            return {"level": level, "type": f"hist_{year}", "rows": 0, "status": "already_done"}

    t0 = time.time()
    if has_sk:
        sk_cols = ", series_kind, variant_id"
        sk_vals = ", mh.series_kind, mh.variant_id"
        sk_conflict = f", series_kind, variant_id"
    else:
        sk_cols = sk_vals = sk_conflict = ""

    cur.execute(f"""
        INSERT INTO {SCHEMA}.{table}
            ({geo_col}, year, value, p50, n, run_id, backtest_id,
             model_version, as_of_date{sk_cols}, jurisdiction, inserted_at, updated_at)
        SELECT
            mh.acct, mh.year,
            AVG(mh.value), AVG(mh.p50), COUNT(*),
            MAX(mh.run_id), MAX(mh.backtest_id),
            MAX(mh.model_version), MAX(mh.as_of_date){sk_vals},
            MAX(mh.jurisdiction), now(), now()
        FROM {SCHEMA}.metrics_parcel_history mh
        WHERE mh.series_kind = %s AND mh.variant_id = %s AND mh.year = %s
          AND LENGTH(mh.acct) = {acct_len}
        GROUP BY mh.acct, mh.year{sk_vals}
        ON CONFLICT ({geo_col}, year{sk_conflict})
        DO UPDATE SET value = EXCLUDED.value, p50 = EXCLUDED.p50,
                      jurisdiction = EXCLUDED.jurisdiction, updated_at = now()
    """, (series_kind, variant_id, year))
    elapsed = time.time() - t0
    conn.close()
    print(f"  ✅ [{level}] ACS hist {year}: {cur.rowcount} rows ({elapsed:.1f}s)")
    return {"level": level, "type": f"hist_{year}", "rows": cur.rowcount, "status": "done"}


@app.function(image=image, secrets=[supabase_secret], timeout=3600, memory=2048, max_containers=5)
def rebuild_acs_zip3_history(series_kind: str, variant_id: str, year: int, force: bool = False):
    """ACS: aggregate tract (len=11) → county-level via LEFT(acct,5) for history."""
    import time
    conn, cur = _connect()
    level, table, geo_col, acct_len = "zip3", "metrics_zip3_history", "zip3", 11

    if not _table_exists(cur, SCHEMA, table):
        return {"level": level, "type": f"hist_{year}", "rows": 0, "status": "skipped"}

    cur.execute("SELECT 1 FROM information_schema.columns WHERE table_schema=%s AND table_name=%s AND column_name='series_kind'", (SCHEMA, table))
    has_sk = cur.fetchone() is not None

    if not force:
        if has_sk:
            cur.execute(f"SELECT 1 FROM {SCHEMA}.{table} WHERE series_kind=%s AND variant_id=%s AND year=%s LIMIT 1", (series_kind, variant_id, year))
        else:
            cur.execute(f"SELECT 1 FROM {SCHEMA}.{table} WHERE year=%s LIMIT 1", (year,))
        if cur.fetchone():
            return {"level": level, "type": f"hist_{year}", "rows": 0, "status": "already_done"}

    t0 = time.time()
    if has_sk:
        sk_cols = ", series_kind, variant_id"
        sk_vals = ", mh.series_kind, mh.variant_id"
        sk_conflict = f", series_kind, variant_id"
    else:
        sk_cols = sk_vals = sk_conflict = ""

    cur.execute(f"""
        INSERT INTO {SCHEMA}.{table}
            ({geo_col}, year, value, p50, n, run_id, backtest_id,
             model_version, as_of_date{sk_cols}, inserted_at, updated_at)
        SELECT
            LEFT(mh.acct, 5), mh.year,
            AVG(mh.value), AVG(mh.p50), COUNT(*),
            MAX(mh.run_id), MAX(mh.backtest_id),
            MAX(mh.model_version), MAX(mh.as_of_date){sk_vals},
            now(), now()
        FROM {SCHEMA}.metrics_parcel_history mh
        WHERE mh.series_kind = %s AND mh.variant_id = %s AND mh.year = %s
          AND LENGTH(mh.acct) = {acct_len}
        GROUP BY LEFT(mh.acct, 5), mh.year{sk_vals}
        ON CONFLICT ({geo_col}, year{sk_conflict})
        DO UPDATE SET value = EXCLUDED.value, p50 = EXCLUDED.p50,
                      updated_at = now()
    """, (series_kind, variant_id, year))
    elapsed = time.time() - t0
    conn.close()
    print(f"  ✅ [{level}] ACS hist {year}: {cur.rowcount} rows ({elapsed:.1f}s)")
    return {"level": level, "type": f"hist_{year}", "rows": cur.rowcount, "status": "done"}


@app.function(image=image, secrets=[supabase_secret], timeout=3600, memory=2048, max_containers=5)
def rebuild_acs_zcta_forecast(horizon: int, force: bool = False):
    """ACS: copy metrics_parcel_forecast (acct=zcta5) → metrics_zcta_forecast."""
    import time
    conn, cur = _connect()

    if not _table_exists(cur, SCHEMA, "metrics_zcta_forecast"):
        print(f"[zcta] ⚠ metrics_zcta_forecast does not exist — skipping")
        return {"level": "zcta", "type": f"forecast_{horizon}m", "rows": 0, "status": "skipped"}

    cur.execute("SELECT 1 FROM information_schema.columns WHERE table_schema=%s AND table_name='metrics_zcta_forecast' AND column_name='series_kind'", (SCHEMA,))
    has_sk = cur.fetchone() is not None

    if not force and _horizon_done(cur, SCHEMA, "metrics_zcta_forecast", horizon, has_sk):
        print(f"  [zcta] ACS horizon {horizon}m — already done, skipping")
        return {"level": "zcta", "type": f"forecast_{horizon}m", "rows": 0, "status": "already_done"}

    t0 = time.time()
    if has_sk:
        sk_cols = ", series_kind, variant_id"
        sk_vals = ", mp.series_kind, mp.variant_id"
        sk_conflict = ", series_kind, variant_id"
        sk_update = ""
    else:
        sk_cols = sk_vals = sk_conflict = sk_update = ""

    cur.execute(f"""
        INSERT INTO {SCHEMA}.metrics_zcta_forecast
            (zcta5, origin_year, horizon_m, forecast_year,
             value, p10, p25, p50, p75, p90, n,
             run_id, backtest_id, model_version, as_of_date, n_scenarios,
             is_backtest{sk_cols}, jurisdiction, inserted_at, updated_at)
        SELECT
            mp.acct,
            mp.origin_year, mp.horizon_m, mp.forecast_year,
            AVG(mp.value), AVG(mp.p10), AVG(mp.p25), AVG(mp.p50),
            AVG(mp.p75), AVG(mp.p90), COUNT(*),
            MAX(mp.run_id), MAX(mp.backtest_id), MAX(mp.model_version),
            MAX(mp.as_of_date), MAX(mp.n_scenarios)::int,
            false{sk_vals}, MAX(mp.jurisdiction), now(), now()
        FROM {SCHEMA}.metrics_parcel_forecast mp
        WHERE mp.series_kind = 'forecast'
          AND mp.variant_id = '__forecast__'
          AND mp.horizon_m = {horizon}
          AND LENGTH(mp.acct) = 5
        GROUP BY mp.acct, mp.origin_year, mp.horizon_m, mp.forecast_year{sk_vals}
        ON CONFLICT (zcta5, origin_year, horizon_m{sk_conflict})
        DO UPDATE SET
            forecast_year = EXCLUDED.forecast_year,
            value = EXCLUDED.value, p10 = EXCLUDED.p10, p25 = EXCLUDED.p25,
            p50 = EXCLUDED.p50, p75 = EXCLUDED.p75, p90 = EXCLUDED.p90,
            n = EXCLUDED.n, run_id = EXCLUDED.run_id,
            model_version = EXCLUDED.model_version,
            as_of_date = EXCLUDED.as_of_date,
            n_scenarios = EXCLUDED.n_scenarios,
            jurisdiction = EXCLUDED.jurisdiction,
            updated_at = now()
    """)
    elapsed = time.time() - t0
    conn.close()
    print(f"  ✅ [zcta] ACS horizon {horizon}m: {cur.rowcount} rows ({elapsed:.1f}s)")
    return {"level": "zcta", "type": f"forecast_{horizon}m", "rows": cur.rowcount, "status": "done"}


@app.function(image=image, secrets=[supabase_secret], timeout=3600, memory=2048, max_containers=5)
def rebuild_acs_zip3_forecast(horizon: int, force: bool = False):
    """ACS: aggregate tract (acct=11-char GEOID) → zip3 via parcel_ladder_v1 crosswalk for forecast."""
    import time
    conn, cur = _connect()

    if not _table_exists(cur, SCHEMA, "metrics_zip3_forecast"):
        print(f"[zip3] ⚠ metrics_zip3_forecast does not exist — skipping")
        return {"level": "zip3", "type": f"forecast_{horizon}m", "rows": 0, "status": "skipped"}

    cur.execute("SELECT 1 FROM information_schema.columns WHERE table_schema=%s AND table_name='metrics_zip3_forecast' AND column_name='series_kind'", (SCHEMA,))
    has_sk = cur.fetchone() is not None

    if not force and _horizon_done(cur, SCHEMA, "metrics_zip3_forecast", horizon, has_sk):
        print(f"  [zip3] ACS horizon {horizon}m — already done, skipping")
        return {"level": "zip3", "type": f"forecast_{horizon}m", "rows": 0, "status": "already_done"}

    t0 = time.time()
    if has_sk:
        sk_cols = ", series_kind, variant_id"
        sk_vals = ", mp.series_kind, mp.variant_id"
        sk_conflict = ", series_kind, variant_id"
    else:
        sk_cols = sk_vals = sk_conflict = ""

    # Use parcel_ladder_v1 as tract→zcta5 crosswalk, then LEFT(zcta5, 3) = zip3
    cur.execute(f"""
        INSERT INTO {SCHEMA}.metrics_zip3_forecast
            (zip3, origin_year, horizon_m, forecast_year,
             value, p10, p25, p50, p75, p90, n,
             run_id, backtest_id, model_version, as_of_date, n_scenarios,
             is_backtest{sk_cols}, jurisdiction, inserted_at, updated_at)
        SELECT
            LEFT(pl.zcta5, 3),
            mp.origin_year, mp.horizon_m, mp.forecast_year,
            AVG(mp.value), AVG(mp.p10), AVG(mp.p25), AVG(mp.p50),
            AVG(mp.p75), AVG(mp.p90), COUNT(*),
            MAX(mp.run_id), MAX(mp.backtest_id), MAX(mp.model_version),
            MAX(mp.as_of_date), MAX(mp.n_scenarios)::int,
            false{sk_vals}, MAX(mp.jurisdiction), now(), now()
        FROM {SCHEMA}.metrics_parcel_forecast mp
        JOIN (
            SELECT DISTINCT tract_geoid20, zcta5
            FROM public.parcel_ladder_v1
            WHERE tract_geoid20 IS NOT NULL AND zcta5 IS NOT NULL
        ) pl ON pl.tract_geoid20 = mp.acct
        WHERE mp.series_kind = 'forecast'
          AND mp.variant_id = '__forecast__'
          AND mp.horizon_m = {horizon}
          AND LENGTH(mp.acct) = 11
          AND pl.zcta5 IS NOT NULL
        GROUP BY LEFT(pl.zcta5, 3), mp.origin_year, mp.horizon_m, mp.forecast_year{sk_vals}
        ON CONFLICT (zip3, origin_year, horizon_m{sk_conflict})
        DO UPDATE SET
            forecast_year = EXCLUDED.forecast_year,
            value = EXCLUDED.value, p10 = EXCLUDED.p10, p25 = EXCLUDED.p25,
            p50 = EXCLUDED.p50, p75 = EXCLUDED.p75, p90 = EXCLUDED.p90,
            n = EXCLUDED.n, run_id = EXCLUDED.run_id,
            model_version = EXCLUDED.model_version,
            as_of_date = EXCLUDED.as_of_date,
            n_scenarios = EXCLUDED.n_scenarios,
            jurisdiction = EXCLUDED.jurisdiction,
            updated_at = now()
    """)
    elapsed = time.time() - t0
    conn.close()
    print(f"  ✅ [zip3] ACS horizon {horizon}m: {cur.rowcount} rows ({elapsed:.1f}s)")
    return {"level": "zip3", "type": f"forecast_{horizon}m", "rows": cur.rowcount, "status": "done"}


@app.function(image=image, secrets=[supabase_secret], timeout=3600, memory=2048, max_containers=5)
def rebuild_acs_zcta_history(series_kind: str, variant_id: str, year: int, force: bool = False):
    """ACS: copy metrics_parcel_history (acct=zcta5) → metrics_zcta_history."""
    import time
    conn, cur = _connect()

    if not _table_exists(cur, SCHEMA, "metrics_zcta_history"):
        return {"level": "zcta", "type": f"hist_{year}", "rows": 0, "status": "skipped"}

    cur.execute("SELECT 1 FROM information_schema.columns WHERE table_schema=%s AND table_name='metrics_zcta_history' AND column_name='series_kind'", (SCHEMA,))
    has_sk = cur.fetchone() is not None

    if not force:
        if has_sk:
            cur.execute(f"SELECT 1 FROM {SCHEMA}.metrics_zcta_history WHERE series_kind=%s AND variant_id=%s AND year=%s LIMIT 1", (series_kind, variant_id, year))
        else:
            cur.execute(f"SELECT 1 FROM {SCHEMA}.metrics_zcta_history WHERE year=%s LIMIT 1", (year,))
        if cur.fetchone():
            return {"level": "zcta", "type": f"hist_{year}", "rows": 0, "status": "already_done"}

    t0 = time.time()
    if has_sk:
        sk_cols = ", series_kind, variant_id"
        sk_vals = ", mh.series_kind, mh.variant_id"
    else:
        sk_cols = sk_vals = ""

    cur.execute(f"""
        INSERT INTO {SCHEMA}.metrics_zcta_history
            (zcta5, year, value, p50, n, run_id, backtest_id,
             model_version, as_of_date{sk_cols}, jurisdiction, inserted_at, updated_at)
        SELECT
            mh.acct, mh.year,
            AVG(mh.value), AVG(mh.p50), COUNT(*),
            MAX(mh.run_id), MAX(mh.backtest_id),
            MAX(mh.model_version), MAX(mh.as_of_date){sk_vals},
            MAX(mh.jurisdiction), now(), now()
        FROM {SCHEMA}.metrics_parcel_history mh
        WHERE mh.series_kind = %s AND mh.variant_id = %s AND mh.year = %s
          AND LENGTH(mh.acct) = 5
        GROUP BY mh.acct, mh.year{sk_vals}
        ON CONFLICT (zcta5, year{sk_cols})
        DO UPDATE SET value = EXCLUDED.value, p50 = EXCLUDED.p50,
                      jurisdiction = EXCLUDED.jurisdiction, updated_at = now()
    """, (series_kind, variant_id, year))
    elapsed = time.time() - t0
    conn.close()
    print(f"  ✅ [zcta] ACS hist {year}: {cur.rowcount} rows ({elapsed:.1f}s)")
    return {"level": "zcta", "type": f"hist_{year}", "rows": cur.rowcount, "status": "done"}


@app.function(image=image, secrets=[supabase_secret], timeout=3600, memory=2048, max_containers=5)
def rebuild_acs_zip3_history(series_kind: str, variant_id: str, year: int, force: bool = False):
    """ACS: aggregate tract (acct=11-char GEOID) → zip3 via parcel_ladder_v1 crosswalk for history."""
    import time
    conn, cur = _connect()

    if not _table_exists(cur, SCHEMA, "metrics_zip3_history"):
        return {"level": "zip3", "type": f"hist_{year}", "rows": 0, "status": "skipped"}

    cur.execute("SELECT 1 FROM information_schema.columns WHERE table_schema=%s AND table_name='metrics_zip3_history' AND column_name='series_kind'", (SCHEMA,))
    has_sk = cur.fetchone() is not None

    if not force:
        if has_sk:
            cur.execute(f"SELECT 1 FROM {SCHEMA}.metrics_zip3_history WHERE series_kind=%s AND variant_id=%s AND year=%s LIMIT 1", (series_kind, variant_id, year))
        else:
            cur.execute(f"SELECT 1 FROM {SCHEMA}.metrics_zip3_history WHERE year=%s LIMIT 1", (year,))
        if cur.fetchone():
            return {"level": "zip3", "type": f"hist_{year}", "rows": 0, "status": "already_done"}

    t0 = time.time()
    if has_sk:
        sk_cols = ", series_kind, variant_id"
        sk_vals = ", mh.series_kind, mh.variant_id"
    else:
        sk_cols = sk_vals = ""

    # Use parcel_ladder_v1 as tract→zcta5 crosswalk, then LEFT(zcta5, 3) = zip3
    cur.execute(f"""
        INSERT INTO {SCHEMA}.metrics_zip3_history
            (zip3, year, value, p50, n, run_id, backtest_id,
             model_version, as_of_date{sk_cols}, jurisdiction, inserted_at, updated_at)
        SELECT
            LEFT(pl.zcta5, 3), mh.year,
            AVG(mh.value), AVG(mh.p50), COUNT(*),
            MAX(mh.run_id), MAX(mh.backtest_id),
            MAX(mh.model_version), MAX(mh.as_of_date){sk_vals},
            MAX(mh.jurisdiction), now(), now()
        FROM {SCHEMA}.metrics_parcel_history mh
        JOIN (
            SELECT DISTINCT tract_geoid20, zcta5
            FROM public.parcel_ladder_v1
            WHERE tract_geoid20 IS NOT NULL AND zcta5 IS NOT NULL
        ) pl ON pl.tract_geoid20 = mh.acct
        WHERE mh.series_kind = %s AND mh.variant_id = %s AND mh.year = %s
          AND LENGTH(mh.acct) = 11
          AND pl.zcta5 IS NOT NULL
        GROUP BY LEFT(pl.zcta5, 3), mh.year{sk_vals}
        ON CONFLICT (zip3, year{sk_cols})
        DO UPDATE SET value = EXCLUDED.value, p50 = EXCLUDED.p50,
                      jurisdiction = EXCLUDED.jurisdiction, updated_at = now()
    """, (series_kind, variant_id, year))
    elapsed = time.time() - t0
    conn.close()
    print(f"  ✅ [zip3] ACS hist {year}: {cur.rowcount} rows ({elapsed:.1f}s)")
    return {"level": "zip3", "type": f"hist_{year}", "rows": cur.rowcount, "status": "done"}
