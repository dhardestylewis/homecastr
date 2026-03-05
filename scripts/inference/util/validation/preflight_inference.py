# =============================================================================
# PREFLIGHT CHECK — run this cell BEFORE the inference script
# =============================================================================
# Verifies all objects, functions, data, and connections are available.
# Paste into a Colab cell and run. Fix any ❌ before launching inference.
# =============================================================================

import os, sys, time, traceback, random
import numpy as np
import pandas as pd

_pass = 0
_fail = 0
_warn = 0

def _ok(msg):
    global _pass; _pass += 1; print(f"  ✅ {msg}")

def _bad(msg):
    global _fail; _fail += 1; print(f"  ❌ {msg}")

def _meh(msg):
    global _warn; _warn += 1; print(f"  ⚠️  {msg}")

def _check(cond, ok_msg, fail_msg):
    if cond:
        _ok(ok_msg)
    else:
        _bad(fail_msg)
    return cond


print("=" * 70)
print("  INFERENCE PREFLIGHT CHECK")
print("=" * 70)

# ── 1. Required globals ─────────────────────────────────────────────────
print("\n[1] REQUIRED GLOBAL VARIABLES")

_check("CKPT_DIR" in dir() or "CKPT_DIR" in globals(),
       f"CKPT_DIR defined = {globals().get('CKPT_DIR', '?')}",
       "CKPT_DIR is not defined")

_check("all_accts" in dir() or "all_accts" in globals(),
       f"all_accts defined, len={len(globals().get('all_accts', []))}",
       "all_accts is not defined")

has_lf = "lf" in dir() or "lf" in globals()
_check(has_lf, "lf (LazyFrame/DataFrame) defined", "lf is not defined")

_check("SUPABASE_DB_URL" in dir() or "SUPABASE_DB_URL" in globals() or os.environ.get("SUPABASE_DB_URL"),
       "SUPABASE_DB_URL available",
       "SUPABASE_DB_URL not set (env var or global)")

# ── 2. Required helper functions ────────────────────────────────────────
print("\n[2] REQUIRED HELPER FUNCTIONS")

required_funcs = [
    "_get_checkpoint_paths",
    "_load_ckpt_into_live_objects",
    "_build_inference_for_accounts_at_origin",
    "_sample_scenarios_for_inference_context",
    "_materialize_actual_prices_for_accounts",
]
for fn in required_funcs:
    _check(fn in dir() or fn in globals(),
           f"{fn}() defined",
           f"{fn}() is NOT defined — inference will fail")

# ── 3. Checkpoint files ────────────────────────────────────────────────
print("\n[3] CHECKPOINT FILES")

ckpt_pairs = []
if "CKPT_DIR" in globals():
    ckpt_dir = globals()["CKPT_DIR"]
    if os.path.isdir(ckpt_dir):
        _ok(f"CKPT_DIR exists: {ckpt_dir}")
        try:
            ckpt_pairs = _get_checkpoint_paths(ckpt_dir)
            ckpt_pairs = sorted([(int(o), p) for o, p in ckpt_pairs], key=lambda x: x[0])
            _ok(f"Found {len(ckpt_pairs)} checkpoints: origins={[o for o,_ in ckpt_pairs]}")
            for o, p in ckpt_pairs:
                if os.path.exists(p):
                    sz_mb = os.path.getsize(p) / 1e6
                    _ok(f"  ckpt origin={o} exists ({sz_mb:.0f} MB)")
                else:
                    _bad(f"  ckpt origin={o} MISSING: {p}")

            origin = globals().get("FORECAST_ORIGIN_YEAR", 2025)
            has_origin = any(o == origin for o, _ in ckpt_pairs)
            _check(has_origin,
                   f"Checkpoint for FORECAST_ORIGIN_YEAR={origin} found",
                   f"No checkpoint for FORECAST_ORIGIN_YEAR={origin}!")
        except Exception as e:
            _bad(f"_get_checkpoint_paths failed: {e}")
    else:
        _bad(f"CKPT_DIR does not exist: {ckpt_dir}")
else:
    _bad("CKPT_DIR not defined, skipping checkpoint check")

# ── 4. Data object (lf) ────────────────────────────────────────────────
print("\n[4] DATA OBJECT (lf) INSPECTION")

if has_lf:
    lf_obj = globals().get("lf")
    lf_type = type(lf_obj).__name__
    _ok(f"lf type: {lf_type}")

    try:
        if hasattr(lf_obj, "columns"):
            cols = list(lf_obj.columns)
        elif hasattr(lf_obj, "schema"):
            cols = list(lf_obj.schema.keys()) if hasattr(lf_obj.schema, "keys") else [str(c) for c in lf_obj.schema]
        else:
            cols = []
        if cols:
            _ok(f"lf has {len(cols)} columns")
            for want in ["acct", "year"]:
                matches = [c for c in cols if want in c.lower()]
                _check(len(matches) > 0,
                       f"  Column matching '{want}' found: {matches[:3]}",
                       f"  No column matching '{want}' in lf")
        else:
            _meh("Could not inspect lf columns")
    except Exception as e:
        _meh(f"Could not inspect lf columns: {e}")

    origin = globals().get("FORECAST_ORIGIN_YEAR", 2025)
    all_accts_g = globals().get("all_accts", [])

    # Materialize check with a random sample
    if all_accts_g and "_materialize_actual_prices_for_accounts" in globals():
        sample = [str(a) for a in random.sample(list(all_accts_g), min(20, len(all_accts_g)))]
        try:
            hist = _materialize_actual_prices_for_accounts(
                lf_obj=lf_obj, accts=sample,
                year_min=2005, year_max=int(origin),
            )
            if hist is not None and not hist.empty:
                _ok(f"_materialize_actual_prices works (sample: {len(hist)} rows)")
                if "year" in hist.columns:
                    years = sorted(hist["year"].unique())
                    has_anchor = int(origin) in [int(y) for y in years]
                    _check(has_anchor,
                           f"Random sample has data at anchor year {origin}",
                           f"Random sample has NO data at anchor year {origin}! Years: {years}")
            else:
                _bad(f"_materialize_actual_prices returned empty for sample accts")
        except Exception as e:
            _bad(f"_materialize_actual_prices failed: {e}")
else:
    _bad("lf not available, skipping data checks")

# ── 5. Inference context smoke test ─────────────────────────────────────
# Tests THREE populations: random sample, resume-remaining, and done accts
print("\n[5] INFERENCE CONTEXT SMOKE TEST")

if has_lf and all_accts_g and "_build_inference_for_accounts_at_origin" in globals():
    origin = globals().get("FORECAST_ORIGIN_YEAR", 2025)

    # Load checkpoint once
    ckpt_path = None
    if ckpt_pairs:
        for o, p in ckpt_pairs:
            if o == origin:
                ckpt_path = p
                break
        if ckpt_path is None:
            ckpt_path = ckpt_pairs[-1][1]

    gm = {}
    if ckpt_path:
        try:
            ckpt = _load_ckpt_into_live_objects(ckpt_path)
            gm = ckpt.get("global_medians", {})
            _ok(f"Checkpoint loaded, global_medians keys={len(gm)}")
        except Exception as e:
            _bad(f"Failed to load checkpoint: {e}")

    def _test_batch(label, accts_list, n=50):
        """Test a batch and return (n_valid, n_tested)."""
        sample = [str(a) for a in accts_list[:n]]
        if not sample:
            _meh(f"  {label}: no accounts to test")
            return 0, 0
        try:
            t0 = time.time()
            ctx = _build_inference_for_accounts_at_origin(
                accts_batch=sample, origin=int(origin), global_medians=gm,
            )
            dt = time.time() - t0
            if ctx is None or len(ctx.get("acct", [])) == 0:
                _bad(f"  {label}: 0/{len(sample)} valid anchors ({dt:.1f}s)")
                return 0, len(sample)
            else:
                n_ctx = len(ctx["acct"])
                pct = 100.0 * n_ctx / len(sample)
                _ok(f"  {label}: {n_ctx}/{len(sample)} valid anchors ({pct:.0f}%) ({dt:.1f}s)")
                return n_ctx, len(sample)
        except Exception as e:
            _bad(f"  {label}: error — {e}")
            return 0, len(sample)

    # 5a) Random sample from full account list
    print("\n  --- 5a) Random sample from ALL accounts ---")
    random_sample = random.sample(list(all_accts_g), min(50, len(all_accts_g)))
    n_valid_random, n_tested_random = _test_batch("Random(all_accts)", random_sample)

    # 5b) Resume-aware: test accounts the run would ACTUALLY process
    print("\n  --- 5b) Resume-aware: accounts the run will actually process ---")
    resume_mode = globals().get("RESUME_MODE", False)
    resume_run_id = globals().get("RESUME_PROD_RUN_ID", None)
    done_accts = set()

    if resume_mode and resume_run_id:
        try:
            db_url = globals().get("SUPABASE_DB_URL") or os.environ.get("SUPABASE_DB_URL", "")
            schema = globals().get("TARGET_SCHEMA", "forecast_20260220_7f31c6e4")
            if db_url:
                import psycopg2
                conn = psycopg2.connect(db_url)
                conn.autocommit = True
                cur = conn.cursor()
                cur.execute(f"""
                    SELECT DISTINCT acct
                    FROM "{schema}"."metrics_parcel_forecast"
                    WHERE run_id = %s
                """, (resume_run_id,))
                done_accts = {row[0] for row in cur.fetchall()}
                cur.close()
                conn.close()
                _ok(f"Resume: {len(done_accts):,} accounts already done in DB")
        except Exception as e:
            _meh(f"Could not query done accounts: {e}")

    if done_accts:
        remaining = [a for a in all_accts_g if str(a) not in done_accts]
        _ok(f"Resume: {len(remaining):,} accounts remaining to process")

        # Test remaining (what resume will actually run)
        remaining_sample = random.sample(remaining, min(50, len(remaining)))
        n_valid_remain, n_tested_remain = _test_batch("Remaining(resume)", remaining_sample)

        # Test done (should mostly pass — sanity check)
        print("\n  --- 5c) Sanity: accounts already forecasted (should pass) ---")
        done_sample = random.sample(list(done_accts), min(50, len(done_accts)))
        n_valid_done, n_tested_done = _test_batch("Done(already forecasted)", done_sample)

        # Verdict on resume viability
        print(f"\n  --- Resume viability ---")
        if n_valid_remain == 0 and n_valid_done > 0:
            _meh(f"⚠️  ALL remaining accounts fail anchor check!")
            _meh(f"   Done accts pass ({n_valid_done}/{n_tested_done}), remaining fail ({n_valid_remain}/{n_tested_remain})")
            _meh(f"   Resume will likely produce ZERO new forecasts.")
            _meh(f"   The {len(done_accts):,} already-forecasted parcels may be the complete result.")
            total = len(all_accts_g)
            _meh(f"   Coverage: {len(done_accts):,}/{total:,} ({100.0*len(done_accts)/total:.1f}%)")
        elif n_valid_remain > 0:
            pct = 100.0 * n_valid_remain / max(n_tested_remain, 1)
            est_new = int(pct / 100.0 * len(remaining))
            _ok(f"Resume should produce ~{est_new:,} new forecasts ({pct:.0f}% anchor rate)")
    else:
        if resume_mode:
            _meh("Resume mode enabled but could not determine done accounts")
        else:
            _ok("Fresh run (no resume) — all accounts will be processed")

    # 5d) Quick sampling test if we got any valid context
    if n_valid_random > 0:
        print("\n  --- 5d) Sampler smoke test ---")
        test_sample = random.sample(list(all_accts_g), min(50, len(all_accts_g)))
        try:
            ctx = _build_inference_for_accounts_at_origin(
                accts_batch=[str(a) for a in test_sample],
                origin=int(origin), global_medians=gm,
            )
            if ctx and len(ctx.get("acct", [])) > 0:
                inf_out = _sample_scenarios_for_inference_context(
                    ctx=ctx, H=5, S=4, origin=int(origin), prop_batch_size=64,
                )
                if inf_out is not None and "acct" in inf_out:
                    _ok(f"Sampling works: {len(inf_out['acct'])} accts, "
                        f"price_levels shape={np.array(inf_out['price_levels']).shape}")
                else:
                    _bad("Sampler returned None or missing 'acct'")
        except Exception as e:
            _bad(f"Sampler failed: {e}")
else:
    _bad("Cannot run smoke test — missing lf, all_accts, or helper functions")

# ── 6. Database connection + write test ─────────────────────────────────
print("\n[6] DATABASE CONNECTION & WRITE TEST")

db_url = globals().get("SUPABASE_DB_URL") or os.environ.get("SUPABASE_DB_URL", "")
if db_url:
    try:
        import psycopg2
        from psycopg2.extras import execute_values
        conn = psycopg2.connect(db_url)
        conn.autocommit = True
        cur = conn.cursor()

        schema = globals().get("TARGET_SCHEMA", "forecast_20260220_7f31c6e4")

        # 6a) Schema + tables exist
        cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s", (schema,))
        _check(cur.fetchone() is not None,
               f"Schema '{schema}' exists",
               f"Schema '{schema}' NOT FOUND in database")

        write_test_tables = []
        for tbl in ["metrics_parcel_forecast", "metrics_parcel_history",
                     "inference_runs", "inference_run_progress"]:
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            """, (schema, tbl))
            if cur.fetchone() is not None:
                _ok(f"  Table {schema}.{tbl} exists")
                write_test_tables.append(tbl)
            else:
                _bad(f"  Table {schema}.{tbl} MISSING")

        cur.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'parcel_ladder_v1'
        """)
        r = cur.fetchone()
        _check(r and r[0] > 0,
               "public.parcel_ladder_v1 exists (needed for aggregation)",
               "public.parcel_ladder_v1 MISSING (aggregation will fail)")

        cur.close()
        conn.close()
        _ok("DB read connection passed")

        # 6b) Write test: INSERT a sentinel row then ROLLBACK (no trace left)
        #      Uses valid CHECK constraint values from schema:
        #        inference_runs.status in ('running','completed','failed','cancelled')
        #        forecast.series_kind in ('forecast','backtest'), variant must match
        #        history.series_kind in ('history','backtest'), variant must match
        print("\n  --- 6b) Write permission test (insert + rollback) ---")
        _preflight_sentinel = "__preflight_test__"

        # Test inference_runs
        if "inference_runs" in write_test_tables:
            try:
                conn2 = psycopg2.connect(db_url)
                conn2.autocommit = False
                cur2 = conn2.cursor()
                cur2.execute(f"""
                    INSERT INTO "{schema}"."inference_runs"
                    (run_id, level_name, mode, origin_year, as_of_date, model_version, n_scenarios, status, started_at)
                    VALUES (%s, 'parcel', 'forecast', 2000, '2000-01-01', 'preflight_test', 1, 'running', now())
                """, (_preflight_sentinel,))
                conn2.rollback()
                conn2.close()
                _ok("Write test: INSERT into inference_runs succeeded (rolled back)")
            except Exception as e:
                try: conn2.rollback()
                except: pass
                try: conn2.close()
                except: pass
                _bad(f"Write test: INSERT into inference_runs FAILED: {e}")

        # Test metrics_parcel_forecast (series_kind='forecast', variant_id='__forecast__')
        if "metrics_parcel_forecast" in write_test_tables:
            try:
                conn2 = psycopg2.connect(db_url)
                conn2.autocommit = False
                cur2 = conn2.cursor()
                cur2.execute(f"""
                    INSERT INTO "{schema}"."metrics_parcel_forecast"
                    (acct, origin_year, horizon_m, forecast_year, value, p10, p25, p50, p75, p90,
                     run_id, variant_id, model_version, as_of_date, n_scenarios,
                     is_backtest, series_kind, inserted_at, updated_at)
                    VALUES ('__preflight__', 2000, 12, 2001, 0, 0, 0, 0, 0, 0,
                            %s, '__forecast__', 'preflight_test', '2000-01-01', 1,
                            false, 'forecast', now(), now())
                """, (_preflight_sentinel,))
                conn2.rollback()
                conn2.close()
                _ok("Write test: INSERT into metrics_parcel_forecast succeeded (rolled back)")
            except Exception as e:
                try: conn2.rollback()
                except: pass
                try: conn2.close()
                except: pass
                _bad(f"Write test: INSERT into metrics_parcel_forecast FAILED: {e}")

        # Test metrics_parcel_history (series_kind='history', variant_id='__history__')
        if "metrics_parcel_history" in write_test_tables:
            try:
                conn2 = psycopg2.connect(db_url)
                conn2.autocommit = False
                cur2 = conn2.cursor()
                cur2.execute(f"""
                    INSERT INTO "{schema}"."metrics_parcel_history"
                    (acct, year, value, p50, n,
                     run_id, variant_id, model_version, as_of_date,
                     series_kind, inserted_at, updated_at)
                    VALUES ('__preflight__', 2000, 0, 0, 1,
                            %s, '__history__', 'preflight_test', '2000-01-01',
                            'history', now(), now())
                """, (_preflight_sentinel,))
                conn2.rollback()
                conn2.close()
                _ok("Write test: INSERT into metrics_parcel_history succeeded (rolled back)")
            except Exception as e:
                try: conn2.rollback()
                except: pass
                try: conn2.close()
                except: pass
                _bad(f"Write test: INSERT into metrics_parcel_history FAILED: {e}")

    except Exception as e:
        _bad(f"DB connection failed: {e}")
else:
    _bad("No SUPABASE_DB_URL — DB writes will fail")

# ── 7. GPU / CUDA ──────────────────────────────────────────────────────
print("\n[7] GPU / CUDA")

try:
    import torch
    if torch.cuda.is_available():
        props = torch.cuda.get_device_properties(0)
        mem_gb = getattr(props, "total_memory", getattr(props, "total_mem", 0)) / 1e9
        _ok(f"CUDA available: {torch.cuda.get_device_name(0)}, mem={mem_gb:.1f} GB")
    else:
        _bad("CUDA NOT available — sampling will be very slow")
except ImportError:
    _meh("torch not imported (may be fine if model doesn't need GPU)")
except Exception as e:
    _meh(f"GPU check error (non-fatal): {e}")

# ── SUMMARY ─────────────────────────────────────────────────────────────
print(f"\n{'=' * 70}")
print(f"  PREFLIGHT SUMMARY: {_pass} passed, {_fail} failed, {_warn} warnings")
if _fail == 0:
    print("  🟢 ALL CHECKS PASSED — safe to launch inference")
else:
    print(f"  🔴 {_fail} CHECK(S) FAILED — fix before launching inference")
print("=" * 70)
