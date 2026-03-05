# =============================================================================
# PRODUCTION + FULL BACKTEST STREAMING PIPELINE (COLAB -> SUPABASE POSTGRES)
# -----------------------------------------------------------------------------
# Target schema: forecast_YYYYMMDD_<uid> (or any schema created by your DDL)
#
# What this writes:
#   1) Parcel history      -> <schema>.metrics_parcel_history
#   2) Parcel forecast fan -> <schema>.metrics_parcel_forecast
#   3) Higher-level aggs   -> <schema>.metrics_{tabblock,tract,zcta,unsd,neighborhood}_{history,forecast}
#   4) Run progress        -> <schema>.inference_runs, <schema>.inference_run_progress
#
# Notes:
# - Uses direct Postgres (psycopg2) for fast bulk upserts.
# - Uses short-lived DB transactions to avoid Supabase pooler idle timeout failures.
# - Uses weighted ON CONFLICT merges for chunked geo aggregations (correct across chunks).
# - Optionally runs a final exact aggregate refresh at the end of each run for bulletproof consistency.
# =============================================================================

# If needed in Colab, run once:
# !pip -q install psycopg2-binary pyarrow

import os
import re
import time
import json
import uuid
import math
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, Future
from datetime import datetime, date

import numpy as np
import pandas as pd

try:
    import pyarrow  # noqa
    _HAS_PARQUET = True
except Exception:
    _HAS_PARQUET = False

# -----------------------------------------------------------------------------
# CUDA / PyTorch performance setup
# -----------------------------------------------------------------------------
try:
    import torch as _torch
    if _torch.cuda.is_available():
        # Let cuDNN auto-tune its conv algorithms for fixed input shapes — free win.
        _torch.backends.cudnn.benchmark = True
        # Avoid storing gradients during inference — halves VRAM use and skips grad ops.
        _torch.set_grad_enabled(False)
        # Use all available CPU threads for intra-op parallelism.
        import os as _os
        _cpu_count = _os.cpu_count() or 4
        _torch.set_num_threads(_cpu_count)
        try:
            _torch.set_num_interop_threads(max(1, _cpu_count // 2))
        except RuntimeError:
            pass  # already set by worldmodel.py
        print(f"[PERF] CUDA: {_torch.cuda.get_device_name(0)} | "
              f"cudnn.benchmark=on | grad=off | CPU threads={_cpu_count}")
    else:
        import os as _os
        _cpu_count = _os.cpu_count() or 4
        _torch.set_num_threads(_cpu_count)
except ImportError:
    pass  # torch not available at import time; worldmodel.py will have loaded it

import psycopg2
from psycopg2.extras import execute_values


# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------
# >>>> REQUIRED <<<<
TARGET_SCHEMA = globals().get("TARGET_SCHEMA", "forecast_queue")   # Default to queue to protect prod
SUPABASE_DB_URL = (os.environ.get("SUPABASE_DB_URL")
                   or os.environ.get("POSTGRES_URL")
                   or os.environ.get("POSTGRES_URL_NON_POOLING")
                   or "")  # transaction pooler URL is fine

# Checkpoint directory — auto-detect from worldmodel.py OUT_DIR if not set explicitly
CKPT_DIR = globals().get("CKPT_DIR") or globals().get("OUT_DIR") or os.environ.get("CKPT_DIR", "")

# Production forecast anchor
FORECAST_ORIGIN_YEAR = int(globals().get("FORECAST_ORIGIN_YEAR", 2025))

# Backtests
RUN_FULL_BACKTEST = False
BACKTEST_ORIGINS = None
WRITE_BACKTEST_HISTORY_VARIANTS = False

# Model sampling
H = int(globals().get("H", 5))                 # annual horizons (1..H years ahead)
S = int(globals().get("S_SCENARIOS", 256))     # scenarios
MODEL_VERSION = "world_model_v11_inducing_token"

# Checkpoint variant — set to "" for baseline, "SF500K" for the 500K-sample variant,
# "SF200K", "SFstrat200K", etc. Must match the suffix in ckpt_origin_{year}_{suffix}.pt.
CKPT_VARIANT_SUFFIX = globals().get("CKPT_VARIANT_SUFFIX", "SF500K")

# Post-Hoc Calibration parameters
# Loads interpolator dict from CKPT_DIR/calibrators_v11_sf_ca.pkl
# See sweep_stage1_calibration.py for fit script.
APPLY_CALIBRATION = globals().get("APPLY_CALIBRATION", True)
VALUE_BRACKETS = [
    ("under_250k", 0, 250_000),
    ("250k_to_500k", 250_000, 500_000),
    ("500k_to_1m", 500_000, 1_000_000),
    ("over_1m", 1_000_000, 100_000_000_000),
]

# -----------------------------------------------------------------------------
# DAG & CHUNKING CONSTANTS
JURISDICTION = globals().get("JURISDICTION", "hcad")

# A100 / sampler tuning (adaptive backoff wrapper will use these)
PROP_BATCH_SIZE_SAMPLER = int(globals().get("PROP_BATCH_SIZE_SAMPLER", 4096))
PROP_BATCH_SIZE_MIN = int(globals().get("PROP_BATCH_SIZE_MIN", 512))
ACCT_BATCH_SIZE_OUTER = int(globals().get("ACCT_BATCH_SIZE_OUTER", 200_000))

# Inference noise temperature
SAMPLER_TAU = float(globals().get("SAMPLER_TAU", 1.0))

# Chunking
PG_BATCH_ROWS = 5000

# Timeout / Retry
PG_STATEMENT_TIMEOUT_MS = 300_000   # 5 minutes per SQL statement
PG_TX_MAX_RETRIES = 100               # retries on transient DB errors
PG_TX_BACKOFF_BASE = 5              # base seconds for exponential backoff

# Schema compatibility:
# Set this True only if <schema>.metrics_parcel_forecast has a column named "n".
# Many DDLs keep only "n_scenarios" on parcel forecast rows.
PARCEL_FORECAST_HAS_N_COL = bool(globals().get("PARCEL_FORECAST_HAS_N_COL", False))

# Aggregate tables are assumed to have "n" (parcel count per geography). Required for weighted chunk merge.
AGG_FORECAST_HAS_N_COL = True

# Final exact refresh (recommended): delete+recompute aggregate rows from parcel rows for this run slice
RUN_FINAL_EXACT_AGG_REFRESH = True

# =============================================================================
# RESUME CONFIG  —  fill these in to restart from where you left off
# =============================================================================
# Set RESUME_MODE = True and paste the run_id / suite_id from the previous run.
# The script will query Supabase for accounts already written under that run_id
# and skip them.  Everything else (upserts, aggregates, final refresh) is
# idempotent so this is safe.
RESUME_MODE = False
RESUME_PROD_RUN_ID = "forecast_2025_20260221T050431Z_8cdcf33c7b7d4ee6a948ecc6bccca160"
RESUME_SUITE_ID    = "suite_20260221T050431Z_5b263b4bbbc344fa998999209fc58549"

# For backtest resume — dict of { origin_year: (run_id, variant_id, backtest_id) }
# Leave as None to skip backtest resume.  Example:
#   RESUME_BACKTEST_RUNS = {
#       2021: ("backtest_2021_20260221T..._abc", "bt_2021_xyz", "bt_2021_xyz"),
#       2022: ("backtest_2022_20260221T..._def", "bt_2022_xyz", "bt_2022_xyz"),
#   }
RESUME_BACKTEST_RUNS = None
# =============================================================================

# Output root (Colab-mounted Google Drive)
if RESUME_MODE and RESUME_SUITE_ID:
    SUITE_ID = RESUME_SUITE_ID
else:
    RUN_STAMP = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    SUITE_UID = uuid.uuid4().hex
    SUITE_ID = f"suite_{RUN_STAMP}_{SUITE_UID}"

OUT_ROOT = f"/content/drive/MyDrive/data_backups/world_model_v10_2_fullpanel/live_inference_runs/{SUITE_ID}"
OUT_PROD_DIR = os.path.join(OUT_ROOT, "production")
OUT_BT_DIR = os.path.join(OUT_ROOT, "backtests")
os.makedirs(OUT_PROD_DIR, exist_ok=True)
os.makedirs(OUT_BT_DIR, exist_ok=True)

# SQL safety
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


# -----------------------------------------------------------------------------
# BASIC HELPERS
# -----------------------------------------------------------------------------
def _ts():
    return time.strftime("%Y-%m-%d %H:%M:%S")

def _assert_ident(x: str, label: str = "identifier"):
    if not isinstance(x, str) or _IDENT_RE.match(x) is None:
        raise ValueError(f"Unsafe {label}: {x!r}")
    return x

def _q_ident(x: str) -> str:
    _assert_ident(x)
    return f'"{x}"'

def _q_table(schema: str, table: str) -> str:
    return f'{_q_ident(schema)}.{_q_ident(table)}'

def _chunk_list(xs, n):
    for i in range(0, len(xs), n):
        yield xs[i:i+n]

def _write_df_chunk(df, out_dir, prefix, chunk_idx):
    os.makedirs(out_dir, exist_ok=True)
    if _HAS_PARQUET:
        fp = os.path.join(out_dir, f"{prefix}_chunk_{chunk_idx:05d}.parquet")
        df.to_parquet(fp, index=False)
    else:
        fp = os.path.join(out_dir, f"{prefix}_chunk_{chunk_idx:05d}.csv.gz")
        df.to_csv(fp, index=False, compression="gzip")
    return fp

def _py_scalar(v):
    if pd.isna(v):
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v)
    if isinstance(v, (np.bool_,)):
        return bool(v)
    return v

def _df_records(df: pd.DataFrame):
    if df is None or df.empty:
        return [], []
    cols = list(df.columns)
    out = []
    for row in df.itertuples(index=False, name=None):
        out.append(tuple(_py_scalar(v) for v in row))
    return cols, out

def _make_run_id(kind: str, origin_year: int):
    return f"{kind}_{origin_year}_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex}"

def _append_csv_row(csv_fp: str, row_dict: dict):
    df = pd.DataFrame([row_dict])
    if os.path.exists(csv_fp):
        df.to_csv(csv_fp, mode="a", header=False, index=False)
    else:
        df.to_csv(csv_fp, index=False)


def _get_completed_accts_from_db(schema: str, run_id: str, table: str = "metrics_parcel_forecast"):
    """
    Query the DB to find which accts already have rows for this run_id.
    Returns a Python set of acct strings.
    """
    q_table = _q_table(schema, table)
    sql = f"SELECT DISTINCT acct FROM {q_table} WHERE run_id = %s"
    with _pg_tx() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (run_id,))
            return {row[0] for row in cur.fetchall()}


# -----------------------------------------------------------------------------
# POSTGRES HELPERS
# -----------------------------------------------------------------------------
def _pg_connect():
    if not SUPABASE_DB_URL:
        raise RuntimeError("SUPABASE_DB_URL is required for this pipeline.")
    
    url = SUPABASE_DB_URL
    if "pooler.supabase.com" in url and "6543" in url:
        url = url.replace(":6543/", ":5432/")
        
    conn = psycopg2.connect(url)
    conn.autocommit = False
    # Set a generous statement timeout so long-running queries fail fast
    # rather than holding a connection indefinitely.
    with conn.cursor() as cur:
        cur.execute(f"SET statement_timeout = {int(PG_STATEMENT_TIMEOUT_MS)}")
    conn.commit()   # apply the SET outside a transaction block
    return conn


def _is_transient_pg_error(exc):
    """Return True if the exception looks like a retryable transient error."""
    import psycopg2.errors
    # Statement timeout, query canceled, connection errors
    if isinstance(exc, (psycopg2.OperationalError, psycopg2.InterfaceError)):
        return True
    # Specific error codes: query_canceled, statement_timeout
    code = getattr(exc, 'pgcode', None)
    if code in ('57014', '57P01', '57P02', '57P03', '08000', '08003', '08006'):
        return True
    return False


@contextmanager
def _pg_tx(retries=None, label=""):
    """
    Open a short-lived connection only for DB writes, then close immediately.
    Prevents Supabase pooler idle disconnects during long GPU sampling.

    Retries up to `retries` times on transient errors with exponential backoff.
    """
    if retries is None:
        retries = PG_TX_MAX_RETRIES

    last_exc = None
    for attempt in range(retries + 1):
        conn = None
        try:
            conn = _pg_connect()
            yield conn
            conn.commit()
            return  # success
        except GeneratorExit:
            # The caller broke out of the with-block (e.g. via return/break).
            # Commit whatever was done and exit without retrying.
            if conn is not None:
                try:
                    conn.commit()
                except Exception:
                    pass
            return
        except Exception as exc:
            last_exc = exc
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
            if attempt < retries and _is_transient_pg_error(exc):
                # Backoff similar to rebuild_aggregates_modal.py
                wait = min(60.0, PG_TX_BACKOFF_BASE * (1.5 ** attempt))
                print(f"[{_ts()}] _pg_tx{' (' + label + ')' if label else ''}: "
                      f"transient error (attempt {attempt+1}/{retries+1}), "
                      f"retrying in {wait:.1f}s: {exc}")
                time.sleep(wait)
            else:
                raise
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
    # Should not reach here, but just in case
    if last_exc is not None:
        raise last_exc

def _upsert_df_pg(conn, schema: str, table: str, df: pd.DataFrame, conflict_cols, update_cols):
    """
    Generic bulk upsert using execute_values.
    Returns attempted row count (deterministic; cur.rowcount is unreliable across pages).
    """
    if df is None or df.empty:
        return 0

    # Deduplicate on conflict columns to avoid
    # "ON CONFLICT DO UPDATE cannot affect row a second time" error
    _ck = [c for c in conflict_cols if c in df.columns]
    if _ck:
        _before = len(df)
        df = df.drop_duplicates(subset=_ck, keep="last")
        if len(df) < _before:
            print(f"[DEDUP] {table}: {_before} -> {len(df)} rows (dropped {_before - len(df)} duplicates on {_ck})")

    q_table = _q_table(schema, table)
    cols, rows = _df_records(df)
    if not rows:
        return 0

    col_sql = ", ".join(_q_ident(c) for c in cols)
    conflict_sql = ", ".join(_q_ident(c) for c in conflict_cols)
    update_sql = ", ".join([f"{_q_ident(c)} = EXCLUDED.{_q_ident(c)}" for c in update_cols])

    sql = f"""
        INSERT INTO {q_table} ({col_sql})
        VALUES %s
        ON CONFLICT ({conflict_sql})
        DO UPDATE SET {update_sql}
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=int(PG_BATCH_ROWS))
        return len(rows)

def _run_insert_start(conn, schema: str, run_id: str, level_name: str, mode: str, origin_year: int, as_of_date: date, notes: str):
    q_table = _q_table(schema, "inference_runs")
    sql = f"""
        INSERT INTO {q_table}
            (run_id, level_name, mode, origin_year, as_of_date, model_version, n_scenarios, status, started_at, notes)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, 'running', now(), %s)
        ON CONFLICT (run_id)
        DO UPDATE SET
            level_name = EXCLUDED.level_name,
            mode = EXCLUDED.mode,
            origin_year = EXCLUDED.origin_year,
            as_of_date = EXCLUDED.as_of_date,
            model_version = EXCLUDED.model_version,
            n_scenarios = EXCLUDED.n_scenarios,
            status = 'running',
            notes = EXCLUDED.notes,
            updated_at = now()
    """
    with conn.cursor() as cur:
        cur.execute(sql, (run_id, level_name, mode, int(origin_year), as_of_date, MODEL_VERSION, int(S), notes))

def _run_update_status(conn, schema: str, run_id: str, status: str):
    q_table = _q_table(schema, "inference_runs")
    if status == "completed":
        sql = f"""
            UPDATE {q_table}
            SET status = %s, completed_at = now(), updated_at = now()
            WHERE run_id = %s
        """
    else:
        sql = f"""
            UPDATE {q_table}
            SET status = %s, updated_at = now()
            WHERE run_id = %s
        """
    with conn.cursor() as cur:
        cur.execute(sql, (status, run_id))

def _run_progress_upsert(
    conn,
    schema: str,
    run_id: str,
    chunk_seq: int,
    level_name: str,
    status: str,
    series_kind: str,
    variant_id: str,
    origin_year: int,
    chunk_rows: int,
    chunk_keys: int,
    rows_upserted_total: int,
    keys_upserted_total: int,
    min_key: str,
    max_key: str,
    horizon_m: int = None,
    year: int = None,
):
    q_table = _q_table(schema, "inference_run_progress")
    sql = f"""
        INSERT INTO {q_table}
        (
            run_id, chunk_seq, level_name, status, series_kind, variant_id, origin_year, horizon_m, year,
            rows_upserted_total, keys_upserted_total, chunk_rows, chunk_keys, min_key, max_key,
            heartbeat_at, inserted_at, updated_at
        )
        VALUES
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now(), now(), now())
        ON CONFLICT (run_id, chunk_seq)
        DO UPDATE SET
            level_name = EXCLUDED.level_name,
            status = EXCLUDED.status,
            series_kind = EXCLUDED.series_kind,
            variant_id = EXCLUDED.variant_id,
            origin_year = EXCLUDED.origin_year,
            horizon_m = EXCLUDED.horizon_m,
            year = EXCLUDED.year,
            rows_upserted_total = EXCLUDED.rows_upserted_total,
            keys_upserted_total = EXCLUDED.keys_upserted_total,
            chunk_rows = EXCLUDED.chunk_rows,
            chunk_keys = EXCLUDED.chunk_keys,
            min_key = EXCLUDED.min_key,
            max_key = EXCLUDED.max_key,
            heartbeat_at = now(),
            updated_at = now()
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                run_id, int(chunk_seq), level_name, status, series_kind, variant_id, int(origin_year),
                horizon_m, year,
                int(rows_upserted_total), int(keys_upserted_total),
                int(chunk_rows), int(chunk_keys),
                min_key, max_key
            ),
        )


# -----------------------------------------------------------------------------
# FORECAST/HISTORY ROW BUILDERS (PARCEL LEVEL)
# -----------------------------------------------------------------------------
def _build_forecast_rows_from_inf_out(
    inf_out,
    origin_year: int,
    run_id: str,
    series_kind: str,
    variant_id: str,
    backtest_id: str = None,
    as_of_date: date = None,
):
    """
    inf_out must contain:
      - acct
      - y_levels (N,S,H)
      - price_levels (N,S,H)
      - anchor_prices (N,)   [optional: if present, writes h=0 anchor row]

    Writes schema-compatible parcel forecast rows:
      PK = (acct, origin_year, horizon_m, series_kind, variant_id)
    """
    as_of_date = as_of_date or datetime.utcnow().date()

    acct_arr = np.asarray(inf_out["acct"]).astype(str)
    _ = inf_out["y_levels"]  # kept for interface validation
    _raw_pl = inf_out["price_levels"]

    # Prefer GPU-resident quantile computation to avoid a round-trip.
    # Falls back to numpy if torch is unavailable or price_levels is already numpy.
    try:
        import torch as _torch
        if isinstance(_raw_pl, _torch.Tensor):
            _pt = _raw_pl.float()  # quantile needs float32 or float64
        else:
            _pt = _torch.as_tensor(np.asarray(_raw_pl, dtype=np.float32))
            if _torch.cuda.is_available():
                _pt = _pt.cuda()
        # shape: (N, S, H) → quantile over dim=1 → (5, N, H)
        _qs = _torch.quantile(_pt, _torch.tensor([0.10, 0.25, 0.50, 0.75, 0.90],
                                                   device=_pt.device), dim=1)
        q10, q25, q50, q75, q90 = (_qs[i].cpu().numpy().astype(np.float64) for i in range(5))
        price_levels_np = _pt.cpu().numpy().astype(np.float64)
        N, S_local, H_local = price_levels_np.shape
    except Exception:
        price_levels_np = np.asarray(_raw_pl, dtype=np.float64)  # (N,S,H)
        N, S_local, H_local = price_levels_np.shape
        q10 = np.percentile(price_levels_np, 10, axis=1)
        q25 = np.percentile(price_levels_np, 25, axis=1)
        q50 = np.percentile(price_levels_np, 50, axis=1)
        q75 = np.percentile(price_levels_np, 75, axis=1)
        q90 = np.percentile(price_levels_np, 90, axis=1)

    rows = []
    is_backtest = (series_kind == "backtest")
    now_iso = datetime.utcnow().isoformat()

    # ── Anchor enforcement: if anchor_prices provided, write h=0 row ──
    _anchor_arr = inf_out.get("anchor_prices", None)

    for i in range(N):
        acct = str(acct_arr[i])

        # h=0 anchor row: value = history anchor BY CONSTRUCTION
        if _anchor_arr is not None:
            anchor_val = float(_anchor_arr[i])
            if np.isfinite(anchor_val) and anchor_val > 0:
                h0_row = {
                    "acct": acct,
                    "origin_year": int(origin_year),
                    "horizon_m": 0,
                    "forecast_year": int(origin_year),
                    "value": anchor_val,
                    "p10": anchor_val,
                    "p25": anchor_val,
                    "p50": anchor_val,
                    "p75": anchor_val,
                    "p90": anchor_val,
                    "run_id": run_id,
                    "backtest_id": backtest_id,
                    "variant_id": variant_id,
                    "model_version": MODEL_VERSION,
                    "as_of_date": as_of_date.isoformat(),
                    "n_scenarios": int(S_local),
                    "is_backtest": bool(is_backtest),
                    "series_kind": series_kind,
                    "inserted_at": now_iso,
                    "updated_at": now_iso,
                }
                if PARCEL_FORECAST_HAS_N_COL:
                    h0_row["n"] = int(S_local)
                rows.append(h0_row)

        for h_idx in range(H_local):
            horizon_k = int(h_idx + 1)
            horizon_m = int(12 * horizon_k)
            forecast_year = int(origin_year + horizon_k)

            row = {
                "acct": acct,
                "origin_year": int(origin_year),
                "horizon_m": horizon_m,
                "forecast_year": forecast_year,
                "value": float(q50[i, h_idx]),
                "p10": float(q10[i, h_idx]),
                "p25": float(q25[i, h_idx]),
                "p50": float(q50[i, h_idx]),
                "p75": float(q75[i, h_idx]),
                "p90": float(q90[i, h_idx]),

                "run_id": run_id,
                "backtest_id": backtest_id,
                "variant_id": variant_id,
                "model_version": MODEL_VERSION,
                "as_of_date": as_of_date.isoformat(),
                "n_scenarios": int(S_local),

                "is_backtest": bool(is_backtest),
                "series_kind": series_kind,

                "inserted_at": now_iso,
                "updated_at": now_iso,
            }

            if PARCEL_FORECAST_HAS_N_COL:
                row["n"] = int(S_local)

            rows.append(row)

    return pd.DataFrame(rows)

def _build_history_rows_for_chunk(
    acct_chunk,
    run_id: str,
    max_year: int,
    series_kind: str,
    variant_id: str,
    backtest_id: str = None,
    min_year: int = 1900,
    as_of_date: date = None,
):
    as_of_date = as_of_date or datetime.utcnow().date()

    hist_df = _materialize_actual_prices_for_accounts(
        lf_obj=lf,
        accts=[str(a) for a in acct_chunk],
        year_min=int(min_year),
        year_max=int(max_year),
    )

    if hist_df is None or hist_df.empty:
        return pd.DataFrame(columns=[
            "acct", "year", "value", "p50", "n",
            "run_id", "backtest_id", "variant_id", "model_version", "as_of_date", "series_kind",
            "inserted_at", "updated_at"
        ])

    hist_df = hist_df.rename(columns={"actual_price": "value"}).copy()
    hist_df["acct"] = hist_df["acct"].astype(str)
    hist_df["year"] = pd.to_numeric(hist_df["year"], errors="coerce").astype("Int64")
    hist_df["value"] = pd.to_numeric(hist_df["value"], errors="coerce")

    hist_df = hist_df[hist_df["year"].notna() & np.isfinite(hist_df["value"])].copy()
    hist_df["year"] = hist_df["year"].astype(int)
    hist_df = hist_df[(hist_df["year"] >= int(min_year)) & (hist_df["year"] <= int(max_year))].copy()

    now_iso = datetime.utcnow().isoformat()
    hist_df["p50"] = hist_df["value"].astype(float)
    hist_df["n"] = 1
    hist_df["run_id"] = run_id
    hist_df["backtest_id"] = backtest_id
    hist_df["variant_id"] = variant_id
    hist_df["model_version"] = MODEL_VERSION
    hist_df["as_of_date"] = as_of_date.isoformat()
    hist_df["series_kind"] = series_kind
    hist_df["inserted_at"] = now_iso
    hist_df["updated_at"] = now_iso

    keep_cols = [
        "acct", "year", "value", "p50", "n",
        "run_id", "backtest_id", "variant_id", "model_version", "as_of_date", "series_kind",
        "inserted_at", "updated_at"
    ]
    out = hist_df[keep_cols].drop_duplicates(subset=["acct", "year", "series_kind", "variant_id"], keep="last").reset_index(drop=True)
    return out


# -----------------------------------------------------------------------------
# SERVER-SIDE AGGREGATION (PARCEL -> TABBLOCK / TRACT / ZCTA / UNSD / NEIGHBORHOOD)
# -----------------------------------------------------------------------------
AGG_LEVELS = [
    ("tabblock",    "tabblock_geoid20", "metrics_tabblock_forecast",    "metrics_tabblock_history"),
    ("tract",       "tract_geoid20",    "metrics_tract_forecast",       "metrics_tract_history"),
    ("zcta",        "zcta5",            "metrics_zcta_forecast",        "metrics_zcta_history"),
    ("unsd",        "unsd_geoid",       "metrics_unsd_forecast",        "metrics_unsd_history"),
    ("neighborhood","neighborhood_id",  "metrics_neighborhood_forecast","metrics_neighborhood_history"),
    ("zip3",        "zip3",             "metrics_zip3_forecast",        "metrics_zip3_history"),
]

def _aggregate_forecast_levels_for_chunk(
    conn,
    schema: str,
    acct_chunk,
    run_id: str,
    origin_year: int,
    series_kind: str,
    variant_id: str,
    backtest_id: str,
    as_of_date: date,
):
    """
    Chunk-local aggregate rows are merged into aggregate tables using weighted ON CONFLICT logic.
    This preserves correctness across chunk boundaries.

    Set SKIP_AGG_CHUNK_WRITES=True (via globals) to bypass all agg writes — used in parallel
    inference mode where multiple shards share geographies and a post-run rebuild is safer.
    """
    if globals().get("SKIP_AGG_CHUNK_WRITES", False):
        return 0
    if not AGG_FORECAST_HAS_N_COL:
        raise RuntimeError("AGG_FORECAST_HAS_N_COL must be True for weighted chunk aggregation.")

    total_rows = 0
    q_parcel = _q_table(schema, "metrics_parcel_forecast")

    for _, ladder_col, tbl_forecast, _ in AGG_LEVELS:
        q_target = _q_table(schema, tbl_forecast)
        
        pl_geo_col = f"pl.{_q_ident(ladder_col)}"
        if ladder_col == "zip3":
            pl_geo_col = "LEFT(pl.zcta5, 3)"

        sql = f"""
            INSERT INTO {q_target}
            (
                {_q_ident(ladder_col)},
                origin_year, horizon_m, forecast_year,
                value, p10, p25, p50, p75, p90, n,
                run_id, backtest_id, variant_id, model_version, as_of_date, n_scenarios,
                is_backtest, series_kind,
                inserted_at, updated_at
            )
            SELECT
                {pl_geo_col} AS {_q_ident(ladder_col)},
                mp.origin_year,
                mp.horizon_m,
                mp.forecast_year,

                AVG(mp.value)::float8 AS value,
                AVG(mp.p10)::float8   AS p10,
                AVG(mp.p25)::float8   AS p25,
                AVG(mp.p50)::float8   AS p50,
                AVG(mp.p75)::float8   AS p75,
                AVG(mp.p90)::float8   AS p90,
                COUNT(*)::int         AS n,

                %s::text     AS run_id,
                %s::text     AS backtest_id,
                %s::text     AS variant_id,
                %s::text     AS model_version,
                %s::date     AS as_of_date,
                MAX(mp.n_scenarios)::int AS n_scenarios,

                %s::boolean  AS is_backtest,
                %s::text     AS series_kind,

                now() AS inserted_at,
                now() AS updated_at
            FROM {q_parcel} mp
            JOIN public.parcel_ladder_v1 pl
              ON pl.acct = mp.acct
            WHERE mp.run_id = %s
              AND mp.origin_year = %s
              AND mp.series_kind = %s
              AND mp.variant_id = %s
              AND mp.acct = ANY(%s)
              AND {pl_geo_col} IS NOT NULL
            GROUP BY {pl_geo_col}, mp.origin_year, mp.horizon_m, mp.forecast_year
            ON CONFLICT ({_q_ident(ladder_col)}, origin_year, horizon_m, series_kind, variant_id)
            DO NOTHING
        """
        # NOTE: DO NOTHING is safe because _clear_stale_agg_for_run() deletes rows for
        # (origin_year, series_kind, variant_id) before the first chunk runs. Within a
        # single run, each acct_chunk is a disjoint set of accounts so geography-level
        # aggregate keys cannot collide between chunks. DO UPDATE with a weighted merge
        # was corrupting values across runs because run_id was absent from the conflict key.
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    run_id,
                    backtest_id,
                    variant_id,
                    MODEL_VERSION,
                    as_of_date,
                    (series_kind == "backtest"),
                    series_kind,
                    run_id,
                    int(origin_year),
                    series_kind,
                    variant_id,
                    list(map(str, acct_chunk)),
                ),
            )
            total_rows += int(cur.rowcount or 0)

    return total_rows

def _aggregate_history_levels_for_chunk(
    conn,
    schema: str,
    acct_chunk,
    run_id: str,
    series_kind: str,
    variant_id: str,
    backtest_id: str,
    as_of_date: date,
):
    """
    Chunk-local history aggregates are merged using weighted ON CONFLICT logic.

    Set SKIP_AGG_CHUNK_WRITES=True (via globals) to bypass all agg writes.
    """
    if globals().get("SKIP_AGG_CHUNK_WRITES", False):
        return 0
    total_rows = 0
    q_parcel = _q_table(schema, "metrics_parcel_history")

    for _, ladder_col, _, tbl_history in AGG_LEVELS:
        q_target = _q_table(schema, tbl_history)
        
        pl_geo_col = f"pl.{_q_ident(ladder_col)}"
        if ladder_col == "zip3":
            pl_geo_col = "LEFT(pl.zcta5, 3)"

        sql = f"""
            INSERT INTO {q_target}
            (
                {_q_ident(ladder_col)},
                year,
                value, p50, n,
                run_id, backtest_id, variant_id, model_version, as_of_date,
                series_kind,
                inserted_at, updated_at
            )
            SELECT
                {pl_geo_col} AS {_q_ident(ladder_col)},
                mh.year,

                AVG(mh.value)::float8 AS value,
                AVG(mh.p50)::float8   AS p50,
                COUNT(*)::int         AS n,

                %s::text AS run_id,
                %s::text AS backtest_id,
                %s::text AS variant_id,
                %s::text AS model_version,
                %s::date AS as_of_date,

                %s::text AS series_kind,

                now() AS inserted_at,
                now() AS updated_at
            FROM {q_parcel} mh
            JOIN public.parcel_ladder_v1 pl
              ON pl.acct = mh.acct
            WHERE mh.run_id = %s
              AND mh.series_kind = %s
              AND mh.variant_id = %s
              AND mh.acct = ANY(%s)
              AND {pl_geo_col} IS NOT NULL
            GROUP BY {pl_geo_col}, mh.year
            ON CONFLICT ({_q_ident(ladder_col)}, year, series_kind, variant_id)
            DO NOTHING
        """
        # NOTE: same reasoning as forecast agg — pre-run clear + DO NOTHING is correct.
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    run_id,
                    backtest_id,
                    variant_id,
                    MODEL_VERSION,
                    as_of_date,
                    series_kind,
                    run_id,
                    series_kind,
                    variant_id,
                    list(map(str, acct_chunk)),
                ),
            )
            total_rows += int(cur.rowcount or 0)

    return total_rows


# -----------------------------------------------------------------------------
# PRE-RUN AGGREGATE CLEAR
# -----------------------------------------------------------------------------
def _clear_stale_agg_for_run(
    conn,
    schema: str,
    origin_year: int,
    series_kind: str,
    variant_id: str,
):
    """
    Deletes existing aggregate forecast rows for (origin_year, series_kind, variant_id)
    before a new inference run starts writing chunks.

    Required because chunk inserts now use ON CONFLICT DO NOTHING (not a weighted merge).
    Without this clear, existing rows from a previous run would silently persist and
    mix with new partial-run data when viewed by the frontend.

    Scope is intentionally tight: other origin years, series_kinds, and variant_ids
    are not touched.
    """
    total_deleted = 0
    for _, _, tbl_forecast, _ in AGG_LEVELS:
        q_target = _q_table(schema, tbl_forecast)
        with conn.cursor() as cur:
            cur.execute(
                f"""
                DELETE FROM {q_target}
                WHERE origin_year = %s
                  AND series_kind = %s
                  AND variant_id = %s
                """,
                (int(origin_year), series_kind, variant_id),
            )
            total_deleted += int(cur.rowcount or 0)
    print(f"[{_ts()}] _clear_stale_agg_for_run: deleted {total_deleted} stale aggregate rows "
          f"for origin={origin_year} series={series_kind} variant={variant_id}")
    return total_deleted


# -----------------------------------------------------------------------------
# FINAL EXACT AGGREGATE REFRESH (OPTIONAL, RECOMMENDED)
# -----------------------------------------------------------------------------
def _check_has_outlier_col(conn, schema: str) -> bool:
    """Return True if metrics_parcel_forecast has an is_outlier column."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name   = 'metrics_parcel_forecast'
              AND column_name  = 'is_outlier'
            """,
            (schema,),
        )
        return cur.fetchone() is not None


def _recompute_forecast_aggregates_exact_for_run(
    conn,
    schema: str,
    run_id: str,
    origin_year: int,
    series_kind: str,
    variant_id: str,
    backtest_id: str,
    as_of_date: date,
):
    """
    Deletes and rebuilds all aggregate forecast rows for this (origin_year, series_kind, variant_id)
    from parcel forecast rows for this run_id. This guarantees exact final aggregates.
    """
    has_outlier = _check_has_outlier_col(conn, schema)
    outlier_filter = "AND coalesce(mp.is_outlier, false) = false" if has_outlier else ""
    print(f"[{_ts()}] is_outlier filter: {'ON' if has_outlier else 'OFF (column absent — skipping)'}")
    q_parcel = _q_table(schema, "metrics_parcel_forecast")

    for _, ladder_col, tbl_forecast, _ in AGG_LEVELS:
        q_target = _q_table(schema, tbl_forecast)
        
        pl_geo_col = f"pl.{_q_ident(ladder_col)}"
        if ladder_col == "zip3":
            pl_geo_col = "LEFT(pl.zcta5, 3)"

        with conn.cursor() as cur:
            cur.execute(
                f"""
                DELETE FROM {q_target}
                WHERE origin_year = %s
                  AND series_kind = %s
                  AND variant_id = %s
                """,
                (int(origin_year), series_kind, variant_id),
            )

        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {q_target}
                (
                    {_q_ident(ladder_col)},
                    origin_year, horizon_m, forecast_year,
                    value, p10, p25, p50, p75, p90, n,
                    run_id, backtest_id, variant_id, model_version, as_of_date, n_scenarios,
                    is_backtest, series_kind,
                    inserted_at, updated_at
                )
                SELECT
                    {pl_geo_col} AS {_q_ident(ladder_col)},
                    mp.origin_year,
                    mp.horizon_m,
                    mp.forecast_year,

                    AVG(mp.value)::float8 AS value,
                    AVG(mp.p10)::float8   AS p10,
                    AVG(mp.p25)::float8   AS p25,
                    AVG(mp.p50)::float8   AS p50,
                    AVG(mp.p75)::float8   AS p75,
                    AVG(mp.p90)::float8   AS p90,
                    COUNT(*)::int         AS n,

                    %s::text     AS run_id,
                    %s::text     AS backtest_id,
                    %s::text     AS variant_id,
                    %s::text     AS model_version,
                    %s::date     AS as_of_date,
                    MAX(mp.n_scenarios)::int AS n_scenarios,

                    %s::boolean  AS is_backtest,
                    %s::text     AS series_kind,

                    now() AS inserted_at,
                    now() AS updated_at
                FROM {q_parcel} mp
                JOIN public.parcel_ladder_v1 pl
                  ON pl.acct = mp.acct
                WHERE mp.run_id = %s
                  AND mp.origin_year = %s
                  AND mp.series_kind = %s
                  AND mp.variant_id = %s
                  {outlier_filter}
                  AND {pl_geo_col} IS NOT NULL
                GROUP BY {pl_geo_col}, mp.origin_year, mp.horizon_m, mp.forecast_year
                """,
                (
                    run_id,
                    backtest_id,
                    variant_id,
                    MODEL_VERSION,
                    as_of_date,
                    (series_kind == "backtest"),
                    series_kind,
                    run_id,
                    int(origin_year),
                    series_kind,
                    variant_id,
                ),
            )

def _recompute_history_aggregates_exact_for_run(
    conn,
    schema: str,
    run_id: str,
    series_kind: str,
    variant_id: str,
    backtest_id: str,
    as_of_date: date,
):
    """
    Deletes and rebuilds all aggregate history rows for this (series_kind, variant_id) from parcel history rows for this run_id.
    """
    q_parcel = _q_table(schema, "metrics_parcel_history")

    for _, ladder_col, _, tbl_history in AGG_LEVELS:
        q_target = _q_table(schema, tbl_history)
        
        pl_geo_col = f"pl.{_q_ident(ladder_col)}"
        if ladder_col == "zip3":
            pl_geo_col = "LEFT(pl.zcta5, 3)"

        with conn.cursor() as cur:
            cur.execute(
                f"""
                DELETE FROM {q_target}
                WHERE series_kind = %s
                  AND variant_id = %s
                """,
                (series_kind, variant_id),
            )

        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {q_target}
                (
                    {_q_ident(ladder_col)},
                    year,
                    value, p50, n,
                    run_id, backtest_id, variant_id, model_version, as_of_date,
                    series_kind,
                    inserted_at, updated_at
                )
                SELECT
                    {pl_geo_col} AS {_q_ident(ladder_col)},
                    mh.year,

                    AVG(mh.value)::float8 AS value,
                    AVG(mh.p50)::float8   AS p50,
                    COUNT(*)::int         AS n,

                    %s::text AS run_id,
                    %s::text AS backtest_id,
                    %s::text AS variant_id,
                    %s::text AS model_version,
                    %s::date AS as_of_date,

                    %s::text AS series_kind,

                    now() AS inserted_at,
                    now() AS updated_at
                FROM {q_parcel} mh
                JOIN public.parcel_ladder_v1 pl
                  ON pl.acct = mh.acct
                WHERE mh.series_kind = %s
                  AND mh.variant_id = %s
                  AND {pl_geo_col} IS NOT NULL
                GROUP BY {pl_geo_col}, mh.year
                """,
                (
                    run_id,
                    backtest_id,
                    variant_id,
                    MODEL_VERSION,
                    as_of_date,
                    series_kind,
                    series_kind,
                    variant_id,
                ),
            )


# -----------------------------------------------------------------------------
# OPTIONAL BACKTEST EVALUATION (LOCAL CSV SUMMARY)
# -----------------------------------------------------------------------------
def _backtest_eval_summary_for_chunk(forecast_chunk_df: pd.DataFrame, acct_chunk, origin_year: int):
    if forecast_chunk_df is None or forecast_chunk_df.empty:
        return pd.DataFrame()

    actuals = _materialize_actual_prices_for_accounts(
        lf_obj=lf,
        accts=[str(a) for a in acct_chunk],
        year_min=int(origin_year + 1),
        year_max=int(origin_year + H),
    )
    if actuals is None or actuals.empty:
        return pd.DataFrame()

    actuals = actuals.rename(columns={"actual_price": "actual_value"}).copy()
    actuals["acct"] = actuals["acct"].astype(str)
    actuals["year"] = pd.to_numeric(actuals["year"], errors="coerce").astype("Int64")
    actuals["actual_value"] = pd.to_numeric(actuals["actual_value"], errors="coerce")
    actuals = actuals[actuals["year"].notna() & np.isfinite(actuals["actual_value"])].copy()
    actuals["year"] = actuals["year"].astype(int)

    fx = forecast_chunk_df[["acct", "origin_year", "horizon_m", "forecast_year", "value", "p10", "p50", "p90"]].copy()
    fx["acct"] = fx["acct"].astype(str)

    m = fx.merge(actuals[["acct", "year", "actual_value"]], left_on=["acct", "forecast_year"], right_on=["acct", "year"], how="inner")
    if m.empty:
        return pd.DataFrame()

    m["horizon_k"] = (m["horizon_m"] // 12).astype(int)
    m["ae"] = (m["p50"] - m["actual_value"]).abs()
    m["ape"] = m["ae"] / m["actual_value"].replace(0, np.nan)
    m["covered_80"] = ((m["actual_value"] >= m["p10"]) & (m["actual_value"] <= m["p90"])).astype(int)

    g = (
        m.groupby("horizon_k", dropna=False)
         .agg(
             n=("acct", "count"),
             mae=("ae", "mean"),
             mape=("ape", "mean"),
             coverage_p10_p90=("covered_80", "mean"),
         )
         .reset_index()
    )
    g["origin_year"] = int(origin_year)
    g["timestamp_utc"] = datetime.utcnow().isoformat()
    return g[["timestamp_utc", "origin_year", "horizon_k", "n", "mae", "mape", "coverage_p10_p90"]]

# -----------------------------------------------------------------------------
# BRIDGE FUNCTIONS  (were previously loose Colab cells — now defined here)
# -----------------------------------------------------------------------------

def _get_checkpoint_paths(ckpt_dir: str):
    """
    Scan ckpt_dir for checkpoint files.
    Prefers v11 checkpoints (ckpt_v11_origin_YYYY*.pt) over v10.2.
    Returns list of (origin_year: int, path: str) tuples.
    """
    import re as _re
    pairs = {}  # origin -> path (last wins, v11 overwrites v10.2)
    _pat = _re.compile(r"ckpt_(?:v11_)?origin_(\d{4})")
    for fn in sorted(os.listdir(ckpt_dir)):
        if not fn.endswith(".pt"):
            continue
        m = _pat.search(fn)
        if m:
            origin = int(m.group(1))
            is_v11 = "v11" in fn
            # v11 always wins; otherwise only set if not already present
            if is_v11 or origin not in pairs:
                pairs[origin] = os.path.join(ckpt_dir, fn)
    return sorted(pairs.items())


def _load_ckpt_into_live_objects(ckpt_path: str):
    """
    torch.load a checkpoint, hydrate global model + v11 modules (or v10.2 fallback).
    Returns the raw ckpt dict (caller reads global_medians from it).
    """
    import torch

    ckpt = torch.load(ckpt_path, map_location="cpu")
    _cfg_ckpt = ckpt.get("cfg", {})
    # Detect v11 from checkpoint keys (retrain_sample_sweep.py doesn't save 'arch')
    _arch = ckpt.get("arch", None)
    if _arch is None:
        # v11 checkpoints always contain gating_net_state_dict; v10.2 never does
        _arch = "v11" if "gating_net_state_dict" in ckpt else "v10.2"

    # Architecture dims — pull from saved config or fall back to worldmodel.py globals
    _hist_len = int(_cfg_ckpt.get("FULL_HIST_LEN", globals().get("FULL_HIST_LEN", 21)))
    _H = int(_cfg_ckpt.get("H", globals().get("H", 5)))
    _num_dim = int(_cfg_ckpt.get("NUM_DIM", globals().get("NUM_DIM", 38)))
    _n_cat = int(_cfg_ckpt.get("N_CAT", globals().get("N_CAT", 12)))

    _device = "cuda" if torch.cuda.is_available() else "cpu"

    if _arch in ("v11", "ab1"):
        # ── v11: denoiser + gating + token_persistence + coh_scale ──
        # Detect actual num_dim from checkpoint weights to handle cross-jurisdiction inference
        _ckpt_num_dim = _num_dim
        if "model_state_dict" in ckpt and "num_enc.0.weight" in ckpt["model_state_dict"]:
            _ckpt_num_dim = int(ckpt["model_state_dict"]["num_enc.0.weight"].shape[1])
            if _ckpt_num_dim != _num_dim:
                print(f"[{_ts()}] ⚠️ Cross-jurisdiction: checkpoint num_dim={_ckpt_num_dim} vs panel num_dim={_num_dim}. Using checkpoint's.")
                _num_dim = _ckpt_num_dim
                # Override globals so worldmodel's feature extraction matches
                globals()["NUM_DIM"] = _num_dim
                # Also truncate num_use to checkpoint's feature count
                _num_use = globals().get("num_use", [])
                if len(_num_use) > _num_dim:
                    globals()["num_use"] = _num_use[:_num_dim]
                    print(f"[{_ts()}]   Truncated num_use to {_num_dim} features: {globals()['num_use']}")

        _model = create_denoiser_v11(target_dim=_H, hist_len=_hist_len,
                                      num_dim=_num_dim, n_cat=_n_cat).to(_device)
        _model.load_state_dict(ckpt["model_state_dict"])
        _model.eval()

        _gating_net = create_gating_network(hist_len=_hist_len,
                                             num_dim=_num_dim, n_cat=_n_cat).to(_device)
        _gating_net.load_state_dict(ckpt["gating_net_state_dict"])
        _gating_net.eval()

        _token_persistence = create_token_persistence().to(_device)
        _token_persistence.load_state_dict(ckpt["token_persistence_state_dict"])
        _token_persistence.eval()

        _coh_scale = create_coherence_scale().to(_device)
        if "coh_scale_state_dict" in ckpt:
            _coh_scale.load_state_dict(ckpt["coh_scale_state_dict"])
        _coh_scale.eval()

        # AB1: load mu_backbone if checkpoint contains it
        _mu_backbone = None
        if ckpt.get("mu_backbone_state_dict") is not None and _arch in ("ab1", "v11"):
            try:
                _mu_backbone = create_mu_backbone(
                    hist_len=_hist_len, num_dim=_num_dim, n_cat=_n_cat, H=_H
                ).to(_device)
                _mu_backbone.load_state_dict(ckpt["mu_backbone_state_dict"])
                _mu_backbone.eval()
                _bb_params = sum(p.numel() for p in _mu_backbone.parameters())
                print(f"[{_ts()}] AB1 mu_backbone loaded ({_bb_params:,} params)")
            except Exception as _bb_err:
                print(f"[{_ts()}] ⚠️ AB1 mu_backbone failed to load (non-fatal): {_bb_err}")
                _mu_backbone = None

        # Publish v11 modules into globals
        globals()["model"] = _model
        globals()["gating_net"] = _gating_net
        globals()["token_persistence"] = _token_persistence
        globals()["coh_scale"] = _coh_scale
        globals()["mu_backbone"] = _mu_backbone  # None if not AB1

        _phi_k = _token_persistence.get_phi_list()
        _sigma_u = _coh_scale.get_sigma()
        _ab1_str = f" AB1=True" if _mu_backbone is not None else ""
        print(f"[{_ts()}] Loaded {'ab1' if _mu_backbone else 'v11'} checkpoint: {os.path.basename(ckpt_path)} "
              f"| H={_H} hist_len={_hist_len} num_dim={_num_dim} n_cat={_n_cat} "
              f"| phi_k={[f'{p:.3f}' for p in _phi_k]} sigma_u={_sigma_u:.3f}{_ab1_str} "
              f"| device={_device}")

    else:
        # ── v10.2 fallback ──
        _g_rank = int(_cfg_ckpt.get("GLOBAL_RANK", globals().get("GLOBAL_RANK", 8)))
        _geo_rank = int(_cfg_ckpt.get("GEO_RANK", globals().get("GEO_RANK", 4)))

        _model = create_denoiser_v102(target_dim=_H, hist_len=_hist_len,
                                       num_dim=_num_dim, n_cat=_n_cat).to(_device)
        _model.load_state_dict(ckpt["model_state_dict"])
        _model.eval()

        _proj_g = GlobalProjection(_g_rank).to(_device)
        _proj_g.load_state_dict(ckpt["proj_g_state_dict"])
        _proj_g.eval()

        _proj_geo = GeoProjection(_geo_rank).to(_device)
        _proj_geo.load_state_dict(ckpt["proj_geo_state_dict"])
        _proj_geo.eval()

        globals()["model"] = _model
        globals()["proj_g"] = _proj_g
        globals()["proj_geo"] = _proj_geo

        print(f"[{_ts()}] Loaded v10.2 checkpoint: {os.path.basename(ckpt_path)} "
              f"| H={_H} hist_len={_hist_len} num_dim={_num_dim} n_cat={_n_cat} "
              f"| device={_device}")

    # Scalers (same format for both versions)
    _y_scaler = SimpleScaler(
        mean=np.array(ckpt["y_scaler_mean"], dtype=np.float32),
        scale=np.array(ckpt["y_scaler_scale"], dtype=np.float32),
    )
    _n_scaler = SimpleScaler(
        mean=np.array(ckpt["n_scaler_mean"], dtype=np.float32),
        scale=np.array(ckpt["n_scaler_scale"], dtype=np.float32),
    )
    _t_scaler = SimpleScaler(
        mean=np.array(ckpt["t_scaler_mean"], dtype=np.float32),
        scale=np.array(ckpt["t_scaler_scale"], dtype=np.float32),
    )

    # v11.1: Cross-jurisdiction hist_len alignment
    # Keep checkpoint's FULL_HIST_LEN intact so model dimensions match.
    # The context builder will pad missing history years with NaN→0.
    _panel_hist_len = int(globals().get("FULL_HIST_LEN", _hist_len))
    if _hist_len != _panel_hist_len:
        print(f"[{_ts()}] ⚠️ Cross-jurisdiction hist_len: checkpoint={_hist_len} vs panel={_panel_hist_len}")
        print(f"[{_ts()}]   Using checkpoint's hist_len={_hist_len} — context builder will pad missing years")
        # Override globals to use checkpoint's hist_len (pad, don't truncate)
        globals()["FULL_HIST_LEN"] = _hist_len

    _model._y_scaler = _y_scaler
    _model._n_scaler = _n_scaler
    _model._t_scaler = _t_scaler

    globals()["y_scaler"] = _y_scaler
    globals()["n_scaler"] = _n_scaler
    globals()["t_scaler"] = _t_scaler

    return ckpt


def _build_inference_for_accounts_at_origin(accts_batch, origin: int, global_medians: dict):
    """
    Wrapper around worldmodel.build_inference_context_chunked_v102.
    Returns a context dict with keys: acct, hist_y, cur_num, cur_cat, region_id, y_anchor, anchor_year.
    Returns None if no valid rows.
    """
    _lf_ref = globals().get("lf")
    _num_use = globals().get("num_use")
    _cat_use = globals().get("cat_use")

    if _lf_ref is None or _num_use is None or _cat_use is None:
        raise RuntimeError("Cannot find 'lf', 'num_use', or 'cat_use' in globals(). "
                           "Did you run worldmodel.py first?")

    result = build_inference_context_chunked_v102(
        lf=_lf_ref,
        accts=accts_batch,
        num_use_local=_num_use,
        cat_use_local=_cat_use,
        global_medians=global_medians,
        anchor_year=int(origin),
    )

    # ── Pre-dedup: keep ONE anchor per account (last = most recent) ──
    # The panel often has multiple rows per property (resales, yearly assessments),
    # producing duplicate anchors. Deduping HERE avoids wasting GPU on forecasts
    # that would be dropped by the post-hoc dedup in _upsert_df_pg.
    if result is not None and "acct" in result:
        _accts = result["acct"]
        _n_before = len(_accts)
        # Find last occurrence index for each unique acct (preserves most-recent anchor)
        _seen = {}
        for _i, _a in enumerate(_accts):
            _seen[_a] = _i  # overwrite → keeps last
        _keep_idx = np.array(sorted(_seen.values()))
        if len(_keep_idx) < _n_before:
            print(f"[{_ts()}] ⚡ Pre-dedup anchors: {_n_before} → {len(_keep_idx)} "
                  f"(removed {_n_before - len(_keep_idx)} duplicate accounts before sampling)")
            result = {k: (v[_keep_idx] if isinstance(v, np.ndarray) and v.shape[0] == _n_before else v)
                      for k, v in result.items()}

    return result


def _apply_calibration_to_batch(y_levels: np.ndarray, anchor_prices: np.ndarray, calib_models: dict):
    """
    Applies Isotonic Regression quantile mapping loaded from Stage 1 sweep.
    y_levels: [B, S, H]
    anchor_prices: [B]  (dollar space)
    """
    B, S, H = y_levels.shape
    sorted_p = (np.arange(S) + 0.5) / S

    # Pre-compute remapped quantiles for all existing calibrators to avoid loop lookup
    # remapped_cache[h][bkt_label] = numpy array [S]
    remapped_cache = {}
    for h in range(1, H + 1):
        remapped_cache[h] = {}
        for bkt_label, _, _ in VALUE_BRACKETS:
            key = (h, bkt_label)
            if key in calib_models:
                rem = calib_models[key].predict(sorted_p)
                remapped_cache[h][bkt_label] = np.clip(rem, 0.001, 0.999)

    for i in range(B):
        bv = anchor_prices[i]
        bkt_label = None
        for lbl, lo, hi in VALUE_BRACKETS:
            if lo <= bv < hi:
                bkt_label = lbl
                break
        
        if bkt_label is None:
            continue
            
        for h_idx in range(H):
            h = h_idx + 1
            if bkt_label in remapped_cache[h]:
                remapped_p = remapped_cache[h][bkt_label]
                fan = y_levels[i, :, h_idx]
                sort_idx = np.argsort(fan)
                sorted_fan = fan[sort_idx]
                
                # Interpolate from old percentiles to remapped PITs
                new_sorted_fan = np.interp(remapped_p, sorted_p, sorted_fan)
                
                # Unsort to preserve feature correlation
                reverse_sort_idx = np.empty_like(sort_idx)
                reverse_sort_idx[sort_idx] = np.arange(S)
                y_levels[i, :, h_idx] = new_sorted_fan[reverse_sort_idx]


def _sample_scenarios_for_inference_context(ctx, H: int, S: int, origin: int, prop_batch_size: int, calib_models: dict = None):
    """
    v11: Run DDIM sampler over the inference context in account batches of prop_batch_size.
    Uses inducing-token paths and gating network for coherent joint scenarios.
    Returns dict with keys: acct, y_levels (N,S,H), price_levels (N,S,H).
    """
    import torch

    _model_ref = globals().get("model")
    _gating_net_ref = globals().get("gating_net")
    _y_scaler_ref = globals().get("y_scaler")

    if _model_ref is None or _gating_net_ref is None:
        raise RuntimeError("Model/gating_net not in globals.  Call _load_ckpt_into_live_objects first.")

    _device = "cuda" if torch.cuda.is_available() else "cpu"

    N = len(ctx["acct"])
    _H = int(H)
    _S = int(S)

    # Build scheduler — must use the TRAINING ladder length (128 steps).
    # The DDIM sampler selects a subset of DIFF_STEPS_SAMPLE timesteps
    # from this ladder.  Using DIFF_STEPS_SAMPLE here creates a different
    # αbar sequence than training, breaking the noise-level contract.
    _diff_steps_train = int(globals().get("DIFF_STEPS_TRAIN", 128))
    _sched = Scheduler(_diff_steps_train, _device)

    # v11: Sample K token paths once (shared across all parcels for coherence)
    _K = int(globals().get("K_TOKENS", 8))
    _token_persistence_ref = globals().get("token_persistence")  # learned per-token phi
    _coh_scale_ref = globals().get("coh_scale")  # learned coherence scale
    if _token_persistence_ref is not None:
        Z_tokens = sample_token_paths(_K, _H, _token_persistence_ref, _S, _device)  # [S, K, H]
    else:
        _phi = float(globals().get("PHI_INIT", 0.80))
        Z_tokens = sample_token_paths(_K, _H, _phi, _S, _device)  # [S, K, H]

    # Batch over accounts
    all_y = []
    all_price = []

    for start in range(0, N, prop_batch_size):
        end = min(start + prop_batch_size, N)

        deltas = sample_ddim_v11(
            model=_model_ref,
            gating_net=_gating_net_ref,
            sched=_sched,
            hist_y_b=ctx["hist_y"][start:end],
            cur_num_b=ctx["cur_num"][start:end],
            cur_cat_b=ctx["cur_cat"][start:end],
            region_id_b=ctx["region_id"][start:end],
            Z_tokens=Z_tokens,
            device=_device,
            anchor_year=origin,
            coh_scale=_coh_scale_ref,
            tau=SAMPLER_TAU,
            mu_backbone=globals().get("mu_backbone"),  # AB1: None if not loaded
        )  # (end-start, S, H) numpy

        # Conversion
        _y_anchor_batch = ctx["y_anchor"][start:end]
        y_levels = _y_anchor_batch[:, None, None] + np.cumsum(deltas, axis=2)  # (B, S, H)
        
        # ── Stage 1 Calibration (Rank-Mapping PIT) ──
        if calib_models and len(calib_models) > 0:
            _anchor_prices_batch = np.expm1(_y_anchor_batch)
            _apply_calibration_to_batch(y_levels, _anchor_prices_batch, calib_models)
            
        price_levels = np.expm1(y_levels)

        # ── Anchor validation: flag implausible forecast-vs-anchor ratios ──
        _anchor_prices = np.expm1(_y_anchor_batch)                    # (B,)
        _median_fcst = np.median(price_levels, axis=1)                # (B, H)
        _safe_anchor = np.maximum(_anchor_prices, 1.0)[:, None]       # (B, 1)
        _growth_ratio = _median_fcst / _safe_anchor                   # (B, H)
        _absurd = (_growth_ratio > 10.0) | (_growth_ratio < 0.01)     # >10× or <1%
        _n_absurd = int(_absurd.any(axis=1).sum())
        if _n_absurd > 0:
            print(f"[{_ts()}] ⚠️  ANCHOR VALIDATION: {_n_absurd}/{end-start} parcels have "
                  f"implausible growth (>10× or <1% of anchor value)")

        all_y.append(y_levels)
        all_price.append(price_levels)

    # Pass anchor through so downstream can enforce anchor identity
    _anchor_prices = np.expm1(ctx["y_anchor"])  # (N,) dollar values

    return {
        "acct": ctx["acct"],
        "y_levels": np.concatenate(all_y, axis=0),
        "price_levels": np.concatenate(all_price, axis=0),
        "anchor_prices": _anchor_prices,  # (N,) for anchor-identity enforcement
    }


def _materialize_actual_prices_for_accounts(lf_obj=None, accts=None, year_min=1900, year_max=2100,
                                             origin=None, max_horizon=None):
    """
    Pull actual observed prices for history rows and backtest evaluation.
    Returns DataFrame with columns: acct, year, actual_price.
    """
    import polars as pl

    _lf_ref = lf_obj if lf_obj is not None else globals().get("lf")
    if _lf_ref is None:
        return None
    if accts is None:
        return None

    # Support the old origin+max_horizon calling convention
    if origin is not None and max_horizon is not None:
        year_min = int(origin) + 1
        year_max = int(origin) + int(max_horizon)

    rows = (
        _lf_ref
        .filter(pl.col("acct").cast(pl.Utf8).is_in(accts))
        .filter(pl.col("yr").is_between(int(year_min), int(year_max)))
        .select(["acct", "yr", "tot_appr_val"])
        .collect()
    )
    if rows.is_empty():
        return None
    df = rows.to_pandas()
    df = df.rename(columns={"yr": "year", "tot_appr_val": "actual_price"})
    return df


# -----------------------------------------------------------------------------
# CHECKPOINT SELECTION + SAMPLER BACKOFF
# -----------------------------------------------------------------------------
def _pick_ckpt_for_origin(ckpt_pairs, origin_year: int):
    """
    Prefer exact origin checkpoint; else latest checkpoint with origin <= target; else newest checkpoint.
    """
    ckpt_pairs_sorted = sorted([(int(o), p) for o, p in ckpt_pairs], key=lambda x: x[0])

    for o, p in ckpt_pairs_sorted:
        if o == int(origin_year):
            return o, p

    eligible = [(o, p) for o, p in ckpt_pairs_sorted if o <= int(origin_year)]
    if eligible:
        return eligible[-1]

    return ckpt_pairs_sorted[-1]

def _sample_scenarios_with_backoff(ctx, H, S, origin, calib_models: dict = None):
    """
    Try large prop_batch_size first; back off on CUDA OOM.
    Returns (inf_out, used_batch_size).
    """
    bs = int(PROP_BATCH_SIZE_SAMPLER)

    while True:
        try:
            inf_out = _sample_scenarios_for_inference_context(
                ctx=ctx,
                H=int(H),
                S=int(S),
                origin=int(origin),
                prop_batch_size=int(bs),
                calib_models=calib_models,
            )
            return inf_out, int(bs)

        except RuntimeError as e:
            msg = str(e).lower()
            oom = ("out of memory" in msg) or ("cuda" in msg and "memory" in msg)
            if (not oom) or (bs <= int(PROP_BATCH_SIZE_MIN)):
                raise

            bs = max(int(PROP_BATCH_SIZE_MIN), bs // 2)

            try:
                import torch
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
            except Exception:
                pass

            print(f"[{_ts()}] OOM during sampling. Retrying with prop_batch_size={bs}")


# -----------------------------------------------------------------------------
# CORE RUNNER (ONE ORIGIN, ONE MODE)
# -----------------------------------------------------------------------------
def _run_one_origin(
    schema: str,
    all_accts_prod,
    origin_year: int,
    mode: str,                 # 'forecast' or 'backtest'
    ckpt_origin: int,
    ckpt_path: str,
    out_dir: str,
    variant_id: str,
    backtest_id: str = None,
    write_history_series: bool = True,
    resume_run_id: str = None,
):
    assert mode in ("forecast", "backtest")
    _assert_ident(schema, "schema")

    # ---- RESUME: reuse previous run_id and skip already-done accounts ----
    if resume_run_id:
        run_id = resume_run_id
        print(f"[{_ts()}] RESUME MODE: reusing run_id={run_id}")
        done_accts = _get_completed_accts_from_db(schema, run_id, "metrics_parcel_forecast")
        n_already = len(done_accts)
        all_accts_prod = [a for a in all_accts_prod if a not in done_accts]
        # Shuffle remaining accounts so chunks get a random mix of forecastable/non-forecastable
        # (avoids runs of empty chunks when accounts are sorted by type)
        import random as _rng
        _rng.seed(42)  # fixed seed for reproducibility across restarts
        _rng.shuffle(all_accts_prod)
        print(f"[{_ts()}] RESUME: {n_already} accounts already done, {len(all_accts_prod)} remaining (shuffled)")
        if not all_accts_prod:
            print(f"[{_ts()}] RESUME: nothing left to do for this run, skipping.")
            return {
                "run_id": run_id, "origin_year": int(origin_year), "mode": mode,
                "variant_id": variant_id, "backtest_id": backtest_id,
                "run_root": os.path.join(out_dir, f"{mode}_origin_{origin_year}_{run_id}"),
                "parcel_forecast_rows_total": 0, "agg_forecast_rows_total": 0,
                "parcel_history_rows_total": 0, "agg_history_rows_total": 0,
                "resumed": True, "skipped_already_done": n_already,
            }
    else:
        run_id = _make_run_id(mode, int(origin_year))

    as_of_date = datetime.utcnow().date()

    series_kind_forecast = "forecast" if mode == "forecast" else "backtest"
    series_kind_history = "history" if mode == "forecast" else "backtest"

    if mode == "forecast":
        assert variant_id == "__forecast__"
        hist_variant_id = "__history__"
        hist_backtest_id = None
    else:
        assert variant_id not in ("__forecast__", "__history__")
        hist_variant_id = variant_id if WRITE_BACKTEST_HISTORY_VARIANTS else None
        hist_backtest_id = backtest_id if WRITE_BACKTEST_HISTORY_VARIANTS else None

    run_root = os.path.join(out_dir, f"{mode}_origin_{origin_year}_{run_id}")
    os.makedirs(run_root, exist_ok=True)
    os.makedirs(os.path.join(run_root, "forecast_chunks"), exist_ok=True)
    os.makedirs(os.path.join(run_root, "history_chunks"), exist_ok=True)
    os.makedirs(os.path.join(run_root, "eval_chunks"), exist_ok=True)

    manifest = {
        "run_id": run_id,
        "mode": mode,
        "schema": schema,
        "origin_year": int(origin_year),
        "series_kind_forecast": series_kind_forecast,
        "variant_id": variant_id,
        "backtest_id": backtest_id,
        "write_history_series": bool(write_history_series),
        "write_backtest_history_variants": bool(WRITE_BACKTEST_HISTORY_VARIANTS),
        "checkpoint_origin": int(ckpt_origin),
        "checkpoint_path": ckpt_path,
        "model_version": MODEL_VERSION,
        "H": int(H),
        "S": int(S),
        "prop_batch_size_sampler_initial": int(PROP_BATCH_SIZE_SAMPLER),
        "prop_batch_size_min": int(PROP_BATCH_SIZE_MIN),
        "acct_batch_size_outer": int(ACCT_BATCH_SIZE_OUTER),
        "parcel_forecast_has_n_col": bool(PARCEL_FORECAST_HAS_N_COL),
        "final_exact_agg_refresh": bool(RUN_FINAL_EXACT_AGG_REFRESH),
        "started_at_utc": datetime.utcnow().isoformat(),
    }
    with open(os.path.join(run_root, "run_manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"[{_ts()}] START {mode.upper()} origin={origin_year} run_id={run_id}")
    print(f"[{_ts()}] CKPT origin={ckpt_origin} path={ckpt_path}")

    # Load checkpoint and model state for this run
    ckpt = _load_ckpt_into_live_objects(ckpt_path)
    global_medians = ckpt.get("global_medians", {})

    # ── torch.compile: kernel fusion for 20-40% faster DDIM sampling ──
    try:
        import torch as _torch
        _use_compile = globals().get("USE_TORCH_COMPILE", True)
        if _use_compile and hasattr(_torch, 'compile') and _torch.cuda.is_available():
            # Safety net: suppress inductor/Triton errors so they fall back to eager
            # rather than crashing the entire inference job.
            try:
                import torch._dynamo
                torch._dynamo.config.suppress_errors = True
            except Exception:
                pass
            _compile_mode = globals().get("COMPILE_MODE", "reduce-overhead")
            # The compiled model replaces the sampler's forward pass in-place
            _model_ref = globals().get("model") or globals().get("_model")
            if _model_ref is not None and not getattr(_model_ref, '_compiled', False):
                print(f"[{_ts()}] torch.compile(model, mode='{_compile_mode}') — first batch will be slow (warmup)")
                _compiled = _torch.compile(_model_ref, mode=_compile_mode)
                # Patch the compiled model back into the global so the sampler uses it
                if 'model' in globals():
                    globals()['model'] = _compiled
                elif '_model' in globals():
                    globals()['_model'] = _compiled
                _compiled._compiled = True  # mark to avoid re-compiling on resume
        elif not _use_compile:
            print(f"[{_ts()}] torch.compile DISABLED (USE_TORCH_COMPILE=False)")
    except Exception as _e:
        print(f"[{_ts()}] torch.compile skipped: {_e}")

    with _pg_tx() as conn:
        _run_insert_start(
            conn=conn,
            schema=schema,
            run_id=run_id,
            level_name="parcel",
            mode=mode,
            origin_year=int(origin_year),
            as_of_date=as_of_date,
            notes=f"H={H}, S={S}, ckpt_origin={ckpt_origin}, variant_id={variant_id}",
        )

    # ── Pre-run: clear stale aggregate rows for this (origin_year, series_kind, variant_id) ──
    # This is the safe way to start fresh. The chunk-level inserts now use DO NOTHING
    # (not a weighted merge), so without this clear the old rows would survive unchanged.
    # Scoped tightly: only rows matching all three columns are deleted — other origin
    # years, other series_kinds, and other variant_ids are untouched.
    # Skip if SKIP_AGG_CHUNK_WRITES is set (parallel mode — agg handled post-run).
    if not globals().get("SKIP_AGG_CHUNK_WRITES", False):
        try:
            with _pg_tx(label="pre_run_agg_clear") as conn:
                _clear_stale_agg_for_run(
                    conn=conn,
                    schema=schema,
                    origin_year=int(origin_year),
                    series_kind=series_kind_forecast,
                    variant_id=variant_id,
                )
            print(f"[{_ts()}] Pre-run aggregate clear done for origin={origin_year} series={series_kind_forecast} variant={variant_id}")
        except Exception as _clr_exc:
            print(f"[{_ts()}] ⚠️  Pre-run aggregate clear failed (non-fatal, proceeding): {_clr_exc}")
    else:
        print(f"[{_ts()}] SKIP_AGG_CHUNK_WRITES=True — skipping pre-run agg clear; rebuild_aggregates.py will handle this post-run")

    n_total = len(all_accts_prod)
    n_done = 0
    parcel_rows_total = 0
    agg_rows_total = 0
    history_rows_total = 0
    history_agg_rows_total = 0
    t_run0 = time.time()

    # Load Calibration models if they exist
    # Fallback chain:
    #   1) Per-origin file from sweep: calibrators_v11_{jur}_o{origin}.pkl (best match)
    #   2) Consolidated production file: calibrators_v11_{jur}.pkl
    #   3) Legacy hardcoded: calibrators_v11_sf_ca.pkl
    calib_models = {}
    if APPLY_CALIBRATION:
        import pickle as _calib_pickle
        import glob as _calib_glob

        # Extract jurisdiction name from CKPT_DIR (e.g. /output/hcad_houston_v11 → hcad_houston)
        _jur_name = None
        jur_match = re.search(r"[/\\]([^/\\]+)_v\d+", CKPT_DIR)
        if jur_match:
            _jur_name = jur_match.group(1)

        calib_path = None
        _calib_candidates = []

        if _jur_name:
            # 1) Per-origin: pick the latest (highest origin year) available
            _per_origin_glob = os.path.join(CKPT_DIR, f"calibrators_v11_{_jur_name}_o*.pkl")
            _per_origin_files = sorted(_calib_glob.glob(_per_origin_glob))
            if _per_origin_files:
                _calib_candidates.append(("per-origin (latest)", _per_origin_files[-1]))

            # 2) Consolidated production file
            _prod_path = os.path.join(CKPT_DIR, f"calibrators_v11_{_jur_name}.pkl")
            if os.path.exists(_prod_path):
                _calib_candidates.append(("consolidated", _prod_path))

        # 3) Legacy fallback
        _legacy_path = os.path.join(CKPT_DIR, "calibrators_v11_sf_ca.pkl")
        if os.path.exists(_legacy_path):
            _calib_candidates.append(("legacy", _legacy_path))

        # Use the first (highest-priority) candidate found
        if _calib_candidates:
            _calib_label, calib_path = _calib_candidates[0]
            with open(calib_path, "rb") as f:
                calib_models = _calib_pickle.load(f)
            print(f"[{_ts()}] 🏆 Loaded {_calib_label} calibrators from {calib_path} ({len(calib_models)} blocks)")
        else:
            _expected = os.path.join(CKPT_DIR, f"calibrators_v11_{_jur_name or 'UNKNOWN'}.pkl")
            print(f"[{_ts()}] ⚠️ APPLY_CALIBRATION=True but no calibrator map found at {_expected}")

    progress_csv = os.path.join(run_root, "progress_log.csv")
    eval_csv = os.path.join(run_root, "backtest_eval_summary.csv")

    # Resume-safe chunk numbering: continue from where previous run left off
    chunk_start = 1
    if resume_run_id:
        fc_dir = os.path.join(run_root, "forecast_chunks")
        hist_dir = os.path.join(run_root, "history_chunks")
        existing_fc = len([f for f in os.listdir(fc_dir) if f.endswith(('.parquet', '.csv.gz'))]) if os.path.isdir(fc_dir) else 0
        existing_hist = len([f for f in os.listdir(hist_dir) if f.endswith(('.parquet', '.csv.gz'))]) if os.path.isdir(hist_dir) else 0
        chunk_start = max(existing_fc, existing_hist) + 1
        print(f"[{_ts()}] RESUME: continuing chunk numbering from {chunk_start} (found {existing_fc} fc, {existing_hist} hist chunks)")

        # ── SPOT CHECK: test 5 random future chunks before committing ──
        import random as _rng

        all_chunks = list(_chunk_list(all_accts_prod, int(ACCT_BATCH_SIZE_OUTER)))
        n_chunks = len(all_chunks)
        n_spot = min(5, n_chunks)
        spot_idxs = sorted(_rng.sample(range(n_chunks), n_spot))

        # ── Diagnostic context ──
        print(f"[{_ts()}] SPOT CHECK diag: origin_year={origin_year} (type={type(origin_year).__name__})")
        print(f"[{_ts()}] SPOT CHECK diag: global_medians keys={len(global_medians)}, "
              f"sample_keys={list(global_medians.keys())[:5]}")
        print(f"[{_ts()}] SPOT CHECK diag: all_accts_prod len={len(all_accts_prod)}, "
              f"type={type(all_accts_prod[0]).__name__} sample={all_accts_prod[:3]}")
        print(f"[{_ts()}] SPOT CHECK diag: n_chunks={n_chunks}, "
              f"ACCT_BATCH_SIZE_OUTER={ACCT_BATCH_SIZE_OUTER}")
        print(f"[{_ts()}] SPOT CHECK: testing {n_spot} random chunks "
              f"(chunk indices {spot_idxs}) ...")

        def _test_chunk_diag(ci):
            """Test a single chunk with full diagnostics."""
            raw_chunk = all_chunks[ci]
            chunk = [str(a) for a in raw_chunk]
            n_chunk = len(chunk)
            print(f"[{_ts()}] SPOT chunk {ci}: n_accts={n_chunk}, "
                  f"acct_range=[{chunk[0]}..{chunk[-1]}], "
                  f"sample_accts={chunk[:3]}, type={type(chunk[0]).__name__}")

            t0 = time.time()
            try:
                ctx = _build_inference_for_accounts_at_origin(
                    accts_batch=chunk,
                    origin=int(origin_year),
                    global_medians=global_medians,
                )
                dt = time.time() - t0

                if ctx is None:
                    print(f"[{_ts()}] SPOT chunk {ci}: returned None ({dt:.1f}s)")
                    return ci, 0, n_chunk, dt
                elif not isinstance(ctx, dict):
                    print(f"[{_ts()}] SPOT chunk {ci}: unexpected type={type(ctx).__name__} ({dt:.1f}s)")
                    return ci, 0, n_chunk, dt
                elif "acct" not in ctx:
                    print(f"[{_ts()}] SPOT chunk {ci}: dict has no 'acct' key, keys={list(ctx.keys())} ({dt:.1f}s)")
                    return ci, 0, n_chunk, dt
                else:
                    n_valid = len(ctx["acct"])
                    print(f"[{_ts()}] SPOT chunk {ci}: {n_valid}/{n_chunk} valid anchors "
                          f"({100.0*n_valid/n_chunk:.0f}%) ({dt:.1f}s)")
                    if n_valid > 0:
                        print(f"[{_ts()}] SPOT chunk {ci}: valid acct_sample={ctx['acct'][:3]}")
                    return ci, n_valid, n_chunk, dt
            except Exception as e:
                dt = time.time() - t0
                import traceback
                print(f"[{_ts()}] SPOT chunk {ci}: EXCEPTION ({dt:.1f}s): {e}")
                traceback.print_exc()
                return ci, 0, n_chunk, dt

        spot_results = []
        for ci in spot_idxs:
            spot_results.append(_test_chunk_diag(ci))

        n_chunks_with_data = 0
        total_valid = 0
        total_tested = 0
        for ci, nv, nt, dt in spot_results:
            total_valid += nv
            total_tested += nt
            if nv > 0:
                n_chunks_with_data += 1

        pct = 100.0 * total_valid / max(total_tested, 1)
        print(f"[{_ts()}] SPOT CHECK summary: {n_chunks_with_data}/{n_spot} chunks have data, "
              f"{total_valid}/{total_tested} accounts valid ({pct:.0f}%)")

        if n_chunks_with_data == 0:
            print(f"[{_ts()}] ⚠️  ALL {n_spot} spot-check chunks produced ZERO forecasts!")
            print(f"[{_ts()}] ⚠️  Resume may not produce new forecasts. Continuing anyway...")
        else:
            est_new = int(pct / 100.0 * n_total)
            print(f"[{_ts()}] ✅ Spot check passed: expect ~{est_new:,} new forecasts")
        # ── end spot check ──

    # ── Thread pool for async DB writes ──
    _db_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="db_write")
    _pending_db_future: Future = None  # type: ignore

    def _wait_for_pending_db():
        """Block until prior async DB write completes; propagate exceptions."""
        nonlocal _pending_db_future
        if _pending_db_future is not None:
            try:
                _pending_db_future.result(timeout=600)  # 10 min max wait
            except Exception as exc:
                print(f"[{_ts()}] ⚠️  Async DB write failed: {exc}")
            _pending_db_future = None

    # ── Thread pool for context prefetch (overlaps CPU ctx build with GPU sampling) ──
    _ctx_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ctx_prefetch")
    _all_chunks = list(_chunk_list(all_accts_prod, int(ACCT_BATCH_SIZE_OUTER)))
    _prefetched_ctx: Future = None  # type: ignore

    def _build_ctx_for_chunk(acct_chunk_raw):
        """Build inference context — safe to run on a background thread (read-only Polars + local numpy)."""
        ac = [str(a) for a in acct_chunk_raw]
        return ac, _build_inference_for_accounts_at_origin(
            accts_batch=ac,
            origin=int(origin_year),
            global_medians=global_medians,
        )

    for chunk_idx, acct_chunk_raw in enumerate(_all_chunks, start=chunk_start):
        # Wait for prior chunk's DB write to finish before reusing counters
        _wait_for_pending_db()

        t0 = time.time()
        used_prop_batch_size = None

        # ---------------------------------------------------------------------
        # 1) History (build locally)
        # ---------------------------------------------------------------------
        hist_chunk_df = pd.DataFrame()
        hist_rows_upserted = 0
        hist_agg_rows_upserted = 0
        # Need acct_chunk early for history; derive from raw or prefetched
        _acct_chunk_for_hist = [str(a) for a in acct_chunk_raw]

        do_hist_this_run = (mode == "forecast" and write_history_series) or (mode == "backtest" and WRITE_BACKTEST_HISTORY_VARIANTS)

        if do_hist_this_run:
            hist_chunk_df = _build_history_rows_for_chunk(
                acct_chunk=_acct_chunk_for_hist,
                run_id=run_id,
                max_year=int(origin_year),
                series_kind=series_kind_history,
                variant_id=(hist_variant_id if hist_variant_id is not None else "__history__"),
                backtest_id=hist_backtest_id,
                min_year=1900,
                as_of_date=as_of_date,
            )

            hist_fp = _write_df_chunk(hist_chunk_df, os.path.join(run_root, "history_chunks"), "metrics_parcel_history", chunk_idx)
            print(f"[{_ts()}] Chunk {chunk_idx}: wrote history chunk rows={len(hist_chunk_df)} -> {hist_fp}")

            if not hist_chunk_df.empty:
                hist_chunk_df["jurisdiction"] = JURISDICTION
                try:
                    with _pg_tx(label=f"hist_chunk_{chunk_idx}") as conn:
                        hist_rows_upserted = _upsert_df_pg(
                            conn=conn,
                            schema=schema,
                            table="metrics_parcel_history",
                            df=hist_chunk_df,
                            conflict_cols=["acct", "year", "series_kind", "variant_id"],
                            update_cols=[
                                "value", "p50", "n",
                                "run_id", "backtest_id", "model_version", "as_of_date",
                                "updated_at"
                            ],
                        )

                        hist_agg_rows_upserted = _aggregate_history_levels_for_chunk(
                            conn=conn,
                            schema=schema,
                            acct_chunk=_acct_chunk_for_hist,
                            run_id=run_id,
                            series_kind=series_kind_history,
                            variant_id=(hist_variant_id if hist_variant_id is not None else "__history__"),
                            backtest_id=hist_backtest_id,
                            as_of_date=as_of_date,
                        )
                except Exception as exc:
                    print(f"[{_ts()}] Chunk {chunk_idx}: ⚠️  HISTORY DB write failed after retries, SKIPPING: {exc}")
                    hist_rows_upserted = 0
                    hist_agg_rows_upserted = 0

                history_rows_total += int(hist_rows_upserted)
                history_agg_rows_total += int(hist_agg_rows_upserted)

        # ---------------------------------------------------------------------
        # 2) Retrieve prefetched context or build synchronously (first chunk)
        # ---------------------------------------------------------------------
        if _prefetched_ctx is not None:
            try:
                acct_chunk, ctx = _prefetched_ctx.result(timeout=600)
            except Exception as _pf_exc:
                print(f"[{_ts()}] Chunk {chunk_idx}: prefetch failed ({_pf_exc}), building synchronously")
                acct_chunk = [str(a) for a in acct_chunk_raw]
                ctx = _build_inference_for_accounts_at_origin(
                    accts_batch=acct_chunk, origin=int(origin_year), global_medians=global_medians,
                )
            _prefetched_ctx = None
        else:
            acct_chunk = [str(a) for a in acct_chunk_raw]
            print(f"[{_ts()}] Chunk {chunk_idx}: building inference context for {len(acct_chunk)} accts "
                  f"[{acct_chunk[0]}..{acct_chunk[-1]}] origin={origin_year}")
            ctx = _build_inference_for_accounts_at_origin(
                accts_batch=acct_chunk, origin=int(origin_year), global_medians=global_medians,
            )

        # Detailed diagnostic on the return value
        if ctx is None:
            _ctx_reason = "ctx=None"
        elif not isinstance(ctx, dict):
            _ctx_reason = f"ctx type={type(ctx).__name__} (expected dict)"
        elif "acct" not in ctx:
            _ctx_reason = f"ctx missing 'acct' key, keys={list(ctx.keys())}"
        elif len(ctx["acct"]) == 0:
            _ctx_reason = f"ctx['acct'] is empty (len=0), keys={list(ctx.keys())}"
        else:
            _ctx_reason = None

        if _ctx_reason:
            print(f"[{_ts()}] Chunk {chunk_idx}: ⚠️  no valid inference rows — {_ctx_reason}")
            n_done += len(acct_chunk)

            with _pg_tx() as conn:
                _run_progress_upsert(
                    conn=conn,
                    schema=schema,
                    run_id=run_id,
                    chunk_seq=chunk_idx,
                    level_name="parcel",
                    status="running",
                    series_kind=series_kind_forecast,
                    variant_id=variant_id,
                    origin_year=int(origin_year),
                    chunk_rows=int(len(hist_chunk_df)) if do_hist_this_run else 0,
                    chunk_keys=int(len(acct_chunk)),
                    rows_upserted_total=int(parcel_rows_total + agg_rows_total + history_rows_total + history_agg_rows_total),
                    keys_upserted_total=int(n_done),
                    min_key=min(acct_chunk) if acct_chunk else None,
                    max_key=max(acct_chunk) if acct_chunk else None,
                )

            _append_csv_row(progress_csv, {
                "timestamp": _ts(),
                "chunk_idx": int(chunk_idx),
                "mode": mode,
                "origin_year": int(origin_year),
                "accts_in_chunk": int(len(acct_chunk)),
                "accts_valid_ctx": 0,
                "prop_batch_size_used": None,
                "parcel_history_rows": int(hist_rows_upserted),
                "parcel_forecast_rows": 0,
                "agg_history_rows": int(hist_agg_rows_upserted),
                "agg_forecast_rows": 0,
                "n_done": int(n_done),
                "n_total": int(n_total),
                "pct_done": float(100.0 * n_done / max(n_total, 1)),
                "elapsed_chunk_sec": float(time.time() - t0),
                "elapsed_run_sec": float(time.time() - t_run0),
            })

            print(f"[{_ts()}] Chunk {chunk_idx}: no valid inference rows | elapsed={time.time()-t0:.1f}s")
            continue

        # Inference context is valid — print diagnostic
        n_ctx = len(ctx["acct"])
        print(f"[{_ts()}] Chunk {chunk_idx}: ✅ {n_ctx}/{len(acct_chunk)} valid anchors, "
              f"sample_valid={ctx['acct'][:3]}")
        # ---------------------------------------------------------------------
        # 3) Sample scenarios (adaptive backoff)
        # ---------------------------------------------------------------------
        inf_out, used_prop_batch_size = _sample_scenarios_with_backoff(
            ctx=ctx,
            H=int(H),
            S=int(S),
            origin=int(origin_year),
            calib_models=calib_models,
        )

        # ── Prefetch: start building context for the NEXT chunk on a background thread ──
        _next_chunk_idx = chunk_idx - chunk_start + 1  # 0-based index into _all_chunks
        if _next_chunk_idx < len(_all_chunks):
            _prefetched_ctx = _ctx_pool.submit(_build_ctx_for_chunk, _all_chunks[_next_chunk_idx])

        if inf_out is None:
            n_done += len(acct_chunk)

            with _pg_tx() as conn:
                _run_progress_upsert(
                    conn=conn,
                    schema=schema,
                    run_id=run_id,
                    chunk_seq=chunk_idx,
                    level_name="parcel",
                    status="running",
                    series_kind=series_kind_forecast,
                    variant_id=variant_id,
                    origin_year=int(origin_year),
                    chunk_rows=int(len(hist_chunk_df)) if do_hist_this_run else 0,
                    chunk_keys=int(len(acct_chunk)),
                    rows_upserted_total=int(parcel_rows_total + agg_rows_total + history_rows_total + history_agg_rows_total),
                    keys_upserted_total=int(n_done),
                    min_key=min(acct_chunk) if acct_chunk else None,
                    max_key=max(acct_chunk) if acct_chunk else None,
                )

            _append_csv_row(progress_csv, {
                "timestamp": _ts(),
                "chunk_idx": int(chunk_idx),
                "mode": mode,
                "origin_year": int(origin_year),
                "accts_in_chunk": int(len(acct_chunk)),
                "accts_valid_ctx": int(len(ctx["acct"])) if ctx is not None else 0,
                "prop_batch_size_used": int(used_prop_batch_size) if used_prop_batch_size is not None else None,
                "parcel_history_rows": int(hist_rows_upserted),
                "parcel_forecast_rows": 0,
                "agg_history_rows": int(hist_agg_rows_upserted),
                "agg_forecast_rows": 0,
                "n_done": int(n_done),
                "n_total": int(n_total),
                "pct_done": float(100.0 * n_done / max(n_total, 1)),
                "elapsed_chunk_sec": float(time.time() - t0),
                "elapsed_run_sec": float(time.time() - t_run0),
            })

            print(f"[{_ts()}] Chunk {chunk_idx}: sampler returned no output | elapsed={time.time()-t0:.1f}s")
            continue

        # ---------------------------------------------------------------------
        # 4) FAIL-FAST GATES: validate inference output before DB write
        # ---------------------------------------------------------------------
        _gate_passed = True

        # Gate 1: finite fraction
        _pl = inf_out["price_levels"]
        _fin_frac = float(np.isfinite(_pl).mean())
        if _fin_frac < 0.95:
            print(f"[{_ts()}] GATE FAIL (finite): only {_fin_frac:.1%} finite values in price_levels")
            _gate_passed = False

        # Gate 2: anchor-to-h1-forecast ratio sanity
        if "anchor_prices" in inf_out:
            _anch = inf_out["anchor_prices"]
            _h1_med = np.median(_pl[:, :, 0], axis=1)  # median across scenarios for h=1
            _safe_anch = np.maximum(_anch, 1.0)
            _ratio = _h1_med / _safe_anch
            _ratio_p99 = float(np.percentile(_ratio[np.isfinite(_ratio)], 99))
            _ratio_p01 = float(np.percentile(_ratio[np.isfinite(_ratio)], 1))
            _n_extreme = int(np.sum((_ratio > 10.0) | (_ratio < 0.01)))
            if _n_extreme > 0.10 * len(_ratio):
                print(f"[{_ts()}] GATE FAIL (anchor ratio): {_n_extreme}/{len(_ratio)} parcels "
                      f"have h1/anchor ratio outside [0.01, 10]. "
                      f"p01={_ratio_p01:.4f} p99={_ratio_p99:.4f}")
                _gate_passed = False
            else:
                print(f"[{_ts()}] GATE OK (anchor ratio): p01={_ratio_p01:.4f} p99={_ratio_p99:.4f} "
                      f"extreme={_n_extreme}/{len(_ratio)}")

        # Gate 3: delta magnitude (in log-space)
        _yl = inf_out["y_levels"]
        _delta_abs_p99 = float(np.percentile(np.abs(_yl - _yl[:, :, :1]), 99))
        if _delta_abs_p99 > 2.0:  # > 2.0 in log1p-space ≈ >600% growth, very suspicious
            print(f"[{_ts()}] GATE WARN (delta magnitude): p99(|delta|) = {_delta_abs_p99:.3f} "
                  f"(> 2.0 threshold in log-space)")
            # Warning only — do not hard fail on this, but log it prominently

        if not _gate_passed:
            print(f"[{_ts()}] GATE: SKIPPING DB write for chunk {chunk_idx} (origin={origin_year})")
            continue

        # ---------------------------------------------------------------------
        # 5) Build parcel forecast rows (local)
        # ---------------------------------------------------------------------
        forecast_chunk_df = _build_forecast_rows_from_inf_out(
            inf_out=inf_out,
            origin_year=int(origin_year),
            run_id=run_id,
            series_kind=series_kind_forecast,
            variant_id=variant_id,
            backtest_id=backtest_id,
            as_of_date=as_of_date,
        )

        fc_fp = _write_df_chunk(forecast_chunk_df, os.path.join(run_root, "forecast_chunks"), "metrics_parcel_forecast", chunk_idx)
        print(f"[{_ts()}] Chunk {chunk_idx}: wrote forecast chunk rows={len(forecast_chunk_df)} -> {fc_fp}")

        # ---------------------------------------------------------------------
        # 5) Upsert parcel forecast + aggregate higher zooms + progress
        #    → dispatched to background thread so GPU can start next chunk
        # ---------------------------------------------------------------------
        parcel_rows_upserted = 0
        agg_rows_upserted = 0

        # Capture loop-local values for the closure
        _fc_df = forecast_chunk_df
        _ac = list(acct_chunk)
        _ci = chunk_idx
        _hr = int(len(hist_chunk_df)) if do_hist_this_run else 0

        def _do_db_write(_fc_df=_fc_df, _ac=_ac, _ci=_ci, _hr=_hr):
            nonlocal parcel_rows_total, agg_rows_total, n_done
            _pru = 0
            _aru = 0
            try:
                with _pg_tx(label=f"forecast_chunk_{_ci}") as conn:
                    if not _fc_df.empty:
                        _fc_df["jurisdiction"] = JURISDICTION
                        parcel_forecast_update_cols = [
                            "forecast_year", "value", "p10", "p25", "p50", "p75", "p90",
                            "run_id", "backtest_id", "model_version", "as_of_date", "n_scenarios",
                            "is_backtest", "updated_at"
                        ]
                        if PARCEL_FORECAST_HAS_N_COL:
                            parcel_forecast_update_cols.insert(7, "n")

                        _pru = _upsert_df_pg(
                            conn=conn,
                            schema=schema,
                            table="metrics_parcel_forecast",
                            df=_fc_df,
                            conflict_cols=["acct", "origin_year", "horizon_m", "series_kind", "variant_id"],
                            update_cols=parcel_forecast_update_cols,
                        )

                        _aru = _aggregate_forecast_levels_for_chunk(
                            conn=conn,
                            schema=schema,
                            acct_chunk=_ac,
                            run_id=run_id,
                            origin_year=int(origin_year),
                            series_kind=series_kind_forecast,
                            variant_id=variant_id,
                            backtest_id=backtest_id,
                            as_of_date=as_of_date,
                        )

                    parcel_rows_total += int(_pru)
                    agg_rows_total += int(_aru)
                    n_done += len(_ac)

                    _run_progress_upsert(
                        conn=conn,
                        schema=schema,
                        run_id=run_id,
                        chunk_seq=_ci,
                        level_name="parcel",
                        status="running",
                        series_kind=series_kind_forecast,
                        variant_id=variant_id,
                        origin_year=int(origin_year),
                        chunk_rows=int(_hr + len(_fc_df)),
                        chunk_keys=int(len(_ac)),
                        rows_upserted_total=int(parcel_rows_total + agg_rows_total + history_rows_total + history_agg_rows_total),
                        keys_upserted_total=int(n_done),
                        min_key=min(_ac) if _ac else None,
                        max_key=max(_ac) if _ac else None,
                    )
            except Exception as exc:
                print(f"[{_ts()}] Chunk {_ci}: ⚠️  FORECAST DB write failed after retries, SKIPPING: {exc}")
                n_done += len(_ac)  # still count as processed

        _pending_db_future = _db_pool.submit(_do_db_write)

        # ---------------------------------------------------------------------
        # 6) Optional local backtest scoring summary (no DB)
        # ---------------------------------------------------------------------
        if mode == "backtest":
            eval_df = _backtest_eval_summary_for_chunk(
                forecast_chunk_df=forecast_chunk_df,
                acct_chunk=acct_chunk,
                origin_year=int(origin_year),
            )
            if eval_df is not None and not eval_df.empty:
                eval_fp = _write_df_chunk(eval_df, os.path.join(run_root, "eval_chunks"), "backtest_eval_summary", chunk_idx)
                print(f"[{_ts()}] Chunk {chunk_idx}: wrote backtest eval summary rows={len(eval_df)} -> {eval_fp}")
                for _, r in eval_df.iterrows():
                    _append_csv_row(eval_csv, {k: _py_scalar(v) for k, v in r.to_dict().items()})

        _append_csv_row(progress_csv, {
            "timestamp": _ts(),
            "chunk_idx": int(chunk_idx),
            "mode": mode,
            "origin_year": int(origin_year),
            "accts_in_chunk": int(len(acct_chunk)),
            "accts_valid_ctx": int(len(ctx["acct"])) if ctx is not None else 0,
            "prop_batch_size_used": int(used_prop_batch_size) if used_prop_batch_size is not None else None,
            "parcel_history_rows": int(hist_rows_upserted),
            "parcel_forecast_rows": int(parcel_rows_upserted),
            "agg_history_rows": int(hist_agg_rows_upserted),
            "agg_forecast_rows": int(agg_rows_upserted),
            "n_done": int(n_done),
            "n_total": int(n_total),
            "pct_done": float(100.0 * n_done / max(n_total, 1)),
            "elapsed_chunk_sec": float(time.time() - t0),
            "elapsed_run_sec": float(time.time() - t_run0),
        })

        print(
            f"[{_ts()}] Chunk {chunk_idx} complete | mode={mode} origin={origin_year} "
            f"| done={n_done}/{n_total} ({100.0*n_done/max(n_total,1):.2f}%) "
            f"| prop_bs={used_prop_batch_size} "
            f"| parcel_fc={parcel_rows_upserted} agg_fc={agg_rows_upserted} "
            f"| parcel_hist={hist_rows_upserted} agg_hist={hist_agg_rows_upserted} "
            f"| elapsed={time.time()-t0:.1f}s"
        )

    # -------------------------------------------------------------------------
    # 7) Final exact aggregate refresh + finalize run status
    # -------------------------------------------------------------------------
    # Drain any pending async DB write before final refresh
    _wait_for_pending_db()
    _db_pool.shutdown(wait=True)
    _ctx_pool.shutdown(wait=False)  # no pending work; just cleanup

    try:
        with _pg_tx(retries=5, label="final_agg_refresh") as conn:
            if RUN_FINAL_EXACT_AGG_REFRESH:
                _recompute_forecast_aggregates_exact_for_run(
                    conn=conn,
                    schema=schema,
                    run_id=run_id,
                    origin_year=int(origin_year),
                    series_kind=series_kind_forecast,
                    variant_id=variant_id,
                    backtest_id=backtest_id,
                    as_of_date=as_of_date,
                )

                if (mode == "forecast" and write_history_series) or (mode == "backtest" and WRITE_BACKTEST_HISTORY_VARIANTS):
                    _recompute_history_aggregates_exact_for_run(
                        conn=conn,
                        schema=schema,
                        run_id=run_id,
                        series_kind=series_kind_history,
                        variant_id=(hist_variant_id if hist_variant_id is not None else "__history__"),
                        backtest_id=hist_backtest_id,
                        as_of_date=as_of_date,
                    )

            _run_update_status(conn, schema, run_id, "completed")
    except Exception as exc:
        print(f"[{_ts()}] ⚠️  Final aggregate refresh FAILED after retries: {exc}")
        print(f"[{_ts()}]    The parcel-level data is safe. Re-run the final refresh separately.")
        # Still try to mark the run as completed
        try:
            with _pg_tx(retries=2, label="mark_completed_fallback") as conn:
                _run_update_status(conn, schema, run_id, "completed")
        except Exception:
            print(f"[{_ts()}]    Could not mark run as completed either.")

    manifest["finished_at_utc"] = datetime.utcnow().isoformat()
    manifest["elapsed_run_sec"] = float(time.time() - t_run0)
    manifest["parcel_forecast_rows_total"] = int(parcel_rows_total)
    manifest["agg_forecast_rows_total"] = int(agg_rows_total)
    manifest["parcel_history_rows_total"] = int(history_rows_total)
    manifest["agg_history_rows_total"] = int(history_agg_rows_total)

    with open(os.path.join(run_root, "run_manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"[{_ts()}] DONE {mode.upper()} origin={origin_year} run_id={run_id}")
    return {
        "run_id": run_id,
        "origin_year": int(origin_year),
        "mode": mode,
        "variant_id": variant_id,
        "backtest_id": backtest_id,
        "run_root": run_root,
        "parcel_forecast_rows_total": int(parcel_rows_total),
        "agg_forecast_rows_total": int(agg_rows_total),
        "parcel_history_rows_total": int(history_rows_total),
        "agg_history_rows_total": int(history_agg_rows_total),
    }


# -----------------------------------------------------------------------------
# MAIN ORCHESTRATION
# -----------------------------------------------------------------------------
# Preconditions expected from your notebook:
#   - CKPT_DIR exists
#   - all_accts exists
#   - lf exists
#   - helpers exist:
#       _get_checkpoint_paths
#       _load_ckpt_into_live_objects
#       _build_inference_for_accounts_at_origin
#       _sample_scenarios_for_inference_context
#       _materialize_actual_prices_for_accounts

_assert_ident(TARGET_SCHEMA, "TARGET_SCHEMA")

ckpt_pairs = _get_checkpoint_paths(CKPT_DIR)
if not ckpt_pairs:
    raise RuntimeError("No checkpoints found in CKPT_DIR")

ckpt_pairs = sorted([(int(o), p) for o, p in ckpt_pairs], key=lambda x: x[0])

# Filter to the chosen variant checkpoint suffix
if CKPT_VARIANT_SUFFIX:
    _suffix_tag = f"_{CKPT_VARIANT_SUFFIX}.pt"
    _filtered = [(o, p) for o, p in ckpt_pairs if os.path.basename(p).endswith(_suffix_tag)]
    if not _filtered:
        raise RuntimeError(
            f"No checkpoints found matching suffix '{_suffix_tag}' in CKPT_DIR={CKPT_DIR!r}.\n"
            f"Available checkpoints: {[os.path.basename(p) for _, p in ckpt_pairs]}"
        )
    ckpt_pairs = _filtered
    print(f"[{_ts()}] CKPT_VARIANT_SUFFIX='{CKPT_VARIANT_SUFFIX}' — using {len(ckpt_pairs)} checkpoint(s): {[os.path.basename(p) for _, p in ckpt_pairs]}")
else:
    # Baseline: exclude any variant-tagged checkpoints (files with >1 underscore after 'origin_YYYY')
    _filtered = [(o, p) for o, p in ckpt_pairs
                 if not any(p.endswith(f"_{v}.pt") for v in ("SF200K", "SF500K", "SFstrat200K"))]
    ckpt_pairs = _filtered or ckpt_pairs  # fallback to all if no baseline found
    print(f"[{_ts()}] CKPT_VARIANT_SUFFIX='' (baseline) — using {len(ckpt_pairs)} checkpoint(s)")

# ── SF-only filter: apply same auto-detect logic as retrain_sample_sweep.py ──
# The SF* checkpoints were trained on state_class == "A1" (HCAD single-family) only.
# Applying them to commercial / condo / vacant parcels produces garbage forecasts.
# We re-filter all_accts here so the inference targets match the training distribution.
_lf_global = globals().get("lf")
_all_accts_input = list(all_accts)

_sf_col, _sf_vals = None, None
if _lf_global is not None:
    try:
        _panel_cols = set(_lf_global.collect_schema().names())
        for _cand_col, _cand_vals in [
            ("state_class",   ["A1"]),
            ("property_type", ["SF", "SFR", "SINGLE"]),
            ("prop_type_cd",  ["A1"]),
            ("impr_tp_cd",    ["1001", "1002", "1003"]),
            ("dwelling_type", ["SFR", "SF", "Single Family", "SINGLE", "Maison", "House", "Detached", "1"]),
        ]:
            if _cand_col in _panel_cols:
                _sf_col, _sf_vals = _cand_col, _cand_vals
                break
    except Exception as _e:
        print(f"[{_ts()}] ⚠️  Could not inspect lf schema for SF filter: {_e}")

if _sf_col and CKPT_VARIANT_SUFFIX:
    import polars as _pl
    _sf_accts_set = set(
        _lf_global
        .filter(_pl.col(_sf_col).cast(_pl.Utf8).str.strip_chars().is_in(_sf_vals))
        .select(_pl.col("acct").cast(_pl.Utf8).str.strip_chars())
        .unique()
        .collect()["acct"]
        .to_list()
    )
    _filtered_accts = [a for a in _all_accts_input if str(a).strip() in _sf_accts_set]
    print(
        f"[{_ts()}] SF filter '{_sf_col}' in {_sf_vals}: "
        f"{len(_filtered_accts):,} / {len(_all_accts_input):,} accounts retained"
    )
    all_accts = _filtered_accts
elif CKPT_VARIANT_SUFFIX:
    print(
        f"[{_ts()}] ⚠️  SF filter: no recognised property-type column found in lf. "
        f"Proceeding with all {len(_all_accts_input):,} accounts — "
        f"non-SF forecasts may be unreliable with a SF-trained checkpoint."
    )
else:
    print(f"[{_ts()}] CKPT_VARIANT_SUFFIX='' (baseline): skipping SF filter, using all accounts.")

# Account order (replace with geography-sorted order if you want more contiguous live map fill)
all_accts_prod = [str(a) for a in all_accts]
n_total = len(all_accts_prod)

suite_manifest = {
    "suite_id": SUITE_ID,
    "schema": TARGET_SCHEMA,
    "model_version": MODEL_VERSION,
    "forecast_origin_year": int(FORECAST_ORIGIN_YEAR),
    "run_full_backtest": bool(RUN_FULL_BACKTEST),
    "H": int(H),
    "S": int(S),
    "prop_batch_size_sampler_initial": int(PROP_BATCH_SIZE_SAMPLER),
    "prop_batch_size_min": int(PROP_BATCH_SIZE_MIN),
    "acct_batch_size_outer": int(ACCT_BATCH_SIZE_OUTER),
    "n_accounts_total": int(n_total),
    "parcel_forecast_has_n_col": bool(PARCEL_FORECAST_HAS_N_COL),
    "final_exact_agg_refresh": bool(RUN_FINAL_EXACT_AGG_REFRESH),
    "started_at_utc": datetime.utcnow().isoformat(),
    "checkpoint_origins_available": [int(o) for o, _ in ckpt_pairs],
}
with open(os.path.join(OUT_ROOT, "suite_manifest.json"), "w") as f:
    json.dump(suite_manifest, f, indent=2)

print(f"[{_ts()}] SUITE_ID={SUITE_ID}")
print(f"[{_ts()}] TARGET_SCHEMA={TARGET_SCHEMA}")
print(f"[{_ts()}] Accounts={n_total}")
print(f"[{_ts()}] Checkpoint origins={suite_manifest['checkpoint_origins_available']}")

results = []

try:
    # -------------------------------------------------------------------------
    # A) PRODUCTION RUN
    # -------------------------------------------------------------------------
    prod_ckpt_origin, prod_ckpt_path = _pick_ckpt_for_origin(ckpt_pairs, FORECAST_ORIGIN_YEAR)

    res_prod = _run_one_origin(
        schema=TARGET_SCHEMA,
        all_accts_prod=all_accts_prod,
        origin_year=int(FORECAST_ORIGIN_YEAR),
        mode="forecast",
        ckpt_origin=int(prod_ckpt_origin),
        ckpt_path=prod_ckpt_path,
        out_dir=OUT_PROD_DIR,
        variant_id="__forecast__",
        backtest_id=None,
        write_history_series=True,
        resume_run_id=RESUME_PROD_RUN_ID if RESUME_MODE else None,
    )
    results.append(res_prod)

    # -------------------------------------------------------------------------
    # B) FULL BACKTEST SUITE
    # -------------------------------------------------------------------------
    if RUN_FULL_BACKTEST:
        if BACKTEST_ORIGINS is None:
            backtest_origins = sorted(
                (int(o) for o, _ in ckpt_pairs if int(o) < int(FORECAST_ORIGIN_YEAR)),
                reverse=True,   # most recent backtest first → live map fills with recent history first
            )
        else:
            backtest_origins = sorted(set(int(x) for x in BACKTEST_ORIGINS), reverse=True)

        bt_suite_short = uuid.uuid4().hex
        print(f"[{_ts()}] Backtest origins -> {backtest_origins}")

        for bt_origin in backtest_origins:
            bt_ckpt_origin, bt_ckpt_path = _pick_ckpt_for_origin(ckpt_pairs, bt_origin)

            # Check if we are resuming a specific backtest origin
            bt_resume_run_id = None
            if RESUME_MODE and RESUME_BACKTEST_RUNS and bt_origin in RESUME_BACKTEST_RUNS:
                bt_resume_run_id, bt_variant_id, bt_backtest_id = RESUME_BACKTEST_RUNS[bt_origin]
            else:
                bt_variant_id = f"bt_{bt_origin}_{bt_suite_short}"
                bt_backtest_id = bt_variant_id

            res_bt = _run_one_origin(
                schema=TARGET_SCHEMA,
                all_accts_prod=all_accts_prod,
                origin_year=int(bt_origin),
                mode="backtest",
                ckpt_origin=int(bt_ckpt_origin),
                ckpt_path=bt_ckpt_path,
                out_dir=OUT_BT_DIR,
                variant_id=bt_variant_id,
                backtest_id=bt_backtest_id,
                write_history_series=False,
                resume_run_id=bt_resume_run_id,
            )
            results.append(res_bt)

    suite_manifest["finished_at_utc"] = datetime.utcnow().isoformat()
    suite_manifest["results"] = results
    with open(os.path.join(OUT_ROOT, "suite_manifest.json"), "w") as f:
        json.dump(suite_manifest, f, indent=2)

    print("")
    print(f"[{_ts()}] ALL RUNS COMPLETE")
    print(f"  Suite root: {OUT_ROOT}")
    print(f"  Results count: {len(results)}")
    for r in results:
        print(f"   - {r['mode']} origin={r['origin_year']} run_id={r['run_id']}")

except Exception as e:
    print(f"[{_ts()}] ERROR: {e}")
    raise