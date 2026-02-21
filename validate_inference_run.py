#!/usr/bin/env python3
"""
=============================================================================
POST-INFERENCE VALIDATION SCRIPT
=============================================================================
Run this AFTER cutting off the Colab inference to confirm:
  1. Which history chunks were written and how many parcels each contains
  2. Which forecast chunks were written (vs skipped as "no valid inference rows")
  3. Whether the parcels in skipped chunks truly lack anchor-year data
  4. Final Supabase row counts and coverage stats

Usage (Colab or local — just needs access to Google Drive + DB):
  python validate_inference_run.py

Or paste into a Colab cell after mounting Drive.

Configuration: edit the constants below to match your run.
=============================================================================
"""

import os
import sys
import json
import glob
from datetime import datetime
from collections import defaultdict

import numpy as np
import pandas as pd

try:
    import psycopg2
    _HAS_PG = True
except ImportError:
    _HAS_PG = False

# =============================================================================
# CONFIG — edit these to match your run
# =============================================================================
SUITE_ID = "suite_20260221T050431Z_5b263b4bbbc344fa998999209fc58549"
RUN_ID   = "forecast_2025_20260221T050431Z_8cdcf33c7b7d4ee6a948ecc6bccca160"
TARGET_SCHEMA = "forecast_20260220_7f31c6e4"
ORIGIN_YEAR = 2025

# Google Drive paths (adjust if your mount point differs)
DRIVE_ROOT   = "/content/drive/MyDrive/data_backups/world_model_v10_2_fullpanel"
SUITE_ROOT   = os.path.join(DRIVE_ROOT, "live_inference_runs", SUITE_ID)
PROD_RUN_DIR = os.path.join(
    SUITE_ROOT, "production",
    f"forecast_origin_{ORIGIN_YEAR}_{RUN_ID}",
)

# Optional: path to the master panel (to check anchor-year coverage)
# Set to None to skip that check.
MASTER_PANEL_PATH = os.path.join(DRIVE_ROOT, "master_panel.parquet")

# Supabase DB URL (set via env var or paste here)
SUPABASE_DB_URL = os.environ.get("SUPABASE_DB_URL", "")

# =============================================================================
# HELPERS
# =============================================================================
def _ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _read_parquet_safe(path):
    """Read a parquet file, return None on failure."""
    try:
        return pd.read_parquet(path)
    except Exception as e:
        print(f"  [WARN] Could not read {path}: {e}")
        return None


def _glob_sorted(pattern):
    return sorted(glob.glob(pattern))


# =============================================================================
# 1) CHUNK FILE INVENTORY
# =============================================================================
def inventory_chunks(run_dir):
    """
    Scan history_chunks/ and forecast_chunks/ directories.
    Returns two dicts:  { chunk_idx: filepath }
    """
    history_dir  = os.path.join(run_dir, "history_chunks")
    forecast_dir = os.path.join(run_dir, "forecast_chunks")

    def _parse(paths):
        out = {}
        for p in paths:
            fname = os.path.basename(p)
            # e.g. metrics_parcel_history_chunk_00001.parquet
            parts = fname.replace(".parquet", "").replace(".csv.gz", "").split("_chunk_")
            if len(parts) == 2:
                try:
                    idx = int(parts[1])
                    out[idx] = p
                except ValueError:
                    pass
        return out

    hist_files = _parse(
        _glob_sorted(os.path.join(history_dir, "*.parquet")) +
        _glob_sorted(os.path.join(history_dir, "*.csv.gz"))
    )
    fc_files = _parse(
        _glob_sorted(os.path.join(forecast_dir, "*.parquet")) +
        _glob_sorted(os.path.join(forecast_dir, "*.csv.gz"))
    )
    return hist_files, fc_files


# =============================================================================
# 2) PER-CHUNK ANALYSIS
# =============================================================================
def analyze_chunks(hist_files, fc_files):
    """
    For each chunk index found in history, check whether a corresponding
    forecast chunk exists and summarise row counts and acct overlap.
    """
    all_idxs = sorted(set(hist_files.keys()) | set(fc_files.keys()))
    rows = []

    for idx in all_idxs:
        row = {"chunk": idx}

        # History
        if idx in hist_files:
            hdf = _read_parquet_safe(hist_files[idx])
            if hdf is not None:
                row["hist_rows"] = len(hdf)
                row["hist_accts"] = hdf["acct"].nunique() if "acct" in hdf.columns else None
                if "year" in hdf.columns:
                    row["hist_year_min"] = int(hdf["year"].min())
                    row["hist_year_max"] = int(hdf["year"].max())
                    # Does any parcel have data AT the anchor year?
                    anchor_mask = hdf["year"] == ORIGIN_YEAR
                    row["hist_accts_at_anchor"] = int(hdf.loc[anchor_mask, "acct"].nunique()) if anchor_mask.any() else 0
                else:
                    row["hist_year_min"] = row["hist_year_max"] = None
                    row["hist_accts_at_anchor"] = 0
            else:
                row["hist_rows"] = None
                row["hist_accts"] = None
                row["hist_accts_at_anchor"] = None
        else:
            row["hist_rows"] = 0
            row["hist_accts"] = 0
            row["hist_accts_at_anchor"] = 0

        # Forecast
        if idx in fc_files:
            fdf = _read_parquet_safe(fc_files[idx])
            if fdf is not None:
                row["fc_rows"] = len(fdf)
                row["fc_accts"] = fdf["acct"].nunique() if "acct" in fdf.columns else None
            else:
                row["fc_rows"] = None
                row["fc_accts"] = None
        else:
            row["fc_rows"] = 0
            row["fc_accts"] = 0

        # Diagnosis
        if row.get("fc_rows", 0) and row["fc_rows"] > 0:
            row["status"] = "OK_FORECAST"
        elif row.get("hist_accts_at_anchor", 0) == 0:
            row["status"] = "SKIP_NO_ANCHOR_DATA"
        elif row.get("hist_rows", 0) == 0:
            row["status"] = "SKIP_NO_HISTORY"
        else:
            row["status"] = "SKIP_HAD_ANCHOR_BUT_NO_FORECAST"  # <-- suspicious

        rows.append(row)

    return pd.DataFrame(rows)


# =============================================================================
# 3) PROGRESS LOG ANALYSIS
# =============================================================================
def read_progress_log(run_dir):
    """Read the progress_log.csv written by the inference script."""
    csv_path = os.path.join(run_dir, "progress_log.csv")
    if os.path.exists(csv_path):
        return pd.read_csv(csv_path)
    return None


# =============================================================================
# 4) SUPABASE COVERAGE CHECK
# =============================================================================
def query_supabase_coverage(schema, run_id, origin_year):
    """
    Query Supabase for final row counts to confirm data actually landed.
    Returns a dict of stats.
    """
    if not _HAS_PG or not SUPABASE_DB_URL:
        return {"error": "psycopg2 not available or SUPABASE_DB_URL not set"}

    stats = {}
    try:
        conn = psycopg2.connect(SUPABASE_DB_URL)
        conn.autocommit = True
        cur = conn.cursor()

        def _safe(name):
            assert all(c.isalnum() or c == '_' for c in name), f"Unsafe: {name}"
            return f'"{name}"'

        qt = lambda t: f'{_safe(schema)}.{_safe(t)}'

        # Parcel forecast
        cur.execute(f"""
            SELECT COUNT(*) AS rows,
                   COUNT(DISTINCT acct) AS accts,
                   MIN(horizon_m) AS min_h, MAX(horizon_m) AS max_h,
                   MIN(forecast_year) AS min_fy, MAX(forecast_year) AS max_fy
            FROM {qt("metrics_parcel_forecast")}
            WHERE run_id = %s AND origin_year = %s
        """, (run_id, origin_year))
        r = cur.fetchone()
        stats["parcel_forecast"] = {
            "rows": r[0], "distinct_accts": r[1],
            "min_horizon_m": r[2], "max_horizon_m": r[3],
            "min_forecast_year": r[4], "max_forecast_year": r[5],
        }

        # Parcel history
        cur.execute(f"""
            SELECT COUNT(*) AS rows,
                   COUNT(DISTINCT acct) AS accts,
                   MIN(year) AS min_year, MAX(year) AS max_year
            FROM {qt("metrics_parcel_history")}
            WHERE run_id = %s
        """, (run_id,))
        r = cur.fetchone()
        stats["parcel_history"] = {
            "rows": r[0], "distinct_accts": r[1],
            "min_year": r[2], "max_year": r[3],
        }

        # Aggregate tables
        for level in ["tabblock", "tract", "zcta", "unsd", "neighborhood"]:
            for kind in ["forecast", "history"]:
                tbl = f"metrics_{level}_{kind}"
                try:
                    if kind == "forecast":
                        cur.execute(f"""
                            SELECT COUNT(*) FROM {qt(tbl)}
                            WHERE origin_year = %s AND series_kind = 'forecast'
                        """, (origin_year,))
                    else:
                        cur.execute(f"""
                            SELECT COUNT(*) FROM {qt(tbl)}
                            WHERE series_kind = 'history'
                        """)
                    stats[tbl] = cur.fetchone()[0]
                except Exception as e:
                    stats[tbl] = f"error: {e}"
                    conn.rollback()

        # Inference run status
        cur.execute(f"""
            SELECT run_id, status, started_at, completed_at,
                   origin_year, model_version, n_scenarios
            FROM {qt("inference_runs")}
            WHERE run_id = %s
        """, (run_id,))
        r = cur.fetchone()
        if r:
            stats["inference_run"] = {
                "run_id": r[0], "status": r[1],
                "started_at": str(r[2]), "completed_at": str(r[3]),
                "origin_year": r[4], "model_version": r[5], "n_scenarios": r[6],
            }

        cur.close()
        conn.close()
    except Exception as e:
        stats["connection_error"] = str(e)

    return stats


# =============================================================================
# 5) MASTER PANEL ANCHOR CHECK (optional)
# =============================================================================
def check_master_panel_anchor_coverage(panel_path, sample_accts=None, origin_year=2025):
    """
    Check whether the master panel has data at the anchor year.
    If sample_accts is provided, check only those; otherwise report overall.
    """
    if not panel_path or not os.path.exists(panel_path):
        return {"skipped": True, "reason": f"Panel not found at {panel_path}"}

    try:
        import pyarrow.parquet as pq
        pf = pq.ParquetFile(panel_path)
        meta = {"num_row_groups": pf.metadata.num_row_groups, "num_rows": pf.metadata.num_rows}

        # Read just acct + year columns to keep memory low
        cols_to_read = ["acct", "year"]
        available_cols = [c for c in cols_to_read if c in pf.schema.names]
        if len(available_cols) < 2:
            # Try alternate column names
            alt_map = {"acct": ["account", "prop_id", "acct_id"], "year": ["yr", "appr_year"]}
            for want, alts in alt_map.items():
                if want not in pf.schema.names:
                    for alt in alts:
                        if alt in pf.schema.names:
                            available_cols.append(alt)
                            break

        if len(available_cols) < 2:
            return {"error": f"Cannot find acct+year columns. Available: {pf.schema.names[:20]}"}

        df = pf.read(columns=available_cols).to_pandas()
        acct_col = [c for c in df.columns if c.lower() in ("acct", "account", "prop_id", "acct_id")][0]
        year_col = [c for c in df.columns if c.lower() in ("year", "yr", "appr_year")][0]

        df[year_col] = pd.to_numeric(df[year_col], errors="coerce")
        total_accts = df[acct_col].nunique()
        anchor_accts = df.loc[df[year_col] == origin_year, acct_col].nunique()

        result = {
            **meta,
            "total_accts": int(total_accts),
            f"accts_with_year_{origin_year}": int(anchor_accts),
            "anchor_coverage_pct": round(100.0 * anchor_accts / max(total_accts, 1), 2),
        }

        if sample_accts is not None:
            sample_set = set(str(a) for a in sample_accts)
            df[acct_col] = df[acct_col].astype(str)
            sample_df = df[df[acct_col].isin(sample_set)]
            result["sample_total"] = len(sample_set)
            result["sample_found_in_panel"] = int(sample_df[acct_col].nunique())
            result[f"sample_with_year_{origin_year}"] = int(
                sample_df.loc[sample_df[year_col] == origin_year, acct_col].nunique()
            )

        return result

    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# MAIN REPORT
# =============================================================================
def main():
    print("=" * 80)
    print(f"  POST-INFERENCE VALIDATION REPORT")
    print(f"  Generated: {_ts()}")
    print(f"  Suite:     {SUITE_ID}")
    print(f"  Run:       {RUN_ID}")
    print(f"  Schema:    {TARGET_SCHEMA}")
    print(f"  Origin:    {ORIGIN_YEAR}")
    print("=" * 80)

    # ------------------------------------------------------------------
    # 1. Manifest
    # ------------------------------------------------------------------
    manifest_path = os.path.join(PROD_RUN_DIR, "run_manifest.json")
    if os.path.exists(manifest_path):
        with open(manifest_path) as f:
            manifest = json.load(f)
        print(f"\n[MANIFEST]")
        for k in ["run_id", "mode", "origin_year", "checkpoint_origin", "model_version",
                   "H", "S", "started_at_utc", "finished_at_utc", "elapsed_run_sec",
                   "parcel_forecast_rows_total", "parcel_history_rows_total"]:
            if k in manifest:
                print(f"  {k}: {manifest[k]}")
    else:
        print(f"\n[MANIFEST] Not found at {manifest_path}")

    # ------------------------------------------------------------------
    # 2. Chunk inventory
    # ------------------------------------------------------------------
    print(f"\n{'='*80}")
    print("[CHUNK INVENTORY]")
    hist_files, fc_files = inventory_chunks(PROD_RUN_DIR)
    print(f"  History chunk files found:  {len(hist_files)}")
    print(f"  Forecast chunk files found: {len(fc_files)}")

    if not hist_files and not fc_files:
        print("  [ERROR] No chunk files found. Check PROD_RUN_DIR path.")
        print(f"  Searched: {PROD_RUN_DIR}")
        return

    # ------------------------------------------------------------------
    # 3. Per-chunk analysis
    # ------------------------------------------------------------------
    print(f"\n{'='*80}")
    print("[PER-CHUNK ANALYSIS]")
    chunk_df = analyze_chunks(hist_files, fc_files)

    # Summary counts
    status_counts = chunk_df["status"].value_counts()
    print(f"\n  Status breakdown:")
    for status, count in status_counts.items():
        print(f"    {status}: {count}")

    # Totals
    total_hist_accts = chunk_df["hist_accts"].sum()
    total_fc_accts = chunk_df["fc_accts"].sum()
    total_anchor_accts = chunk_df["hist_accts_at_anchor"].sum()
    print(f"\n  Total history accts across all chunks:            {total_hist_accts}")
    print(f"  Total accts with anchor year ({ORIGIN_YEAR}) data:  {total_anchor_accts}")
    print(f"  Total forecast accts across all chunks:           {total_fc_accts}")
    print(f"  Forecast coverage of anchor-eligible:             "
          f"{100.0 * total_fc_accts / max(total_anchor_accts, 1):.1f}%")

    # Flag suspicious chunks
    suspicious = chunk_df[chunk_df["status"] == "SKIP_HAD_ANCHOR_BUT_NO_FORECAST"]
    if not suspicious.empty:
        print(f"\n  ⚠️  SUSPICIOUS: {len(suspicious)} chunk(s) had anchor-year data but produced NO forecast:")
        print(suspicious.to_string(index=False))
    else:
        print(f"\n  ✅ No suspicious chunks (all skips are due to missing anchor-year data)")

    # Print full table
    print(f"\n  Full chunk table:")
    pd.set_option("display.max_columns", 20)
    pd.set_option("display.width", 160)
    print(chunk_df.to_string(index=False))

    # Collect sample accts from skipped chunks for panel check
    skipped_accts = []
    for idx in chunk_df.loc[chunk_df["fc_rows"] == 0, "chunk"].values:
        if idx in hist_files:
            hdf = _read_parquet_safe(hist_files[idx])
            if hdf is not None and "acct" in hdf.columns:
                skipped_accts.extend(hdf["acct"].unique()[:10].tolist())
    skipped_accts = list(set(str(a) for a in skipped_accts))[:50]

    # ------------------------------------------------------------------
    # 4. Progress log
    # ------------------------------------------------------------------
    print(f"\n{'='*80}")
    print("[PROGRESS LOG]")
    prog = read_progress_log(PROD_RUN_DIR)
    if prog is not None:
        print(f"  Entries: {len(prog)}")
        zero_fc = prog[prog.get("parcel_forecast_rows", prog.get("parcel_fc_rows", pd.Series(dtype=int))) == 0] if "parcel_forecast_rows" in prog.columns else pd.DataFrame()
        if not zero_fc.empty:
            print(f"  Chunks with 0 forecast rows: {len(zero_fc)}")
        if "pct_done" in prog.columns:
            print(f"  Final pct_done: {prog['pct_done'].iloc[-1]:.2f}%")
        if "n_done" in prog.columns and "n_total" in prog.columns:
            print(f"  Final n_done/n_total: {prog['n_done'].iloc[-1]}/{prog['n_total'].iloc[-1]}")
    else:
        print("  progress_log.csv not found")

    # ------------------------------------------------------------------
    # 5. Supabase coverage
    # ------------------------------------------------------------------
    print(f"\n{'='*80}")
    print("[SUPABASE DATABASE COVERAGE]")
    if _HAS_PG and SUPABASE_DB_URL:
        db_stats = query_supabase_coverage(TARGET_SCHEMA, RUN_ID, ORIGIN_YEAR)
        for k, v in db_stats.items():
            if isinstance(v, dict):
                print(f"\n  {k}:")
                for kk, vv in v.items():
                    print(f"    {kk}: {vv}")
            else:
                print(f"  {k}: {v}")
    else:
        print("  Skipped (psycopg2 not available or SUPABASE_DB_URL not set)")

    # ------------------------------------------------------------------
    # 6. Master panel anchor check (optional)
    # ------------------------------------------------------------------
    print(f"\n{'='*80}")
    print("[MASTER PANEL ANCHOR COVERAGE]")
    if MASTER_PANEL_PATH:
        panel_result = check_master_panel_anchor_coverage(
            MASTER_PANEL_PATH,
            sample_accts=skipped_accts if skipped_accts else None,
            origin_year=ORIGIN_YEAR,
        )
        for k, v in panel_result.items():
            print(f"  {k}: {v}")
    else:
        print("  Skipped (MASTER_PANEL_PATH not set)")

    # ------------------------------------------------------------------
    # 7. Summary verdict
    # ------------------------------------------------------------------
    print(f"\n{'='*80}")
    print("[VERDICT]")
    n_ok = int((chunk_df["status"] == "OK_FORECAST").sum())
    n_skip_no_anchor = int((chunk_df["status"] == "SKIP_NO_ANCHOR_DATA").sum())
    n_skip_no_hist = int((chunk_df["status"] == "SKIP_NO_HISTORY").sum())
    n_suspicious = int((chunk_df["status"] == "SKIP_HAD_ANCHOR_BUT_NO_FORECAST").sum())
    n_total_chunks = len(chunk_df)

    print(f"  Total chunks:                        {n_total_chunks}")
    print(f"  ✅ OK (forecast produced):            {n_ok}")
    print(f"  ⏭️  Skipped (no anchor-year data):     {n_skip_no_anchor}")
    print(f"  ⏭️  Skipped (no history at all):       {n_skip_no_hist}")
    print(f"  ⚠️  Suspicious (had anchor, no fc):    {n_suspicious}")

    if n_suspicious > 0:
        print(f"\n  🔴 ACTION NEEDED: {n_suspicious} chunks had parcels with {ORIGIN_YEAR} data")
        print(f"     but the model produced no forecast. This could indicate a bug in")
        print(f"     _build_inference_for_accounts_at_origin or data format issues.")
    elif n_ok == 0:
        print(f"\n  🔴 NO FORECASTS PRODUCED AT ALL. Check the inference pipeline.")
    else:
        coverage = 100.0 * total_fc_accts / max(total_hist_accts, 1)
        print(f"\n  🟢 Run looks healthy. {total_fc_accts:,} parcels forecasted")
        print(f"     out of {total_hist_accts:,} total ({coverage:.1f}% coverage).")
        if n_skip_no_anchor > 0:
            print(f"     {n_skip_no_anchor} chunks skipped because parcels lacked {ORIGIN_YEAR} data")
            print(f"     (likely non-residential or not yet appraised — expected).")

    print(f"\n{'='*80}")
    print(f"  Report complete at {_ts()}")
    print("=" * 80)

    # Optionally save chunk analysis to CSV
    out_csv = os.path.join(PROD_RUN_DIR, "validation_chunk_analysis.csv")
    try:
        chunk_df.to_csv(out_csv, index=False)
        print(f"\n  Chunk analysis saved to: {out_csv}")
    except Exception:
        pass


if __name__ == "__main__":
    main()
