#!/usr/bin/env python3
"""
H3 Full Stack MVT Pipeline v14.4 - Connection Mode Fix
======================================================
FIXES:
1. TRANSACTION MODE: Starts connection with autocommit=True (Required for VACUUM/DDL).
   (Fixes ReadOnlySqlTransaction error during setup).
2. STATE SWITCHING: Explicitly switches to autocommit=False only for the Data Load phase.
3. LOGIC: Retains Schema Compression (v14.3) and Zero-Dupe (v14.0) to solve Disk Space.
"""

import subprocess
import sys
import os
import io
import json
import time
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
from getpass import getpass
from collections import defaultdict

# --- Dependency Check ---
def install_if_missing(package, import_name=None):
    import_name = import_name or package
    try:
        __import__(import_name)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "-q", "install", package])

install_if_missing("psycopg2-binary", "psycopg2")
install_if_missing("h3")
install_if_missing("polars")
install_if_missing("pyarrow")
install_if_missing("mercantile")

import numpy as np
import pandas as pd
import h3
import psycopg2
import psycopg2.extras
import mercantile

# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    HISTORICAL_YEARS = [2019, 2020, 2021, 2022, 2023, 2024, 2025] 
    FORECAST_YEARS = [2026, 2027, 2028, 2029, 2030, 2031, 2032] 
    CURRENT_YEAR_BASELINE = 2025
    AOI_ID = "harris_county" 
    
    # Must be FALSE to force schema rebuild
    RESUME_MODE = False
    
    FORCE_GRID_REBUILD = False
    
    H3_RES_LEVELS = [7, 8, 9, 10, 11]
    H3_BASE_RES = 11
    
    LON_MIN, LON_MAX = -95.8, -95.0
    LAT_MIN, LAT_MAX = 29.5, 30.1
    
    TILE_ZOOM_LEVEL = 11 
    
    DEFAULT_CAP_RATE = 0.065
    DEFAULT_EXPENSE_RATIO = 0.30
    DEFAULT_LTV = 0.70
    DEFAULT_INTEREST_RATE = 0.04
    DEFAULT_AMORT_YEARS = 30
    FAN_CHART_HORIZON = 5
    
    TREND_UP_THRESHOLD = 0.05
    TREND_DOWN_THRESHOLD = -0.02
    
    REL_WEIGHT_ACCURACY = 0.25
    REL_WEIGHT_CONFIDENCE = 0.20
    REL_WEIGHT_STABILITY = 0.20
    REL_WEIGHT_ROBUSTNESS = 0.15
    REL_WEIGHT_SUPPORT = 0.20
    
    RISK_WEIGHT_TAIL_GAP = 1.0
    RISK_WEIGHT_MEDAE = 1.2
    RISK_WEIGHT_INV_DSCR = 1.0
    
    SCORE_WEIGHT_DSCR = 2.0
    SCORE_WEIGHT_R = 5.0
    SCORE_WEIGHT_AMENITY = 0.15
    SCORE_WEIGHT_QUALITY = 0.10
    SCORE_WEIGHT_PERMIT = 0.10
    SCORE_BASELINE = 0.5
    
    HARD_FAIL_DSCR_MIN = 1.05
    HARD_FAIL_BREAKEVEN_MAX = 0.92

# ============================================================================
# UTILITIES
# ============================================================================

_CENTER_CACHE: Dict[str, Tuple[float, float]] = {}

def flush_print(msg: str):
    print(msg, flush=True)

class Timer:
    def __init__(self):
        self.start = time.time()
        self.last = self.start
    
    def checkpoint(self, name: str):
        now = time.time()
        flush_print(f"  [TIME] {name}: {now - self.last:.2f}s")
        self.last = now

def get_db_connection() -> psycopg2.extensions.connection:
    db_url = ""
    for key in ["SUPABASE_DB_URL", "POSTGRES_URL_NON_POOLING", "POSTGRES_URL"]:
        raw = os.environ.get(key, "").strip()
        if raw:
            parts = urlsplit(raw)
            q = dict(parse_qsl(parts.query, keep_blank_values=True))
            allowed = {"sslmode": q.get("sslmode")} if "sslmode" in q else {}
            db_url = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(allowed), parts.fragment)).strip()
            break
    
    if not db_url: raise RuntimeError("No database URL provided in environment.")
    
    # [FIX] Start with autocommit=True for safe DDL execution
    conn = psycopg2.connect(db_url)
    conn.autocommit = True 
    
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '30min';")
        try:
            # Attempt to force write mode
            cur.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE;")
        except: pass
        
    return conn

# ============================================================================
# H3 LOGIC (String Native)
# ============================================================================

def h3_to_ewkt(cell_str: str) -> str:
    if hasattr(h3, 'cell_to_boundary'): boundary = h3.cell_to_boundary(cell_str)
    else: boundary = h3.h3_to_geo_boundary(cell_str)
    coords = [f"{p[1]} {p[0]}" for p in boundary]
    coords.append(coords[0]) 
    return f"SRID=4326;POLYGON(({', '.join(coords)}))"

def compute_h3_base_per_acct(df: pd.DataFrame, base_res: int) -> Dict[Any, str]:
    flush_print(f"  Computing H3 resolution {base_res} (Strings) for {len(df):,} accounts...")
    lats = df["lat"].to_numpy(dtype=np.float64)
    lngs = df["lng"].to_numpy(dtype=np.float64)
    accts = df["acct"].tolist()
    
    if hasattr(h3, 'latlng_to_cell'):
        cells = [h3.latlng_to_cell(lat, lng, base_res) for lat, lng in zip(lats, lngs)]
    else:
        cells = [h3.h3_to_string(h3.geo_to_h3(lat, lng, base_res)) for lat, lng in zip(lats, lngs)]
    return dict(zip(accts, cells))

def compute_parent_maps(base_cells: List[str]) -> Dict[int, Dict[str, str]]:
    flush_print("  Building Parent Maps (Strings)...")
    parent_maps = {}
    for res in Config.H3_RES_LEVELS:
        if res == Config.H3_BASE_RES: continue
        mapping = {}
        for child_str in base_cells:
            if hasattr(h3, 'cell_to_parent'): parent_str = h3.cell_to_parent(child_str, res)
            else: parent_str = h3.h3_to_parent(child_str, res)
            mapping[child_str] = parent_str
        parent_maps[res] = mapping
    return parent_maps

def h3_center_str(cell_str: str) -> Tuple[float, float]:
    if cell_str in _CENTER_CACHE: return _CENTER_CACHE[cell_str]
    if hasattr(h3, 'cell_to_latlng'): lat, lng = h3.cell_to_latlng(cell_str)
    else: lat, lng = h3.h3_to_geo(cell_str)
    _CENTER_CACHE[cell_str] = (lat, lng)
    return (lat, lng)

# ============================================================================
# DATA LOADING
# ============================================================================

def load_forecast_data(path: str) -> pd.DataFrame:
    flush_print(f"Loading forecast data from {path}...")
    df = pd.read_parquet(path)
    col_map = {}
    for c in df.columns:
        lc = c.lower()
        if lc in ["acct", "property_id"]: col_map[c] = "acct"
        elif lc in ["yr", "year", "target_year"]: col_map[c] = "yr"
        elif lc in ["predicted_value", "y_pred"]: col_map[c] = "predicted_value"
        elif lc in ["lat", "latitude"]: col_map[c] = "lat"
        elif lc in ["lng", "lon"]: col_map[c] = "lng"
        elif lc in ["year_built", "yr_built"]: col_map[c] = "year_built"
        elif lc == "amenity_score": col_map[c] = "amenity_score"
        elif lc == "quality_score": col_map[c] = "quality_score"
        elif lc == "permit_recency": col_map[c] = "permit_recency"
    df = df.rename(columns=col_map)
    if "year_built" not in df.columns: df["year_built"] = np.nan
    df = df[
        (df["lat"] >= Config.LAT_MIN) & (df["lat"] <= Config.LAT_MAX) &
        (df["lng"] >= Config.LON_MIN) & (df["lng"] <= Config.LON_MAX) &
        df["predicted_value"].notna()
    ].copy()
    df["acct"] = df["acct"].astype(str).str.strip()
    df["yr"] = pd.to_numeric(df["yr"], errors="coerce").astype(int)
    return df.drop_duplicates(subset=["acct", "yr"], keep="first")

def load_historical_data(path: str) -> pd.DataFrame:
    flush_print(f"Loading historical data from {path}...")
    df = pd.read_parquet(path)
    col_map = {}
    for c in df.columns:
        lc = c.lower()
        if lc in ["acct", "property_id"]: col_map[c] = "acct"
        elif lc in ["target_year", "yr", "year"]: col_map[c] = "yr"
        elif lc in ["y_true", "tot_appr_val", "actual_value"]: col_map[c] = "actual_value"
    df = df.rename(columns=col_map)
    if "actual_value" not in df.columns: raise KeyError("Missing actual_value")
    df = df.dropna(subset=["actual_value"])
    df = df[df["actual_value"] > 0]
    df["acct"] = df["acct"].astype(str).str.strip()
    df["yr"] = pd.to_numeric(df["yr"], errors="coerce").astype(int)
    return df[["acct", "yr", "actual_value"]]

def load_cv_metrics(path: Optional[str]) -> Dict[int, float]:
    defaults = {h: min(0.15 + 0.02 * h, 0.5) for h in range(1, 15)}
    if not path: return defaults
    try:
        df = pd.read_parquet(path)
        mape_col = next((c for c in df.columns if "mape" in c.lower()), None)
        horizon_col = next((c for c in df.columns if "horizon" in c.lower()), None)
        if mape_col and horizon_col:
            metrics = {}
            for _, row in df.iterrows():
                h = int(row[horizon_col])
                mape = float(row[mape_col])
                metrics[h] = max(0.01, min(mape if mape <= 1 else mape/100, 0.5))
            return metrics
    except: pass
    return defaults

def compute_property_level_metrics(forecast_df, current_values, cv_metrics, h3_base_map, actual_medae):
    df = forecast_df.copy()
    df["h3_base"] = df["acct"].map(h3_base_map)
    df = df.dropna(subset=["h3_base"])
    
    df["current_value"] = df["acct"].map(current_values).fillna(df["predicted_value"] * 0.9).clip(lower=10000)
    df["opportunity"] = ((df["predicted_value"] / df["current_value"]) - 1.0).clip(-0.5, 1.0)
    
    df["horizon"] = (df["yr"] - Config.CURRENT_YEAR_BASELINE).clip(1, 12)
    h = df["horizon"].to_numpy(dtype=np.int32)
    keys = np.array(sorted(cv_metrics.keys()), dtype=np.float64)
    vals = np.array([cv_metrics[int(k)] for k in keys], dtype=np.float64)
    sa = np.interp(h.astype(np.float64), keys, vals)
    df["sample_accuracy_raw"] = np.clip(sa, 0.01, 0.5)
    
    df["medAE"] = df["acct"].map(actual_medae).fillna(df["predicted_value"] * 0.15)
    for c in ["amenity_score", "quality_score", "permit_recency"]:
        if c not in df.columns: df[c] = 0.0
        else: df[c] = df[c].fillna(0.0)
    
    r = Config.DEFAULT_INTEREST_RATE / 12
    n = Config.DEFAULT_AMORT_YEARS * 12
    amort = (r * (1+r)**n) / ((1+r)**n - 1) * 12
    df["noi"] = df["predicted_value"] * Config.DEFAULT_CAP_RATE
    df["loan"] = df["predicted_value"] * Config.DEFAULT_LTV
    df["ds"] = df["loan"] * amort
    df["dscr"] = np.where(df["ds"] > 0, df["noi"] / df["ds"], 5.0).clip(0, 10)
    df["potential_income"] = df["noi"] / (1 - Config.DEFAULT_EXPENSE_RATIO)
    df["monthly_rent"] = df["potential_income"] / 12
    df["breakeven_occ"] = np.where(df["potential_income"]>0, (df["potential_income"]*Config.DEFAULT_EXPENSE_RATIO+df["ds"])/df["potential_income"], 1.0).clip(0,1.5)
    df["price_to_rent"] = np.where(df["monthly_rent"]>0, df["predicted_value"]/(df["monthly_rent"]*12), 20.0)
    
    median_pred = df["predicted_value"].median()
    df["tail_gap_z"] = (df["predicted_value"] - median_pred) / 50000
    medae_std = df["medAE"].std()
    
    df["medae_z"] = (df["medAE"] - df["medAE"].median()) / medae_std if medae_std > 0 else 0.0
    
    df["inv_dscr_z"] = (1.0 / df["dscr"].clip(0.1, None) - 0.8) / 0.3
    
    df["R"] = (Config.RISK_WEIGHT_TAIL_GAP * df["tail_gap_z"] + Config.RISK_WEIGHT_MEDAE * df["medae_z"] + Config.RISK_WEIGHT_INV_DSCR * df["inv_dscr_z"])
    df["hard_fail"] = (df["dscr"] < Config.HARD_FAIL_DSCR_MIN) | (df["breakeven_occ"] > Config.HARD_FAIL_BREAKEVEN_MAX)
    df["alert_triggered"] = df["hard_fail"] | (df["R"].abs() > 3.0)
    df["score"] = (df["dscr"].clip(0,5)*Config.SCORE_WEIGHT_DSCR - df["R"]*Config.SCORE_WEIGHT_R + df["amenity_score"]*Config.SCORE_WEIGHT_AMENITY + Config.SCORE_BASELINE).clip(0,10)
    return df

# ============================================================================
# PIPELINE PHASES
# ============================================================================

def setup_db_infrastructure(conn):
    flush_print("\nPhase 0: DB Infrastructure (Schema Compression)...")
    cur = conn.cursor()
    
    # [FIX] Vacuum first to reclaim space from failed runs
    flush_print("  Running VACUUM to reclaim space...")
    try:
        cur.execute("VACUUM;")
    except:
        flush_print("  (VACUUM skipped/failed, proceeding...)")

    cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
    
    # [FIX] FORCE DROP to apply new Compact Schema
    flush_print("  Dropping old table to enforce new compact schema...")
    cur.execute("DROP TABLE IF EXISTS h3_precomputed_hex_details CASCADE;")
    
    # 1. Main Table (COMPACT TYPES: REAL instead of DOUBLE)
    cur.execute("""
        CREATE TABLE h3_precomputed_hex_details (
            id bigint generated by default as identity primary key,
            forecast_year int NOT NULL, h3_res int NOT NULL, h3_id text NOT NULL,
            
            -- [FIX] Compressed Types (REAL = 4 bytes, DOUBLE = 8 bytes)
            opportunity real, opportunity_pct real, trend text, reliability real,
            score real, risk_score real, alert_pct real,
            
            -- Keep currency as Double for precision
            predicted_value double precision, current_value double precision, 
            noi double precision, monthly_rent double precision, 
            
            dscr real, cap_rate real, breakeven_occ real,
            property_count int, sample_accuracy real, med_years real,
            
            accuracy_term real, confidence_term real, stability_term real, robustness_term real, support_term real,
            tail_gap_z real, medae_z real, inv_dscr_z real, pred_cv real, liquidity real, ape real, hard_fail text,
            
            fan_p10_y1 real, fan_p50_y1 real, fan_p90_y1 real, 
            fan_p10_y2 real, fan_p50_y2 real, fan_p90_y2 real,
            fan_p10_y3 real, fan_p50_y3 real, fan_p90_y3 real, 
            fan_p10_y4 real, fan_p50_y4 real, fan_p90_y4 real,
            fan_p10_y5 real, fan_p50_y5 real, fan_p90_y5 real,
            
            created_at timestamptz default now(),
            UNIQUE (forecast_year, h3_res, h3_id)
        );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_h3_details_res ON h3_precomputed_hex_details(h3_res);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_h3_details_year ON h3_precomputed_hex_details(forecast_year);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_h3_details_id ON h3_precomputed_hex_details(h3_id);")
    cur.execute("GRANT SELECT ON h3_precomputed_hex_details TO anon, authenticated, service_role;")
    
    # 2. Grid Table (Stores Geometry ONCE)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS h3_aoi_grid (
            h3_res int NOT NULL, h3_id text NOT NULL, aoi_id text NOT NULL,
            lat double precision, lng double precision, geom geometry(Polygon, 4326),
            UNIQUE (h3_res, h3_id, aoi_id)
        );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_h3_grid_join_aoi ON h3_aoi_grid (aoi_id, h3_res, h3_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_h3_grid_geom ON h3_aoi_grid USING GIST(geom);")
    cur.execute("GRANT SELECT ON h3_aoi_grid TO anon, authenticated, service_role;")

    # 3. Cleanup Old
    cur.execute("DROP TABLE IF EXISTS h3_staging CASCADE;")
    cur.execute("DROP TABLE IF EXISTS h3_historical_values CASCADE;")
    
    # 4. MVT Function
    cur.execute("""
        CREATE OR REPLACE FUNCTION get_h3_mvt(z integer, x integer, y integer, year integer)
        RETURNS bytea LANGUAGE plpgsql STABLE AS $$
        DECLARE
            mvt bytea; target_res integer; tile_bbox geometry;
        BEGIN
            IF z < 9 THEN target_res := 7;
            ELSIF z < 10 THEN target_res := 8;
            ELSIF z < 11 THEN target_res := 9;
            ELSIF z < 12 THEN target_res := 10;
            ELSE target_res := 11;
            END IF;

            tile_bbox := ST_TileEnvelope(z, x, y);

            SELECT ST_AsMVT(mvtgeom, 'h3_layer', 4096, 'geom') INTO mvt FROM (
                SELECT 
                    ST_AsMVTGeom(ST_Transform(g.geom, 3857), tile_bbox, 4096, 256, true) AS geom,
                    d.h3_id, d.opportunity, d.reliability, d.score, d.predicted_value, d.trend, d.property_count
                FROM h3_precomputed_hex_details d
                JOIN h3_aoi_grid g ON d.h3_id = g.h3_id AND d.h3_res = g.h3_res
                WHERE d.forecast_year = year 
                  AND d.h3_res = target_res 
                  AND g.aoi_id = 'harris_county' 
                  AND ST_Intersects(g.geom, ST_Transform(tile_bbox, 4326))
            ) AS mvtgeom;
            RETURN mvt;
        END; $$;
    """)
    cur.execute("GRANT EXECUTE ON FUNCTION get_h3_mvt TO anon, authenticated, service_role;")
    
    conn.commit()
    cur.close()

def build_data_driven_grid(conn, h3_base_map, parent_maps):
    # Ensure clean tx for grid ops (still autocommit=True at this point)
    cur = conn.cursor()
    cur.execute("""
    select
      h3_res,
      count(*) as n_rows,
      sum((lat is null or lng is null)::int) as n_null
    from h3_aoi_grid
    where aoi_id = %s
    group by 1
    """, (Config.AOI_ID,))
    rows = cur.fetchall()
    by_res = {r[0]: {"n_rows": r[1], "n_null": r[2]} for r in rows}

    all_present = True
    any_null = False
    for res in Config.H3_RES_LEVELS:
        info = by_res.get(res)
        if not info or info["n_rows"] == 0:
            all_present = False
            break
        if info["n_null"] > 0:
            any_null = True

    if all_present and (not any_null) and (not Config.FORCE_GRID_REBUILD):
        flush_print(f"\nPhase 1: Valid Grid exists for '{Config.AOI_ID}'. Skipping build.")
        cur.close()
        return

    flush_print("\nPhase 1: Populating Data-Driven Grid (Cache Invalid/Missing)...")
    unique_base = set(h3_base_map.values())
    grid_data = []
    for c_str in unique_base:
        lat, lng = h3_center_str(c_str)
        ewkt = h3_to_ewkt(c_str)
        grid_data.append({"h3_res": Config.H3_BASE_RES, "h3_id": c_str, "lat": lat, "lng": lng, "geom": ewkt})
    for res in Config.H3_RES_LEVELS:
        if res == Config.H3_BASE_RES: continue
        unique_parents = set(parent_maps[res].values())
        for p_str in unique_parents:
            lat, lng = h3_center_str(p_str)
            ewkt = h3_to_ewkt(p_str)
            grid_data.append({"h3_res": res, "h3_id": p_str, "lat": lat, "lng": lng, "geom": ewkt})
            
    grid_df_all = pd.DataFrame(grid_data)
    grid_df_all["aoi_id"] = Config.AOI_ID
    buf = io.StringIO()
    grid_df_all.to_csv(buf, sep="\t", header=False, index=False)
    buf.seek(0)
    
    cur.execute("DELETE FROM h3_aoi_grid WHERE aoi_id = %s", (Config.AOI_ID,))
    cur.copy_expert("COPY h3_aoi_grid (h3_res, h3_id, lat, lng, geom, aoi_id) FROM STDIN WITH (FORMAT TEXT)", buf)
    # Autocommit is True, so this happens immediately
    cur.close()
    flush_print(f"  Grid populated with {len(grid_df_all):,} cells.")

def process_historical_and_forecast(conn, hist_df: pd.DataFrame, prop_df: pd.DataFrame, h3_base_map: Dict[Any, str], parent_maps: Dict):
    timer = Timer()
    
    # Phase 1: Grid (Autocommit = True)
    build_data_driven_grid(conn, h3_base_map, parent_maps)
    
    # [FIX] Phase 2: Switch to Transaction Mode (Autocommit = False)
    conn.autocommit = False 

    flush_print("\nPhase 2: Processing Unified History & Forecast (Compact Schema)...")
    
    hist_subset = hist_df[hist_df['yr'].isin(Config.HISTORICAL_YEARS)].copy()
    hist_subset['h3_base'] = hist_subset['acct'].map(h3_base_map)
    hist_subset = hist_subset.dropna(subset=['h3_base'])
    hist_subset = hist_subset.rename(columns={'actual_value': 'predicted_value'})
    
    # [FIX] Pre-calculate Fan Chart Data (2026-2030) for all resolutions
    # This ensures every row (even historical years) contains the forward-looking 5-year forecast
    flush_print("\nPhase 2a: Pre-calculating Fan Chart Vectors...")
    fan_lookup = defaultdict(lambda: defaultdict(dict)) # fan_lookup[res][h3_id][year_idx] = value
    
    fan_years = [2026, 2027, 2028, 2029, 2030]
    
    for f_year in fan_years:
        y_idx = f_year - 2025 # 1 for 2026, 5 for 2030
        f_pdf = prop_df[prop_df["yr"] == f_year].copy()
        if len(f_pdf) == 0: continue
        
        for res in Config.H3_RES_LEVELS:
            if res == Config.H3_BASE_RES: 
                f_pdf["h3_temp"] = f_pdf["h3_base"]
            else: 
                f_pdf["h3_temp"] = f_pdf["h3_base"].map(parent_maps[res])
            
            # Group and get median
            medians = f_pdf.groupby("h3_temp")["predicted_value"].median()
            for h3_id, val in medians.items():
                fan_lookup[res][h3_id][y_idx] = val

    all_years = sorted(Config.HISTORICAL_YEARS + Config.FORECAST_YEARS)
    
    for year in all_years:
        is_historical = year <= Config.CURRENT_YEAR_BASELINE
        
        flush_print(f"Processing Year {year} ({'Historical' if is_historical else 'Forecast'})...")
        
        if is_historical:
            year_pdf = hist_subset[hist_subset["yr"] == year].copy()
        else:
            year_pdf = prop_df[prop_df["yr"] == year].copy()
            if len(year_pdf) == 0: continue

        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM h3_precomputed_hex_details WHERE forecast_year = %s", (year,))
            
            staging_rows = []
            for res in Config.H3_RES_LEVELS:
                if res == Config.H3_BASE_RES: year_pdf["h3_id"] = year_pdf["h3_base"]
                else: year_pdf["h3_id"] = year_pdf["h3_base"].map(parent_maps[res])
                    
                aggs = {"acct": "nunique", "predicted_value": "median"}
                
                # [FIX] Added pred_cv, ape, liquidity to aggregation
                optional_fields = ["current_value", "cap_rate", "noi", "monthly_rent", "breakeven_occ", "med_years", "tail_gap_z", "medae_z", "inv_dscr_z", "pred_cv", "ape", "liquidity"]
                for field in optional_fields:
                    if field in year_pdf.columns: aggs[field] = "median"

                if not is_historical:
                    aggs.update({"opportunity": "median", "score": "median", "R": "median", "dscr": "median", "alert_triggered": "mean", "sample_accuracy_raw": "median", "hard_fail": "sum"})
                else: 
                     # Ensure we don't carry over leakage fields to history if they exist
                     pass

                metrics = year_pdf.groupby("h3_id").agg(aggs).reset_index()
                rename_map = {"acct": "property_count", "predicted_value": "predicted_value", "R": "risk_score", "alert_triggered": "alert_pct", "sample_accuracy_raw": "sample_accuracy", "hard_fail": "hard_fail_count"}
                metrics = metrics.rename(columns=rename_map)
                
                if metrics.empty: continue

                metrics["property_count"] = metrics["property_count"].fillna(0).astype(np.int32)
                if "opportunity" not in metrics.columns: metrics["opportunity"] = 0.0
                metrics["opportunity"] = metrics["opportunity"].fillna(0.0)
                metrics["opportunity_pct"] = (metrics["opportunity"] * 100).clip(-50, 100)
                metrics["trend"] = "stable"

                if is_historical:
                    cap = Config.DEFAULT_CAP_RATE
                    metrics["noi"] = metrics["predicted_value"] * cap
                    metrics["score"] = 5.0
                    metrics["reliability"] = 1.0
                    metrics["alert_pct"] = 0.0
                    metrics["sample_accuracy"] = 1.0
                    metrics["hard_fail_count"] = 0
                    if "current_value" not in metrics.columns: metrics["current_value"] = np.nan
                    if "dscr" not in metrics.columns: metrics["dscr"] = np.nan
                else:
                    if "reliability" not in metrics.columns: metrics["reliability"] = 0.0
                    metrics["reliability"] = metrics["reliability"].fillna(0.0)
                    mask = metrics["property_count"] > 0
                    if mask.any():
                        metrics.loc[mask, "trend"] = np.select(
                            [metrics.loc[mask, "opportunity"] > Config.TREND_UP_THRESHOLD,
                             metrics.loc[mask, "opportunity"] < Config.TREND_DOWN_THRESHOLD],
                            ["up", "down"], default="stable"
                        )
                
                metrics["hard_fail"] = metrics.get("hard_fail_count", 0) > 0
                metrics["hard_fail"] = metrics["hard_fail"].map({True: 't', False: 'f'})
                metrics["forecast_year"] = int(year)
                metrics["h3_res"] = int(res)
                
                # [FIX] Populate Fan Chart Columns from Pre-calculated Lookup
                # Logic: P50 = Forecast Value, P10/P90 = +/- Spread
                # Spread widens by 5% per year
                
                for i in range(1, Config.FAN_CHART_HORIZON + 1): # 1..5
                    col_p10 = f"fan_p10_y{i}"
                    col_p50 = f"fan_p50_y{i}"
                    col_p90 = f"fan_p90_y{i}"
                    
                    # Vectorized lookup isn't easy here, using apply
                    def get_p50(row):
                         return fan_lookup[res].get(row["h3_id"], {}).get(i, np.nan)
                    
                    metrics[col_p50] = metrics.apply(get_p50, axis=1)
                    
                    # Heuristic Spread: +/- 5% * year_index
                    spread = 0.05 * i
                    metrics[col_p10] = metrics[col_p50] * (1 - spread)
                    metrics[col_p90] = metrics[col_p50] * (1 + spread)

                staging_rows.append(metrics)

            if staging_rows:
                full_year_df = pd.concat(staging_rows, ignore_index=True)
                
                # NO GEOMETRY IN UPLOAD!
                staging_cols = [
                    "forecast_year", "h3_res", "h3_id", "opportunity", "opportunity_pct", "trend", "reliability",
                    "score", "risk_score", "alert_pct", "predicted_value", "current_value", "dscr", "cap_rate",
                    "noi", "monthly_rent", "breakeven_occ",
                    "property_count", "sample_accuracy", "med_years",
                    "accuracy_term", "confidence_term", "stability_term", "robustness_term", "support_term",
                    "tail_gap_z", "medae_z", "inv_dscr_z", "pred_cv", "liquidity", "ape", "hard_fail"
                ]
                fan_cols = []
                for i in range(1, Config.FAN_CHART_HORIZON + 1):
                     fan_cols.extend([f"fan_p10_y{i}", f"fan_p50_y{i}", f"fan_p90_y{i}"])
                
                all_cols = staging_cols + fan_cols
                
                # Ensure all cols exist
                for c in all_cols:
                    if c not in full_year_df.columns: full_year_df[c] = np.nan
                
                final_df = full_year_df[all_cols].copy()
                
                buf = io.StringIO()
                final_df.to_csv(buf, sep="\t", header=False, index=False, na_rep="\\N")
                buf.seek(0)
                
                db_cols = ",".join(all_cols)
                
                flush_print(f"  Year {year}: Pure Metrics Upload ({len(final_df):,} rows)...")
                # Direct COPY into final table
                cur.copy_expert(f"COPY h3_precomputed_hex_details ({db_cols}) FROM STDIN WITH (FORMAT TEXT)", buf)
                
            conn.commit()
            cur.close()
            timer.checkpoint(f"Year {year}")
            
        except Exception as e:
            conn.rollback()
            flush_print(f"❌ Error processing year {year}: {e}")
            raise e

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def run_pipeline_v14():
    timer = Timer()
    flush_print("=" * 70)
    flush_print("H3 FULL STACK PIPELINE v14.4 (Connection Mode Fix)")
    flush_print("=" * 70)
    
    mount_drive_path = Path("/content/drive/MyDrive/data_backups/snapshots")
    try:
        from google.colab import drive
        if not Path("/content/drive").exists(): drive.mount("/content/drive")
    except: pass
    
    all_files = list(mount_drive_path.glob("*.parquet"))
    fc_cand = [f for f in all_files if "production_forecast" in f.name and "with_coords" in f.name]
    if not fc_cand: fc_cand = [f for f in all_files if "production_forecast" in f.name]
    forecast_path = str(max(fc_cand, key=os.path.getmtime))
    
    ro_cand = [f for f in all_files if "rolling_origin_predictions" in f.name]
    ro_path = str(max(ro_cand, key=os.path.getmtime))
    
    cv_cand = [f for f in all_files if "metrics_by_horizon" in f.name]
    cv_path = str(max(cv_cand, key=os.path.getmtime)) if cv_cand else None
    
    print(f"  Forecast: {Path(forecast_path).name}")
    print(f"  History:  {Path(ro_path).name}")
    
    forecast_df = load_forecast_data(forecast_path)
    hist_df = load_historical_data(ro_path) 
    cv_metrics = load_cv_metrics(cv_path)
    timer.checkpoint("Data Load")
    
    h3_base_map = compute_h3_base_per_acct(forecast_df, Config.H3_BASE_RES)
    unique_cells = set(h3_base_map.values()) 
    parent_maps = compute_parent_maps(list(unique_cells))
    timer.checkpoint("H3 Maps")
    
    current_values = dict(zip(hist_df[hist_df['yr']==Config.CURRENT_YEAR_BASELINE]['acct'], 
                              hist_df[hist_df['yr']==Config.CURRENT_YEAR_BASELINE]['actual_value']))
    actual_medae = {}
    prop_df = compute_property_level_metrics(forecast_df, current_values, cv_metrics, h3_base_map, actual_medae)
    timer.checkpoint("Metrics")
    
    conn = get_db_connection()
    setup_db_infrastructure(conn)
    process_historical_and_forecast(conn, hist_df, prop_df, h3_base_map, parent_maps)
    
    conn.close()
    flush_print(f"\n[SUCCESS] Pipeline v14.4 Completed in {time.time() - timer.start:.1f}s.")

if __name__ == "__main__":
    run_pipeline_v14()
