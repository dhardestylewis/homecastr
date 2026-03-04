# Single Colab cell: HCAD World Model v10.2-FULLPANEL (STABLE SAMPLING PATCHED)
# Fully consolidated, ASCII-only
#
# Full-panel capability:
# - Account-chunked materialization to on-disk NPZ shards (no giant in-RAM collect)
# - Shard-based diffusion training (batches copied to GPU per-step)
# - Chunked inference context builder (optional cap)
#
# Preserves v10.2 fixes:
# - Year-aligned labels (no row-shift year gaps)
# - Inference context builder (no label requirement)
# - Sampling-time shared initial noise distribution (matches training coherence)
# - Vectorized region_id and categorical hashing in Polars (no Python loops for hashing)
# - ONE model, ONE objective: predict H-step log1p deltas of tot_appr_val
#
# Numerical stability fixes (this rewrite):
# - Sampler runs in float32 (autocast disabled) until proven stable
# - Inline finite guards and clipping inside DDIM loop
# - Hard floor on scaler scales (prevents huge z-scores)
# - Optional z-score clipping at sampling-time only (conditioning)
# - Default scaled shard dtype float32 (float16 only after stability is proven)
# - Missing lag fill uses per-lag median (not zeros) in shard build and inference

import os, sys, time, math, json, warnings, hashlib, subprocess, contextlib, inspect, shutil
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any

warnings.filterwarnings("ignore")

def ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")

# -----------------------------
# 0) Install deps (Colab-safe)
# -----------------------------
def _pip_install(pkgs: List[str]) -> None:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q"] + pkgs)

def ensure_import(mod: str, pip_name: Optional[str] = None) -> None:
    try:
        __import__(mod)
    except Exception:
        _pip_install([pip_name or mod])

ensure_import("polars", "polars")
ensure_import("pyarrow", "pyarrow")
ensure_import("wandb", "wandb")
import wandb

try:
    import torch
except ImportError:
    _pip_install(["torch", "--index-url", "https://download.pytorch.org/whl/cu121"])

import numpy as np
import polars as pl

import torch
import torch.nn as nn
import torch.nn.functional as F

torch.backends.cudnn.benchmark = True
try:
    torch.set_float32_matmul_precision("high")
except Exception:
    pass

# -----------------------------
# 0b) W&B experiment tracking
# -----------------------------
_WB_RUN = None

def init_wandb(
    project: str = "homecastr",
    entity: str = "dhardestylewis-columbia-university",
    name: Optional[str] = None,
    tags: Optional[List[str]] = None,
    extra_config: Optional[Dict[str, Any]] = None,
    mode: str = "online",
) -> None:
    """Initialize W&B run.  Call once before training loop."""
    global _WB_RUN
    wb_cfg = dict(extra_config or {})
    try:
        # Auto-login using cached credentials or WANDB_API_KEY env var.
        # If neither exists, fall back to offline mode (no interactive prompt).
        _WANDB_KEY_DEFAULT = "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"
        if not wandb.api.api_key:
            _api_key = os.environ.get("WANDB_API_KEY", _WANDB_KEY_DEFAULT)
            if _api_key:
                wandb.login(key=_api_key, relogin=False)
            else:
                print(f"[{ts()}] W&B: no API key found, falling back to offline mode")
                mode = "offline"
        _WB_RUN = wandb.init(
            project=project,
            entity=entity,
            name=name,
            tags=tags or [],
            config=wb_cfg,
            mode=mode,
            reinit=True,
        )
        print(f"[{ts()}] W&B run initialized: {_WB_RUN.url}")
    except Exception as e:
        print(f"[{ts()}] W&B init failed (continuing without): {e}")
        _WB_RUN = None

def wb_log(data: Dict[str, Any], **kw) -> None:
    """Log to W&B if active, otherwise silently skip."""
    if _WB_RUN is not None:
        try:
            wandb.log(data, **kw)
        except Exception:
            pass

def wb_config_update(data: Dict[str, Any]) -> None:
    """Update W&B run config if active."""
    if _WB_RUN is not None:
        try:
            wandb.config.update(data, allow_val_change=True)
        except Exception:
            pass

def wb_log_artifact(path: str, name: str, artifact_type: str = "model") -> None:
    """Log a file as W&B artifact."""
    if _WB_RUN is not None:
        try:
            art = wandb.Artifact(name, type=artifact_type)
            art.add_file(path)
            _WB_RUN.log_artifact(art)
        except Exception as e:
            print(f"[{ts()}] W&B artifact log failed: {e}")

# -----------------------------
# 1) Config - v10.2 FULLPANEL
# -----------------------------
# Use globals().get() so inference_modal.py can inject these before exec
PANEL_PATH_DRIVE = "/content/drive/MyDrive/HCAD_Archive_Aggregates/hcad_master_panel_2005_2025_leakage_strict_FIXEDYR_WITHGIS.parquet"
PANEL_PATH_LOCAL = "/content/local_panel.parquet"
PANEL_PATH = globals().get("PANEL_PATH", PANEL_PATH_LOCAL if os.path.exists(PANEL_PATH_LOCAL) else PANEL_PATH_DRIVE)

OUT_DIR = globals().get("OUT_DIR", "/content/drive/MyDrive/data_backups/world_model_v10_2_fullpanel")
os.makedirs(OUT_DIR, exist_ok=True)

SEED = 42
rng = np.random.default_rng(SEED)
np.random.seed(SEED)
torch.manual_seed(SEED)

# Time contract
MIN_YEAR = 2005
MAX_YEAR = 2025
SEAM_YEAR = 2025
H = 5
FULL_HIST_LEN = MAX_YEAR - MIN_YEAR + 1  # 21

EVAL_ORIGINS = [2021, 2022, 2023, 2024]
FULL_HORIZON_ONLY = False

# Mode toggles
FULL_PANEL_MODE = False
TRAIN_MAX_ACCTS = None
INFER_MAX_PARCELS = 500

# Sampling mode controls (used only if FULL_PANEL_MODE=False)
# Override via env var WM_SAMPLE_FRACTION for scaling sweeps
ACCT_SAMPLE_FRACTION = float(os.environ.get("WM_SAMPLE_FRACTION", "0.02"))
MAX_ACCTS_DIFFUSION = int(os.environ.get("WM_MAX_ACCTS", "50000"))

# Full-panel materialization controls
ACCT_CHUNK_SIZE_TRAIN = 300_000
ACCT_CHUNK_SIZE_INFER = 200_000
MAX_ROWS_PER_SHARD = 8_000_000

# Hash bucket sizes (kept for categorical hashing + gating spatial input)
HASH_BUCKET_SIZE = 32768
GEO_BUCKETS = 4096
GEO_CELL_DEG = 0.01
REGION_EMB_DIM = 32

# v11: Inducing-token latent structure
K_TOKENS = 8         # number of shared latent shock sources (capacity)
K_ACTIVE = 4         # top-k tokens each parcel actually uses (sparsity)
TOKEN_RANK = 1       # rank per token path (scalar shock)
PHI_INIT = 0.80      # initial AR(1) persistence (per-token, learned via TokenPersistence)
GATING_HIDDEN = 64   # hidden dim of parcel gating network
S_BLOCK = 9999       # process all scenarios in one pass (A100 40GB has headroom); was 16

# Training config
DIFF_BATCH = 131072  # v11 uses more VRAM per batch (gating + tokens); was 524288 for v10.2
DIFF_LR = 4e-4
DIFF_EPOCHS = 60
DIFF_EPOCHS_WARMSTART = 20
DIFF_STEPS_TRAIN = 128
DIFF_STEPS_SAMPLE = 20

# Denoiser
DENOISER_HIDDEN = 256
DENOISER_LAYERS = 4
CONV_KERNEL_SIZE = 3

# Inference
INFERENCE_BATCH_SIZE = 16384
S_SCENARIOS = 256

# Precision / compilation
USE_BF16 = True
USE_TORCH_COMPILE = True
COMPILE_MODE = "max-autotune"

# Scaler floors (critical) — v11.1: raised from 1e-2 to 3e-2 for better PI coverage
# Cross-jurisdiction eval showed HCAD under-covering at 58% (target 80%);
# Seattle/France at 82-88% which is well-calibrated. 3e-2 should bring HCAD
# closer to 80% without blowing out Seattle/France intervals.
SCALE_FLOOR_Y = 3e-2
SCALE_FLOOR_NUM = 3e-2
SCALE_FLOOR_TGT = 3e-2

# Sampling stability controls
SAMPLER_DISABLE_AUTOCAST = False   # BF16 autocast on A100 — 2× matmul throughput; safe with nan_to_num guards
SAMPLER_Z_CLIP = 20.0        # conditioning z-score clip (sampling only)
SAMPLER_NOISE_CLIP = 10.0    # clamp noise_hat each step
SAMPLER_X0_CLIP = 50.0       # clamp x0_pred each step
SAMPLER_X_CLIP = 50.0        # clamp x each step
SAMPLER_REPORT_BAD_STEP = False    # suppresses per-step GPU→CPU syncs (.item() calls)
# v11.2: DDIM stochasticity parameter η (Song et al. 2020, Eq. 12)
# η=0.0 = deterministic DDIM (original, near-zero VarRatio)
# η=1.0 = stochastic DDPM (injects noise at every reverse step)
# This is the primary lever for opening up the scenario fan.
SAMPLER_ETA = 0.3

# Scaled shard dtype
SCALED_SHARDS_FLOAT16 = False  # keep float32 until stability is proven

print(f"[{ts()}] HCAD World Model v11.0 FULLPANEL (INDUCING-TOKEN DIFFUSION)")
print(f"[{ts()}] FULL_PANEL_MODE={FULL_PANEL_MODE} FULL_HORIZON_ONLY={FULL_HORIZON_ONLY}")
print(f"[{ts()}] K_TOKENS={K_TOKENS} K_ACTIVE={K_ACTIVE} PHI_INIT={PHI_INIT}")
print(f"[{ts()}] DIFF_BATCH={DIFF_BATCH} DIFF_LR={DIFF_LR} DIFF_EPOCHS={DIFF_EPOCHS} DIFF_EPOCHS_WARMSTART={DIFF_EPOCHS_WARMSTART}")
print(f"[{ts()}] SCALE_FLOOR_Y={SCALE_FLOOR_Y} SCALE_FLOOR_NUM={SCALE_FLOOR_NUM} SCALE_FLOOR_TGT={SCALE_FLOOR_TGT}")
print(f"[{ts()}] SAMPLER_DISABLE_AUTOCAST={SAMPLER_DISABLE_AUTOCAST} SAMPLER_Z_CLIP={SAMPLER_Z_CLIP}")

cfg = {
    "version": "v11.0_inducing_token_diffusion",
    "FULL_PANEL_MODE": bool(FULL_PANEL_MODE),
    "FULL_HORIZON_ONLY": bool(FULL_HORIZON_ONLY),
    "MIN_YEAR": int(MIN_YEAR),
    "MAX_YEAR": int(MAX_YEAR),
    "SEAM_YEAR": int(SEAM_YEAR),
    "H": int(H),
    "EVAL_ORIGINS": list(EVAL_ORIGINS),
    "ACCT_SAMPLE_FRACTION": float(ACCT_SAMPLE_FRACTION),
    "MAX_ACCTS_DIFFUSION": int(MAX_ACCTS_DIFFUSION),
    "ACCT_CHUNK_SIZE_TRAIN": int(ACCT_CHUNK_SIZE_TRAIN),
    "ACCT_CHUNK_SIZE_INFER": int(ACCT_CHUNK_SIZE_INFER),
    "MAX_ROWS_PER_SHARD": int(MAX_ROWS_PER_SHARD),
    "HASH_BUCKET_SIZE": int(HASH_BUCKET_SIZE),
    "GEO_BUCKETS": int(GEO_BUCKETS),
    "GEO_CELL_DEG": float(GEO_CELL_DEG),
    "REGION_EMB_DIM": int(REGION_EMB_DIM),
    "K_TOKENS": int(K_TOKENS),
    "K_ACTIVE": int(K_ACTIVE),
    "TOKEN_RANK": int(TOKEN_RANK),
    "PHI_INIT": float(PHI_INIT),
    "GATING_HIDDEN": int(GATING_HIDDEN),
    "DIFF_BATCH": int(DIFF_BATCH),
    "DIFF_LR": float(DIFF_LR),
    "DIFF_EPOCHS": int(DIFF_EPOCHS),
    "DIFF_EPOCHS_WARMSTART": int(DIFF_EPOCHS_WARMSTART),
    "DIFF_STEPS_TRAIN": int(DIFF_STEPS_TRAIN),
    "DIFF_STEPS_SAMPLE": int(DIFF_STEPS_SAMPLE),
    "INFERENCE_BATCH_SIZE": int(INFERENCE_BATCH_SIZE),
    "S_SCENARIOS": int(S_SCENARIOS),
    "USE_BF16": bool(USE_BF16),
    "USE_TORCH_COMPILE": bool(USE_TORCH_COMPILE),
    "COMPILE_MODE": str(COMPILE_MODE),
    "SCALE_FLOOR_Y": float(SCALE_FLOOR_Y),
    "SCALE_FLOOR_NUM": float(SCALE_FLOOR_NUM),
    "SCALE_FLOOR_TGT": float(SCALE_FLOOR_TGT),
    "SAMPLER_Z_CLIP": float(SAMPLER_Z_CLIP),
    "SAMPLER_NOISE_CLIP": float(SAMPLER_NOISE_CLIP),
    "SAMPLER_X0_CLIP": float(SAMPLER_X0_CLIP),
    "SAMPLER_X_CLIP": float(SAMPLER_X_CLIP),
    "SCALED_SHARDS_FLOAT16": bool(SCALED_SHARDS_FLOAT16),
}
with open(os.path.join(OUT_DIR, "run_config.json"), "w") as f:
    json.dump(cfg, f, indent=2)

# -----------------------------
# 2) Mount Drive + load panel
# -----------------------------
try:
    from google.colab import drive
    if not os.path.exists("/content/drive"):
        drive.mount("/content/drive", force_remount=False)
except Exception as e:
    print(f"[{ts()}] Drive mount skipped: {e}")

if not os.path.exists(PANEL_PATH):
    if os.path.exists(PANEL_PATH_DRIVE):
        PANEL_PATH = PANEL_PATH_DRIVE
    else:
        raise FileNotFoundError("Panel not found")

print(f"[{ts()}] Loading panel: {PANEL_PATH}")
lf = pl.scan_parquet(PANEL_PATH)
schema = lf.collect_schema()
cols = schema.names()
cols_set = set(cols)

# Contract checks
assert all(c in cols_set for c in ["acct", "yr", "tot_appr_val"]), "Missing required columns"
forbidden = ["tot_mkt_val", "assessed_val", "land_val", "bld_val"]
assert not any(c in cols_set for c in forbidden), "Forbidden columns present"
print(f"[{ts()}] Panel acceptance: PASS")

# Feature selection
cat_cols = [c for c in cols if c not in ["acct", "yr", "tot_appr_val"] and "str" in str(schema.get(c)).lower()]
num_cols = [c for c in cols if c not in ["acct", "yr", "tot_appr_val"] and c not in cat_cols]

# Drop columns with Null type (all-None columns from unmapped canonical fields)
# These inflate NUM_DIM but collapse in shard builder, causing shape mismatches
_null_cols = [c for c in num_cols if str(schema[c]).lower() in ("null", "unknown")]
if _null_cols:
    print(f"[{ts()}] FEATURE AUDIT — DROPPING Null-type columns: {_null_cols}")
    num_cols = [c for c in num_cols if c not in set(_null_cols)]

# Explicit feature curation:
# DROP: features reassessed annually by the assessor (quasi-leaky)
#       + lag-1 valuation components (redundant with prior_ and decompose the target)
#       + metadata columns with no predictive value
NUM_DROP = {
    "bld_mean_accrued_depr_pct",     # reassessed annually
    "bld_sum_cama_replacement_cost", # reassessed annually
    "tot_mkt_val_lag1",              # redundant with prior_tot_mkt_val
    "assessed_val_lag1",             # valuation component, reassessed
    "land_val_lag1",                 # valuation component, reassessed
    "bld_val_lag1",                  # valuation component, reassessed
    "x_features_val_lag1",           # valuation component, reassessed
    "ag_val_lag1",                   # valuation component, reassessed
    "new_construction_val_lag1",     # valuation component, reassessed
    "tot_rcn_val_lag1",              # valuation component, reassessed
    "gis_year_used",                 # GIS metadata, not a property feature
    # v4 leakage fix:
    "land_sum_land_val",           # current-year land value, component of target
    "jur_value_sum_value",         # current-year jurisdiction value aggregate
    "jur_value_mean_value",        # current-year jurisdiction value aggregate
}
# PROMOTE: move these to the front so they're always included
NUM_PROMOTE = ["gis_lat", "gis_lon"]

num_cols = [c for c in num_cols if c not in NUM_DROP]
# Move promoted features to front (if they exist in the panel)
promoted = [c for c in NUM_PROMOTE if c in num_cols]
remaining = [c for c in num_cols if c not in set(promoted)]
num_cols = promoted + remaining

if NUM_DROP:
    print(f"[{ts()}] FEATURE AUDIT — DROPPED numeric: {sorted(NUM_DROP)}")
if promoted:
    print(f"[{ts()}] FEATURE AUDIT — PROMOTED numeric: {promoted}")

num_use = num_cols  # use all available (after drops)
cat_use = cat_cols  # use all available
NUM_DIM = int(len(num_use))
N_CAT = int(len(cat_use))
print(f"[{ts()}] Features: {NUM_DIM} numeric, {N_CAT} categorical")
print(f"[{ts()}] FEATURE AUDIT — numeric ({NUM_DIM}): {num_use}")
print(f"[{ts()}] FEATURE AUDIT — categorical ({N_CAT}): {cat_use}")
num_unused = [c for c in cols if c not in ["acct", "yr", "tot_appr_val"] and c not in set(cat_cols) and c not in set(num_use)]
if num_unused:
    print(f"[{ts()}] FEATURE AUDIT — UNUSED/DROPPED numeric ({len(num_unused)}): {num_unused}")
cat_unused = [c for c in cat_cols if c not in set(cat_use)]
if cat_unused:
    print(f"[{ts()}] FEATURE AUDIT — UNUSED categorical ({len(cat_unused)}): {cat_unused}")
wb_config_update({
    "features/num_use": num_use,
    "features/cat_use": cat_use,
    "features/num_available": num_cols,
    "features/cat_available": cat_cols,
    "features/num_unused": num_cols[30:] if len(num_cols) > 30 else [],
    "features/cat_unused": cat_cols[10:] if len(cat_cols) > 10 else [],
})

# Geo column discovery (GIS-aware)
GEO_COL = None
for c in ["gis_zip", "h3_8", "h3_7", "h3_9", "geoid", "tract", "zip", "zipcode"]:
    if c in cols_set:
        GEO_COL = c
        break
HAS_LATLON = ("gis_lat" in cols_set) and ("gis_lon" in cols_set)
print(f"[{ts()}] GEO_COL={GEO_COL} HAS_LATLON={HAS_LATLON}")

# -----------------------------
# PATCH A) Hist lag fill that cannot create "all-missing -> 0.0" columns
# Also computes a safe fallback log1p(level) for rows that are entirely missing (rare).
# -----------------------------
def compute_year_median_log1p_level(lf: pl.LazyFrame, min_year: int, max_year: int) -> Dict[int, float]:
    dfm = (
        lf
        .filter(pl.col("yr").is_between(int(min_year), int(max_year)))
        .filter(pl.col("tot_appr_val") > 0)
        .select([
            pl.col("yr").cast(pl.Int32).alias("yr"),
            pl.col("tot_appr_val").log1p().alias("y_log"),
        ])
        .group_by("yr")
        .agg(pl.col("y_log").median().alias("y_med"))
        .collect()
    )
    out = {}
    for r in dfm.iter_rows(named=True):
        out[int(r["yr"])] = float(r["y_med"])
    return out

Y_MED_BY_YEAR = compute_year_median_log1p_level(lf, MIN_YEAR, MAX_YEAR)
Y_FALLBACK_LOG1P = float(Y_MED_BY_YEAR.get(int(MIN_YEAR), 0.0))
print(f"[{ts()}] Y_FALLBACK_LOG1P (year {MIN_YEAR} median) = {Y_FALLBACK_LOG1P:.6f}")

def fill_hist_lags_no_zeros(hist_mat: np.ndarray, fallback_value: float) -> np.ndarray:
    """
    hist_mat: [N, FULL_HIST_LEN], oldest->newest (lag_{L-1} ... lag_0), may contain NaN for missing years.
    Goal:
      - Fill partially-missing columns using column medians.
      - Fill fully-missing columns (e.g., calendar underflow) using per-row earliest observed value (not a constant).
      - Never emit 0.0 due to NaN medians; only use fallback for rows that are entirely missing.
    """
    if hist_mat.size == 0:
        return hist_mat.astype(np.float32)

    X = hist_mat.astype(np.float32, copy=False)

    # Step 1) Fill columns that have at least one finite value with that column median.
    col_med = np.nanmedian(X, axis=0)  # NaN for fully-missing columns
    finite_col = np.isfinite(col_med)
    if finite_col.any():
        Xm = X[:, finite_col]
        Xm = np.where(np.isfinite(Xm), Xm, col_med[finite_col][None, :]).astype(np.float32)
        X[:, finite_col] = Xm

    # Step 2) Remaining NaNs are from fully-missing columns (calendar underflow) or rare full-row gaps.
    # Fill per-row with the earliest finite value in that row (after Step 1).
    finite_mask = np.isfinite(X)
    any_finite_row = finite_mask.any(axis=1)

    # Find first finite per row scanning left->right (oldest->newest).
    idx_first = np.argmax(finite_mask, axis=1)  # 0 if none finite; guarded below
    row_first = X[np.arange(X.shape[0]), idx_first]
    row_first = np.where(any_finite_row, row_first, float(fallback_value)).astype(np.float32)

    X = np.where(np.isfinite(X), X, row_first[:, None]).astype(np.float32)

    # Step 3) Final guard
    X = np.nan_to_num(X, nan=float(fallback_value), posinf=float(fallback_value), neginf=float(fallback_value)).astype(np.float32)
    return X

# -----------------------------
# 3) Polars stable hashing helpers
# -----------------------------
_HASH_HAS_SEED = False
try:
    _HASH_HAS_SEED = "seed" in inspect.signature(pl.Expr.hash).parameters
except Exception:
    _HASH_HAS_SEED = False

def stable_hash_expr(e: pl.Expr) -> pl.Expr:
    if _HASH_HAS_SEED:
        return e.hash(seed=SEED)
    return e.hash()

def bucket_hash_expr_str(col_expr: pl.Expr, n_buckets: int) -> pl.Expr:
    return (
        pl.when(col_expr.is_null())
        .then(pl.lit(0))
        .otherwise(
            (stable_hash_expr(col_expr.cast(pl.Utf8)) % (n_buckets - 1) + 1).cast(pl.Int64)
        )
        .cast(pl.Int64)
    )

def build_region_id_expr() -> pl.Expr:
    if GEO_COL and GEO_COL in cols_set:
        return bucket_hash_expr_str(pl.col(GEO_COL), GEO_BUCKETS).alias("region_id")
    if HAS_LATLON:
        lat_col = "gis_lat"
        lon_col = "gis_lon"
        lat_bin = (pl.col(lat_col).fill_null(0.0) / GEO_CELL_DEG).floor().cast(pl.Int64)
        lon_bin = (pl.col(lon_col).fill_null(0.0) / GEO_CELL_DEG).floor().cast(pl.Int64)
        key = (lat_bin * 1_000_000 + lon_bin).cast(pl.Utf8)
        return bucket_hash_expr_str(key, GEO_BUCKETS).alias("region_id")
    return bucket_hash_expr_str(pl.col("acct").cast(pl.Utf8), GEO_BUCKETS).alias("region_id")

def build_cat_hash_exprs(cat_cols_local: List[str]) -> List[pl.Expr]:
    exprs = []
    for c in cat_cols_local:
        if c in cols_set:
            exprs.append(bucket_hash_expr_str(pl.col(c), HASH_BUCKET_SIZE).alias(f"cat_{c}"))
        else:
            exprs.append(pl.lit(0).cast(pl.Int64).alias(f"cat_{c}"))
    return exprs

# -----------------------------
# 4) Account discovery and sampling
# -----------------------------
print(f"[{ts()}] Discovering accounts...")
accts_df = lf.select(pl.col("acct").cast(pl.Utf8).unique()).collect()
all_accts = accts_df["acct"].to_list()
print(f"[{ts()}] all_accts={len(all_accts):,}")

if TRAIN_MAX_ACCTS is not None:
    all_accts = all_accts[:int(TRAIN_MAX_ACCTS)]
    print(f"[{ts()}] TRAIN_MAX_ACCTS applied: {len(all_accts):,}")

if not FULL_PANEL_MODE:
    sampled_accts = [
        a for a in all_accts
        if a is not None and int(hashlib.md5(str(a).encode()).hexdigest(), 16) % 10000 < int(ACCT_SAMPLE_FRACTION * 10000)
    ][:MAX_ACCTS_DIFFUSION]
    train_accts = sampled_accts
    infer_accts = sampled_accts
    print(f"[{ts()}] Sampled accounts: {len(sampled_accts):,}")
else:
    train_accts = all_accts
    infer_accts = all_accts
    print(f"[{ts()}] Full-panel accounts: {len(train_accts):,}")

# -----------------------------
# 5) Local scratch dir resolver
# -----------------------------
def resolve_local_work_dirs(out_dir_drive: str, scratch_root: str = "/content/wm_scratch") -> Dict[str, str]:
    os.makedirs(scratch_root, exist_ok=True)
    raw_shard_root = os.path.join(scratch_root, "train_shards_raw")
    scaled_shard_root = os.path.join(scratch_root, "train_shards_scaled")
    os.makedirs(raw_shard_root, exist_ok=True)
    os.makedirs(scaled_shard_root, exist_ok=True)
    return {
        "OUT_DIR_DRIVE": out_dir_drive,
        "SCRATCH_ROOT": scratch_root,
        "RAW_SHARD_ROOT": raw_shard_root,
        "SCALED_SHARD_ROOT": scaled_shard_root,
    }

def copy_small_artifacts_to_drive(src_path: str, dst_dir_drive: str) -> str:
    os.makedirs(dst_dir_drive, exist_ok=True)
    base = os.path.basename(src_path)
    dst_path = os.path.join(dst_dir_drive, base)
    try:
        shutil.copy2(src_path, dst_path)
        return dst_path
    except Exception as e:
        print(f"[{ts()}] WARNING copy_to_drive failed: {e}")
        return src_path

work_dirs = resolve_local_work_dirs(OUT_DIR)
print(f"[{ts()}] work_dirs={work_dirs}")

# -----------------------------
# 6) Shard I/O (mmap-friendly)
# -----------------------------
def _np_savez_shard(
    path: str,
    hist_y: np.ndarray,
    cur_num: np.ndarray,
    cur_cat: np.ndarray,
    region_id: np.ndarray,
    target: np.ndarray,      # [N,H]
    mask: np.ndarray,        # [N,H] float32
    yr_label: np.ndarray,    # [N,H] int32
    y_anchor: np.ndarray,
    anchor_year: np.ndarray,
    acct: np.ndarray,
) -> None:
    np.savez(
        path,
        hist_y=hist_y.astype(np.float32, copy=False),
        cur_num=cur_num.astype(np.float32, copy=False),
        cur_cat=cur_cat.astype(np.int64, copy=False),
        region_id=region_id.astype(np.int64, copy=False),
        target=target.astype(np.float32, copy=False),
        mask=mask.astype(np.float32, copy=False),
        yr_label=yr_label.astype(np.int32, copy=False),
        y_anchor=y_anchor.astype(np.float32, copy=False),
        anchor_year=anchor_year.astype(np.int32, copy=False),
        acct=acct.astype(object, copy=False),
    )

def _iter_shards_npz(shard_paths: List[str]):
    for p in shard_paths:
        z = np.load(p, allow_pickle=True)
        yield p, z

# -----------------------------
# 7) Master shards at max_origin, then derive per-origin shards
# -----------------------------
def _nan_fill_with_median_per_col(X: np.ndarray) -> np.ndarray:
    # X: [N,D]
    if X.size == 0:
        return X.astype(np.float32)
    X = X.astype(np.float32, copy=False)
    med = np.nanmedian(X, axis=0)
    med = np.where(np.isfinite(med), med, 0.0).astype(np.float32)
    X2 = np.where(np.isfinite(X), X, med[None, :]).astype(np.float32)
    return X2

# -----------------------------
# PATCH B) Master shard builder: use fill_hist_lags_no_zeros so early-lag columns never collapse
# -----------------------------
def build_master_training_shards_v102_local(
    lf: pl.LazyFrame,
    accts: List[str],
    num_use_local: List[str],
    cat_use_local: List[str],
    max_origin: int,
    full_horizon_only: bool,
    work_dirs: Dict[str, str],
    acct_chunk_size: int = ACCT_CHUNK_SIZE_TRAIN,
    max_rows_per_shard: int = MAX_ROWS_PER_SHARD,
) -> Dict[str, Any]:
    origin = int(max_origin)
    train_max_year = int(origin - 1)

    if full_horizon_only:
        anchor_cutoff = int(origin - 1 - H)
    else:
        anchor_cutoff = int(origin - 2)

    out_root = work_dirs["RAW_SHARD_ROOT"]
    shard_dir = os.path.join(out_root, f"master_origin_{origin}")
    os.makedirs(shard_dir, exist_ok=True)

    print(f"[{ts()}] Building MASTER shards at max_origin={origin}")
    print(f"[{ts()}] train_max_year={train_max_year} anchor_cutoff={anchor_cutoff} full_horizon_only={full_horizon_only}")
    print(f"[{ts()}] shard_dir={shard_dir}")

    region_expr = build_region_id_expr()
    cat_hash_exprs = build_cat_hash_exprs(cat_use_local)

    shift_exprs: List[pl.Expr] = []
    for k in range(0, H + 1):
        shift_exprs.append(pl.col("y_log").shift(-k).over("acct").alias(f"y_shift_{k}"))
        shift_exprs.append(pl.col("yr").shift(-k).over("acct").alias(f"yr_shift_{k}"))

    hist_exprs = [pl.col("y_log").shift(i).over("acct").alias(f"lag_{i}") for i in range(FULL_HIST_LEN)]
    global_medians_accum: Dict[str, List[float]] = {c: [] for c in num_use_local}

    shard_paths: List[str] = []
    shard_id = 0
    n_train_total = 0
    max_anchor_year_seen: Optional[int] = None
    max_label_year_used = 0

    buf_hist: List[np.ndarray] = []
    buf_num: List[np.ndarray] = []
    buf_cat: List[np.ndarray] = []
    buf_rid: List[np.ndarray] = []
    buf_tgt: List[np.ndarray] = []
    buf_msk: List[np.ndarray] = []
    buf_yr: List[np.ndarray] = []
    buf_yanc: List[np.ndarray] = []
    buf_ay: List[np.ndarray] = []
    buf_acct: List[np.ndarray] = []
    buf_rows = 0

    def flush() -> None:
        nonlocal shard_id, buf_rows
        if buf_rows <= 0:
            return
        hist_y = np.concatenate(buf_hist, axis=0)
        cur_num = np.concatenate(buf_num, axis=0) if buf_num else np.zeros((hist_y.shape[0], 0), np.float32)
        cur_cat = np.concatenate(buf_cat, axis=0) if buf_cat else np.zeros((hist_y.shape[0], 0), np.int64)
        region_id = np.concatenate(buf_rid, axis=0)
        target = np.concatenate(buf_tgt, axis=0)
        mask = np.concatenate(buf_msk, axis=0)
        yr_label = np.concatenate(buf_yr, axis=0)
        y_anchor = np.concatenate(buf_yanc, axis=0)
        anchor_year = np.concatenate(buf_ay, axis=0)
        acct_arr = np.concatenate(buf_acct, axis=0).astype(object)

        shard_path = os.path.join(shard_dir, f"shard_{shard_id:05d}.npz")
        if shard_id == 0:
            print(f"  [DIAG-RAW] flush shard_0: hist_y={hist_y.shape} cur_num={cur_num.shape} cur_cat={cur_cat.shape} region_id={region_id.shape}")
        _np_savez_shard(
            shard_path,
            hist_y=hist_y,
            cur_num=cur_num,
            cur_cat=cur_cat,
            region_id=region_id,
            target=target,
            mask=mask,
            yr_label=yr_label,
            y_anchor=y_anchor,
            anchor_year=anchor_year,
            acct=acct_arr,
        )
        shard_paths.append(shard_path)
        print(f"[{ts()}] Wrote MASTER {os.path.basename(shard_path)} rows={hist_y.shape[0]:,}")

        shard_id += 1
        buf_hist.clear()
        buf_num.clear()
        buf_cat.clear()
        buf_rid.clear()
        buf_tgt.clear()
        buf_msk.clear()
        buf_yr.clear()
        buf_yanc.clear()
        buf_ay.clear()
        buf_acct.clear()
        buf_rows = 0

    t0_all = time.time()

    for s in range(0, len(accts), int(acct_chunk_size)):
        acct_chunk = accts[s:s + int(acct_chunk_size)]
        if not acct_chunk:
            continue

        t0 = time.time()
        base_q = (
            lf
            .filter(pl.col("acct").cast(pl.Utf8).is_in(acct_chunk))
            .filter(pl.col("yr").is_between(MIN_YEAR, MAX_YEAR))
            .filter(pl.col("tot_appr_val") > 0)
            .with_columns([
                pl.col("acct").cast(pl.Utf8).alias("acct"),
                pl.col("yr").cast(pl.Int32).alias("yr"),
                pl.col("tot_appr_val").log1p().alias("y_log"),
            ])
            .sort(["acct", "yr"])
        )

        q = (
            base_q
            .with_columns(shift_exprs + hist_exprs + [region_expr] + cat_hash_exprs)
            .filter(pl.col("yr") <= int(anchor_cutoff))
        )

        df = q.collect()
        dt = time.time() - t0
        print(f"[{ts()}] MASTER acct_chunk={s}:{s+len(acct_chunk)} collect_rows={len(df):,} time={dt:.1f}s")
        if len(df) == 0:
            continue

        anchor_years = df["yr"].to_numpy().astype(np.int32)
        if anchor_years.size > 0:
            ay_max = int(anchor_years.max())
            if (max_anchor_year_seen is None) or (ay_max > max_anchor_year_seen):
                max_anchor_year_seen = ay_max

        n = int(len(df))
        target = np.zeros((n, H), dtype=np.float32)
        mask = np.zeros((n, H), dtype=np.float32)
        yr_label = np.zeros((n, H), dtype=np.int32)

        for k in range(1, H + 1):
            y_curr = df[f"y_shift_{k}"].to_numpy().astype(np.float32)
            y_prev = df[f"y_shift_{k-1}"].to_numpy().astype(np.float32)
            yr_curr = df[f"yr_shift_{k}"].to_numpy()
            yr_prev = df[f"yr_shift_{k-1}"].to_numpy()

            expected_curr = anchor_years + k
            expected_prev = anchor_years + (k - 1)

            year_aligned = (yr_curr == expected_curr) & (yr_prev == expected_prev)
            valid = np.isfinite(y_curr) & np.isfinite(y_prev) & year_aligned & (yr_curr <= int(train_max_year))

            target[:, k - 1] = np.where(valid, y_curr - y_prev, 0.0).astype(np.float32)
            mask[:, k - 1] = valid.astype(np.float32)
            yr_label[:, k - 1] = expected_curr.astype(np.int32)

            if valid.any():
                max_label_year_used = max(max_label_year_used, int(np.max(yr_curr[valid])))

        if full_horizon_only:
            keep = (mask.sum(axis=1) == float(H))
        else:
            keep = (mask.sum(axis=1) >= 1.0)

        if not keep.any():
            continue

        idx = np.where(keep)[0]
        n_keep = int(idx.size)
        n_train_total += n_keep

        # Build hist_y (oldest->newest) and fill safely (never 0.0 from NaN medians)
        hist_cols = [f"lag_{i}" for i in range(FULL_HIST_LEN - 1, -1, -1)]
        hist_mat = np.column_stack([df[c].to_numpy().astype(np.float32) for c in hist_cols]).astype(np.float32)
        hist_mat = fill_hist_lags_no_zeros(hist_mat, fallback_value=float(Y_FALLBACK_LOG1P))
        hist_y = hist_mat[idx].astype(np.float32)

        y_anchor = df["y_log"].to_numpy().astype(np.float32)[idx]
        region_id = df["region_id"].to_numpy().astype(np.int64)[idx]
        acct_arr = df["acct"].to_numpy().astype(object)[idx]
        anchor_year_keep = anchor_years[idx]

        cur_num_list = []
        for c in num_use_local:
            if c in df.columns:
                vals = df[c].to_numpy().astype(np.float32)
                med = float(np.nanmedian(vals)) if np.isfinite(np.nanmedian(vals)) else 0.0
                global_medians_accum[c].append(med)
                cur_num_list.append(np.nan_to_num(vals, nan=med).astype(np.float32)[idx])
            else:
                global_medians_accum[c].append(0.0)
                cur_num_list.append(np.zeros(n_keep, dtype=np.float32))
        cur_num = np.column_stack(cur_num_list).astype(np.float32) if cur_num_list else np.zeros((n_keep, 0), np.float32)

        cur_cat_list = []
        for c in cat_use_local:
            hc = f"cat_{c}"
            if hc in df.columns:
                cur_cat_list.append(df[hc].to_numpy().astype(np.int64)[idx])
            else:
                cur_cat_list.append(np.zeros(n_keep, dtype=np.int64))
        cur_cat = np.column_stack(cur_cat_list).astype(np.int64) if cur_cat_list else np.zeros((n_keep, 0), np.int64)

        buf_hist.append(hist_y)
        buf_num.append(cur_num)
        buf_cat.append(cur_cat)
        buf_rid.append(region_id)
        buf_tgt.append(target[idx].astype(np.float32))
        buf_msk.append(mask[idx].astype(np.float32))
        buf_yr.append(yr_label[idx].astype(np.int32))
        buf_yanc.append(y_anchor.astype(np.float32))
        buf_ay.append(anchor_year_keep.astype(np.int32))
        buf_acct.append(acct_arr.astype(object))
        buf_rows += n_keep

        if buf_rows >= int(max_rows_per_shard):
            flush()

    flush()

    global_medians: Dict[str, float] = {}
    for c in num_use_local:
        vals = global_medians_accum.get(c, [])
        global_medians[c] = float(np.median(np.asarray(vals, dtype=np.float32))) if vals else 0.0

    if max_anchor_year_seen is not None:
        assert int(max_anchor_year_seen) <= int(origin - 1), f"Anchor leakage (master): {max_anchor_year_seen} > {origin-1}"
    assert int(max_label_year_used) <= int(origin - 1), f"Label leakage (master): {max_label_year_used} > {origin-1}"

    dt_all = time.time() - t0_all
    print(f"[{ts()}] MASTER build done shards={len(shard_paths)} n_train={n_train_total:,} time={dt_all:.1f}s")
    print(f"[{ts()}] MASTER max_anchor_year={max_anchor_year_seen} max_label_year_used={max_label_year_used}")

    return {
        "max_origin": int(origin),
        "shards": shard_paths,
        "n_train": int(n_train_total),
        "global_medians": global_medians,
        "max_anchor_year": max_anchor_year_seen,
        "max_label_year_used": int(max_label_year_used),
        "master_dir": shard_dir,
    }

def derive_origin_shards_from_master(
    master_shard_paths: List[str],
    origin: int,
    full_horizon_only: bool,
    work_dirs: Dict[str, str],
    max_rows_per_shard: int = MAX_ROWS_PER_SHARD,
) -> Dict[str, Any]:
    origin = int(origin)
    out_root = work_dirs["RAW_SHARD_ROOT"]
    shard_dir = os.path.join(out_root, f"origin_{origin}")
    os.makedirs(shard_dir, exist_ok=True)

    print(f"[{ts()}] Deriving origin shards from master origin={origin} dir={shard_dir}")

    shard_paths: List[str] = []
    shard_id = 0
    n_train_total = 0
    max_anchor_year_seen: Optional[int] = None
    max_label_year_used = 0

    buf_hist: List[np.ndarray] = []
    buf_num: List[np.ndarray] = []
    buf_cat: List[np.ndarray] = []
    buf_rid: List[np.ndarray] = []
    buf_tgt: List[np.ndarray] = []
    buf_msk: List[np.ndarray] = []
    buf_yr: List[np.ndarray] = []
    buf_yanc: List[np.ndarray] = []
    buf_ay: List[np.ndarray] = []
    buf_acct: List[np.ndarray] = []
    buf_rows = 0

    def flush() -> None:
        nonlocal shard_id, buf_rows
        if buf_rows <= 0:
            return
        hist_y = np.concatenate(buf_hist, axis=0)
        cur_num = np.concatenate(buf_num, axis=0) if buf_num else np.zeros((hist_y.shape[0], 0), np.float32)
        cur_cat = np.concatenate(buf_cat, axis=0) if buf_cat else np.zeros((hist_y.shape[0], 0), np.int64)
        region_id = np.concatenate(buf_rid, axis=0)
        target = np.concatenate(buf_tgt, axis=0)
        mask = np.concatenate(buf_msk, axis=0)
        yr_label = np.concatenate(buf_yr, axis=0)
        y_anchor = np.concatenate(buf_yanc, axis=0)
        anchor_year = np.concatenate(buf_ay, axis=0)
        acct_arr = np.concatenate(buf_acct, axis=0).astype(object)

        shard_path = os.path.join(shard_dir, f"shard_{shard_id:05d}.npz")
        _np_savez_shard(
            shard_path,
            hist_y=hist_y,
            cur_num=cur_num,
            cur_cat=cur_cat,
            region_id=region_id,
            target=target,
            mask=mask,
            yr_label=yr_label,
            y_anchor=y_anchor,
            anchor_year=anchor_year,
            acct=acct_arr,
        )
        shard_paths.append(shard_path)
        print(f"[{ts()}] Wrote ORIGIN {os.path.basename(shard_path)} rows={hist_y.shape[0]:,}")

        shard_id += 1
        buf_hist.clear()
        buf_num.clear()
        buf_cat.clear()
        buf_rid.clear()
        buf_tgt.clear()
        buf_msk.clear()
        buf_yr.clear()
        buf_yanc.clear()
        buf_ay.clear()
        buf_acct.clear()
        buf_rows = 0

    t0 = time.time()
    required_max_label = int(origin - 1)

    for _, z in _iter_shards_npz(master_shard_paths):
        hist_y = z["hist_y"].astype(np.float32, copy=False)
        cur_num = z["cur_num"].astype(np.float32, copy=False)
        cur_cat = z["cur_cat"].astype(np.int64, copy=False)
        region_id = z["region_id"].astype(np.int64, copy=False)
        target = z["target"].astype(np.float32, copy=False)
        mask = z["mask"].astype(np.float32, copy=False)
        yr_label = z["yr_label"].astype(np.int32, copy=False)
        y_anchor = z["y_anchor"].astype(np.float32, copy=False)
        anchor_year = z["anchor_year"].astype(np.int32, copy=False)
        acct = z["acct"].astype(object, copy=False)

        if hist_y.shape[0] == 0:
            continue

        allowed = (yr_label <= required_max_label).astype(np.float32)
        mask_new = mask * allowed

        if full_horizon_only:
            keep = (mask_new.sum(axis=1) == float(H))
        else:
            keep = (mask_new.sum(axis=1) >= 1.0)

        if not keep.any():
            continue

        idx = np.where(keep)[0]
        n_keep = int(idx.size)
        n_train_total += n_keep

        ay_max = int(np.max(anchor_year[idx])) if n_keep > 0 else None
        if ay_max is not None:
            if (max_anchor_year_seen is None) or (ay_max > max_anchor_year_seen):
                max_anchor_year_seen = ay_max

        yr_used = yr_label[idx][mask_new[idx] > 0.0]
        if yr_used.size > 0:
            max_label_year_used = max(max_label_year_used, int(np.max(yr_used)))

        buf_hist.append(hist_y[idx])
        buf_num.append(cur_num[idx] if cur_num.shape[1] > 0 else np.zeros((n_keep, 0), np.float32))
        buf_cat.append(cur_cat[idx] if cur_cat.shape[1] > 0 else np.zeros((n_keep, 0), np.int64))
        buf_rid.append(region_id[idx])
        buf_tgt.append(target[idx])
        buf_msk.append(mask_new[idx])
        buf_yr.append(yr_label[idx])
        buf_yanc.append(y_anchor[idx])
        buf_ay.append(anchor_year[idx])
        buf_acct.append(acct[idx])
        buf_rows += n_keep

        if buf_rows >= int(max_rows_per_shard):
            flush()

    flush()

    if max_anchor_year_seen is not None:
        assert int(max_anchor_year_seen) <= int(origin - 1), f"Anchor leakage (origin): {max_anchor_year_seen} > {origin-1}"
    assert int(max_label_year_used) <= int(origin - 1), f"Label leakage (origin): {max_label_year_used} > {origin-1}"

    dt = time.time() - t0
    print(f"[{ts()}] Derived origin shards done origin={origin} shards={len(shard_paths)} n_train={n_train_total:,} time={dt:.1f}s")
    return {
        "origin": int(origin),
        "shards": shard_paths,
        "n_train": int(n_train_total),
        "max_anchor_year": max_anchor_year_seen,
        "max_label_year_used": int(max_label_year_used),
        "required_max_label": int(origin - 1),
        "leakage_free": bool(int(max_label_year_used) <= int(origin - 1)),
    }

# -----------------------------
# 8) Streaming scalers with hard scale floors
# -----------------------------
class RunningMeanVar:
    def __init__(self, dim: int):
        self.dim = int(dim)
        self.n = 0
        self.mean = np.zeros((self.dim,), dtype=np.float64)
        self.M2 = np.zeros((self.dim,), dtype=np.float64)

    def update(self, X: np.ndarray) -> None:
        if X.size == 0:
            return
        X = X.astype(np.float64, copy=False)
        n_b = int(X.shape[0])
        mean_b = X.mean(axis=0)
        var_b = X.var(axis=0)
        if self.n == 0:
            self.n = n_b
            self.mean = mean_b
            self.M2 = var_b * n_b
            return
        n_a = int(self.n)
        mean_a = self.mean
        M2_a = self.M2
        n = n_a + n_b
        delta = mean_b - mean_a
        mean = mean_a + delta * (float(n_b) / float(n))
        M2 = M2_a + var_b * n_b + (delta * delta) * (float(n_a) * float(n_b) / float(n))
        self.n = n
        self.mean = mean
        self.M2 = M2

    def finalize(self, scale_floor: float) -> Tuple[np.ndarray, np.ndarray]:
        if self.n <= 1:
            mu = self.mean.astype(np.float32)
            sc = np.full((self.dim,), float(scale_floor), dtype=np.float32)
            return mu, sc
        var = self.M2 / float(max(1, int(self.n)))
        std = np.sqrt(np.maximum(var, 0.0))
        std = np.maximum(std, float(scale_floor))
        return self.mean.astype(np.float32), std.astype(np.float32)

class SimpleScaler:
    def __init__(self, mean: np.ndarray, scale: np.ndarray):
        self.mean_ = mean.astype(np.float32)
        self.scale_ = scale.astype(np.float32)

    def transform(self, X: np.ndarray) -> np.ndarray:
        if X.size == 0:
            return X.astype(np.float32)
        return ((X.astype(np.float32) - self.mean_) / self.scale_).astype(np.float32)

    def inverse_transform(self, X: np.ndarray) -> np.ndarray:
        if X.size == 0:
            return X.astype(np.float32)
        return (X.astype(np.float32) * self.scale_ + self.mean_).astype(np.float32)

# -----------------------------
# PATCH D) Robust y_scaler + fail-fast saturation gate
# -----------------------------
def _robust_loc_scale(X: np.ndarray, scale_floor: float) -> Tuple[np.ndarray, np.ndarray]:
    X = X.astype(np.float32, copy=False)
    med = np.nanmedian(X, axis=0).astype(np.float32)
    q25 = np.nanpercentile(X, 25, axis=0).astype(np.float32)
    q75 = np.nanpercentile(X, 75, axis=0).astype(np.float32)
    sc = (q75 - q25) / 1.349
    sc = np.where(np.isfinite(sc), sc, float(scale_floor)).astype(np.float32)
    sc = np.maximum(sc, float(scale_floor)).astype(np.float32)
    med = np.where(np.isfinite(med), med, 0.0).astype(np.float32)
    return med, sc

def fit_scalers_from_shards_v102_robust_y(
    shard_paths: List[str],
    num_dim: int,
    scale_floor_y: float,
    scale_floor_num: float,
    scale_floor_tgt: float,
    max_y_rows: int = 500_000,
) -> Tuple[SimpleScaler, SimpleScaler, SimpleScaler]:
    """
    - y_scaler: robust median/IQR computed on a subsample of hist_y rows (fast, stable).
    - num_scaler + tgt_scaler: mean/std streaming (unchanged).
    """
    # Streaming stats for num and target
    t_stat = RunningMeanVar(H)
    n_stat = RunningMeanVar(int(num_dim)) if int(num_dim) > 0 else None

    # Subsample for robust y
    y_samples = []
    y_rows = 0

    for _, z in _iter_shards_npz(shard_paths):
        hy = z["hist_y"].astype(np.float32, copy=False)
        tg = z["target"].astype(np.float32, copy=False)
        t_stat.update(tg)
        if n_stat is not None:
            _cn = z["cur_num"].astype(np.float32, copy=False)
            if not hasattr(n_stat, '_diag_done'):
                print(f"  [DIAG-SCALER] first shard cur_num.shape={_cn.shape} vs n_stat.dim={n_stat.dim}")
                n_stat._diag_done = True
            n_stat.update(_cn)

        if y_rows < int(max_y_rows) and hy.shape[0] > 0:
            take = min(int(hy.shape[0]), int(max_y_rows - y_rows))
            if take > 0:
                y_samples.append(hy[:take].astype(np.float32, copy=False))
                y_rows += int(take)

        if y_rows >= int(max_y_rows):
            break

    if y_rows <= 0:
        # This should not happen if shards exist, but keep safe.
        y_mu = np.zeros((FULL_HIST_LEN,), dtype=np.float32)
        y_sc = np.full((FULL_HIST_LEN,), float(scale_floor_y), dtype=np.float32)
    else:
        Y = np.concatenate(y_samples, axis=0).astype(np.float32, copy=False)
        y_mu, y_sc = _robust_loc_scale(Y, scale_floor=float(scale_floor_y))

    t_mu, t_sc = t_stat.finalize(scale_floor=float(scale_floor_tgt))
    if n_stat is not None:
        n_mu, n_sc = n_stat.finalize(scale_floor=float(scale_floor_num))
    else:
        n_mu = np.zeros((0,), dtype=np.float32)
        n_sc = np.ones((0,), dtype=np.float32)

    return SimpleScaler(y_mu, y_sc), SimpleScaler(n_mu, n_sc), SimpleScaler(t_mu, t_sc)

def assert_y_scaler_contract(
    y_scaler: SimpleScaler,
    shard_paths: List[str],
    z_clip: float = 20.0,
    max_check_rows: int = 200_000,
    max_sat_frac: float = 0.02,
) -> None:
    """
    Fail-fast if the standardized hist_y saturates the sampler regime again.
    This is the exact failure you saw (|hy_z|>20 on a large fraction).
    """
    checked = 0
    xs = []
    for _, z in _iter_shards_npz(shard_paths):
        hy = z["hist_y"].astype(np.float32, copy=False)
        if hy.shape[0] == 0:
            continue
        take = min(int(hy.shape[0]), int(max_check_rows - checked))
        if take <= 0:
            break
        xs.append(hy[:take])
        checked += int(take)
        if checked >= int(max_check_rows):
            break

    if checked <= 0:
        raise RuntimeError("assert_y_scaler_contract: no hist_y rows available")

    X = np.concatenate(xs, axis=0).astype(np.float32, copy=False)
    Z = y_scaler.transform(X).astype(np.float32, copy=False)

    absz = np.abs(Z)
    sat = float(np.mean(absz > float(z_clip)))
    p95 = float(np.percentile(absz, 95))
    p99 = float(np.percentile(absz, 99))
    p999 = float(np.percentile(absz, 99.9))

    print(f"[{ts()}] y_scaler_contract checked_rows={checked:,} sat_frac(|z|>{z_clip})={sat:.6f} absz_p95={p95:.3f} absz_p99={p99:.3f} absz_p99_9={p999:.3f}")

    if not np.isfinite(sat) or sat > float(max_sat_frac):
        raise RuntimeError(f"y_scaler_contract FAIL: saturation frac {sat:.6f} > {max_sat_frac}")

# -----------------------------
# 9) One-time scaled shard materialization
# -----------------------------
def write_scaled_shards_v102(
    shard_paths_raw: List[str],
    out_dir_scaled: str,
    y_scaler: SimpleScaler,
    n_scaler: SimpleScaler,
    t_scaler: SimpleScaler,
    num_dim: int,
    keep_acct: bool = False,
    use_float16: bool = False,
) -> List[str]:
    os.makedirs(out_dir_scaled, exist_ok=True)
    out_paths: List[str] = []

    for p, z in _iter_shards_npz(shard_paths_raw):
        hist_y = z["hist_y"].astype(np.float32, copy=False)
        cur_num = z["cur_num"].astype(np.float32, copy=False)
        cur_cat = z["cur_cat"].astype(np.int64, copy=False)
        region_id = z["region_id"].astype(np.int64, copy=False)
        target = z["target"].astype(np.float32, copy=False)
        mask = z["mask"].astype(np.float32, copy=False)
        y_anchor = z["y_anchor"].astype(np.float32, copy=False)
        anchor_year = z["anchor_year"].astype(np.int32, copy=False)

        hist_y_s = y_scaler.transform(hist_y)
        if int(num_dim) > 0:
            cur_num_s = n_scaler.transform(cur_num)
        else:
            cur_num_s = np.zeros((hist_y_s.shape[0], 0), dtype=np.float32)
        if not hasattr(write_scaled_shards_v102, '_diag_done'):
            print(f"  [DIAG-SCALED] cur_num={cur_num.shape} -> cur_num_s={cur_num_s.shape} n_scaler.mean_={n_scaler.mean_.shape} num_dim_param={num_dim}")
            write_scaled_shards_v102._diag_done = True
        x0_s = t_scaler.transform(target)

        if use_float16:
            hist_y_s = hist_y_s.astype(np.float16, copy=False)
            cur_num_s = cur_num_s.astype(np.float16, copy=False)
            x0_s = x0_s.astype(np.float16, copy=False)
            mask_s = mask.astype(np.float16, copy=False)
        else:
            hist_y_s = hist_y_s.astype(np.float32, copy=False)
            cur_num_s = cur_num_s.astype(np.float32, copy=False)
            x0_s = x0_s.astype(np.float32, copy=False)
            mask_s = mask.astype(np.float32, copy=False)

        cur_cat_i = cur_cat.astype(np.int32, copy=False)
        region_id_i = region_id.astype(np.int32, copy=False)

        base = os.path.basename(p)
        out_p = os.path.join(out_dir_scaled, base.replace(".npz", "_scaled.npz"))

        if keep_acct:
            acct = z["acct"].astype(object, copy=False)
            np.savez(
                out_p,
                hist_y_s=hist_y_s,
                cur_num_s=cur_num_s,
                x0_s=x0_s,
                mask=mask_s,
                cur_cat=cur_cat_i,
                region_id=region_id_i,
                anchor_year=anchor_year,
                y_anchor=y_anchor,
                acct=acct,
            )
        else:
            np.savez(
                out_p,
                hist_y_s=hist_y_s,
                cur_num_s=cur_num_s,
                x0_s=x0_s,
                mask=mask_s,
                cur_cat=cur_cat_i,
                region_id=region_id_i,
                anchor_year=anchor_year,
                y_anchor=y_anchor,
            )

        out_paths.append(out_p)

    return out_paths

# -----------------------------
# 10) Inducing-token latent structure
# -----------------------------
class TokenPersistence(nn.Module):
    """
    Learned per-token AR(1) persistence values.
    Holds K unconstrained logits; actual phi_k = sigmoid(logit_k) * 0.99
    so each token can learn its own temporal smoothness (constrained to [0, 0.99]).
    """
    def __init__(self, K: int, phi_init: float = 0.80):
        super().__init__()
        # inverse sigmoid: logit = log(phi / (0.99 - phi))
        init_val = math.log(phi_init / max(0.001, 0.99 - phi_init))
        self.phi_logits = nn.Parameter(torch.full((K,), init_val))

    def get_phi(self) -> torch.Tensor:
        """Returns [K] tensor of persistence values in (0, 0.99)."""
        return torch.sigmoid(self.phi_logits) * 0.99

    def get_phi_list(self) -> list:
        """Returns list of floats for logging."""
        with torch.no_grad():
            return (torch.sigmoid(self.phi_logits) * 0.99).cpu().tolist()

def sample_token_paths_learned(K: int, H: int, phi_vec: torch.Tensor, S: int, device: str) -> torch.Tensor:
    """
    Sample K independent AR(1) token paths per scenario with per-token phi.
    phi_vec: [K] tensor of persistence values.
    Returns: [S, K, H] tensor of shock values.

    Uses torch.stack (no inplace ops) so gradients flow cleanly back to phi_vec.
    """
    phi_vec = phi_vec.to(device)
    innovation_std = torch.sqrt(torch.clamp(1.0 - phi_vec ** 2, min=1e-6))  # [K]
    z_steps = []
    z_prev = torch.randn((S, K), device=device)
    z_steps.append(z_prev)
    for t in range(1, H):
        eta = torch.randn((S, K), device=device)
        z_prev = phi_vec.unsqueeze(0) * z_prev + innovation_std.unsqueeze(0) * eta
        z_steps.append(z_prev)
    return torch.stack(z_steps, dim=2)  # [S, K, H]

def sample_token_paths(K: int, H: int, phi, S: int, device: str) -> torch.Tensor:
    """
    Unified entry point: phi can be a float (legacy) or a Tensor/TokenPersistence.
    """
    if isinstance(phi, TokenPersistence):
        return sample_token_paths_learned(K, H, phi.get_phi(), S, device)
    elif isinstance(phi, torch.Tensor):
        return sample_token_paths_learned(K, H, phi, S, device)
    else:
        # Scalar fallback
        phi_f = float(phi)
        innovation_std = math.sqrt(max(0.0, 1.0 - phi_f ** 2))
        Z = torch.zeros((S, K, H), device=device)
        Z[:, :, 0] = torch.randn((S, K), device=device)
        for t in range(1, H):
            eta = torch.randn((S, K), device=device)
            Z[:, :, t] = phi_f * Z[:, :, t - 1] + innovation_std * eta
        return Z

# Legacy compat: sample_ar1_path still used by backtest scripts
def sample_ar1_path(n_steps: int, rank: int, phi: float, batch_size: int, device: str) -> torch.Tensor:
    Z = torch.zeros((batch_size, n_steps, rank), device=device)
    Z[:, 0, :] = torch.randn((batch_size, rank), device=device)
    innovation_std = math.sqrt(max(0.0, 1.0 - phi ** 2))
    for k in range(1, n_steps):
        eta = torch.randn((batch_size, rank), device=device)
        Z[:, k, :] = phi * Z[:, k - 1, :] + innovation_std * eta
    return Z

class GatingNetwork(nn.Module):
    """
    Learns per-parcel sparse mixing weights over K shared token paths.
    Input: hist_y [B, hist_len], cur_num [B, num_dim], cur_cat [B, n_cat], region_id [B]
    Output: alpha [B, K] — sparse softmax weights (top-k active, rest zeroed)
    """
    def __init__(self, hist_len: int, num_dim: int, n_cat: int, K: int, k: int, hidden: int = 64, use_macro: bool = True):
        super().__init__()
        self.K = int(K)
        self.k = int(k)
        self.cat_emb_dim = 16
        self.cat_embs = nn.ModuleList([nn.Embedding(HASH_BUCKET_SIZE, self.cat_emb_dim) for _ in range(max(1, int(n_cat)))])
        cat_total = self.cat_emb_dim * max(1, int(n_cat))
        self.region_emb = nn.Embedding(GEO_BUCKETS, REGION_EMB_DIM)
        
        self.use_macro = use_macro
        self.macro_emb_dim = 16 if use_macro else 0
        if use_macro:
            self.year_emb = nn.Embedding(100, self.macro_emb_dim)

        in_dim = hist_len + max(1, num_dim) + cat_total + REGION_EMB_DIM + self.macro_emb_dim
        print(f"  [DIAG] GatingNetwork in_dim={in_dim} = hist_len({hist_len}) + num_dim({max(1,num_dim)}) + cat_total({cat_total}) + region({REGION_EMB_DIM}) + macro({self.macro_emb_dim})")
        self._diag_printed = False
        self.net = nn.Sequential(
            nn.Linear(in_dim, hidden),
            nn.GELU(),
            nn.Linear(hidden, hidden),
            nn.GELU(),
            nn.Linear(hidden, K),
        )

    def forward(self, hist_y: torch.Tensor, cur_num: torch.Tensor,
                cur_cat: torch.Tensor, region_id: torch.Tensor, origin_year: Optional[torch.Tensor] = None) -> torch.Tensor:
        B = hist_y.shape[0]
        # Encode categoricals
        if cur_cat.shape[1] > 0 and len(self.cat_embs) > 0:
            cat_vecs = []
            for j, emb in enumerate(self.cat_embs):
                v = cur_cat[:, min(j, cur_cat.shape[1]-1)].clamp(0, HASH_BUCKET_SIZE - 1).long()
                cat_vecs.append(emb(v))
            cat_vec = torch.cat(cat_vecs, dim=1)
        else:
            cat_vec = torch.zeros((B, self.cat_emb_dim), device=hist_y.device, dtype=hist_y.dtype)

        # Encode region
        region_vec = self.region_emb(region_id.clamp(0, GEO_BUCKETS - 1).long())

        # Encode macro
        if self.use_macro and origin_year is not None:
             yr_idx = (origin_year - 2000).clamp(0, 99).long()
             macro_vec = self.year_emb(yr_idx)
        else:
             macro_vec = torch.zeros((B, self.macro_emb_dim), device=hist_y.device, dtype=hist_y.dtype)

        # Zero-pad cur_num if empty
        if cur_num.shape[1] == 0:
            cur_num = torch.zeros((B, 1), device=hist_y.device, dtype=hist_y.dtype)

        # Concat all inputs
        x = torch.cat([hist_y, cur_num, cat_vec, region_vec, macro_vec], dim=1)
        if not self._diag_printed:
            print(f"  [DIAG] GatingNetwork.forward: hist={hist_y.shape[1]} num={cur_num.shape[1]} cat_vec={cat_vec.shape[1]} region={region_vec.shape[1]} => x={x.shape[1]} vs net expects {self.net[0].in_features}")
            self._diag_printed = True
        logits = self.net(x)  # [B, K]

        # Top-k sparse softmax
        if self.k < self.K:
            topk_vals, topk_idx = torch.topk(logits, self.k, dim=1)
            mask = torch.full_like(logits, float('-inf'))
            mask.scatter_(1, topk_idx, topk_vals)
            alpha = torch.softmax(mask, dim=1)
        else:
            alpha = torch.softmax(logits, dim=1)

        return alpha  # [B, K]

def compute_shared_driver(alpha: torch.Tensor, Z_tokens: torch.Tensor) -> torch.Tensor:
    """
    Compute per-parcel shared driver from token paths.
    alpha: [B, K] mixing weights
    Z_tokens: [B, K, H] (batch-matched) → use einsum "bk,bkh->bh"
             [S, K, H] (scenario dim) → use einsum "nk,skh->nsh" (returns [N,S,H])
    Returns: u_i [B, H] (if batch-matched) or [N, S, H] (if scenario dim)
    """
    if Z_tokens.dim() == 3 and Z_tokens.shape[0] != alpha.shape[0]:
        # Z_tokens is [S, K, H], alpha is [N, K] → produce [N, S, H]
        return torch.einsum("nk,skh->nsh", alpha, Z_tokens)
    else:
        # Z_tokens is [B, K, H], already expanded → produce [B, H]
        return torch.einsum("bk,bkh->bh", alpha, Z_tokens)

class HeteroscedasticTokenModel(nn.Module):
    """
    Challenger Baseline: Direct heteroscedastic likelihood model (Student-t/Gaussian)
    with shared token factors.
    Out: mu (H), sigma (H), beta (H, K) per parcel.
    """
    def __init__(self, hist_len: int, num_dim: int, n_cat: int, K: int, H: int, hidden: int = 128, use_macro: bool = True):
        super().__init__()
        self.K = int(K)
        self.H = int(H)
        self.cat_emb_dim = 16
        self.cat_embs = nn.ModuleList([nn.Embedding(HASH_BUCKET_SIZE, self.cat_emb_dim) for _ in range(max(1, int(n_cat)))])
        cat_total = self.cat_emb_dim * max(1, int(n_cat))
        self.region_emb = nn.Embedding(GEO_BUCKETS, REGION_EMB_DIM)
        
        self.use_macro = use_macro
        self.macro_emb_dim = 16 if use_macro else 0
        if use_macro:
            self.year_emb = nn.Embedding(100, self.macro_emb_dim)

        in_dim = hist_len + max(1, num_dim) + cat_total + REGION_EMB_DIM + self.macro_emb_dim
        
        self.net = nn.Sequential(
            nn.Linear(in_dim, hidden),
            nn.LayerNorm(hidden),
            nn.GELU(),
            nn.Linear(hidden, hidden),
            nn.LayerNorm(hidden),
            nn.GELU(),
            nn.Linear(hidden, self.H * (2 + self.K))
        )
        
    def forward(self, hist_y: torch.Tensor, cur_num: torch.Tensor,
                cur_cat: torch.Tensor, region_id: torch.Tensor, origin_year: Optional[torch.Tensor] = None):
        B = hist_y.shape[0]
        # Encode categoricals
        if cur_cat.shape[1] > 0 and len(self.cat_embs) > 0:
            cat_vecs = []
            for j, emb in enumerate(self.cat_embs):
                v = cur_cat[:, min(j, cur_cat.shape[1]-1)].clamp(0, HASH_BUCKET_SIZE - 1).long()
                cat_vecs.append(emb(v))
            cat_vec = torch.cat(cat_vecs, dim=1)
        else:
            cat_vec = torch.zeros((B, self.cat_emb_dim), device=hist_y.device, dtype=hist_y.dtype)

        # Encode region
        region_vec = self.region_emb(region_id.clamp(0, GEO_BUCKETS - 1).long())

        # Encode macro
        if self.use_macro and origin_year is not None:
             yr_idx = (origin_year - 2000).clamp(0, 99).long()
             macro_vec = self.year_emb(yr_idx)
        else:
             macro_vec = torch.zeros((B, self.macro_emb_dim), device=hist_y.device, dtype=hist_y.dtype)

        if cur_num.shape[1] == 0:
            cur_num = torch.zeros((B, 1), device=hist_y.device, dtype=hist_y.dtype)

        x = torch.cat([hist_y, cur_num, cat_vec, region_vec, macro_vec], dim=1)
        out = self.net(x)  # [B, H * (2 + K)]
        out = out.view(B, self.H, 2 + self.K)
        
        mu = out[:, :, 0]  # [B, H]
        sigma = nn.functional.softplus(out[:, :, 1]) + 1e-4  # [B, H]
        beta = out[:, :, 2:]  # [B, H, K]
        
        return mu, sigma, beta

class CoherenceScale(nn.Module):
    """
    Learned global scale for coherence strength.
    sigma_u in (0, 2): controls how much the shared driver u_i contributes
    relative to idiosyncratic noise.
    noise = sigma_u * u_i + eps_idio
    """
    def __init__(self, init_logit: float = 0.0):
        super().__init__()
        self.logit = nn.Parameter(torch.tensor([init_logit], dtype=torch.float32))

    def forward(self) -> torch.Tensor:
        return 2.0 * torch.sigmoid(self.logit)  # scalar in (0, 2)

    def get_sigma(self) -> float:
        with torch.no_grad():
            return float(2.0 * torch.sigmoid(self.logit).item())

# -----------------------------
# 11) Denoiser with token conditioning
# -----------------------------
class SinTime(nn.Module):
    def __init__(self, dim: int):
        super().__init__()
        self.dim = int(dim)
    def forward(self, t: torch.Tensor) -> torch.Tensor:
        half = self.dim // 2
        if half <= 1:
            return torch.zeros((t.shape[0], self.dim), device=t.device, dtype=t.dtype)
        freqs = torch.exp(torch.arange(half, device=t.device, dtype=t.dtype) * (-math.log(10000.0) / float(half - 1)))
        ang = t.unsqueeze(1) * freqs.unsqueeze(0)
        return torch.cat([torch.sin(ang), torch.cos(ang)], dim=1)

class FiLMLayer(nn.Module):
    def __init__(self, cond_dim: int, channels: int):
        super().__init__()
        self.scale = nn.Linear(cond_dim, channels)
        self.shift = nn.Linear(cond_dim, channels)
    def forward(self, x: torch.Tensor, cond: torch.Tensor) -> torch.Tensor:
        return x * (1 + self.scale(cond).unsqueeze(-1)) + self.shift(cond).unsqueeze(-1)

class TokenCondEncoder(nn.Module):
    """Encodes the per-parcel shared driver u_i [B, H] into a conditioning vector."""
    def __init__(self, horizon: int, hidden: int):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(horizon, hidden),
            nn.GELU(),
            nn.Linear(hidden, hidden),
        )
    def forward(self, u_i: torch.Tensor) -> torch.Tensor:
        return self.net(u_i)

class Conv1dDenoiserV11(nn.Module):
    """
    v11 denoiser: conditioned on u_i (shared driver) instead of separate Zg/Zgeo.
    Receives per-parcel token-conditioned context via the gating network.
    """
    def __init__(self, target_dim: int, hist_len: int, num_dim: int, n_cat: int, hidden: int, n_layers: int, kernel_size: int):
        super().__init__()
        self.target_dim = int(target_dim)
        self.cat_emb_dim = 16

        self.cat_embs = nn.ModuleList([nn.Embedding(HASH_BUCKET_SIZE, self.cat_emb_dim) for _ in range(int(n_cat))])
        cat_dim = self.cat_emb_dim * int(n_cat)

        self.region_emb = nn.Embedding(GEO_BUCKETS, REGION_EMB_DIM)
        self.region_enc = nn.Sequential(nn.Linear(REGION_EMB_DIM, hidden), nn.GELU(), nn.Linear(hidden, hidden))

        # v11: token conditioning replaces macro_enc + geo_enc
        self.token_cond_enc = TokenCondEncoder(self.target_dim, hidden)

        self.hist_enc = nn.Sequential(nn.Linear(int(hist_len), hidden), nn.GELU(), nn.Linear(hidden, hidden))
        self.num_enc = nn.Sequential(nn.Linear(max(1, int(num_dim)), hidden), nn.GELU(), nn.Linear(hidden, hidden))
        self.cat_enc = nn.Sequential(nn.Linear(max(1, int(cat_dim)), hidden), nn.GELU(), nn.Linear(hidden, hidden))

        self.t_dim = 128
        self.t_emb = SinTime(self.t_dim)
        self.t_enc = nn.Sequential(nn.Linear(self.t_dim, hidden), nn.GELU(), nn.Linear(hidden, hidden))

        self.input_proj = nn.Conv1d(1, hidden, kernel_size=1)
        self.conv_blocks = nn.ModuleList()
        self.film_layers = nn.ModuleList()
        for _ in range(int(n_layers)):
            self.conv_blocks.append(nn.Sequential(
                nn.Conv1d(hidden, hidden, kernel_size, padding=kernel_size // 2),
                nn.GELU(),
                nn.Conv1d(hidden, hidden, kernel_size, padding=kernel_size // 2),
            ))
            self.film_layers.append(FiLMLayer(hidden, hidden))
        self.output_proj = nn.Conv1d(hidden, 1, kernel_size=1)

        self._y_scaler = None
        self._n_scaler = None
        self._t_scaler = None

    def forward(self, x_t, t, hist_y, cur_num, cur_cat, region_id, u_i):
        """
        x_t: [B, H] noised target
        t: [B] diffusion timestep
        hist_y: [B, hist_len] scaled history
        cur_num: [B, num_dim] scaled numerics
        cur_cat: [B, n_cat] categorical indices
        region_id: [B] region bucket ids
        u_i: [B, H] per-parcel shared driver from gating network
        """
        h_hist = self.hist_enc(hist_y)
        if cur_num.shape[1] > 0:
            h_num = self.num_enc(cur_num)
        else:
            h_num = self.num_enc(torch.zeros((x_t.shape[0], 1), device=x_t.device, dtype=x_t.dtype))
        h_t = self.t_enc(self.t_emb(t))

        if cur_cat.shape[1] > 0 and len(self.cat_embs) > 0:
            cat_vecs = []
            for j, emb in enumerate(self.cat_embs):
                v = cur_cat[:, j].clamp(0, HASH_BUCKET_SIZE - 1).long()
                cat_vecs.append(emb(v))
            cat_vec = torch.cat(cat_vecs, dim=1)
        else:
            cat_vec = torch.zeros((x_t.shape[0], 1), device=x_t.device, dtype=x_t.dtype)

        h_cat = self.cat_enc(cat_vec)

        region_vec = self.region_emb(region_id.clamp(0, GEO_BUCKETS - 1).long())
        h_region = self.region_enc(region_vec)

        # v11: single token conditioning replaces macro + geo
        h_token = self.token_cond_enc(u_i)

        h_cond = h_hist + h_num + h_cat + h_region + h_token + h_t

        x = self.input_proj(x_t.unsqueeze(1))
        for conv, film in zip(self.conv_blocks, self.film_layers):
            x = film(conv(x) + x, h_cond)
        return self.output_proj(x).squeeze(1)

def create_denoiser_v11(target_dim: int, hist_len: int, num_dim: int, n_cat: int) -> nn.Module:
    return Conv1dDenoiserV11(target_dim, hist_len, num_dim, n_cat, DENOISER_HIDDEN, DENOISER_LAYERS, CONV_KERNEL_SIZE)

def create_gating_network(hist_len: int, num_dim: int, n_cat: int, use_macro: bool = True) -> nn.Module:
    return GatingNetwork(
        hist_len=int(hist_len),
        num_dim=int(num_dim),
        n_cat=int(n_cat),
        K=int(K_TOKENS),
        k=int(K_ACTIVE),
        use_macro=bool(use_macro),
        hidden=GATING_HIDDEN
    )

def create_token_persistence() -> TokenPersistence:
    return TokenPersistence(K=K_TOKENS, phi_init=PHI_INIT)

def create_coherence_scale() -> CoherenceScale:
    return CoherenceScale(init_logit=0.0)  # sigma_u ≈ 1.0 at init

# Legacy compat aliases (keep imports working in backtest scripts)
GlobalProjection = None
GeoProjection = None

# -----------------------------
# 12) Diffusion scheduler
# -----------------------------
class Scheduler:
    def __init__(self, steps: int, device: str):
        betas = torch.linspace(1e-4, 0.02, int(steps), device=device)
        alphas = 1.0 - betas
        self.abar = torch.cumprod(alphas, dim=0)
        self.sqrt_abar = torch.sqrt(self.abar)
        self.sqrt_om = torch.sqrt(1.0 - self.abar)
        self.steps = int(steps)

    def q(self, x0, t_idx, noise):
        return self.sqrt_abar[t_idx].view(-1, 1) * x0 + self.sqrt_om[t_idx].view(-1, 1) * noise

# -----------------------------
# 13) Autocast helpers
# -----------------------------
def get_autocast_ctx(device: str):
    if USE_BF16 and device == "cuda" and torch.cuda.is_available():
        return torch.autocast(device_type="cuda", dtype=torch.bfloat16)
    return contextlib.nullcontext()

# -----------------------------
# 14) Shard-based diffusion training loop (inducing-token v11)
# -----------------------------
def train_diffusion_v11(
    shard_paths: List[str],
    origin: int,
    epochs: int,
    model: nn.Module,
    gating_net: nn.Module,
    token_persistence: TokenPersistence,
    coh_scale: CoherenceScale,
    device: str,
    num_dim: int,
    n_cat: int,
    work_dirs: Dict[str, str],
) -> Tuple[SimpleScaler, SimpleScaler, SimpleScaler, List[float], List[str]]:
    if not shard_paths:
        raise ValueError(f"No shards for origin {origin}")

    phi_init = token_persistence.get_phi_list()
    sigma_init = coh_scale.get_sigma()
    print(f"[{ts()}] train_diffusion_v11 origin={origin} shards={len(shard_paths)} epochs={epochs} K={K_TOKENS} k={K_ACTIVE}")
    print(f"[{ts()}] initial phi_k = {[f'{p:.3f}' for p in phi_init]}  sigma_u={sigma_init:.3f}")

    # v11.2: relaxed floor — 0.25 was too aggressive and crushed parcel-level
    # conditioning signal (ρ ≈ 0).  assert_y_scaler_contract + SAMPLER_Z_CLIP
    # provide sufficient safeguards against tiny scales.
    y_floor = max(float(SCALE_FLOOR_Y), 0.10)

    y_scaler, n_scaler, t_scaler = fit_scalers_from_shards_v102_robust_y(
        shard_paths=shard_paths,
        num_dim=int(num_dim),
        scale_floor_y=float(y_floor),
        scale_floor_num=float(SCALE_FLOOR_NUM),
        scale_floor_tgt=float(SCALE_FLOOR_TGT),
        max_y_rows=500_000,
    )

    # Fail-fast if we are back in the saturation regime.
    assert_y_scaler_contract(
        y_scaler=y_scaler,
        shard_paths=shard_paths,
        z_clip=float(SAMPLER_Z_CLIP) if SAMPLER_Z_CLIP is not None else 20.0,
        max_check_rows=200_000,
        max_sat_frac=0.02,
    )

    model._y_scaler = y_scaler
    model._n_scaler = n_scaler
    model._t_scaler = t_scaler

    scaled_dir = os.path.join(work_dirs["SCALED_SHARD_ROOT"], f"origin_{int(origin)}")
    scaled_paths = write_scaled_shards_v102(
        shard_paths_raw=shard_paths,
        out_dir_scaled=scaled_dir,
        y_scaler=y_scaler,
        n_scaler=n_scaler,
        t_scaler=t_scaler,
        num_dim=int(num_dim),
        keep_acct=False,
        use_float16=bool(SCALED_SHARDS_FLOAT16),
    )
    print(f"[{ts()}] Scaled shards ready: {len(scaled_paths)} dir={scaled_dir} float16={SCALED_SHARDS_FLOAT16}")
    sys.stdout.flush()

    print(f"[{ts()}] Creating scheduler..."); sys.stdout.flush()
    sched = Scheduler(DIFF_STEPS_TRAIN, device=device)
    # Separate param groups: 10x LR for phi_logits so token persistence can learn
    param_groups = [
        {"params": list(model.parameters()), "lr": DIFF_LR},
        {"params": list(gating_net.parameters()), "lr": DIFF_LR},
        {"params": list(token_persistence.parameters()), "lr": DIFF_LR * 10},
        {"params": list(coh_scale.parameters()), "lr": DIFF_LR * 5},
    ]
    n_params = sum(len(pg["params"]) for pg in param_groups)
    print(f"[{ts()}] Creating optimizer ({n_params} params, 4 groups, phi_lr={DIFF_LR*10:.1e})..."); sys.stdout.flush()
    try:
        opt = torch.optim.AdamW(param_groups, weight_decay=1e-4, fused=True)
    except TypeError:
        opt = torch.optim.AdamW(param_groups, weight_decay=1e-4)

    lr_sched = torch.optim.lr_scheduler.CosineAnnealingLR(opt, T_max=int(epochs), eta_min=DIFF_LR * 0.1)

    model.train()
    gating_net.train()
    token_persistence.train()
    coh_scale.train()
    print(f"[{ts()}] Models set to train mode"); sys.stdout.flush()

    autocast_ctx = get_autocast_ctx(device)
    losses: List[float] = []

    # No pre-sampled token bank — sample fresh per-batch so phi gradients flow
    print(f"[{ts()}] Per-batch token sampling enabled (differentiable phi_k)"); sys.stdout.flush()

    # Diagnostic accumulators (per-epoch)
    _diag_alpha_sum = None
    _diag_alpha_sq_sum = None  # for effective_k
    _diag_alpha_entropy_sum = 0.0
    _diag_alpha_count = 0

    scaled_paths_local = list(scaled_paths)

    _total_batches = 0
    print(f"[{ts()}] Starting training loop: {int(epochs)} epochs"); sys.stdout.flush()

    for ep in range(int(epochs)):
        t_ep0 = time.time()
        rng.shuffle(scaled_paths_local)
        ep_losses: List[float] = []
        _diag_alpha_sum = torch.zeros(K_TOKENS, device=device)
        _diag_alpha_sq_sum = torch.zeros(K_TOKENS, device=device)
        _diag_alpha_entropy_sum = 0.0
        _diag_alpha_count = 0

        # (token paths sampled fresh per-batch — no bank refresh needed)

        for _shard_idx, (_, z) in enumerate(_iter_shards_npz(scaled_paths_local)):
            hist_y_s = z["hist_y_s"]
            cur_num_s = z["cur_num_s"]
            x0_s = z["x0_s"]
            mask = z["mask"]
            cur_cat = z["cur_cat"].astype(np.int64, copy=False)
            region_id = z["region_id"].astype(np.int64, copy=False)

            n = int(x0_s.shape[0])
            if n == 0:
                continue

            perm = rng.permutation(n)

            for start in range(0, n, int(DIFF_BATCH)):
                b = perm[start:start + int(DIFF_BATCH)]
                B = int(b.size)
                if B == 0:
                    continue

                hy = torch.from_numpy(hist_y_s[b]).to(device, non_blocking=True)
                if int(num_dim) > 0:
                    xn = torch.from_numpy(cur_num_s[b]).to(device, non_blocking=True)
                else:
                    xn = torch.zeros((B, 0), device=device, dtype=torch.float32)
                if _total_batches == 0:
                    print(f"  [DIAG-TRAIN] batch0: hist_y_s={hist_y_s.shape} cur_num_s={cur_num_s.shape} hy={hy.shape} xn={xn.shape} num_dim_param={num_dim}")

                x0 = torch.from_numpy(x0_s[b]).to(device, non_blocking=True).float()
                m = torch.from_numpy(mask[b]).to(device, non_blocking=True).float()

                xc = torch.from_numpy(cur_cat[b]).to(device, non_blocking=True)
                rid = torch.from_numpy(region_id[b]).to(device, non_blocking=True)

                # Sample fresh token paths PER BATCH — phi gradients flow through
                # the AR(1) recurrence via reparameterization trick.
                # torch.randn terms are constants w.r.t. phi, so autograd works.
                Z_k = sample_token_paths_learned(
                    K_TOKENS, H, token_persistence.get_phi(), 1, device
                )  # [1, K, H]
                Z_k = Z_k.expand(B, -1, -1).clone()  # [B, K, H] — clone() avoids inplace grad error

                eps_idio = torch.randn_like(x0)  # [B, H] idiosyncratic noise

                try:
                    with autocast_ctx:
                        # Compute per-parcel mixing weights
                        oy = torch.full((B,), int(origin), dtype=torch.long, device=device)
                        alpha = gating_net(hy.float(), xn.float(), xc, rid, origin_year=oy)  # [B, K]

                        # Accumulate diagnostics (detached, no grad impact)
                        with torch.no_grad():
                            _diag_alpha_sum += alpha.sum(dim=0)
                            _diag_alpha_sq_sum += (alpha ** 2).sum(dim=0)
                            # Entropy: -Σ α log(α+ε)
                            _diag_alpha_entropy_sum += float(-(alpha * torch.log(alpha + 1e-8)).sum().item())
                            _diag_alpha_count += B

                        # Compute shared driver with learned coherence scale
                        sigma_u = coh_scale()  # scalar in (0, 2)
                        u_i = compute_shared_driver(alpha, Z_k)  # [B, H]
                        u_i_scaled = sigma_u * u_i

                        # Structured noise: ε̃ = sigma_u * u_i * h_scale + ε_idio
                        # v11.2: apply sqrt(1..H) horizon scaling to match the sampler
                        # (sample_ddim_v11 line 2084).  Without this, the model trains
                        # on flat noise but samples with horizon-scaled noise.
                        h_scale = torch.sqrt(torch.arange(1, H + 1, device=device, dtype=torch.float32)).unsqueeze(0)  # [1, H]
                        noise = u_i_scaled * h_scale + eps_idio

                        t_idx = torch.randint(0, int(DIFF_STEPS_TRAIN), (B,), device=device)
                        xt = sched.q(x0, t_idx, noise)
                        noise_hat = model(xt, t_idx.float(), hy.float(), xn.float(), xc, rid, u_i_scaled)

                        # v11.2: min-SNR weighted MSE (Hang et al. 2023)
                        # Flat MSE lets low-noise timesteps dominate → model learns
                        # conditional mean but not distribution shape.  min-SNR(γ=5)
                        # caps per-timestep weight so high-noise steps (where the
                        # model must learn the distribution) contribute meaningfully.
                        # This is critical for stochastic sampling (η>0) to produce
                        # calibrated prediction intervals.
                        _snr = sched.abar[t_idx] / (1.0 - sched.abar[t_idx]).clamp(min=1e-8)  # [B]
                        _min_snr_gamma = 5.0
                        _snr_weight = torch.minimum(_snr, torch.full_like(_snr, _min_snr_gamma)) / _snr.clamp(min=1e-8)  # [B]
                        _snr_weight = _snr_weight.unsqueeze(1)  # [B, 1] for broadcasting

                        # Per-horizon loss normalization with SNR weighting:
                        _per_h_sq = (noise_hat - noise) ** 2 * m * _snr_weight  # [B, H]
                        _per_h_count = m.sum(dim=0).clamp(min=1.0)  # [H]
                        _per_h_mse = _per_h_sq.sum(dim=0) / _per_h_count  # [H]
                        _n_active_h = (m.sum(dim=0) > 0).sum().clamp(min=1)
                        loss = _per_h_mse.sum() / _n_active_h

                    opt.zero_grad(set_to_none=True)
                    loss.backward()
                    all_params = [p for pg in param_groups for p in pg["params"]]
                    torch.nn.utils.clip_grad_norm_(all_params, 1.0)
                    opt.step()

                    ep_losses.append(float(loss.item()))
                    _total_batches += 1

                    # Progress: print every 10 batches within an epoch
                    if _total_batches <= 3 or _total_batches % 10 == 0:
                        print(f"  batch {_total_batches} ep={ep} loss={float(loss.item()):.5f} B={B}", flush=True)

                except Exception as _batch_err:
                    print(f"\n❌ TRAINING ERROR at ep={ep} batch={_total_batches} B={B}: {type(_batch_err).__name__}: {_batch_err}", flush=True)
                    import traceback; traceback.print_exc(); sys.stdout.flush()
                    raise

        lr_sched.step()
        mean_loss = float(np.mean(ep_losses)) if ep_losses else float("nan")
        losses.append(mean_loss)

        # v11.1: Early stopping — if loss hasn't improved for PATIENCE epochs, stop.
        # Prevents bad checkpoints (e.g. HCAD o2022 with 3% coverage from overtraining).
        EARLY_STOP_PATIENCE = 5
        if len(losses) > EARLY_STOP_PATIENCE:
            recent_best = min(losses[-EARLY_STOP_PATIENCE:])
            overall_best = min(losses[:-EARLY_STOP_PATIENCE])
            if recent_best > overall_best * 1.001:  # no improvement (within 0.1%)
                print(f"[{ts()}] ⚠️ Early stopping at epoch {ep+1}: loss {mean_loss:.6f} hasn't improved for {EARLY_STOP_PATIENCE} epochs (best={min(losses):.6f})")
                break

        dt_ep = time.time() - t_ep0

        # Compute diagnostics
        phi_vals = token_persistence.get_phi_list()
        sigma_u_val = coh_scale.get_sigma()
        alpha_mean = (_diag_alpha_sum / max(1, _diag_alpha_count)).cpu().tolist() if _diag_alpha_count > 0 else [0.0] * K_TOKENS
        alpha_sq_mean = (_diag_alpha_sq_sum / max(1, _diag_alpha_count)).cpu().tolist() if _diag_alpha_count > 0 else [1.0] * K_TOKENS
        # Effective k: 1 / Σ_k E[α_k²]  (averaged across parcels)
        eff_k = 1.0 / max(sum(alpha_sq_mean), 1e-8)
        # Mean entropy per parcel
        mean_entropy = _diag_alpha_entropy_sum / max(1, _diag_alpha_count)

        if (ep == 0) or ((ep + 1) % max(1, int(epochs) // 5) == 0):
            print(f"[{ts()}] origin={origin} ep={ep+1}/{epochs} loss={mean_loss:.6f} time={dt_ep:.1f}s")
            print(f"[{ts()}]   phi_k = {[f'{p:.3f}' for p in phi_vals]}  sigma_u={sigma_u_val:.3f}")
            print(f"[{ts()}]   alpha_mean = {[f'{a:.3f}' for a in alpha_mean]}  eff_k={eff_k:.2f}  entropy={mean_entropy:.3f}")
            sys.stdout.flush()
        else:
            # Short progress line every epoch with ETA
            _remaining_ep = int(epochs) - (ep + 1)
            _eta_s = _remaining_ep * dt_ep
            _eta_min = _eta_s / 60.0
            print(f"  ep {ep+1}/{epochs} loss={mean_loss:.6f} {dt_ep:.0f}s/ep  ETA={_eta_min:.1f}min", flush=True)

        # W&B per-epoch logging with full diagnostics
        log_data = {
            f"train/loss_origin_{origin}": mean_loss,
            "train/loss": mean_loss,
            "train/epoch": ep + 1,
            "train/origin": origin,
            "train/lr": float(lr_sched.get_last_lr()[0]),
            "tokens/sigma_u": sigma_u_val,
            "tokens/effective_k": eff_k,
            "tokens/alpha_entropy": mean_entropy,
        }
        # Per-token phi values
        for k_idx, phi_k in enumerate(phi_vals):
            log_data[f"tokens/phi_{k_idx}"] = phi_k
        # Per-token mean alpha (usage) and load
        for k_idx, a_k in enumerate(alpha_mean):
            log_data[f"tokens/alpha_mean_{k_idx}"] = a_k
        wb_log(log_data)

    # Print final learned values
    print(f"[{ts()}] FINAL phi_k = {[f'{p:.4f}' for p in token_persistence.get_phi_list()]}")
    print(f"[{ts()}] FINAL sigma_u = {coh_scale.get_sigma():.4f}")

    return y_scaler, n_scaler, t_scaler, losses, scaled_paths

# -----------------------------
# 15) Chunked inference context builder (no future labels)
# -----------------------------
# -----------------------------
# PATCH C) Inference context builder: safe hist lag fill (no zeros, no underflow distortion)
# -----------------------------
def build_inference_context_chunked_v102(
    lf: pl.LazyFrame,
    accts: List[str],
    num_use_local: List[str],
    cat_use_local: List[str],
    global_medians: Dict[str, float],
    anchor_year: int,
    acct_chunk_size: int = ACCT_CHUNK_SIZE_INFER,
    max_parcels: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    if global_medians is None:
        global_medians = {}

    print(f"[{ts()}] Building inference context anchor_year={anchor_year} max_parcels={max_parcels}")

    region_expr = build_region_id_expr()
    cat_hash_exprs = build_cat_hash_exprs(cat_use_local)
    hist_exprs = [pl.col("y_log").shift(i).over("acct").alias(f"lag_{i}") for i in range(FULL_HIST_LEN)]

    hist_buf: List[np.ndarray] = []
    num_buf: List[np.ndarray] = []
    cat_buf: List[np.ndarray] = []
    rid_buf: List[np.ndarray] = []
    yanc_buf: List[np.ndarray] = []
    acct_buf: List[np.ndarray] = []
    total = 0

    for s in range(0, len(accts), int(acct_chunk_size)):
        acct_chunk = accts[s:s + int(acct_chunk_size)]
        if not acct_chunk:
            continue

        base_q = (
            lf
            .filter(pl.col("acct").cast(pl.Utf8).is_in(acct_chunk))
            .filter(pl.col("yr").is_between(MIN_YEAR, int(anchor_year)))
            .filter(pl.col("tot_appr_val") > 0)
            .with_columns([
                pl.col("acct").cast(pl.Utf8).alias("acct"),
                pl.col("yr").cast(pl.Int32).alias("yr"),
                pl.col("tot_appr_val").log1p().alias("y_log"),
            ])
            .sort(["acct", "yr"])
            .with_columns(hist_exprs + [region_expr] + cat_hash_exprs)
        )

        df = base_q.filter(pl.col("yr") == int(anchor_year)).collect()
        if len(df) == 0:
            continue

        hist_cols = [f"lag_{i}" for i in range(FULL_HIST_LEN - 1, -1, -1)]
        hist_mat = np.column_stack([df[c].to_numpy().astype(np.float32) for c in hist_cols]).astype(np.float32)
        hist_mat = fill_hist_lags_no_zeros(hist_mat, fallback_value=float(Y_FALLBACK_LOG1P))
        hist_y = hist_mat.astype(np.float32)

        y_anchor = df["y_log"].to_numpy().astype(np.float32)
        region_id = df["region_id"].to_numpy().astype(np.int64)
        acct_arr = df["acct"].to_numpy().astype(object)

        cur_num_list = []
        for c in num_use_local:
            if c in df.columns:
                vals = df[c].to_numpy().astype(np.float32)
                med = float(global_medians.get(c, 0.0))
                cur_num_list.append(np.nan_to_num(vals, nan=med).astype(np.float32))
            else:
                cur_num_list.append(np.full(len(df), float(global_medians.get(c, 0.0)), dtype=np.float32))
        cur_num = np.column_stack(cur_num_list).astype(np.float32) if cur_num_list else np.zeros((len(df), 0), np.float32)

        cur_cat_list = []
        for c in cat_use_local:
            hc = f"cat_{c}"
            if hc in df.columns:
                cur_cat_list.append(df[hc].to_numpy().astype(np.int64))
            else:
                cur_cat_list.append(np.zeros(len(df), dtype=np.int64))
        cur_cat = np.column_stack(cur_cat_list).astype(np.int64) if cur_cat_list else np.zeros((len(df), 0), np.int64)

        if max_parcels is not None:
            need = int(max_parcels) - int(total)
            if need <= 0:
                break
            take = min(int(len(df)), int(need))
            hist_y = hist_y[:take]
            cur_num = cur_num[:take]
            cur_cat = cur_cat[:take]
            region_id = region_id[:take]
            y_anchor = y_anchor[:take]
            acct_arr = acct_arr[:take]

        hist_buf.append(hist_y)
        num_buf.append(cur_num)
        cat_buf.append(cur_cat)
        rid_buf.append(region_id)
        yanc_buf.append(y_anchor)
        acct_buf.append(acct_arr)
        total += int(hist_y.shape[0])

        if (max_parcels is not None) and (int(total) >= int(max_parcels)):
            break

    if total == 0:
        print(f"[{ts()}] No inference anchors found at anchor_year={anchor_year}")
        return None

    hist_y_out = np.concatenate(hist_buf, axis=0)
    cur_num_out = np.concatenate(num_buf, axis=0) if num_buf else np.zeros((total, 0), np.float32)
    cur_cat_out = np.concatenate(cat_buf, axis=0) if cat_buf else np.zeros((total, 0), np.int64)
    rid_out = np.concatenate(rid_buf, axis=0)
    y_anchor_out = np.concatenate(yanc_buf, axis=0)
    acct_out = np.concatenate(acct_buf, axis=0).astype(object)

    print(f"[{ts()}] Inference context built n={total:,}")
    return {
        "hist_y": hist_y_out.astype(np.float32),
        "cur_num": cur_num_out.astype(np.float32),
        "cur_cat": cur_cat_out.astype(np.int64),
        "region_id": rid_out.astype(np.int64),
        "y_anchor": y_anchor_out.astype(np.float32),
        "acct": acct_out.astype(object),
        "anchor_year": int(anchor_year),
        "n_parcels": int(total),
    }

# -----------------------------
# 16) Stable coherent DDIM sampler (float32 + guards + clipping)
# -----------------------------
@torch.no_grad()
def sample_ddim_v11(
    model: nn.Module,
    gating_net: nn.Module,
    sched: Scheduler,
    hist_y_b: np.ndarray,
    cur_num_b: np.ndarray,
    cur_cat_b: np.ndarray,
    region_id_b: np.ndarray,
    Z_tokens: torch.Tensor,    # [S, K, H] pre-sampled token paths
    device: str,
    anchor_year: int = 2021,   # explicitly pass anchor year for macro embedding
    coh_scale: CoherenceScale = None,  # optional: if None, sigma_u=1.0
    tau: float = 1.0,           # optional: temperature scaling for idiosyncratic noise
) -> np.ndarray:
    """
    v11 DDIM sampler with inducing-token coherence and S-block chunking.
    Processes scenarios in blocks of S_BLOCK to keep peak VRAM proportional
    to N * S_BLOCK, not N * S.
    """
    model.eval()
    gating_net.eval()

    N = int(hist_y_b.shape[0])
    S = int(Z_tokens.shape[0])
    K = int(Z_tokens.shape[1])
    if N == 0:
        return np.zeros((0, S, H), dtype=np.float32)

    # Coherence scale
    sigma_u = 1.0
    if coh_scale is not None:
        sigma_u = coh_scale.get_sigma()

    # Conditioning scaling
    hy_np = model._y_scaler.transform(hist_y_b).astype(np.float32)
    if SAMPLER_Z_CLIP is not None:
        hy_np = np.clip(hy_np, -float(SAMPLER_Z_CLIP), float(SAMPLER_Z_CLIP)).astype(np.float32)

    if cur_num_b.shape[1] > 0:
        xn_np = model._n_scaler.transform(cur_num_b).astype(np.float32)
        if SAMPLER_Z_CLIP is not None:
            xn_np = np.clip(xn_np, -float(SAMPLER_Z_CLIP), float(SAMPLER_Z_CLIP)).astype(np.float32)
    else:
        xn_np = np.zeros((N, 0), dtype=np.float32)

    hy_absmax = float(np.max(np.abs(hy_np))) if hy_np.size > 0 else 0.0
    xn_absmax = float(np.max(np.abs(xn_np))) if xn_np.size > 0 else 0.0
    sb = int(S_BLOCK)
    print(f"[{ts()}] SAMPLER v11 conditioning absmax hy={hy_absmax:.3f} xn={xn_absmax:.3f} "
          f"N={N} S={S} K={K} S_BLOCK={sb} sigma_u={sigma_u:.3f} tau={tau:.3f}")

    # Move conditioning to device (shared across all S-blocks)
    hy = torch.from_numpy(hy_np).pin_memory().to(device=device, dtype=torch.float32, non_blocking=True)
    xn = torch.from_numpy(xn_np).pin_memory().to(device=device, dtype=torch.float32, non_blocking=True) if xn_np.shape[1] > 0 else torch.zeros((N, 0), device=device, dtype=torch.float32)
    xc = torch.from_numpy(cur_cat_b.astype(np.int64)).pin_memory().to(device=device, non_blocking=True)
    rid = torch.from_numpy(region_id_b.astype(np.int64)).pin_memory().to(device=device, non_blocking=True)

    # Compute per-parcel gating weights ONCE (shared across all scenarios)
    oy = torch.full((N,), int(anchor_year), dtype=torch.long, device=device)
    alpha = gating_net(hy.float(), xn.float(), xc, rid, origin_year=oy)  # [N, K]

    # Move Z_tokens to device
    if Z_tokens.device != torch.device(device):
        Z_tokens = Z_tokens.to(device)

    # DDIM schedule indices
    T = int(sched.steps)
    idx = np.linspace(0, T - 1, int(DIFF_STEPS_SAMPLE)).round().astype(int)
    idx = np.unique(idx)[::-1].copy()

    # Autocast context
    if SAMPLER_DISABLE_AUTOCAST:
        autocast_ctx = contextlib.nullcontext()
    else:
        autocast_ctx = get_autocast_ctx(device)

    # Output buffer (CPU, filled incrementally per S-block)
    out = np.empty((N, S, H), dtype=np.float32)

    first_bad_step = None

    # ── S-block loop: process scenarios in chunks ──
    for s0 in range(0, S, sb):
        sb_actual = min(sb, S - s0)
        Z_blk = Z_tokens[s0:s0 + sb_actual]  # [sb_actual, K, H]

        # Compute shared driver for this block via einsum (no N×S×K×H intermediate)
        # alpha: [N, K], Z_blk: [sb_actual, K, H] → u_i_blk: [N, sb_actual, H]
        u_i_blk = torch.einsum("nk,skh->nsh", alpha, Z_blk)  # [N, sb_actual, H]
        u_i_blk = sigma_u * u_i_blk
        # v11.1: horizon-scaled sigma — uncertainty grows with sqrt(horizon)
        # This prevents PI collapse at far horizons (HCAD h5: 20% → target ~70%)
        # and keeps near-horizon PIs tight. Universal across all jurisdictions.
        horizon_scale = torch.sqrt(torch.arange(1, H + 1, device=device, dtype=torch.float32)).unsqueeze(0).unsqueeze(0)  # [1, 1, H]
        u_i_blk = u_i_blk * horizon_scale  # [N, sb_actual, H]
        u_i_flat = u_i_blk.reshape(N * sb_actual, H)  # [N*sb_actual, H]

        # Expand conditioning for this block only
        hy_exp = hy.repeat_interleave(sb_actual, dim=0)
        xn_exp = xn.repeat_interleave(sb_actual, dim=0)
        xc_exp = xc.repeat_interleave(sb_actual, dim=0)
        rid_exp = rid.repeat_interleave(sb_actual, dim=0)

        # Initial x: structured noise = sigma_u * u_i + tau * idiosyncratic
        idio_noise = torch.randn((N * sb_actual, H), device=device, dtype=torch.float32)
        x = u_i_flat + idio_noise * tau
        x = x.float()

        # DDIM denoising loop for this block
        for i_step, t_idx in enumerate(idx):
            t = torch.full((N * sb_actual,), float(t_idx), device=device, dtype=torch.float32)

            with autocast_ctx:
                noise_hat = model(
                    x, t, hy_exp, xn_exp, xc_exp, rid_exp, u_i_flat,
                ).to(dtype=torch.float32)

            # Finite guards and clipping
            noise_hat = torch.nan_to_num(noise_hat, nan=0.0, posinf=0.0, neginf=0.0)
            noise_hat = noise_hat.clamp(-float(SAMPLER_NOISE_CLIP), float(SAMPLER_NOISE_CLIP))

            abar = sched.abar[int(t_idx)].to(dtype=torch.float32)
            if (i_step + 1) < len(idx):
                abar_prev = sched.abar[int(idx[i_step + 1])].to(dtype=torch.float32)
            else:
                abar_prev = torch.tensor(1.0, device=device, dtype=torch.float32)

            sqrt_abar = torch.sqrt(abar).clamp(min=1e-6)
            sqrt_om = torch.sqrt(1.0 - abar).clamp(min=1e-6)

            x0_pred = (x - sqrt_om * noise_hat) / sqrt_abar
            x0_pred = torch.nan_to_num(x0_pred, nan=0.0, posinf=0.0, neginf=0.0)
            x0_pred = x0_pred.clamp(-float(SAMPLER_X0_CLIP), float(SAMPLER_X0_CLIP))

            if (i_step + 1) < len(idx):
                # v11.2: Stochastic DDIM (Song et al. 2020 Eq. 12)
                # η=0 → deterministic DDIM, η=1 → DDPM
                _eta = float(SAMPLER_ETA)
                if _eta > 0.0:
                    # Compute σ_t = η * √((1-ā_{t-1})/(1-ā_t)) * √(1 - ā_t/ā_{t-1})
                    _sigma_sq = _eta ** 2 * ((1.0 - abar_prev) / (1.0 - abar).clamp(min=1e-8)) * (1.0 - abar / abar_prev.clamp(min=1e-8))
                    _sigma = torch.sqrt(_sigma_sq.clamp(min=0.0))
                    # Direction coefficient shrinks to maintain correct marginal
                    _dir_coeff = torch.sqrt((1.0 - abar_prev - _sigma_sq).clamp(min=0.0))
                    # Stochastic reverse step
                    _eps = torch.randn_like(x)
                    x = torch.sqrt(abar_prev).clamp(min=0.0) * x0_pred + _dir_coeff * noise_hat + _sigma * _eps
                else:
                    # Original deterministic DDIM
                    x = torch.sqrt(abar_prev).clamp(min=0.0) * x0_pred + torch.sqrt(1.0 - abar_prev).clamp(min=0.0) * noise_hat
            else:
                x = x0_pred

            x = torch.nan_to_num(x, nan=0.0, posinf=0.0, neginf=0.0)
            x = x.clamp(-float(SAMPLER_X_CLIP), float(SAMPLER_X_CLIP))

            if SAMPLER_REPORT_BAD_STEP and s0 == 0:  # only report for first block
                bad = (~torch.isfinite(x)).any(dim=1)
                bad_frac = float(bad.float().mean().item())
                if (first_bad_step is None) and (bad_frac > 0.0):
                    first_bad_step = int(i_step)
                if (i_step == 0) or (i_step == len(idx) - 1) or ((i_step % 5) == 0):
                    x_absmax = float(x.abs().max().item())
                    print(f"[{ts()}] SAMPLER step={i_step}/{len(idx)} t_idx={int(t_idx)} bad_frac={bad_frac:.4f} x_absmax={x_absmax:.3f}")

        # Write this block's results to output buffer
        x_blk = model._t_scaler.inverse_transform(x.detach().cpu().numpy().astype(np.float32))
        out[:, s0:s0 + sb_actual, :] = x_blk.reshape(N, sb_actual, H)

        # Free GPU memory for this block
        del x, u_i_blk, u_i_flat, hy_exp, xn_exp, xc_exp, rid_exp, idio_noise

    if first_bad_step is not None:
        print(f"[{ts()}] SAMPLER first_bad_step={first_bad_step}")

    return out

# -----------------------------
# 16) Challenger Model Training
# -----------------------------
def train_challenger_model_v11(
    shard_paths: List[str],
    origin: int,
    epochs: int,
    model: nn.Module,
    device: str,
    num_dim: int,
    n_cat: int,
    work_dirs: dict,
):
    """
    Train the HeteroscedasticTokenModel using Student-t Negative Log Likelihood.
    Minimizes the marginal NLL over the H-step forecast horizon.
    """
    print(f"[{ts()}] Starting train_challenger_model_v11 for origin {origin} on {device}")
    
    # ── 1) Fit local scalers identically to diffusion ──
    print(f"[{ts()}] Fitting local tabular scalers for Stage 3 Challenger...")
    
    hist_y_list = []
    cur_num_list = []
    x0_list = []
    
    for _, z in _iter_shards_npz(shard_paths):
        hist_y_s = z["hist_y_s"]
        cur_num_s = z["cur_num_s"]
        deltas = z["deltas"]
        mask = z["mask"]
        
        # Only fit scalers on unmasked data
        for i_h in range(deltas.shape[1]):
            valid = mask[:, i_h] > 0
            if valid.sum() > 0:
                hist_y_list.append(hist_y_s[valid])
                if int(num_dim) > 0:
                    cur_num_list.append(cur_num_s[valid])
                x0_list.append(deltas[valid, i_h])
                
    if not hist_y_list:
        raise ValueError("No valid training data found in shards.")

    y_mat = np.concatenate(hist_y_list, axis=0)
    y_scaler = RobustScaler(quantile_range=(1.0, 99.0)).fit(y_mat)

    n_scaler = None
    if int(num_dim) > 0:
        n_mat = np.concatenate(cur_num_list, axis=0)
        n_scaler = RobustScaler(quantile_range=(1.0, 99.0)).fit(n_mat)
    else:
        n_scaler = RobustScaler()
        n_scaler.mean_ = np.array([0.0])
        n_scaler.scale_ = np.array([1.0])

    t_mat = np.concatenate(x0_list, axis=0).reshape(-1, 1)
    t_scaler = RobustScaler(quantile_range=(1.0, 99.0)).fit(t_mat)

    # ── 2) Scale shards ──
    scaled_dir = os.path.join(work_dirs["SCALED_SHARD_ROOT"], f"challenger_origin_{int(origin)}")
    scaled_paths = write_scaled_shards_v102(
        shard_paths_raw=shard_paths,
        out_dir_scaled=scaled_dir,
        y_scaler=y_scaler,
        n_scaler=n_scaler,
        t_scaler=t_scaler,
        num_dim=int(num_dim),
        keep_acct=False,
        use_float16=bool(SCALED_SHARDS_FLOAT16),
    )

    opt = torch.optim.AdamW(model.parameters(), lr=1e-3, weight_decay=1e-4)
    lr_sched = torch.optim.lr_scheduler.CosineAnnealingLR(opt, T_max=int(epochs), eta_min=1e-4)
    model.train()
    
    losses = []
    _total_batches = 0
    rng = np.random.default_rng(42)
    scaled_paths_local = list(scaled_paths)
    
    # Degrees of freedom for Student-t
    nu = 4.0
    
    for ep in range(int(epochs)):
        rng.shuffle(scaled_paths_local)
        ep_losses = []
        
        for _shard_idx, (_, z) in enumerate(_iter_shards_npz(scaled_paths_local)):
            hist_y_s = z["hist_y_s"]
            cur_num_s = z["cur_num_s"]
            x0_s = z["x0_s"]  # [N, H]  scaled target
            mask = z["mask"]
            cur_cat = z["cur_cat"].astype(np.int64, copy=False)
            region_id = z["region_id"].astype(np.int64, copy=False)

            n = int(x0_s.shape[0])
            if n == 0: continue
            
            perm = rng.permutation(n)
            
            for start in range(0, n, int(DIFF_BATCH)):
                b = perm[start:start + int(DIFF_BATCH)]
                B = int(b.size)
                if B == 0: continue
                
                hy = torch.from_numpy(hist_y_s[b]).to(device, non_blocking=True).float()
                if int(num_dim) > 0:
                    xn = torch.from_numpy(cur_num_s[b]).to(device, non_blocking=True).float()
                else:
                    xn = torch.zeros((B, 0), device=device, dtype=torch.float32)
                
                xc = torch.from_numpy(cur_cat[b]).to(device, non_blocking=True)
                rid = torch.from_numpy(region_id[b]).to(device, non_blocking=True)
                y_true = torch.from_numpy(x0_s[b]).to(device, non_blocking=True).float()
                m = torch.from_numpy(mask[b]).to(device, non_blocking=True).float()
                
                oy = torch.full((B,), int(origin), dtype=torch.long, device=device)
                
                opt.zero_grad(set_to_none=True)
                
                mu, sigma, beta = model(hy, xn, xc, rid, origin_year=oy)
                
                # Marginal variance assuming tokens z ~ N(0, 1) mutually independent
                # V_h = sum_k beta_{kh}^2 + sigma_h^2
                V = (beta**2).sum(dim=2) + sigma**2  # [B, H]
                S = torch.sqrt(V)  # scale parameter [B, H]
                
                # Student-t NLL: -log(S) - (nu + 1)/2 * log(1 + (y - mu)^2 / (nu * S^2))
                # Negative of the likelihood:
                nll = torch.log(S) + ((nu + 1) / 2.0) * torch.log(1.0 + ((y_true - mu)**2) / (nu * S**2))
                
                # Apply mask and average
                loss = (nll * m).sum() / m.sum().clamp(min=1.0)
                
                loss.backward()
                torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
                opt.step()
                
                ep_losses.append(loss.item())
                _total_batches += 1
                
        lr_sched.step()
        ep_mean = float(np.mean(ep_losses)) if ep_losses else 0.0
        losses.append(ep_mean)
        print(f"[{ts()}] Challenger Epoch {ep:02d} | Loss: {ep_mean:.4f} | LR: {lr_sched.get_last_lr()[0]:.1e}")
        
    return y_scaler, n_scaler, t_scaler, losses, scaled_paths

# -----------------------------
# 17) Acceptance tests
# -----------------------------
def acceptance_test_increment_consistency(deltas: np.ndarray, y_anchor: np.ndarray) -> Dict[str, Any]:
    cumsum_d = np.cumsum(deltas, axis=2)
    y_levels = y_anchor[:, None, None] + cumsum_d
    max_err = 0.0
    for k in range(1, deltas.shape[2]):
        diff = y_levels[:, :, k] - y_levels[:, :, k - 1]
        err = float(np.max(np.abs(diff - deltas[:, :, k])))
        if err > max_err:
            max_err = err
    return {"max_reconstruction_error": float(max_err), "status": "PASS" if max_err < 1e-5 else "FAIL"}

def acceptance_test_finite(deltas: np.ndarray) -> Dict[str, Any]:
    finite = np.isfinite(deltas)
    good_rows = int(np.all(finite, axis=(1, 2)).sum())
    n = int(deltas.shape[0])
    return {"n": n, "finite_rows": good_rows, "finite_frac": float(good_rows / max(1, n)), "status": "PASS" if good_rows == n else "FAIL"}

