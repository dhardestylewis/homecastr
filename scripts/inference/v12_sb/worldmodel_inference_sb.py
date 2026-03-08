# =============================================================================
# World Model v12_sb — Inference-only code
# Conditional Residual Path-Space Schrödinger Bridge
# =============================================================================
# This file contains all model class definitions needed for torch.load()
# compatibility, plus the new Euler-Maruyama bridge sampler.
# NO training loop — training code is in worldmodel_sb.py.

import os, sys, time, math, json, warnings, hashlib, subprocess, contextlib, inspect, shutil
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any

warnings.filterwarnings("ignore")

def ts():
    return time.strftime("%H:%M:%S")

def _pip_install(pkgs: List[str]):
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q"] + pkgs)

def ensure_import(mod: str, pip_name: Optional[str]=None):
    try: __import__(mod)
    except ImportError: _pip_install([pip_name or mod])

try:
    import wandb
except ImportError:
    wandb = None

import numpy as np
import polars as pl
import torch
import torch.nn as nn
import torch.nn.functional as F
torch.backends.cudnn.benchmark = True
_WB_RUN = None

# =============================================================================
# W&B helpers (inference-safe: silently skip if no run)
# =============================================================================
def init_wandb(project: str='homecastr', entity: str='dhardestylewis-columbia-university',
               name: Optional[str]=None, tags: Optional[List[str]]=None,
               extra_config: Optional[Dict[str, Any]]=None, mode: str='online'):
    global _WB_RUN
    if wandb is None: return
    _tags = list(tags or []) + ["v12_sb"]
    _cfg = dict(extra_config or {})
    _WB_RUN = wandb.init(project=project, entity=entity, name=name, tags=_tags,
                         config=_cfg, mode=mode, reinit=True)

def wb_log(data: Dict[str, Any], **kw):
    if _WB_RUN is not None: _WB_RUN.log(data, **kw)

def wb_config_update(data: Dict[str, Any]):
    if _WB_RUN is not None: _WB_RUN.config.update(data)

def wb_log_artifact(path: str, name: str, artifact_type: str='model'):
    if _WB_RUN is None: return
    art = wandb.Artifact(name, type=artifact_type)
    art.add_file(path)
    _WB_RUN.log_artifact(art)

# =============================================================================
# Config — v12_sb
# =============================================================================
PANEL_PATH_DRIVE = '/content/drive/MyDrive/HCAD_Archive_Aggregates/hcad_master_panel_2005_2025_leakage_strict_FIXEDYR_WITHGIS.parquet'
PANEL_PATH_LOCAL = '/content/local_panel.parquet'
SEED = 42
MIN_YEAR = 2005
MAX_YEAR = 2025
SEAM_YEAR = 2025
H = 5
FULL_PANEL_MODE = True
FULL_HORIZON_ONLY = True
OUT_DIR = globals().get("OUT_DIR", "/content/drive/MyDrive/worldmodel_out_v12sb")
FULL_HIST_LEN = globals().get("FULL_HIST_LEN", 10)

HASH_BUCKET_SIZE = 32768
GEO_BUCKETS = 4096
GEO_CELL_DEG = 0.01
REGION_EMB_DIM = 32

# v11 inducing-token config (kept for coherence)
K_TOKENS = 8
K_ACTIVE = 4
GATING_HIDDEN = 64
PHI_INIT = 0.80

# AB1 config
AB1_ENABLED = bool(int(os.environ.get("AB1_ENABLED", "1")))
FT_D_MODEL = int(os.environ.get("FT_D_MODEL", "128"))
FT_N_HEADS = int(os.environ.get("FT_N_HEADS", "4"))
FT_N_LAYERS = int(os.environ.get("FT_N_LAYERS", "3"))
FT_DROPOUT = float(os.environ.get("FT_DROPOUT", "0.1"))
LAMBDA_MU = float(os.environ.get("LAMBDA_MU", "1.0"))
LAMBDA_DRIFT = float(os.environ.get("LAMBDA_DRIFT", "1.0"))
MU_BACKBONE_CHUNK = int(os.environ.get("MU_BACKBONE_CHUNK", "8192"))

# v12_sb: Bridge-specific config
N_BRIDGE_STEPS = int(os.environ.get("N_BRIDGE_STEPS", "20"))      # Euler-Maruyama substeps at inference
BRIDGE_SIGMA_MIN = float(os.environ.get("BRIDGE_SIGMA_MIN", "0.01"))
BRIDGE_SIGMA_MAX = float(os.environ.get("BRIDGE_SIGMA_MAX", "1.0"))  # sigma in SF2M
DRIFT_HIDDEN = int(os.environ.get("DRIFT_HIDDEN", "256"))
DRIFT_LAYERS = int(os.environ.get("DRIFT_LAYERS", "4"))
DRIFT_LR = float(os.environ.get("DRIFT_LR", "4e-4"))

# SF2M-specific config
OT_REG_FACTOR = float(os.environ.get("OT_REG_FACTOR", "2.0"))  # reg = OT_REG_FACTOR * sigma**2
SCORE_LOSS_WEIGHT = float(os.environ.get("SCORE_LOSS_WEIGHT", "1.0"))
OT_MICROBATCH = int(os.environ.get("OT_MICROBATCH", "512"))  # OT coupling microbatch size
OT_COND_WEIGHT = float(os.environ.get("OT_COND_WEIGHT", "0.15"))  # weight of condition features in OT cost
FREEZE_AB1 = bool(int(os.environ.get("FREEZE_AB1", "1")))  # freeze mu_backbone during SF2M training
INFERENCE_MODE = os.environ.get("INFERENCE_MODE", "sde")  # "sde", "low_noise", or "ode"

# POT (Python Optimal Transport) for minibatch entropic OT coupling
try:
    import ot as pot
    _HAS_POT = True
except ImportError:
    pot = None
    _HAS_POT = False

# Training config
DIFF_BATCH = 131072
if AB1_ENABLED:
    DIFF_BATCH = DIFF_BATCH // 4
DIFF_LR = 4e-4
DIFF_EPOCHS = 60
DIFF_EPOCHS_WARMSTART = 20

# Sampling config
S_SCENARIOS = int(os.environ.get("S_SCENARIOS", "50"))
S_BLOCK = int(os.environ.get("S_BLOCK", "10"))
INFERENCE_BATCH_SIZE = int(os.environ.get("INFERENCE_BATCH_SIZE", "50000"))
USE_BF16 = bool(int(os.environ.get("USE_BF16", "1")))
USE_TORCH_COMPILE = False
COMPILE_MODE = "reduce-overhead"
SCALE_FLOOR_Y = 0.01
SCALE_FLOOR_NUM = 0.01
SCALE_FLOOR_TGT = 0.03
SAMPLER_DISABLE_AUTOCAST = False
SAMPLER_Z_CLIP = 20.0
SAMPLER_NOISE_CLIP = 10.0
SAMPLER_X0_CLIP = 50.0
SAMPLER_X_CLIP = 50.0
SAMPLER_REPORT_BAD_STEP = False
SCALED_SHARDS_FLOAT16 = False
GEO_COL = None

ACCT_CHUNK_SIZE_TRAIN = 300_000
ACCT_CHUNK_SIZE_INFER = 200_000
MAX_ROWS_PER_SHARD = 8_000_000

# =============================================================================
# Data plumbing — identical to v11
# =============================================================================
def compute_year_median_log1p_level(lf: pl.LazyFrame, min_year: int, max_year: int):
    df = lf.filter(pl.col("yr").is_between(int(min_year), int(max_year))).filter(
        pl.col("tot_appr_val") > 0).with_columns(
        pl.col("tot_appr_val").log1p().alias("y_log")).group_by("yr").agg(
        pl.col("y_log").median().alias("med")).collect()
    return {int(r["yr"]): float(r["med"]) for r in df.iter_rows(named=True)}

def fill_hist_lags_no_zeros(hist_mat: np.ndarray, fallback_value: float) -> np.ndarray:
    out = hist_mat.copy().astype(np.float32)
    N, L = out.shape
    for c in range(L):
        col = out[:, c]
        mask = np.isfinite(col)
        if mask.sum() > 0:
            med = float(np.nanmedian(col))
            out[~mask, c] = med
        else:
            for r in range(N):
                row = out[r, :]
                obs = row[np.isfinite(row)]
                if obs.size > 0:
                    out[r, c] = float(obs[0])
                else:
                    out[r, c] = fallback_value
    return out

_HASH_HAS_SEED = False

def stable_hash_expr(e: pl.Expr):
    return e.hash(seed=42, seed_1=0, seed_2=0, seed_3=0) if _HASH_HAS_SEED else e.hash()

def bucket_hash_expr_str(col_expr: pl.Expr, n_buckets: int):
    return (stable_hash_expr(col_expr.cast(pl.Utf8)) % n_buckets).cast(pl.Int64)

def build_region_id_expr():
    # Use GEO_COL (geoid etc.) if available, else lat/lon, else hash acct
    _geo_col = globals().get("GEO_COL", None)
    _cols = globals().get("cols_set", set())
    _has_ll = globals().get("HAS_LATLON", False)
    if _geo_col and _geo_col in _cols:
        return bucket_hash_expr_str(pl.col(_geo_col), GEO_BUCKETS).alias("region_id")
    if _has_ll:
        lat_bin = (pl.col("gis_lat").fill_null(0.0) / 0.05).floor().cast(pl.Int64)
        lon_bin = (pl.col("gis_lon").fill_null(0.0) / 0.05).floor().cast(pl.Int64)
        key = (lat_bin * 1_000_000 + lon_bin).cast(pl.Utf8)
        return bucket_hash_expr_str(key, GEO_BUCKETS).alias("region_id")
    return bucket_hash_expr_str(pl.col("acct").cast(pl.Utf8), GEO_BUCKETS).alias("region_id")

def build_cat_hash_exprs(cat_cols_local: List[str]):
    return [bucket_hash_expr_str(pl.col(c), HASH_BUCKET_SIZE).alias(f"cat_{c}")
            for c in cat_cols_local]

def resolve_local_work_dirs(out_dir_drive: str, scratch_root: str='/content/wm_scratch'):
    d = {"OUT_DIR_DRIVE": out_dir_drive, "RAW_SHARD_ROOT": os.path.join(scratch_root, "raw_shards"),
         "SCALED_SHARD_ROOT": os.path.join(scratch_root, "scaled_shards")}
    for v in d.values(): os.makedirs(v, exist_ok=True)
    return d

def copy_small_artifacts_to_drive(src_path: str, dst_dir_drive: str):
    os.makedirs(dst_dir_drive, exist_ok=True)
    dst = os.path.join(dst_dir_drive, os.path.basename(src_path))
    try:
        shutil.copy2(src_path, dst)
    except Exception as e:
        print(f"[{ts()}] copy_small_artifacts_to_drive WARN: {e}")
    return dst

def _np_savez_shard(path, hist_y, cur_num, cur_cat, region_id, target, mask, yr_label,
                    y_anchor, anchor_year, acct):
    np.savez(path, hist_y=hist_y, cur_num=cur_num, cur_cat=cur_cat, region_id=region_id,
             target=target, mask=mask, yr_label=yr_label, y_anchor=y_anchor,
             anchor_year=anchor_year, acct=acct)

def _iter_shards_npz(shard_paths: List[str]):
    for p in shard_paths:
        z = np.load(p, allow_pickle=True)
        yield p, z

def _nan_fill_with_median_per_col(X: np.ndarray):
    for c in range(X.shape[1]):
        col = X[:, c]
        mask = np.isfinite(col)
        if mask.sum() > 0 and (~mask).sum() > 0:
            X[~mask, c] = float(np.nanmedian(col))
    return X

# =============================================================================
# Scalers — identical to v11
# =============================================================================
class RunningMeanVar:
    def __init__(self, dim: int):
        self.dim = int(dim); self.n = 0
        self.mean = np.zeros((self.dim,), dtype=np.float64)
        self.M2 = np.zeros((self.dim,), dtype=np.float64)
    def update(self, X: np.ndarray):
        X = X.astype(np.float64, copy=False)
        n_b = int(X.shape[0])
        if n_b == 0: return
        mean_b = np.nanmean(X, axis=0)
        var_b = np.nanvar(X, axis=0, ddof=0)
        n_a, mean_a, M2_a = self.n, self.mean, self.M2
        n = n_a + n_b; delta = mean_b - mean_a
        mean = mean_a + delta * (float(n_b) / float(n))
        M2 = M2_a + var_b * n_b + delta * delta * (float(n_a) * float(n_b) / float(n))
        self.n, self.mean, self.M2 = n, mean, M2
    def finalize(self, scale_floor: float):
        if self.n <= 1:
            return (self.mean.astype(np.float32), np.full((self.dim,), float(scale_floor), dtype=np.float32))
        var = self.M2 / float(max(1, int(self.n)))
        std = np.sqrt(np.maximum(var, 0.0))
        std = np.maximum(std, float(scale_floor))
        return (self.mean.astype(np.float32), std.astype(np.float32))

class SimpleScaler:
    def __init__(self, mean: np.ndarray, scale: np.ndarray):
        self.mean_ = mean.astype(np.float32); self.scale_ = scale.astype(np.float32)
    def transform(self, X: np.ndarray):
        if X.size == 0: return X.astype(np.float32)
        return ((X.astype(np.float32) - self.mean_) / self.scale_).astype(np.float32)
    def inverse_transform(self, X: np.ndarray):
        if X.size == 0: return X.astype(np.float32)
        return (X.astype(np.float32) * self.scale_ + self.mean_).astype(np.float32)

def _robust_loc_scale(X, scale_floor):
    X = X.astype(np.float32, copy=False)
    med = np.nanmedian(X, axis=0).astype(np.float32)
    q25 = np.nanpercentile(X, 25, axis=0).astype(np.float32)
    q75 = np.nanpercentile(X, 75, axis=0).astype(np.float32)
    sc = (q75 - q25) / 1.349
    sc = np.where(np.isfinite(sc), sc, float(scale_floor)).astype(np.float32)
    sc = np.maximum(sc, float(scale_floor)).astype(np.float32)
    med = np.where(np.isfinite(med), med, 0.0).astype(np.float32)
    return (med, sc)

def fit_scalers_from_shards_v102_robust_y(shard_paths, num_dim, scale_floor_y,
                                           scale_floor_num, scale_floor_tgt, max_y_rows=500000):
    t_stat = RunningMeanVar(H)
    n_stat = RunningMeanVar(int(num_dim)) if int(num_dim) > 0 else None
    y_samples, y_rows = [], 0
    for _, z in _iter_shards_npz(shard_paths):
        hy = z['hist_y'].astype(np.float32, copy=False)
        tg = z['target'].astype(np.float32, copy=False)
        t_stat.update(tg)
        if n_stat is not None:
            n_stat.update(z['cur_num'].astype(np.float32, copy=False))
        if y_rows < int(max_y_rows) and hy.shape[0] > 0:
            take = min(int(hy.shape[0]), int(max_y_rows - y_rows))
            if take > 0: y_samples.append(hy[:take]); y_rows += take
        if y_rows >= int(max_y_rows): break
    if y_rows <= 0:
        y_mu = np.zeros((FULL_HIST_LEN,), dtype=np.float32)
        y_sc = np.full((FULL_HIST_LEN,), float(scale_floor_y), dtype=np.float32)
    else:
        Y = np.concatenate(y_samples, axis=0)
        y_mu, y_sc = _robust_loc_scale(Y, float(scale_floor_y))
    t_mu, t_sc = t_stat.finalize(float(scale_floor_tgt))
    if n_stat is not None:
        n_mu, n_sc = n_stat.finalize(float(scale_floor_num))
    else:
        n_mu, n_sc = np.zeros((0,), dtype=np.float32), np.ones((0,), dtype=np.float32)
    return (SimpleScaler(y_mu, y_sc), SimpleScaler(n_mu, n_sc), SimpleScaler(t_mu, t_sc))

def assert_y_scaler_contract(y_scaler, shard_paths, z_clip=20.0, max_check_rows=200000, max_sat_frac=0.025):
    checked, xs = 0, []
    for _, z in _iter_shards_npz(shard_paths):
        hy = z['hist_y'].astype(np.float32, copy=False)
        if hy.shape[0] == 0: continue
        take = min(int(hy.shape[0]), int(max_check_rows - checked))
        if take <= 0: break
        xs.append(hy[:take]); checked += take
        if checked >= int(max_check_rows): break
    if checked <= 0: raise RuntimeError('no hist_y rows')
    Z = y_scaler.transform(np.concatenate(xs, axis=0))
    sat = float(np.mean(np.abs(Z) > float(z_clip)))
    if not np.isfinite(sat) or sat > float(max_sat_frac):
        raise RuntimeError(f'y_scaler saturation {sat:.6f} > {max_sat_frac}')

def write_scaled_shards_v102(shard_paths_raw, out_dir_scaled, y_scaler, n_scaler, t_scaler,
                              num_dim, keep_acct=False, use_float16=False):
    os.makedirs(out_dir_scaled, exist_ok=True)
    out_paths = []
    for p, z in _iter_shards_npz(shard_paths_raw):
        hist_y_s = y_scaler.transform(z['hist_y'].astype(np.float32, copy=False))
        cur_num_s = n_scaler.transform(z['cur_num'].astype(np.float32, copy=False)) if int(num_dim) > 0 else np.zeros((hist_y_s.shape[0], 0), dtype=np.float32)
        x0_s = t_scaler.transform(z['target'].astype(np.float32, copy=False))
        mask = z['mask'].astype(np.float32, copy=False)
        dtype = np.float16 if use_float16 else np.float32
        base = os.path.basename(p)
        out_p = os.path.join(out_dir_scaled, base.replace('.npz', '_scaled.npz'))
        np.savez(out_p, hist_y_s=hist_y_s.astype(dtype), cur_num_s=cur_num_s.astype(dtype),
                 x0_s=x0_s.astype(dtype), mask=mask.astype(dtype),
                 cur_cat=z['cur_cat'].astype(np.int32, copy=False),
                 region_id=z['region_id'].astype(np.int32, copy=False),
                 anchor_year=z['anchor_year'].astype(np.int32, copy=False),
                 y_anchor=z['y_anchor'].astype(np.float32, copy=False))
        out_paths.append(out_p)
    return out_paths

# =============================================================================
# Inducing-token coherence — identical to v11
# =============================================================================
class TokenPersistence(nn.Module):
    """Learned per-token AR(1) persistence values. phi_k = sigmoid(logit_k) * 0.99."""
    def __init__(self, K: int, phi_init: float=0.8):
        super().__init__()
        init_val = math.log(phi_init / max(0.001, 0.99 - phi_init))
        self.phi_logits = nn.Parameter(torch.full((K,), init_val))
    def get_phi(self):
        return torch.sigmoid(self.phi_logits) * 0.99
    def get_phi_list(self):
        with torch.no_grad(): return (torch.sigmoid(self.phi_logits) * 0.99).cpu().tolist()

def sample_token_paths_learned(K, H, phi_vec, S, device):
    phi_vec = phi_vec.to(device)
    innovation_std = torch.sqrt(torch.clamp(1.0 - phi_vec ** 2, min=1e-6))
    z_steps = []
    z_prev = torch.randn((S, K), device=device)
    z_steps.append(z_prev)
    for t in range(1, H):
        eta = torch.randn((S, K), device=device)
        z_prev = phi_vec.unsqueeze(0) * z_prev + innovation_std.unsqueeze(0) * eta
        z_steps.append(z_prev)
    return torch.stack(z_steps, dim=2)

def sample_token_paths(K, H, phi, S, device):
    if isinstance(phi, TokenPersistence):
        return sample_token_paths_learned(K, H, phi.get_phi(), S, device)
    elif isinstance(phi, torch.Tensor):
        return sample_token_paths_learned(K, H, phi, S, device)
    else:
        phi_f = float(phi)
        innovation_std = math.sqrt(max(0.0, 1.0 - phi_f ** 2))
        Z = torch.zeros((S, K, H), device=device)
        Z[:, :, 0] = torch.randn((S, K), device=device)
        for t in range(1, H):
            Z[:, :, t] = phi_f * Z[:, :, t - 1] + innovation_std * torch.randn((S, K), device=device)
        return Z

class GatingNetwork(nn.Module):
    """Per-parcel sparse mixing weights over K shared token paths."""
    def __init__(self, hist_len, num_dim, n_cat, K, k, hidden=64, use_macro=True):
        super().__init__()
        self.K, self.k, self.cat_emb_dim = int(K), int(k), 16
        self.cat_embs = nn.ModuleList([nn.Embedding(HASH_BUCKET_SIZE, self.cat_emb_dim)
                                       for _ in range(max(1, int(n_cat)))])
        cat_total = self.cat_emb_dim * max(1, int(n_cat))
        self.region_emb = nn.Embedding(GEO_BUCKETS, REGION_EMB_DIM)
        self.use_macro = use_macro
        self.macro_emb_dim = 16 if use_macro else 0
        if use_macro: self.year_emb = nn.Embedding(100, self.macro_emb_dim)
        in_dim = hist_len + max(1, num_dim) + cat_total + REGION_EMB_DIM + self.macro_emb_dim
        self._diag_printed = False
        self.net = nn.Sequential(nn.Linear(in_dim, hidden), nn.GELU(),
                                  nn.Linear(hidden, hidden), nn.GELU(), nn.Linear(hidden, K))
    def forward(self, hist_y, cur_num, cur_cat, region_id, origin_year=None):
        B = hist_y.shape[0]
        if cur_cat.shape[1] > 0 and len(self.cat_embs) > 0:
            cat_vecs = [emb(cur_cat[:, min(j, cur_cat.shape[1]-1)].clamp(0, HASH_BUCKET_SIZE-1).long())
                        for j, emb in enumerate(self.cat_embs)]
            cat_vec = torch.cat(cat_vecs, dim=1)
        else:
            cat_vec = torch.zeros((B, self.cat_emb_dim), device=hist_y.device, dtype=hist_y.dtype)
        region_vec = self.region_emb(region_id.clamp(0, GEO_BUCKETS-1).long())
        if self.use_macro and origin_year is not None:
            macro_vec = self.year_emb((origin_year - 2000).clamp(0, 99).long())
        else:
            macro_vec = torch.zeros((B, self.macro_emb_dim), device=hist_y.device, dtype=hist_y.dtype)
        if cur_num.shape[1] == 0:
            cur_num = torch.zeros((B, 1), device=hist_y.device, dtype=hist_y.dtype)
        x = torch.cat([hist_y, cur_num, cat_vec, region_vec, macro_vec], dim=1)
        logits = self.net(x)
        if self.k < self.K:
            topk_vals, topk_idx = torch.topk(logits, self.k, dim=1)
            mask = torch.full_like(logits, float('-inf'))
            mask.scatter_(1, topk_idx, topk_vals)
            return torch.softmax(mask, dim=1)
        return torch.softmax(logits, dim=1)

def compute_shared_driver(alpha, Z_tokens):
    if Z_tokens.dim() == 3 and Z_tokens.shape[0] != alpha.shape[0]:
        return torch.einsum("nk,skh->nsh", alpha, Z_tokens)
    return torch.einsum("bk,bkh->bh", alpha, Z_tokens)

class CoherenceScale(nn.Module):
    """Learned sigma_u in (0, 2)."""
    def __init__(self, init_logit=0.0):
        super().__init__()
        self.logit = nn.Parameter(torch.tensor([init_logit], dtype=torch.float32))
    def forward(self): return 2.0 * torch.sigmoid(self.logit)
    def get_sigma(self):
        with torch.no_grad(): return float(2.0 * torch.sigmoid(self.logit).item())

# =============================================================================
# AB1: FT-Transformer mu backbone — identical to v11
# =============================================================================
class FTTransformerBackbone(nn.Module):
    """AB1 deterministic backbone: predicts mu_hat [B, H] and ctx_emb [B, hidden]."""
    def __init__(self, hist_len, num_dim, n_cat, H,
                 d_model=FT_D_MODEL, n_heads=FT_N_HEADS,
                 n_layers=FT_N_LAYERS, dropout=FT_DROPOUT,
                 denoiser_hidden=DRIFT_HIDDEN):
        super().__init__()
        self.H, self.d_model = int(H), int(d_model)
        self.num_dim, self.n_cat = max(1, int(num_dim)), max(1, int(n_cat))
        self._diag_printed = False
        self.num_projs = nn.ModuleList([nn.Linear(1, d_model) for _ in range(self.num_dim)])
        self.cat_emb_dim = 16
        self.cat_embs = nn.ModuleList([nn.Embedding(HASH_BUCKET_SIZE, self.cat_emb_dim) for _ in range(self.n_cat)])
        self.cat_projs = nn.ModuleList([nn.Linear(self.cat_emb_dim, d_model) for _ in range(self.n_cat)])
        self.hist_proj = nn.Linear(int(hist_len), d_model)
        self.region_emb = nn.Embedding(GEO_BUCKETS, REGION_EMB_DIM)
        self.region_proj = nn.Linear(REGION_EMB_DIM, d_model)
        self.year_emb = nn.Embedding(100, d_model)
        self.cls_token = nn.Parameter(torch.randn(1, 1, d_model) * 0.02)
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=d_model, nhead=n_heads, dim_feedforward=d_model*4,
            dropout=dropout, activation='gelu', batch_first=True, norm_first=True)
        self.transformer = nn.TransformerEncoder(encoder_layer, num_layers=n_layers,
                                                  norm=nn.LayerNorm(d_model))
        self.mu_head = nn.Sequential(nn.Linear(d_model, d_model), nn.GELU(), nn.Linear(d_model, H))
        self.ctx_proj = nn.Linear(d_model, denoiser_hidden)

    def _build_tokens(self, hist_y, cur_num, cur_cat, region_id, origin_year=None):
        B = hist_y.shape[0]; tokens = [self.cls_token.expand(B, -1, -1)]
        tokens.append(self.hist_proj(hist_y).unsqueeze(1))
        if cur_num.shape[1] > 0:
            for j in range(min(cur_num.shape[1], len(self.num_projs))):
                tokens.append(self.num_projs[j](cur_num[:, j:j+1]).unsqueeze(1))
        else:
            tokens.append(self.num_projs[0](torch.zeros((B,1), device=hist_y.device, dtype=hist_y.dtype)).unsqueeze(1))
        if cur_cat.shape[1] > 0:
            for j in range(min(cur_cat.shape[1], len(self.cat_embs))):
                tokens.append(self.cat_projs[j](self.cat_embs[j](cur_cat[:, j].clamp(0, HASH_BUCKET_SIZE-1).long())).unsqueeze(1))
        else:
            tokens.append(self.cat_projs[0](torch.zeros((B, self.cat_emb_dim), device=hist_y.device, dtype=hist_y.dtype)).unsqueeze(1))
        tokens.append(self.region_proj(self.region_emb(region_id.clamp(0, GEO_BUCKETS-1).long())).unsqueeze(1))
        if origin_year is not None:
            tokens.append(self.year_emb((origin_year - 2000).clamp(0, 99).long()).unsqueeze(1))
        return torch.cat(tokens, dim=1)

    def forward(self, hist_y, cur_num, cur_cat, region_id, origin_year=None):
        seq = self._build_tokens(hist_y, cur_num, cur_cat, region_id, origin_year)
        out = self.transformer(seq)
        cls_out = out[:, 0, :]
        return self.mu_head(cls_out), self.ctx_proj(cls_out)

def _chunked_mu_forward(mu_backbone, hist_y, cur_num, cur_cat, region_id,
                         origin_year=None, chunk_size=MU_BACKBONE_CHUNK):
    B = hist_y.shape[0]
    if B <= chunk_size:
        with torch.no_grad(): return mu_backbone(hist_y, cur_num, cur_cat, region_id, origin_year=origin_year)
    mu_parts, ctx_parts = [], []
    with torch.no_grad():
        for i in range(0, B, chunk_size):
            j = min(i + chunk_size, B)
            oy_c = origin_year[i:j] if origin_year is not None else None
            mu_c, ctx_c = mu_backbone(hist_y[i:j], cur_num[i:j], cur_cat[i:j], region_id[i:j], origin_year=oy_c)
            mu_parts.append(mu_c); ctx_parts.append(ctx_c)
    return torch.cat(mu_parts, 0), torch.cat(ctx_parts, 0)

# =============================================================================
# v12_sb NEW: Bridge time embedding
# =============================================================================
class SinBridgeTime(nn.Module):
    """Sinusoidal embedding for bridge time s ∈ [0, 1]."""
    def __init__(self, dim: int):
        super().__init__()
        self.dim = int(dim)
    def forward(self, s: torch.Tensor) -> torch.Tensor:
        """s: [B] bridge time values in [0, 1]."""
        half = self.dim // 2
        if half <= 1:
            return torch.zeros((s.shape[0], self.dim), device=s.device, dtype=s.dtype)
        freqs = torch.exp(torch.arange(half, device=s.device, dtype=s.dtype) *
                          (-math.log(10000.0) / float(half - 1)))
        # Scale s to [0, 1000] range for richer frequency coverage
        ang = (s * 1000.0).unsqueeze(1) * freqs.unsqueeze(0)
        return torch.cat([torch.sin(ang), torch.cos(ang)], dim=1)

class FiLMLayer(nn.Module):
    def __init__(self, cond_dim, channels):
        super().__init__()
        self.scale = nn.Linear(cond_dim, channels)
        self.shift = nn.Linear(cond_dim, channels)
    def forward(self, x, cond):
        return x * (1 + self.scale(cond).unsqueeze(-1)) + self.shift(cond).unsqueeze(-1)

# =============================================================================
# v12_sb SF2M: SF2MNetwork — shared MLP trunk + flow head + score head
# =============================================================================
class SF2MNetwork(nn.Module):
    """
    Unified SF2M network with shared conditioning trunk and two lightweight heads.
    Architecture choice: MLP-based (not Conv1d) since H=5 is too short for conv benefits.

    Shared trunk encodes all conditioning (history, numerics, cats, region, time, token driver,
    AB1 context) into a single hidden representation h.
    Then:
        v_theta = flow_head(h)      — velocity field [B, H]
        scaled_s = score_head(h)    — scaled score: sigma(t)^2 * nabla_log_p_t [B, H]

    The score head outputs the SCALED score (g(t)^2 * nabla log p_t) rather than the raw
    score, following the paper's recommendation for numerical stability.
    """
    def __init__(self, target_dim, hist_len, num_dim, n_cat, hidden, n_layers):
        super().__init__()
        self.target_dim = int(target_dim)
        self.hidden = int(hidden)

        # ── Conditioning encoders ──
        self.cat_emb_dim = 16
        n_cat_safe = max(1, int(n_cat))
        self.cat_embs = nn.ModuleList([nn.Embedding(HASH_BUCKET_SIZE, self.cat_emb_dim)
                                       for _ in range(n_cat_safe)])
        cat_dim = self.cat_emb_dim * n_cat_safe
        self.region_emb = nn.Embedding(GEO_BUCKETS, REGION_EMB_DIM)
        self.region_enc = nn.Sequential(nn.Linear(REGION_EMB_DIM, hidden), nn.GELU(), nn.Linear(hidden, hidden))
        self.token_cond_enc = nn.Sequential(nn.Linear(self.target_dim, hidden), nn.GELU(), nn.Linear(hidden, hidden))
        self.hist_enc = nn.Sequential(nn.Linear(int(hist_len), hidden), nn.GELU(), nn.Linear(hidden, hidden))
        self.num_enc = nn.Sequential(nn.Linear(max(1, int(num_dim)), hidden), nn.GELU(), nn.Linear(hidden, hidden))
        self.cat_enc = nn.Sequential(nn.Linear(max(1, int(cat_dim)), hidden), nn.GELU(), nn.Linear(hidden, hidden))

        # Bridge time embedding
        self.s_dim = 128
        self.s_emb = SinBridgeTime(self.s_dim)
        self.s_enc = nn.Sequential(nn.Linear(self.s_dim, hidden), nn.GELU(), nn.Linear(hidden, hidden))

        # AB1 context embedding projection
        self.ctx_proj = nn.Sequential(nn.Linear(hidden, hidden), nn.GELU(), nn.Linear(hidden, hidden))

        # ── Shared MLP trunk (state + conditioning → hidden) ──
        # Input: x_t [H] projected to hidden, added to conditioning
        self.state_proj = nn.Linear(self.target_dim, hidden)

        trunk_blocks = []
        for _ in range(int(n_layers)):
            trunk_blocks.append(nn.Sequential(
                nn.Linear(hidden, hidden),
                nn.GELU(),
                nn.Linear(hidden, hidden),
            ))
        self.trunk_blocks = nn.ModuleList(trunk_blocks)
        self.trunk_norm = nn.LayerNorm(hidden)

        # ── Two lightweight heads ──
        self.flow_head = nn.Sequential(
            nn.Linear(hidden, hidden), nn.GELU(),
            nn.Linear(hidden, self.target_dim),
        )
        self.score_head = nn.Sequential(
            nn.Linear(hidden, hidden), nn.GELU(),
            nn.Linear(hidden, self.target_dim),
        )

        # Scalers (attached at training time)
        self._y_scaler = None
        self._n_scaler = None
        self._t_scaler = None

    def _encode_cond(self, hist_y, cur_num, cur_cat, region_id, u_i, s, ctx_emb=None):
        """Encode all conditioning inputs into a single hidden vector."""
        h_hist = self.hist_enc(hist_y)
        h_num = self.num_enc(cur_num) if cur_num.shape[1] > 0 else \
                self.num_enc(torch.zeros((hist_y.shape[0], 1), device=hist_y.device, dtype=hist_y.dtype))
        h_s = self.s_enc(self.s_emb(s))
        if cur_cat.shape[1] > 0 and len(self.cat_embs) > 0:
            cat_vecs = [emb(cur_cat[:, j].clamp(0, HASH_BUCKET_SIZE-1).long())
                        for j, emb in enumerate(self.cat_embs)]
            cat_vec = torch.cat(cat_vecs, dim=1)
        else:
            cat_vec = torch.zeros((hist_y.shape[0], 1), device=hist_y.device, dtype=hist_y.dtype)
        h_cat = self.cat_enc(cat_vec)
        h_region = self.region_enc(self.region_emb(region_id.clamp(0, GEO_BUCKETS-1).long()))
        h_token = self.token_cond_enc(u_i)
        h_cond = h_hist + h_num + h_cat + h_region + h_token + h_s
        if ctx_emb is not None:
            h_cond = h_cond + self.ctx_proj(ctx_emb)
        return h_cond

    def forward(self, x_s, s, hist_y, cur_num, cur_cat, region_id, u_i, ctx_emb=None):
        """
        Returns (v_hat, scaled_s_hat):
            v_hat:       [B, H] flow/velocity prediction
            scaled_s_hat: [B, H] scaled score = sigma(t)^2 * nabla_log_p_t
        """
        h_cond = self._encode_cond(hist_y, cur_num, cur_cat, region_id, u_i, s, ctx_emb)
        h = self.state_proj(x_s) + h_cond
        for block in self.trunk_blocks:
            h = h + block(h)  # residual MLP blocks
        h = self.trunk_norm(h)
        return self.flow_head(h), self.score_head(h)

    def forward_flow_only(self, x_s, s, hist_y, cur_num, cur_cat, region_id, u_i, ctx_emb=None):
        """For ODE inference mode: only compute flow, skip score head."""
        h_cond = self._encode_cond(hist_y, cur_num, cur_cat, region_id, u_i, s, ctx_emb)
        h = self.state_proj(x_s) + h_cond
        for block in self.trunk_blocks:
            h = h + block(h)
        h = self.trunk_norm(h)
        return self.flow_head(h)

# Backward compatibility aliases
FlowNetwork = SF2MNetwork
ScoreNetwork = SF2MNetwork
PathDriftNetwork = SF2MNetwork

# =============================================================================
# v12_sb SF2M: OTCoupler — condition-aware minibatch OT with microbatching
# =============================================================================
class OTCoupler:
    """
    Condition-aware minibatch entropic OT coupling for SF2M.

    Key optimizations over naive OT:
    1. MICROBATCHING: OT is O(B^2) — run it on small microbatches (512-2048),
       not the full 64k training batch.
    2. CONDITION-AWARE COST: The cost matrix blends residual-space distance with
       conditioning-space distance, so OT pairs similar parcels rather than
       creating train-test mismatch (per C2OT / ICCV 2025).
    3. Falls back to independent coupling if POT is unavailable.
    """
    def __init__(self, method: str = "exact", reg: float = 2.0,
                 microbatch: int = OT_MICROBATCH, cond_weight: float = OT_COND_WEIGHT):
        self.method = method
        self.reg = float(reg)
        self.microbatch = int(microbatch)
        self.cond_weight = float(cond_weight)

    # Diagnostic counters
    _diag_off_diagonal = 0
    _diag_total_pairs = 0

    def _solve_ot_batch(self, x0, x1, cond0=None, cond1=None):
        """Solve OT on a single microbatch. Returns reindexed (x0, x1)."""
        B = x0.shape[0]
        if B <= 1:
            return x0, x1

        with torch.no_grad():
            # Residual-space cost
            M = torch.cdist(x0.detach().float(), x1.detach().float()) ** 2

            # Condition-aware cost: blend in conditioning distance
            if cond0 is not None and cond1 is not None and self.cond_weight > 0:
                M_cond = torch.cdist(cond0.detach().float(), cond1.detach().float()) ** 2
                # Normalize both cost matrices to comparable scales
                m_scale = M.max().clamp(min=1e-8)
                c_scale = M_cond.max().clamp(min=1e-8)
                M = (1.0 - self.cond_weight) * (M / m_scale) + self.cond_weight * (M_cond / c_scale)

            M_np = M.cpu().numpy()
            a = np.ones(B, dtype=np.float64) / B
            b = np.ones(B, dtype=np.float64) / B

        if self.method == "exact":
            pi = pot.emd(a, b, M_np)
        else:
            pi = pot.sinkhorn(a, b, M_np, reg=self.reg)

        if not np.all(np.isfinite(pi)) or np.abs(pi.sum()) < 1e-8:
            return x0, x1

        p = pi.flatten()
        p = p / p.sum()
        choices = np.random.choice(B * B, p=p, size=B, replace=True)
        i_idx, j_idx = np.divmod(choices, B)

        # Diagnostic: track off-diagonal transport fraction
        OTCoupler._diag_off_diagonal += int(np.sum(i_idx != j_idx))
        OTCoupler._diag_total_pairs += B

        return x0[i_idx], x1[j_idx]

    @staticmethod
    def get_transport_diagnostic():
        """Return fraction of OT pairs that are off-diagonal (i != j).
        If close to 0, OT is degenerating to identity."""
        total = OTCoupler._diag_total_pairs
        if total == 0:
            return {'off_diagonal_frac': 0.0, 'total_pairs': 0}
        frac = float(OTCoupler._diag_off_diagonal) / float(total)
        return {'off_diagonal_frac': frac, 'total_pairs': total}

    @staticmethod
    def reset_diagnostic():
        OTCoupler._diag_off_diagonal = 0
        OTCoupler._diag_total_pairs = 0

    def couple(self, x0, x1, cond0=None, cond1=None):
        """
        Couple (x0, x1) using OT with microbatching.
        cond0, cond1: [B, D_cond] optional conditioning features for condition-aware cost.
        """
        if not _HAS_POT:
            return x0, x1

        B = x0.shape[0]
        if B <= self.microbatch:
            return self._solve_ot_batch(x0, x1, cond0, cond1)

        # Microbatching: split into chunks, solve OT per chunk, reassemble
        perm = np.random.permutation(B)
        x0_out = torch.empty_like(x0)
        x1_out = torch.empty_like(x1)

        for start in range(0, B, self.microbatch):
            end = min(start + self.microbatch, B)
            idx = perm[start:end]
            c0 = cond0[idx] if cond0 is not None else None
            c1 = cond1[idx] if cond1 is not None else None
            x0_mb, x1_mb = self._solve_ot_batch(x0[idx], x1[idx], c0, c1)
            x0_out[idx] = x0_mb
            x1_out[idx] = x1_mb

        return x0_out, x1_out

# =============================================================================
# v12_sb SF2M: BridgeSchedule — SB probability path with score + flow targets
# =============================================================================
class BridgeSchedule:
    """
    Schrodinger Bridge probability path for SF2M.
    Path:     x_t = (1-t)*x_0 + t*x_1 + sigma*sqrt(t*(1-t))*eps
    Sigma:    sigma(t) = sigma_max * sqrt(t*(1-t))
    Velocity: u_t = (1-2t)/(2t(1-t)) * (x_t - mu_t) + (x_1 - x_0)
    Score:    nabla log p_t = -(x_t - mu_t) / (sigma^2 * t * (1-t))
    Lambda:   lambda(t) = 2*sigma(t) / sigma^2
    """
    def __init__(self, n_steps: int = N_BRIDGE_STEPS, sigma_max: float = BRIDGE_SIGMA_MAX,
                 sigma_min: float = BRIDGE_SIGMA_MIN, device: str = 'cpu'):
        self.n_steps = int(n_steps)
        self.sigma_max = float(sigma_max)
        self.sigma_min = float(sigma_min)
        self.device = device
        self.dt = 1.0 / float(n_steps)

    def sigma(self, s: torch.Tensor) -> torch.Tensor:
        """sigma(t) = sigma_max * sqrt(t * (1-t)), floored at sigma_min."""
        raw = self.sigma_max * torch.sqrt(s * (1.0 - s))
        return torch.clamp(raw, min=self.sigma_min)

    def interpolate(self, x_0, x_1, t, z):
        """x_t = (1-t)*x_0 + t*x_1 + sigma(t)*z"""
        t_col = t.unsqueeze(1)
        sig = self.sigma(t).unsqueeze(1)
        return (1.0 - t_col) * x_0 + t_col * x_1 + sig * z

    def compute_mu_t(self, x_0, x_1, t):
        """mu_t = (1-t)*x_0 + t*x_1"""
        t_col = t.unsqueeze(1)
        return (1.0 - t_col) * x_0 + t_col * x_1

    def compute_conditional_flow(self, x_0, x_1, t, x_t):
        """SB conditional velocity: u_t = (1-2t)/(2t(1-t)) * (x_t - mu_t) + (x_1 - x_0)"""
        t_col = t.unsqueeze(1)
        mu_t = self.compute_mu_t(x_0, x_1, t)
        denom = 2.0 * t_col * (1.0 - t_col) + 1e-8
        sigma_t_prime_over_sigma_t = (1.0 - 2.0 * t_col) / denom
        return sigma_t_prime_over_sigma_t * (x_t - mu_t) + (x_1 - x_0)

    def compute_conditional_score(self, x_0, x_1, t, x_t):
        """Analytic conditional score: nabla log p_t = -(x_t - mu_t) / (sigma^2 * t*(1-t))"""
        t_col = t.unsqueeze(1)
        mu_t = self.compute_mu_t(x_0, x_1, t)
        var_t = (self.sigma_max ** 2) * t_col * (1.0 - t_col) + 1e-8
        return -(x_t - mu_t) / var_t

    def compute_scaled_score_target(self, x_0, x_1, t, x_t):
        """Scaled score target: sigma(t)^2 * nabla log p_t = -(x_t - mu_t)
        This is numerically more stable than the raw score."""
        mu_t = self.compute_mu_t(x_0, x_1, t)
        return -(x_t - mu_t)

    def compute_lambda(self, t):
        """Score loss weighting: lambda(t) = 2*sigma(t) / sigma^2.
        NOTE: Currently unused because score_head outputs the scaled score
        (sigma^2 * nabla_log_p_t) and the loss is computed directly against
        the scaled target. This is available for future use if switching
        to raw (unscaled) score parameterization."""
        sig_t = self.sigma(t)
        return (2.0 * sig_t / (self.sigma_max ** 2 + 1e-8)).unsqueeze(1)

    def conditional_velocity(self, x_0, x_1):
        """Legacy alias."""
        return x_1 - x_0

# =============================================================================
# Factory functions
# =============================================================================
def create_sf2m_network(target_dim, hist_len, num_dim, n_cat):
    return SF2MNetwork(target_dim, hist_len, num_dim, n_cat,
                            DRIFT_HIDDEN, DRIFT_LAYERS)

def create_ot_coupler(sigma_max=BRIDGE_SIGMA_MAX, method="sinkhorn"):
    reg = OT_REG_FACTOR * sigma_max ** 2
    return OTCoupler(method=method, reg=reg, microbatch=OT_MICROBATCH,
                     cond_weight=OT_COND_WEIGHT)

# Backward compat aliases
create_flow_network = create_sf2m_network
create_score_network = create_sf2m_network
create_drift_network = create_sf2m_network

def create_gating_network(hist_len, num_dim, n_cat, use_macro=True):
    return GatingNetwork(hist_len, num_dim, n_cat, K=K_TOKENS, k=K_ACTIVE,
                          use_macro=use_macro, hidden=GATING_HIDDEN)

def create_token_persistence():
    return TokenPersistence(K=K_TOKENS, phi_init=PHI_INIT)

def create_coherence_scale():
    return CoherenceScale(init_logit=0.0)

def create_mu_backbone(hist_len, num_dim, n_cat, H_dim=H):
    return FTTransformerBackbone(hist_len, num_dim, n_cat, H_dim)

# Legacy compat
GlobalProjection = None
GeoProjection = None

# =============================================================================
# Autocast helpers
# =============================================================================
def get_autocast_ctx(device: str):
    if USE_BF16 and device == "cuda" and torch.cuda.is_available():
        return torch.autocast(device_type="cuda", dtype=torch.bfloat16)
    return contextlib.nullcontext()

# =============================================================================
# Inference context builder — identical to v11
# =============================================================================
Y_FALLBACK_LOG1P = 0.0  # overwritten at runtime

def build_inference_context_chunked_v102(lf, accts, num_use_local, cat_use_local,
                                          global_medians, anchor_year,
                                          acct_chunk_size=ACCT_CHUNK_SIZE_INFER,
                                          max_parcels=None):
    if global_medians is None: global_medians = {}
    print(f'[{ts()}] Building inference context anchor_year={anchor_year} max_parcels={max_parcels}')
    # Cross-jurisdiction safety: filter feature lists to columns present in panel
    _panel_cols = set(lf.collect_schema().names())
    _orig_num, _orig_cat = list(num_use_local), list(cat_use_local)
    num_use_local = [c for c in num_use_local if c in _panel_cols]
    cat_use_local = [c for c in cat_use_local if c in _panel_cols]
    if len(num_use_local) < len(_orig_num):
        _dropped_n = set(_orig_num) - set(num_use_local)
        print(f'[{ts()}] ⚠️ Cross-jurisdiction: dropped {len(_dropped_n)} numeric features not in panel: {_dropped_n}')
    if len(cat_use_local) < len(_orig_cat):
        _dropped_c = set(_orig_cat) - set(cat_use_local)
        print(f'[{ts()}] ⚠️ Cross-jurisdiction: dropped {len(_dropped_c)} categorical features not in panel: {_dropped_c}')
    region_expr = build_region_id_expr()
    cat_hash_exprs = build_cat_hash_exprs(cat_use_local)
    hist_exprs = [pl.col('y_log').shift(i).over('acct').alias(f'lag_{i}') for i in range(FULL_HIST_LEN)]
    hist_buf, num_buf, cat_buf, rid_buf, yanc_buf, acct_buf = [], [], [], [], [], []
    total = 0
    for s in range(0, len(accts), int(acct_chunk_size)):
        acct_chunk = accts[s:s + int(acct_chunk_size)]
        if not acct_chunk: continue
        base_q = (lf.filter(pl.col('acct').cast(pl.Utf8).is_in(acct_chunk))
                    .filter(pl.col('yr').is_between(MIN_YEAR, int(anchor_year)))
                    .filter(pl.col('tot_appr_val') > 0)
                    .with_columns([pl.col('acct').cast(pl.Utf8).alias('acct'),
                                   pl.col('yr').cast(pl.Int32).alias('yr'),
                                   pl.col('tot_appr_val').log1p().alias('y_log')])
                    .sort(['acct', 'yr'])
                    .with_columns(hist_exprs + [region_expr] + cat_hash_exprs))
        df = base_q.filter(pl.col('yr') == int(anchor_year)).collect()
        if len(df) == 0: continue
        hist_cols = [f'lag_{i}' for i in range(FULL_HIST_LEN - 1, -1, -1)]
        hist_mat = np.column_stack([df[c].to_numpy().astype(np.float32) for c in hist_cols])
        hist_mat = fill_hist_lags_no_zeros(hist_mat, fallback_value=float(Y_FALLBACK_LOG1P))
        hist_y = hist_mat.astype(np.float32)
        y_anchor = df['y_log'].to_numpy().astype(np.float32)
        region_id = df['region_id'].to_numpy().astype(np.int64)
        acct_arr = df['acct'].to_numpy().astype(object)
        cur_num_list = []
        for c in num_use_local:
            if c in df.columns:
                vals = df[c].to_numpy().astype(np.float32)
                med = float(global_medians.get(c, 0.0))
                cur_num_list.append(np.nan_to_num(vals, nan=med))
            else:
                cur_num_list.append(np.full(len(df), float(global_medians.get(c, 0.0)), dtype=np.float32))
        cur_num = np.column_stack(cur_num_list) if cur_num_list else np.zeros((len(df), 0), np.float32)
        cur_cat_list = []
        for c in cat_use_local:
            hc = f'cat_{c}'
            cur_cat_list.append(df[hc].to_numpy().astype(np.int64) if hc in df.columns
                                else np.zeros(len(df), dtype=np.int64))
        cur_cat = np.column_stack(cur_cat_list) if cur_cat_list else np.zeros((len(df), 0), np.int64)
        if max_parcels is not None:
            need = int(max_parcels) - total
            if need <= 0: break
            take = min(len(df), need)
            hist_y, cur_num, cur_cat = hist_y[:take], cur_num[:take], cur_cat[:take]
            region_id, y_anchor, acct_arr = region_id[:take], y_anchor[:take], acct_arr[:take]
        hist_buf.append(hist_y); num_buf.append(cur_num); cat_buf.append(cur_cat)
        rid_buf.append(region_id); yanc_buf.append(y_anchor); acct_buf.append(acct_arr)
        total += hist_y.shape[0]
        if max_parcels is not None and total >= int(max_parcels): break
    if total == 0:
        print(f'[{ts()}] No inference anchors found at anchor_year={anchor_year}'); return None
    return {'hist_y': np.concatenate(hist_buf, 0).astype(np.float32),
            'cur_num': np.concatenate(num_buf, 0).astype(np.float32) if num_buf else np.zeros((total, 0), np.float32),
            'cur_cat': np.concatenate(cat_buf, 0).astype(np.int64) if cat_buf else np.zeros((total, 0), np.int64),
            'region_id': np.concatenate(rid_buf, 0).astype(np.int64),
            'y_anchor': np.concatenate(yanc_buf, 0).astype(np.float32),
            'acct': np.concatenate(acct_buf, 0).astype(object),
            'anchor_year': int(anchor_year), 'n_parcels': int(total)}

# =============================================================================
# v12_sb SF2M: Euler-Maruyama SDE sampler with flow + score (unified network)
# =============================================================================
@torch.no_grad()
def sample_sf2m_v12(
    sf2m_net: nn.Module,        # SF2MNetwork (unified: returns flow + scaled_score)
    gating_net: nn.Module,
    bridge_sched: BridgeSchedule,
    hist_y_b: np.ndarray,
    cur_num_b: np.ndarray,
    cur_cat_b: np.ndarray,
    region_id_b: np.ndarray,
    Z_tokens: torch.Tensor,    # [S, K, H] pre-sampled token paths
    device: str,
    anchor_year: int = 2021,
    coh_scale: CoherenceScale = None,
    mu_backbone: Optional[nn.Module] = None,
    mode: str = INFERENCE_MODE,  # "sde", "low_noise", "ode"
) -> np.ndarray:
    """
    SF2M sampler with configurable inference modes:
      - "sde":      dx = [v + 0.5*scaled_s] dt + sigma*dW  (full stochastic)
      - "low_noise": dx = [v + 0.5*scaled_s] dt + 0.3*sigma*dW  (reduced noise)
      - "ode":      dx = v dt  (probability flow ODE, deterministic)

    SF2MNetwork.forward returns (v_hat, scaled_s_hat) where scaled_s_hat = sigma^2 * nabla log p_t.
    So the SDE drift is: v + 0.5 * scaled_s (no extra sigma^2 factor needed).

    Source: x_0 ~ N(0, I_H) per parcel per scenario.
    Returns: deltas [N, S, H] in the same space as v11 output.
    """
    sf2m_net.eval()
    gating_net.eval()
    _ab1_active = (mu_backbone is not None) and AB1_ENABLED
    if _ab1_active: mu_backbone.eval()

    N = int(hist_y_b.shape[0])
    S = int(Z_tokens.shape[0])
    K = int(Z_tokens.shape[1])
    if N == 0:
        return np.zeros((0, S, H), dtype=np.float32)

    sigma_u = 1.0
    if coh_scale is not None:
        sigma_u = coh_scale.get_sigma()

    # Noise scale per mode
    noise_scale = {"sde": 1.0, "low_noise": 0.3, "ode": 0.0}.get(mode, 1.0)
    use_score = (mode != "ode")

    # Scale conditioning
    hy_np = sf2m_net._y_scaler.transform(hist_y_b).astype(np.float32)
    if SAMPLER_Z_CLIP is not None:
        hy_np = np.clip(hy_np, -float(SAMPLER_Z_CLIP), float(SAMPLER_Z_CLIP))
    if cur_num_b.shape[1] > 0:
        xn_np = sf2m_net._n_scaler.transform(cur_num_b).astype(np.float32)
        if SAMPLER_Z_CLIP is not None:
            xn_np = np.clip(xn_np, -float(SAMPLER_Z_CLIP), float(SAMPLER_Z_CLIP))
    else:
        xn_np = np.zeros((N, 0), dtype=np.float32)

    sb = int(S_BLOCK)
    print(f"[{ts()}] SAMPLER SF2M N={N} S={S} K={K} S_BLOCK={sb} mode={mode} "
          f"sigma_u={sigma_u:.3f} sigma_max={bridge_sched.sigma_max:.3f} n_steps={bridge_sched.n_steps}")

    # Move conditioning to device
    hy = torch.from_numpy(hy_np).to(device=device, dtype=torch.float32)
    xn = torch.from_numpy(xn_np).to(device=device, dtype=torch.float32) if xn_np.shape[1] > 0 \
         else torch.zeros((N, 0), device=device, dtype=torch.float32)
    xc = torch.from_numpy(cur_cat_b.astype(np.int64)).to(device=device)
    rid = torch.from_numpy(region_id_b.astype(np.int64)).to(device=device)

    # Gating weights ONCE
    oy = torch.full((N,), int(anchor_year), dtype=torch.long, device=device)
    alpha = gating_net(hy.float(), xn.float(), xc, rid, origin_year=oy)

    # AB1: compute mu_hat ONCE
    _mu_hat_t = None
    _ctx_emb_t = None
    if _ab1_active:
        _mu_hat_t, _ctx_emb_t = _chunked_mu_forward(
            mu_backbone, hy.float(), xn.float(), xc, rid, origin_year=oy)

    if Z_tokens.device != torch.device(device):
        Z_tokens = Z_tokens.to(device)

    autocast_ctx = get_autocast_ctx(device) if not SAMPLER_DISABLE_AUTOCAST else contextlib.nullcontext()

    # Output buffer
    out = np.empty((N, S, H), dtype=np.float32)
    dt = bridge_sched.dt

    # S-block loop
    for s0 in range(0, S, sb):
        sb_actual = min(sb, S - s0)
        Z_blk = Z_tokens[s0:s0 + sb_actual]

        # Shared driver
        u_i_blk = torch.einsum("nk,skh->nsh", alpha, Z_blk)
        u_i_blk = sigma_u * u_i_blk
        horizon_scale = torch.sqrt(torch.arange(1, H+1, device=device, dtype=torch.float32)).unsqueeze(0).unsqueeze(0)
        u_i_blk = u_i_blk * horizon_scale
        u_i_flat = u_i_blk.reshape(N * sb_actual, H)

        # Expand conditioning
        hy_exp = hy.repeat_interleave(sb_actual, dim=0)
        xn_exp = xn.repeat_interleave(sb_actual, dim=0)
        xc_exp = xc.repeat_interleave(sb_actual, dim=0)
        rid_exp = rid.repeat_interleave(sb_actual, dim=0)

        ctx_emb_exp = None
        if _ab1_active and _ctx_emb_t is not None:
            ctx_emb_exp = _ctx_emb_t.repeat_interleave(sb_actual, dim=0)

        # SF2M: Initialize from source distribution x_0 ~ N(0, I_H)
        x = torch.randn((N * sb_actual, H), device=device, dtype=torch.float32)

        # Euler-Maruyama integration
        for step_i in range(bridge_sched.n_steps):
            t_val = float(step_i) / float(bridge_sched.n_steps)
            t_val = max(1e-4, min(t_val, 1.0 - 1e-4))
            t_tensor = torch.full((N * sb_actual,), t_val, device=device, dtype=torch.float32)

            with autocast_ctx:
                if use_score:
                    # Full forward: flow + scaled score from unified network
                    v_hat, scaled_s_hat = sf2m_net(
                        x, t_tensor, hy_exp, xn_exp, xc_exp, rid_exp,
                        u_i_flat, ctx_emb=ctx_emb_exp)
                    v_hat = v_hat.to(dtype=torch.float32)
                    scaled_s_hat = scaled_s_hat.to(dtype=torch.float32)
                else:
                    # ODE mode: flow only, skip score head
                    v_hat = sf2m_net.forward_flow_only(
                        x, t_tensor, hy_exp, xn_exp, xc_exp, rid_exp,
                        u_i_flat, ctx_emb=ctx_emb_exp).to(dtype=torch.float32)

            v_hat = torch.nan_to_num(v_hat, nan=0.0, posinf=0.0, neginf=0.0)
            v_hat = v_hat.clamp(-float(SAMPLER_X_CLIP), float(SAMPLER_X_CLIP))

            if use_score:
                scaled_s_hat = torch.nan_to_num(scaled_s_hat, nan=0.0, posinf=0.0, neginf=0.0)
                scaled_s_hat = scaled_s_hat.clamp(-float(SAMPLER_X_CLIP), float(SAMPLER_X_CLIP))
                # drift = v + 0.5 * scaled_s  (since scaled_s = sigma^2 * true_score)
                drift = v_hat + 0.5 * scaled_s_hat
            else:
                drift = v_hat

            # Stochastic noise with TIME-VARYING sigma(t) (matches training score scaling)
            if noise_scale > 0 and step_i < bridge_sched.n_steps - 1:
                sig_t = bridge_sched.sigma(t_tensor[0:1]).item()
                noise = torch.randn_like(x)
                x = x + drift * dt + noise_scale * sig_t * math.sqrt(dt) * noise
            else:
                x = x + drift * dt

            x = torch.nan_to_num(x, nan=0.0, posinf=0.0, neginf=0.0)
            x = x.clamp(-float(SAMPLER_X_CLIP), float(SAMPLER_X_CLIP))

        # x is now the predicted residual path in scaled target space
        x_out = x.detach()
        if _ab1_active and _mu_hat_t is not None:
            mu_hat_exp = _mu_hat_t.repeat_interleave(sb_actual, dim=0)
            x_out = x_out + mu_hat_exp

        # Inverse-transform from scaled target space back to delta space
        x_blk = sf2m_net._t_scaler.inverse_transform(x_out.cpu().numpy().astype(np.float32))
        out[:, s0:s0 + sb_actual, :] = x_blk.reshape(N, sb_actual, H)

        del x, u_i_blk, u_i_flat, hy_exp, xn_exp, xc_exp, rid_exp

    return out

# Backward compat alias
sample_bridge_v12 = sample_sf2m_v12

# =============================================================================
# Acceptance tests
# =============================================================================
def acceptance_test_increment_consistency(deltas, y_anchor):
    cumsum_d = np.cumsum(deltas, axis=2)
    y_levels = y_anchor[:, None, None] + cumsum_d
    max_err = 0.0
    for k in range(1, deltas.shape[2]):
        err = float(np.max(np.abs(y_levels[:, :, k] - y_levels[:, :, k-1] - deltas[:, :, k])))
        if err > max_err: max_err = err
    return {'max_reconstruction_error': float(max_err), 'status': 'PASS' if max_err < 1e-5 else 'FAIL'}

def acceptance_test_finite(deltas):
    finite = np.isfinite(deltas)
    good_rows = int(np.all(finite, axis=(1, 2)).sum())
    n = int(deltas.shape[0])
    return {'n': n, 'finite_rows': good_rows, 'finite_frac': float(good_rows / max(1, n)),
            'status': 'PASS' if good_rows == n else 'FAIL'}

