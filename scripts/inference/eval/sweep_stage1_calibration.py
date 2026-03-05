"""
Modal evaluation wrapper for Properlytic world model v11.
Runs inference across all checkpoints and computes backtest metrics logged to W&B.

Usage:
    modal run scripts/inference/eval/sweep_stage1_calibration.py --jurisdiction sf_ca
"""
import modal
import os
import sys

# ─── Parse args at module load for descriptive Modal app name ───
_jur = "unknown"
_ori = "unknown"
for i, arg in enumerate(sys.argv):
    if arg == "--jurisdiction" and i + 1 < len(sys.argv):
        _jur = sys.argv[i + 1]
    if arg == "--origin" and i + 1 < len(sys.argv):
        _ori = sys.argv[i + 1]

app = modal.App(f"sweep-calib-{_jur}")

inference_image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "polars>=0.20",
        "pyarrow>=14.0",
        "numpy>=1.24",
        "scipy>=1.11",
        "torch>=2.1",
        "wandb>=0.16",
        "google-cloud-storage>=2.10",
        "scikit-learn>=1.3",
        "properscoring",
        "diptest",
    )
    .add_local_dir("scripts", remote_path="/scripts")
)

gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
wandb_secret = modal.Secret.from_name("wandb-creds", required_keys=["WANDB_API_KEY"])
eval_volume = modal.Volume.from_name("properlytic-checkpoints")


@app.function(
    image=inference_image,
    gpu="A10G",  # Inference can use smaller GPUs
    timeout=7200,
    secrets=[gcs_secret, wandb_secret],
    volumes={"/output": eval_volume},
)
def evaluate_checkpoints(
    jurisdiction: str = "sf_ca",
    bucket_name: str = "properlytic-raw-data",
    origin: int = 2019,
    sample_size: int = 20_000,
    scenarios: int = 128,
    version_tag: str = "v11",
    eta: float = 0.0,
    diff_steps: int = 20,
    tau: float = 1.0,
):
    import os, json, time, tempfile, glob, pickle
    import wandb
    import numpy as np
    import polars as pl
    import torch
    from scipy.stats import kstest, spearmanr
    
    ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")

    # ─── Auth ───
    creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON", "")
    if creds_json:
        with open("/tmp/gcs_creds.json", "w") as f:
            f.write(creds_json)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/gcs_creds.json"
    
    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # ─── 1. Access WorldModel Code from Local Mount ───
    print(f"[{ts()}] Getting worldmodel logic from local mount /scripts...")
    import shutil
    try:
        shutil.copy("/scripts/inference/worldmodel.py", "/tmp/worldmodel.py")
        print(f"[{ts()}] Successfully copied source files to /tmp/")
    except FileNotFoundError as e:
        print(f"[{ts()}] Error: Could not find script. {e}")
        raise
    
    # ─── 2. Inject Runtime Global Overrides ───
    # We patch constants to point to local Modal paths
    patch_code = f"""
import os
os.environ["JURISDICTION"] = "{jurisdiction}"
os.environ["WM_MAX_ACCTS"] = "{sample_size}"
os.environ["WM_PANEL_OVERRIDE_PATH"] = "/tmp/panel_actuals.parquet"
# Do *not* run build shards or train loops upon import
os.environ["SKIP_WM_MAIN"] = "1"
os.environ["INFERENCE_ONLY"] = "1"
"""
    with open("/tmp/worldmodel.py", "r") as f:
        wm_code = f.read()
        
    modified_wm_code = patch_code + "\n" + wm_code.replace("if __name__ == '__main__' or globals().get('__colab__'):", "if False:")
    
    # CRITICAL: Patch SimpleScaler.transform to handle dimension mismatches
    # When checkpoint was trained with N features but panel has M features,
    # the scaler arrays (N,) can't broadcast with input (batch, M).
    # This makes transform auto-align by padding/truncating scaler arrays.
    dim_safe_patch = '''
import numpy as _np_patch
_original_SimpleScaler = SimpleScaler
class _DimSafeScaler:
    def __init__(self, mean, scale):
        self.mean = _np_patch.asarray(mean).ravel()
        self.scale = _np_patch.asarray(scale).ravel()
    def transform(self, x):
        d = x.shape[-1] if hasattr(x, 'shape') else len(x)
        m, s = self.mean, self.scale
        if len(m) < d:
            m = _np_patch.concatenate([m, _np_patch.zeros(d - len(m))])
            s = _np_patch.concatenate([s, _np_patch.ones(d - len(s))])
        elif len(m) > d:
            m = m[:d]
            s = s[:d]
        return (x - m) / _np_patch.maximum(s, 1e-8)
    def inverse_transform(self, x):
        d = x.shape[-1] if hasattr(x, 'shape') else len(x)
        m, s = self.mean, self.scale
        if len(m) < d:
            m = _np_patch.concatenate([m, _np_patch.zeros(d - len(m))])
            s = _np_patch.concatenate([s, _np_patch.ones(d - len(s))])
        elif len(m) > d:
            m = m[:d]
            s = s[:d]
        return x * s + m
SimpleScaler = _DimSafeScaler
'''
    # Insert the patch AFTER SimpleScaler is defined but BEFORE it's used
    # Find the class definition and inject after it
    if 'class SimpleScaler' in modified_wm_code:
        # Insert after the class definition ends (find next class or top-level def)
        import re as _re
        _match = _re.search(r'class SimpleScaler.*?(?=\nclass |\ndef [a-zA-Z]|\n[A-Z_]+\s*=)', modified_wm_code, _re.DOTALL)
        if _match:
            insert_pos = _match.end()
            modified_wm_code = modified_wm_code[:insert_pos] + "\n" + dim_safe_patch + "\n" + modified_wm_code[insert_pos:]
            print(f"[{ts()}] Injected dimension-safe SimpleScaler patch")
        else:
            # Fallback: just append after full code
            modified_wm_code += "\n" + dim_safe_patch
            print(f"[{ts()}] Appended dimension-safe SimpleScaler patch (fallback)")
    
    # Robust path patching: regex-replace ANY PANEL_PATH assignment
    import re
    modified_wm_code = re.sub(
        r'^PANEL_PATH_LOCAL\s*=\s*.*$',
        'PANEL_PATH_LOCAL = "/tmp/panel_actuals.parquet"',
        modified_wm_code, flags=re.MULTILINE
    )
    modified_wm_code = re.sub(
        r'^PANEL_PATH_DRIVE\s*=\s*.*$',
        'PANEL_PATH_DRIVE = "/tmp/panel_actuals.parquet"',
        modified_wm_code, flags=re.MULTILINE
    )
    modified_wm_code = re.sub(
        r'^PANEL_PATH\s*=\s*(?!.*\bpl\.).*$',
        'PANEL_PATH = "/tmp/panel_actuals.parquet"',
        modified_wm_code, flags=re.MULTILINE
    )
    # Force bypassing the explicit panel existence check
    modified_wm_code = modified_wm_code.replace('if not os.path.exists(PANEL_PATH):', 'if False:')
    
    # ─── 2.5 Load and Format Panel (Must happen BEFORE worldmodel exec) ───
    print(f"[{ts()}] Loading {jurisdiction} ground truth from panel...")
    panel_blob_path = f"panel/jurisdiction={jurisdiction}/part.parquet"
    if jurisdiction == "all":
        panel_blob_path = "panel/grand_panel/part.parquet"
        
    local_panel_path = "/tmp/panel_actuals.parquet"
    bucket.blob(panel_blob_path).download_to_filename(local_panel_path)
    df_actuals = pl.read_parquet(local_panel_path)
    
    print(f"[{ts()}] Panel loaded: {len(df_actuals):,} rows, columns: {list(df_actuals.columns[:10])}")
    
    # Only filter by jurisdiction for grand_panel (multi-jurisdiction). 
    # Single-jurisdiction panels are already filtered by GCS path.
    if jurisdiction == "all":
        pass  # grand_panel: keep all
    elif "jurisdiction" in df_actuals.columns:
        unique_jurs = df_actuals["jurisdiction"].unique().to_list()
        if len(unique_jurs) > 1:
            # Multi-jurisdiction panel: filter
            df_actuals = df_actuals.filter(pl.col("jurisdiction") == jurisdiction)
            print(f"[{ts()}] Filtered to jurisdiction={jurisdiction}: {len(df_actuals):,} rows")
        else:
            print(f"[{ts()}] Single-jurisdiction panel (value={unique_jurs}), skipping filter")

    # Clean Census suppression values (-666666666)
    for _col in df_actuals.columns:
        if df_actuals[_col].dtype in (pl.Float64, pl.Float32, pl.Int64, pl.Int32):
            df_actuals = df_actuals.with_columns(
                pl.when(pl.col(_col) < -600_000_000).then(None).otherwise(pl.col(_col)).alias(_col)
            )

    # ─── Pre-rename: Coalesce missing columns before mapping ───
    # Support ACS median_home_value as property_value
    if "property_value" not in df_actuals.columns:
        if "median_home_value" in df_actuals.columns:
            df_actuals = df_actuals.with_columns(pl.col("median_home_value").alias("property_value"))
            print(f"[{ts()}] Derived property_value from median_home_value (ACS)")
        elif "sale_price" in df_actuals.columns:
            df_actuals = df_actuals.with_columns(pl.col("sale_price").alias("property_value"))
            print(f"[{ts()}] Derived property_value from sale_price")
        elif "assessed_value" in df_actuals.columns:
            df_actuals = df_actuals.with_columns(pl.col("assessed_value").alias("property_value"))
            print(f"[{ts()}] Derived property_value from assessed_value")
        elif "tot_appr_val" in df_actuals.columns:
            df_actuals = df_actuals.with_columns(pl.col("tot_appr_val").alias("property_value"))
            print(f"[{ts()}] Using existing tot_appr_val as property_value")
    
    # Derive parcel_id from geoid if missing (ACS)
    if "parcel_id" not in df_actuals.columns and "acct" not in df_actuals.columns:
        if "geoid" in df_actuals.columns:
            df_actuals = df_actuals.with_columns(pl.col("geoid").alias("parcel_id"))
            print(f"[{ts()}] Using geoid as parcel_id (ACS)")
    
    # Ensure year exists (fallback from sale_date)
    if "year" not in df_actuals.columns and "yr" not in df_actuals.columns:
        if "sale_date" in df_actuals.columns:
            df_actuals = df_actuals.with_columns(
                pl.col("sale_date").cast(pl.Utf8).str.slice(0, 4).cast(pl.Int64, strict=False).alias("year")
            )
            print(f"[{ts()}] Derived year from sale_date")
    
    # Filter to valid appraisal values and map to standard WorldModel canonical mappings
    rename_map = {
        "parcel_id": "acct",
        "year": "yr",
        "property_value": "tot_appr_val",
        "sqft": "living_area",
        "land_area": "land_ar",
        "year_built": "yr_blt",
        "bedrooms": "bed_cnt",
        "bathrooms": "full_bath",
        "stories": "nbr_story",
        "lat": "gis_lat",
        "lon": "gis_lon",
    }
    actual_renames = {k: v for k, v in rename_map.items() if k in df_actuals.columns}
    
    # Drop any existing columns that clash with our target names
    drop_targets = [v for k, v in actual_renames.items() if v in df_actuals.columns]
    if drop_targets:
        df_actuals = df_actuals.drop(drop_targets)

    df_actuals = df_actuals.rename(actual_renames)
    
    # Ensure tot_appr_val exists after rename
    if "tot_appr_val" not in df_actuals.columns:
        # Last resort: find any numeric column that looks like a value
        val_candidates = [c for c in df_actuals.columns if any(v in c.lower() for v in ["val", "price", "amount"])]
        if val_candidates:
            df_actuals = df_actuals.with_columns(pl.col(val_candidates[0]).alias("tot_appr_val"))
            print(f"[{ts()}] Used '{val_candidates[0]}' as tot_appr_val")
        else:
            print(f"[{ts()}] ❌ No value column found. Columns: {df_actuals.columns}")
            return
    
    # Ensure tot_appr_val is numeric (may be string from some panel builds)
    if "tot_appr_val" in df_actuals.columns:
        val_dtype = df_actuals["tot_appr_val"].dtype
        val_nulls = df_actuals["tot_appr_val"].null_count()
        print(f"[{ts()}] tot_appr_val diagnostics: dtype={val_dtype}, nulls={val_nulls}/{len(df_actuals)}")
        if val_dtype == pl.Utf8:
            df_actuals = df_actuals.with_columns(
                pl.col("tot_appr_val").str.replace_all(",", "").str.replace_all("£", "").str.replace_all("$", "").str.replace_all("€", "")
                .cast(pl.Float64, strict=False).alias("tot_appr_val")
            )
            print(f"[{ts()}] Cast tot_appr_val from Utf8 -> Float64")
        elif val_dtype not in (pl.Float64, pl.Float32, pl.Int64, pl.Int32):
            df_actuals = df_actuals.with_columns(pl.col("tot_appr_val").cast(pl.Float64, strict=False))
            print(f"[{ts()}] Cast tot_appr_val from {val_dtype} -> Float64")
    
    df_actuals = df_actuals.filter(pl.col("tot_appr_val").is_not_null() & (pl.col("tot_appr_val") > 0))

    # CRITICAL: Drop leaky valuation columns — must match train_modal.py preprocessing
    # Without this, worldmodel discovers extra features, causing position misalignment
    leaky_cols = ["sale_price", "property_value", "assessed_value", "land_value", "improvement_value", "median_home_value"]
    drop_leaks = [c for c in leaky_cols if c in df_actuals.columns]
    if drop_leaks:
        print(f"[{ts()}] Dropping leaky columns (matching training): {drop_leaks}")
        df_actuals = df_actuals.drop(drop_leaks)

    # Drop all-null columns — must match train_modal.py preprocessing
    null_counts = df_actuals.null_count()
    n_rows = len(df_actuals)
    all_null_cols = [c for c in df_actuals.columns if null_counts[c][0] == n_rows and c not in ("acct", "yr", "tot_appr_val")]
    if all_null_cols:
        print(f"[{ts()}] Dropping {len(all_null_cols)} all-null columns: {all_null_cols}")
        df_actuals = df_actuals.drop(all_null_cols)

    # Write back the properly formatted panel for worldmodel to consume
    # Ensure yr column is properly typed (enrichment may have converted int->float)
    if "yr" in df_actuals.columns:
        yr_dtype = df_actuals["yr"].dtype
        yr_null_count = df_actuals["yr"].null_count()
        yr_sample = df_actuals["yr"].drop_nulls().head(5).to_list() if yr_null_count < len(df_actuals) else []
        print(f"[{ts()}] yr diagnostics: dtype={yr_dtype}, nulls={yr_null_count}/{len(df_actuals)}, sample={yr_sample}")
        
        # Cast float years to int (pandas enrichment creates float64 from NaN merges)
        if yr_dtype in (pl.Float64, pl.Float32):
            df_actuals = df_actuals.with_columns(pl.col("yr").cast(pl.Int64, strict=False))
            print(f"[{ts()}] Cast yr from {yr_dtype} -> Int64")
        elif yr_dtype == pl.Utf8:
            df_actuals = df_actuals.with_columns(
                pl.col("yr").cast(pl.Float64, strict=False).cast(pl.Int64, strict=False)
            )
            print(f"[{ts()}] Cast yr from Utf8 -> Int64")
    
    # Filter out bad year rows
    df_actuals = df_actuals.filter(pl.col("yr").is_not_null() & (pl.col("yr") >= 1990))
    if df_actuals["acct"].dtype != pl.Utf8:
        df_actuals = df_actuals.with_columns(pl.col("acct").cast(pl.Utf8))
    if len(df_actuals) == 0:
        print(f"[{ts()}] ❌ No rows remaining after filtering yr >= 1990. Aborting.")
        return
    df_actuals.write_parquet(local_panel_path)
    
    # Patch MIN_YEAR and MAX_YEAR based on the actual dataset
    yr_min = max(int(df_actuals["yr"].min()), 1990)  # floor to 1990
    yr_max = int(df_actuals["yr"].max())
    print(f"[{ts()}] Patching worldmodel constants: yr_min={yr_min}, yr_max={yr_max}")
    
    modified_wm_code = modified_wm_code.replace("MIN_YEAR = 2005", f"MIN_YEAR = {yr_min}")
    modified_wm_code = modified_wm_code.replace("MAX_YEAR = 2025", f"MAX_YEAR = {yr_max}")
    modified_wm_code = modified_wm_code.replace("SEAM_YEAR = 2025", f"SEAM_YEAR = {yr_max}")
    # Patch S_BLOCK to reduce VRAM requirements during inference
    modified_wm_code = modified_wm_code.replace("S_BLOCK = 9999", "S_BLOCK = 16")
    
    exec_globals = {}
    print(f"[{ts()}] Executing worldmodel.py context...")
    exec(modified_wm_code, exec_globals)

    # Resolve required objects
    lf = exec_globals["lf"]
    num_use_local = exec_globals["num_use"]
    cat_use_local = exec_globals["cat_use"]
    create_denoiser = exec_globals["create_denoiser_v11"]
    
    # New v11 objects required for the Token Diffusion model
    create_gating_network = exec_globals["create_gating_network"]
    create_token_persistence = exec_globals["create_token_persistence"]
    create_coherence_scale = exec_globals["create_coherence_scale"]
    sample_token_paths = exec_globals["sample_token_paths_learned"]
    
    GlobalProjection = exec_globals["GlobalProjection"]
    GeoProjection = exec_globals["GeoProjection"]
    SimpleScaler = exec_globals["SimpleScaler"]
    Scheduler = exec_globals["Scheduler"]
    sample_ddim = exec_globals["sample_ddim_v11"]
    build_inference_context = exec_globals["build_inference_context_chunked_v102"]
    
    _device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"[{ts()}] Context loaded. Device: {_device}")

    # ─── 3. Load Actuals Ground Truth ───
    df_actuals = pl.read_parquet(local_panel_path)
    
    actual_vals = {}
    for yr in [origin] + [origin + h for h in range(1, 6)]:
        yr_df = df_actuals.filter(pl.col("yr") == yr).select(["acct", "tot_appr_val"])
        actual_vals[yr] = dict(zip(yr_df["acct"].to_list(), yr_df["tot_appr_val"].to_list()))
    print(f"[{ts()}] Actuals loaded for origin {origin} and horizon")

    # ─── 4. Inference loop over checkpoints ───
    variant_raw_results = {}
    MAX_HORIZON = 5
    VALUE_BRACKETS = [
        ("<200K",     0,        200_000),
        ("200K-500K", 200_000,  500_000),
        ("500K-1M",   500_000,  1_000_000),
        ("1M+",       1_000_000, 1e18),
        ("ALL",       0,        1e18),
    ]

    _version_tag = version_tag
    run_group = f"sweep_stage1_{_version_tag}_{jurisdiction}"
    
    wandb.init(
        project="homecastr",
        entity="dhardestylewis-columbia-university",
        name=f"{run_group}_o{origin}_eta{eta}_tau{tau}_steps{diff_steps}",
        group=run_group,
        tags=["sweep", _version_tag, jurisdiction, "stage1_calib"],
        config={"sample_size": sample_size, "scenarios": scenarios, "origin": origin, "version": _version_tag, "eta": eta, "tau": tau, "diff_steps": diff_steps},
        job_type="sweep"
    )

    ckpt_dir = f"/output/{jurisdiction}_v11"
    
    import glob
    candidates = glob.glob(os.path.join(ckpt_dir, f"ckpt_v11_origin_{origin}*.pt")) + \
                 glob.glob(os.path.join(ckpt_dir, f"ckpt_origin_{origin}*.pt"))
    
    # GCS fallback: download checkpoint if not in Modal volume
    if not candidates:
        print(f"[{ts()}] Checkpoint not in volume, trying GCS fallback...")
        gcs_ckpt_prefix = f"checkpoints/{jurisdiction}/"
        blobs = list(bucket.list_blobs(prefix=gcs_ckpt_prefix))
        ckpt_blobs = [b for b in blobs if f"origin_{origin}" in b.name and b.name.endswith(".pt")]
        if ckpt_blobs:
            os.makedirs(ckpt_dir, exist_ok=True)
            for cb in ckpt_blobs:
                local_ckpt = os.path.join(ckpt_dir, os.path.basename(cb.name))
                cb.download_to_filename(local_ckpt)
                print(f"[{ts()}] Downloaded GCS checkpoint: {cb.name} ({cb.size/1e6:.1f} MB)")
            candidates = glob.glob(os.path.join(ckpt_dir, f"ckpt_v11_origin_{origin}*.pt")) + \
                         glob.glob(os.path.join(ckpt_dir, f"ckpt_origin_{origin}*.pt"))
    
    if not candidates:
        print(f"[{ts()}] ⚠️ Checkpoint not found for origin {origin} in {ckpt_dir} or GCS, skipping")
        return
    ckpt_path = candidates[0]

    print(f"\n[{ts()}] ── Processing Origin {origin} ({os.path.basename(ckpt_path)}) ──")
    
    import sys
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    
    # Load Model
    ckpt = torch.load(ckpt_path, map_location=_device)
    _cfg = ckpt.get("cfg", {})
    
    def _strip(d): return {k.replace("_orig_mod.", ""): v for k, v in d.items()}
    sd = _strip(ckpt["model_state_dict"])
    hist_len = sd["hist_enc.0.weight"].shape[1]
    num_dim = sd["num_enc.0.weight"].shape[1]
    n_cat = len([k for k in sd if k.startswith("cat_embs.") and k.endswith(".weight")])
    H = int(_cfg.get("H", MAX_HORIZON))

    # CRITICAL: Use checkpoint's saved feature lists if available (best alignment).
    # Fallback: handle v3→v4 feature dimension mismatch.
    _panel_num_dim = len(num_use_local)
    if "num_use" in ckpt and ckpt["num_use"]:
        saved_num = ckpt["num_use"]
        print(f"[{ts()}] Using checkpoint feature list ({len(saved_num)} features): {saved_num[:5]}...")
        num_use_local = saved_num
    elif num_dim > len(num_use_local):
        # v3 checkpoint (33 features) with v4 panel (30 features) — pad with placeholders
        n_pad = num_dim - len(num_use_local)
        print(f"[{ts()}] ⚠️ Checkpoint expects {num_dim} features but panel has {len(num_use_local)}. Padding {n_pad} zero columns.")
        num_use_local = num_use_local + [f"_pad_{i}" for i in range(n_pad)]
    elif num_dim < len(num_use_local):
        print(f"[{ts()}] ⚠️ Truncating num_use: panel has {len(num_use_local)}, checkpoint has {num_dim}.")
        num_use_local = num_use_local[:num_dim]
    if "cat_use" in ckpt and ckpt["cat_use"]:
        cat_use_local = ckpt["cat_use"]

    model = create_denoiser(target_dim=H, hist_len=hist_len, num_dim=num_dim, n_cat=n_cat)
    model.load_state_dict(sd)
    model = model.to(_device).eval()
    
    # v11 Token Diffusion Components
    gating_sd = None
    if "gating_state_dict" in ckpt:
        gating_sd = _strip(ckpt["gating_state_dict"])
    elif "gating_net_state_dict" in ckpt:
        gating_sd = _strip(ckpt["gating_net_state_dict"])
        
    has_macro = gating_sd is not None and "year_emb.weight" in gating_sd
    
    gating_net = create_gating_network(hist_len=hist_len, num_dim=num_dim, n_cat=n_cat, use_macro=has_macro)
    if gating_sd is not None:
        gating_net.load_state_dict(gating_sd)
    gating_net = gating_net.to(_device).eval()
    
    token_persistence = create_token_persistence()
    if "token_persistence_state" in ckpt:
        token_persistence.load_state_dict(ckpt["token_persistence_state"])
    elif "token_persistence_state_dict" in ckpt:
        token_persistence.load_state_dict(ckpt["token_persistence_state_dict"])
        
    coh_scale = create_coherence_scale()
    if "coherence_scale_state" in ckpt:
        coh_scale.load_state_dict(ckpt["coherence_scale_state"])
    elif "coh_scale_state_dict" in ckpt:
        coh_scale.load_state_dict(ckpt["coh_scale_state_dict"])

    # v11 Doesn't have GlobalProjection / GeoProjection
    model._y_scaler = SimpleScaler(mean=np.array(ckpt["y_scaler_mean"]), scale=np.array(ckpt["y_scaler_scale"]))
    _n_mean = np.array(ckpt["n_scaler_mean"])
    _n_scale = np.array(ckpt["n_scaler_scale"])
    # Align scaler dimensions to num_dim (panel may have more/fewer features than scaler)
    if len(_n_mean) < num_dim:
        n_pad = num_dim - len(_n_mean)
        _n_mean = np.concatenate([_n_mean, np.zeros(n_pad)])
        _n_scale = np.concatenate([_n_scale, np.ones(n_pad)])
        print(f"[{ts()}] Padded n_scaler: {len(_n_mean)-n_pad} → {len(_n_mean)}")
    elif len(_n_mean) > num_dim:
        _n_mean = _n_mean[:num_dim]
        _n_scale = _n_scale[:num_dim]
        print(f"[{ts()}] Truncated n_scaler to {num_dim}")
    model._n_scaler = SimpleScaler(mean=_n_mean, scale=_n_scale)
    model._t_scaler = SimpleScaler(mean=np.array(ckpt["t_scaler_mean"]), scale=np.array(ckpt["t_scaler_scale"]))
    global_medians = ckpt.get("global_medians", {})

    # ─── A2: Log per-origin scaler arrays (cross-origin comparison in main()) ───
    _raw_n_mean = np.array(ckpt["n_scaler_mean"], dtype=np.float64)
    _raw_n_scale = np.array(ckpt["n_scaler_scale"], dtype=np.float64)
    for fi in range(min(len(_raw_n_mean), 10)):  # log top-10 features for W&B visibility
        wandb.log({
            f"scaler/n_mean_f{fi}/o{origin}": float(_raw_n_mean[fi]),
            f"scaler/n_scale_f{fi}/o{origin}": float(_raw_n_scale[fi]),
        })
    print(f"[{ts()}] A2: Logged scaler arrays ({len(_raw_n_mean)} features) for cross-origin comparison")

    # Context Setup
    origin_accts = list(actual_vals.get(origin, {}).keys())
    if len(origin_accts) < 100:
        print(f"[{ts()}] ⚠️ Insufficient ground truth at origin {origin}, skipping")
        return

    np.random.seed(42 + origin)
    sample_accts = np.random.choice(origin_accts, min(sample_size, len(origin_accts)), replace=False).tolist()

    ctx = build_inference_context(
        lf=lf, accts=sample_accts, num_use_local=num_use_local, cat_use_local=cat_use_local,
        global_medians=global_medians, anchor_year=origin, max_parcels=len(sample_accts)
    )
    
    # Align cur_num dimensions to match checkpoint's num_dim
    actual_dim = ctx["cur_num"].shape[1]
    if actual_dim < num_dim:
        n_pad = num_dim - actual_dim
        pad = torch.zeros(ctx["cur_num"].shape[0], n_pad, dtype=ctx["cur_num"].dtype, device=ctx["cur_num"].device)
        ctx["cur_num"] = torch.cat([ctx["cur_num"], pad], dim=1)
        print(f"[{ts()}] Padded cur_num: {actual_dim} → {ctx['cur_num'].shape[1]} (added {n_pad} zero cols)")
    elif actual_dim > num_dim:
        ctx["cur_num"] = ctx["cur_num"][:, :num_dim]
        print(f"[{ts()}] Truncated cur_num: {actual_dim} → {num_dim}")
    
    n_valid = len(ctx["acct"])
    print(f"[{ts()}] Built context for {n_valid:,} valid parcels")

    # Scenarios
    sched = Scheduler(int(_cfg.get("DIFF_STEPS_TRAIN", 128)), device=_device)
    
    # Sample shared latent shock paths
    phi_vec = token_persistence.get_phi()
    Z_tokens = sample_token_paths(K=int(_cfg.get("K_TOKENS", 8)), H=H, phi_vec=phi_vec, S=scenarios, device=_device)

    # ── Dimension diagnostics & force-alignment ──
    # Truncate hist_y to FULL_HIST_LEN (ACS may have more years than checkpoint expects)
    _full_hist_len = int(_cfg.get("FULL_HIST_LEN", 15))
    if ctx["hist_y"].shape[1] > _full_hist_len:
        print(f"[{ts()}] Truncating hist_y: {ctx['hist_y'].shape[1]} → {_full_hist_len} (keeping most recent)")
        ctx["hist_y"] = ctx["hist_y"][:, -_full_hist_len:]
    
    # Find gating network input dimension from state dict
    _gating_in_dim = None
    for k, v in sd.items():
        if "gating" in k.lower() and "0.weight" in k and v.dim() == 2:
            _gating_in_dim = v.shape[1]
            break
    
    _hist_dim = ctx["hist_y"].shape[1]
    _num_dim_actual = ctx["cur_num"].shape[1]
    _cat_dim = ctx["cur_cat"].shape[1] if len(ctx["cur_cat"].shape) > 1 else 1
    _reg_dim = ctx["region_id"].shape[1] if len(ctx["region_id"].shape) > 1 else 1
    _total = _hist_dim + _num_dim_actual + _cat_dim + _reg_dim
    print(f"[{ts()}] Dimension check: hist_y={_hist_dim} cur_num={_num_dim_actual} cur_cat={_cat_dim} region_id={_reg_dim} total={_total} gating_expects={_gating_in_dim}")
    
    # Force cur_num to exactly match gating network expectations
    if _gating_in_dim is not None:
        _expected_num = _gating_in_dim - _hist_dim - _cat_dim - _reg_dim
        if _expected_num > 0 and _num_dim_actual != _expected_num:
            print(f"[{ts()}] Force-aligning cur_num: {_num_dim_actual} → {_expected_num}")
            if _num_dim_actual < _expected_num:
                pad = torch.zeros(ctx["cur_num"].shape[0], _expected_num - _num_dim_actual, dtype=ctx["cur_num"].dtype, device=ctx["cur_num"].device)
                ctx["cur_num"] = torch.cat([ctx["cur_num"], pad], dim=1)
            else:
                ctx["cur_num"] = ctx["cur_num"][:, :_expected_num]
    
    # Configure generation parameters from sweep kwargs
    import os
    os.environ["SAMPLER_ETA"] = str(eta)
    os.environ["DIFF_STEPS_SAMPLE"] = str(diff_steps)

    # Inference chunking (with NaN/clip telemetry guardrail)
    batch_size = min(256, n_valid)
    all_deltas = []
    _nan_batches = 0
    _inf_batches = 0
    _total_batches = 0
    for b_start in range(0, n_valid, batch_size):
        b_end = min(b_start + batch_size, n_valid)
        b_deltas = sample_ddim(
            model=model, gating_net=gating_net, sched=sched,
            hist_y_b=ctx["hist_y"][b_start:b_end], cur_num_b=ctx["cur_num"][b_start:b_end],
            cur_cat_b=ctx["cur_cat"][b_start:b_end], region_id_b=ctx["region_id"][b_start:b_end],
            Z_tokens=Z_tokens, coh_scale=coh_scale, device=_device, tau=tau,
        )
        _total_batches += 1
        if np.any(np.isnan(b_deltas)):
            _nan_batches += 1
            b_deltas = np.nan_to_num(b_deltas, nan=0.0)
        if np.any(np.isinf(b_deltas)):
            _inf_batches += 1
            b_deltas = np.clip(b_deltas, -10, 10)
        all_deltas.append(b_deltas)
    if _nan_batches > 0 or _inf_batches > 0:
        print(f"[{ts()}] ⚠️ Sampling telemetry: {_nan_batches}/{_total_batches} NaN batches, {_inf_batches}/{_total_batches} Inf batches")
    wandb.log({
        f"telemetry/nan_batch_frac/o{origin}": _nan_batches / max(_total_batches, 1),
        f"telemetry/inf_batch_frac/o{origin}": _inf_batches / max(_total_batches, 1),
    })

    deltas = np.concatenate(all_deltas, axis=0)
    
    # ── NEW: Operational Stability Gates (Global) ──
    # Non-finite rate
    non_finite_rate = float(1.0 - np.isfinite(deltas).mean())
    # Outlier rate: log-delta magnitude > 1.0 (approx 170% growth)
    outlier_rate = float((np.abs(deltas) > 1.0).mean())
    wandb.log({
        f"stability/non_finite_rate/o{origin}": non_finite_rate,
        f"stability/outlier_rate/o{origin}": outlier_rate,
    })
    print(f"[{ts()}] Stability gates: non-finite={non_finite_rate:.2e}, outlier={outlier_rate:.2e}")
    
    accts = ctx["acct"]
    ya = ctx["y_anchor"]
    y_levels = ya[:, None, None] + np.cumsum(deltas, axis=2)  # [N, S, H] log-space

    # ─── 4.5 POST-HOC PIT CALIBRATION (Stage 1 OOT) ───
    print(f"\n[{ts()}] ── Post-hoc PIT Calibration (OOT) ──")
    from sklearn.isotonic import IsotonicRegression
    import pickle
    import os
    
    calib_dir = f"/output/{jurisdiction}_{version_tag}"
    os.makedirs(calib_dir, exist_ok=True)
    
    # We will ALWAYS fit a new calibrator on the current origin's FULL dataset
    # and save it for the next origin.
    # We will ONLY apply calibration if a previous origin's calibrator exists.
    
    prev_origin = origin - 1
    calib_load_path = os.path.join(calib_dir, f"calibrators_{version_tag}_{jurisdiction}_o{prev_origin}.pkl")
    calib_save_path = os.path.join(calib_dir, f"calibrators_{version_tag}_{jurisdiction}_o{origin}.pkl")
    
    calib_models = {}
    # Reload Volume to pick up calibrators saved by previous origin's container
    try:
        eval_volume.reload()
        print(f"[{ts()}] Volume reloaded")
    except Exception as ve:
        print(f"[{ts()}] \u26a0\ufe0f Volume reload failed: {ve}")
    if os.path.exists(calib_load_path):
        try:
            with open(calib_load_path, "rb") as f:
                calib_models = pickle.load(f)
            print(f"[{ts()}] \U0001f504 Loaded {len(calib_models)} calibrators from OOT origin {prev_origin}")
        except Exception as e:
            print(f"[{ts()}] \u26a0\ufe0f Failed to load OOT calibrator {calib_load_path}: {e}")
    else:
        print(f"[{ts()}] \u26a0\ufe0f No OOT calibrator found at {calib_load_path}. Will evaluate uncalibrated.")

    y_levels_calib = y_levels.copy()
    y_levels_test = y_levels.copy()
    accts_calib = accts
    accts_test = accts
    ya_calib = ya
    ya_test = ya
    
    base_v = actual_vals.get(origin, {})
    
    print(f"[{ts()}] OOT Split: Fitting on fully OOT origin {origin}")
    
    # Start with existing calibrators from previous origins (to preserve long horizons)
    # and overwrite with newly fitted short horizons.
    new_calib_models = calib_models.copy() if calib_models else {}
    for h in range(1, MAX_HORIZON + 1):
        h_idx = h - 1
        eyr = origin + h
        if eyr not in actual_vals:
            continue
        future_v = actual_vals[eyr]
        
        for bkt_label, bkt_lo, bkt_hi in VALUE_BRACKETS:
            # Collect PITs for fitting
            pits_calib = []
            for i in range(len(accts_calib)):
                acct = str(accts_calib[i]).strip()
                bv = base_v.get(acct, 0)
                av = future_v.get(acct)
                if bv <= 0 or av is None or not (bkt_lo <= bv < bkt_hi):
                    continue
                fan_prices = np.exp(y_levels_calib[i, :, h_idx])
                pits_calib.append(float(np.mean(fan_prices <= av)))
            
            if len(pits_calib) > 50:
                sorted_pits = np.sort(pits_calib)
                empirical_cdf = np.arange(1, len(sorted_pits) + 1) / len(sorted_pits)
                ir = IsotonicRegression(out_of_bounds='clip')
                ir.fit(empirical_cdf, sorted_pits)
                new_calib_models[(h, bkt_label)] = ir
                print(f"  [Calib Fit] h={h} bkt={bkt_label}: fit {len(pits_calib)} samples for FUTURE origin")
            
    try:
        with open(calib_save_path, "wb") as f:
            pickle.dump(new_calib_models, f)
        print(f"[{ts()}] \U0001f3c6 Saved {len(new_calib_models)} calibrators to {calib_save_path}")

        # Also save a consolidated production copy (no _o{origin} suffix)
        # so inference_pipeline.py can auto-discover it as the latest calibrator.
        calib_prod_path = os.path.join(calib_dir, f"calibrators_{version_tag}_{jurisdiction}.pkl")
        with open(calib_prod_path, "wb") as f:
            pickle.dump(new_calib_models, f)
        print(f"[{ts()}] \U0001f4e6 Saved consolidated production calibrators to {calib_prod_path}")

        # Flush to Modal Volume so next container can load these calibrators
        try:
            eval_volume.commit()
            print(f"[{ts()}] \U0001f4be Volume committed — calibrators visible to next origin")
        except Exception as ve:
            print(f"[{ts()}] \u26a0\ufe0f Volume commit failed: {ve}")
    except Exception as e:
        print(f"[{ts()}] \u26a0\ufe0f Failed to save calibrators: {e}")

    if calib_models:
        scenarios_count = y_levels_test.shape[1]
        sorted_p = (np.arange(scenarios_count) + 0.5) / scenarios_count 
        
        for h in range(1, MAX_HORIZON + 1):
            h_idx = h - 1
            eyr = origin + h
            if eyr not in actual_vals:
                continue
                
            for bkt_label, bkt_lo, bkt_hi in VALUE_BRACKETS:
                if (h, bkt_label) not in calib_models:
                    continue
                    
                calibrator = calib_models[(h, bkt_label)]
                remapped_p = calibrator.predict(sorted_p)
                remapped_p = np.clip(remapped_p, 0.001, 0.999)
                
                for i in range(len(accts_test)):
                    acct = str(accts_test[i]).strip()
                    bv = base_v.get(acct, 0)
                    if bv <= 0 or not (bkt_lo <= bv < bkt_hi):
                        continue
                    
                    fan = y_levels_test[i, :, h_idx]
                    sort_idx = np.argsort(fan)
                    sorted_fan = fan[sort_idx]
                    new_sorted_fan = np.interp(remapped_p, sorted_p, sorted_fan)
                    
                    reverse_sort_idx = np.empty_like(sort_idx)
                    reverse_sort_idx[sort_idx] = np.arange(scenarios_count)
                    y_levels_test[i, :, h_idx] = new_sorted_fan[reverse_sort_idx]

    # Replace the evaluation data with the TEST SET (which may or may not be calibrated)
    accts = accts_test
    ya = ya_test
    uncalibrated_medians = np.median(y_levels, axis=1) # [N, H] before overwriting y_levels
    y_levels = y_levels_test
    y_anchor = ya_test
    base_vals_arr = np.array([base_v.get(str(a).strip(), 0) for a in accts])
    
    variant_raw_results[("v11", origin)] = {
        "accts": list(accts),
        "y_anchor": ya,
        "y_levels": y_levels,
        "uncalibrated_medians": uncalibrated_medians,
        "base_val": base_vals_arr,
        "deltas": y_levels - ya[:, None, None], # Approximate deltas
    }

    # ─── 5. W&B Logging Metrics Engine ───
    print(f"\n[{ts()}] ── Computing and Logging Metric to W&B (Calibrated OUT-OF-SAMPLE) ──")
    
    key = ("v11", origin)
    if key in variant_raw_results:
        res = variant_raw_results[key]
        y_levels = res["y_levels"]
        uncalibrated_medians = res.get("uncalibrated_medians")
        y_anchor = res["y_anchor"]
        accts = res["accts"]
        base_v = actual_vals.get(origin, {})
        
        for h in range(1, MAX_HORIZON + 1):
            h_idx = h - 1
            eyr = origin + h
            if eyr not in actual_vals:
                continue
                
            future_v = actual_vals[eyr]
            
            for bkt_label, bkt_lo, bkt_hi in VALUE_BRACKETS:
                fan_widths = []
                pits = []
                preds = []
                acts = []
                fan_hits = 0
                fan_checks = 0
                crps_vals = []   # per-parcel CRPS (dollar space)
                crps_log_vals = []  # v11.1: per-parcel CRPS (log space) — robust to outliers
                int_scores = []  # interval scores
                
                for i in range(len(accts)):
                    acct = str(accts[i]).strip()
                    bv = base_v.get(acct, 0)
                    av = future_v.get(acct)
                    
                    if bv <= 0 or av is None or not (bkt_lo <= bv < bkt_hi):
                        continue
                        
                    fan = y_levels[i, :, h_idx]
                    p10 = np.expm1(np.percentile(fan, 10))
                    p50 = np.expm1(np.percentile(fan, 50))
                    p90 = np.expm1(np.percentile(fan, 90))
                    
                    if p50 > 0:
                        fan_widths.append((p90 - p10) / p50 * 100)
                        
                    fan_prices = np.exp(fan)
                    pits.append(float(np.mean(fan_prices <= av)))
                    if uncalibrated_medians is not None:
                        pred_growth = float(np.expm1(uncalibrated_medians[i, h_idx] - y_anchor[i]) * 100)
                    else:
                        pred_growth = float(np.expm1(np.nanmedian(fan) - y_anchor[i]) * 100)
                    actual_growth = float((av - bv) / bv * 100)
                    preds.append(pred_growth)
                    acts.append(actual_growth)
                    
                    # NEW: quantile monotonicity check
                    qp = np.percentile(fan, [10, 25, 50, 75, 90])
                    if not np.all(np.diff(qp) >= 0):
                        print(f"[{ts()}] ⚠️ Quantile monotonicity violation for {acct} h={h}")
                    
                    # Coverage: does actual fall within P10-P90 fan? (dollar-space)
                    p10_dollar = np.exp(np.nanpercentile(fan, 10))
                    p90_dollar = np.exp(np.nanpercentile(fan, 90))
                    fan_checks += 1
                    if p10_dollar <= av <= p90_dollar:
                        fan_hits += 1
                    
                    # CRPS (dollar space) — proper scoring rule
                    try:
                        from properscoring import crps_ensemble
                        _crps = crps_ensemble(av, fan_prices)
                        crps_vals.append(float(_crps) / max(bv, 1) * 100)  # normalize as % of base
                        # v11.1: log-space CRPS — immune to outlier contamination
                        _crps_log = crps_ensemble(np.log1p(av), fan)  # fan is already in log1p space
                        crps_log_vals.append(float(_crps_log) * 100)  # as percentage points
                    except Exception:
                        pass
                    
                    # Interval Score (penalizes miscalibration AND width)
                    alpha = 0.20  # 80% interval
                    width = p90_dollar - p10_dollar
                    penalty_lo = (2.0/alpha) * max(0, p10_dollar - av)
                    penalty_hi = (2.0/alpha) * max(0, av - p90_dollar)
                    int_scores.append(float((width + penalty_lo + penalty_hi) / max(bv, 1) * 100))
                    
                tag = f"o{origin}_h{h}_{bkt_label}"
                preds_arr = np.array(preds)
                acts_arr = np.array(acts)
                
                if fan_widths:
                    avg_fw = float(np.mean(fan_widths))
                    std_fw = float(np.std(fan_widths))
                    wandb.log({f"eval/fan_width/{tag}": avg_fw, f"eval/fan_std/{tag}": std_fw})
                
                if len(pits) > 20:
                    med_pit = float(np.median(pits))
                    ks = float(kstest(pits, 'uniform').statistic)
                    # Handle constant predictions which cause spearmanr to return NaN/warnings
                    if len(set(preds)) > 1 and len(set(acts)) > 1:
                        rho, rho_p = spearmanr(preds, acts)
                        rho = float(rho)
                        rho_p = float(rho_p)
                    else:
                        rho = 0.0
                        rho_p = 1.0
                    
                    # ── New metrics from backtest.py / deep_variant_analysis.py ──
                    abs_err = np.abs(preds_arr - acts_arr)
                    mdae = float(np.median(abs_err))
                    mae = float(np.mean(abs_err))
                    # MAPE (from inference_pipeline.py)
                    nonzero_acts = acts_arr[acts_arr != 0]
                    nonzero_preds = preds_arr[acts_arr != 0]
                    mape = float(np.mean(np.abs(nonzero_preds - nonzero_acts) / np.abs(nonzero_acts))) if len(nonzero_acts) > 0 else float('nan')
                    # Bias: median pred - median actual (from deep_variant_analysis.py)
                    bias = float(np.median(preds_arr) - np.median(acts_arr))
                    # % negative growth predictions (from compare_checkpoint_variants.py)
                    pct_neg = float(np.mean(preds_arr < 0) * 100)
                    # Median predicted growth (from compare_checkpoint_variants.py)
                    med_growth = float(np.median(preds_arr))
                    # Coverage % (from deep_variant_analysis.py + inference_pipeline.py)
                    coverage = float(fan_hits / fan_checks * 100) if fan_checks > 0 else float('nan')
                    
                    # CRPS and Interval Score
                    crps_mean = float(np.mean(crps_vals)) if crps_vals else float('nan')
                    crps_med = float(np.median(crps_vals)) if crps_vals else float('nan')
                    crps_log_mean = float(np.mean(crps_log_vals)) if crps_log_vals else float('nan')
                    crps_log_med = float(np.median(crps_log_vals)) if crps_log_vals else float('nan')
                    int_score_mean = float(np.mean(int_scores)) if int_scores else float('nan')
                    
                    # PIT histogram (20 bins) — for calibration visualization
                    pit_hist, _ = np.histogram(pits, bins=20, range=(0, 1))
                    pit_hist_norm = pit_hist / max(pit_hist.sum(), 1)  # normalize
                    # Reliability = sum of squared deviations from uniform
                    pit_reliability = float(np.sum((pit_hist_norm - 0.05)**2) * 20)  # 0=perfect
                    
                    # ── NEW: Conditional Calibration on Predicted Width ──
                    # Divide parcels into 10 deciles of predicted width (fan width %)
                    if len(fan_widths) > 20 and len(acts) > 20:
                        fan_arr = np.array(fan_widths)
                        # Recompute hits precisely for valid rows
                        hits_arr = np.array([(np.exp(np.nanpercentile(y_levels[idx, :, h_idx], 10)) <= future_v.get(str(accts[idx]).strip(), 0) <= np.exp(np.nanpercentile(y_levels[idx, :, h_idx], 90))) 
                                             for idx in range(len(accts)) 
                                             if base_v.get(str(accts[idx]).strip(), 0) > 0 and future_v.get(str(accts[idx]).strip()) is not None and (bkt_lo <= base_v.get(str(accts[idx]).strip(), 0) < bkt_hi) and np.expm1(np.percentile(y_levels[idx, :, h_idx], 50)) > 0])
                        
                        try:
                            # Bin into 10 quantiles of width
                            deciles = np.percentile(fan_arr, np.linspace(0, 100, 11))
                            decile_widths = []
                            decile_covs = []
                            for d in range(10):
                                mask = (fan_arr >= deciles[d]) & (fan_arr <= deciles[d+1])
                                if mask.sum() > 0:
                                    decile_widths.append(fan_arr[mask].mean())
                                    decile_covs.append(hits_arr[mask].mean() * 100) # percentage
                            
                            # Fit line: coverage = intercept + slope * width
                            from scipy.stats import linregress
                            if len(decile_widths) > 2:
                                slope, intercept, r_value, p_value, std_err = linregress(decile_widths, decile_covs)
                                wandb.log({
                                    f"eval/cond_calib_slope/{tag}": float(slope),
                                    f"eval/cond_calib_intercept/{tag}": float(intercept),
                                })
                        except Exception as elr:
                           print(f"[{ts()}] Error computing conditional calibration: {elr}")

                    
                    wandb.log({
                        # Original 3 core metrics
                        f"eval/rho/{tag}": rho,
                        f"eval/pit_med/{tag}": med_pit,
                        f"eval/pit_ks/{tag}": ks,
                        # Accuracy metrics
                        f"eval/mdae/{tag}": mdae,
                        f"eval/mae/{tag}": mae,
                        f"eval/mape/{tag}": mape,
                        # Calibration & bias
                        f"eval/coverage/{tag}": coverage,
                        f"eval/bias/{tag}": bias,
                        f"eval/pct_neg/{tag}": pct_neg,
                        f"eval/med_growth/{tag}": med_growth,
                        # Significance
                        f"eval/rho_p/{tag}": rho_p,
                        # ── NEW: Proper scoring rules ──
                        f"eval/crps/{tag}": crps_mean,
                        f"eval/crps_med/{tag}": crps_med,
                        f"eval/crps_log/{tag}": crps_log_mean,
                        f"eval/crps_log_med/{tag}": crps_log_med,
                        f"eval/interval_score/{tag}": int_score_mean,
                        # ── NEW: Calibration decomposition ──
                        f"eval/pit_reliability/{tag}": pit_reliability,
                    })
                    # PIT histogram bins (for visualization)
                    for b_idx in range(20):
                        wandb.log({f"eval/pit_hist/{tag}_bin{b_idx}": float(pit_hist_norm[b_idx])})
                    
                    print(f"  [{tag}] ρ:{rho:+.3f} MdAE:{mdae:.1f}% CRPS:{crps_mean:.2f}% CRPS_log:{crps_log_mean:.3f}% IS:{int_score_mean:.1f}% Covg:{coverage:.1f}% Bias:{bias:+.1f}pp PIT_rel:{pit_reliability:.3f} (n={len(pits)})")
    
    # ─── Spatial Coherence (Cross-Parcel Correlation) ───
    key = ("v11", origin)
    if key in variant_raw_results:
        y_levels = variant_raw_results[key]["y_levels"]
        N = y_levels.shape[0]
        
        for h in [1, 3, 5]:
            max_p = min(500, N)
            idx = np.random.choice(N, max_p, replace=False) if N > max_p else np.arange(N)
            y_h = y_levels[idx, :, h - 1]
            corr_mat = np.corrcoef(y_h)
            corrs = corr_mat[np.triu_indices(len(idx), k=1)]
            corrs = corrs[np.isfinite(corrs)]
            if len(corrs) > 0:
                wandb.log({
                    f"eval/parcel_corr/o{origin}_h{h}": float(np.mean(corrs)),
                    f"eval/parcel_corr_std/o{origin}_h{h}": float(np.std(corrs))
                })
    
    # ─── NEW: Scenario Diversity Metrics ───
    if key in variant_raw_results:
        deltas = variant_raw_results[key]["deltas"]  # [N, S, H]
        N, S, H_dim = deltas.shape
        for h in range(min(H_dim, 5)):
            # Inter-scenario std (averaged across parcels)
            scenario_std = np.std(deltas[:, :, h], axis=1)  # [N]
            # Pairwise scenario correlation (sample 50 parcels)
            samp = min(50, N)
            idx = np.random.choice(N, samp, replace=False)
            scenario_corrs = []
            for ii in idx:
                sc = deltas[ii, :, h]  # [S]
                if np.std(sc) > 1e-10:
                    # Correlation between this parcel's scenarios and its neighbors'
                    jj = np.random.choice(N, 1)[0]
                    if np.std(deltas[jj, :, h]) > 1e-10:
                        c = np.corrcoef(sc, deltas[jj, :, h])[0, 1]
                        if np.isfinite(c):
                            scenario_corrs.append(c)
            wandb.log({
                f"eval/scenario_std/o{origin}_h{h+1}": float(np.mean(scenario_std)),
                f"eval/scenario_std_std/o{origin}_h{h+1}": float(np.std(scenario_std)),
                f"eval/scenario_cross_corr/o{origin}_h{h+1}": float(np.mean(scenario_corrs)) if scenario_corrs else 0.0,
            })
            
            # Cross-horizon dependence: correlation of Delta y_h and Delta y_h+1 across scenarios
            if h < min(H_dim, 5) - 1:
                horizon_corrs = []
                # Compute for a sample of 200 parcels
                samp_h = min(200, N)
                idx_h = np.random.choice(N, samp_h, replace=False)
                for ii in idx_h:
                    sc_h = deltas[ii, :, h]
                    sc_h_next = deltas[ii, :, h+1]
                    if np.std(sc_h) > 1e-10 and np.std(sc_h_next) > 1e-10:
                        c_h = np.corrcoef(sc_h, sc_h_next)[0, 1]
                        if np.isfinite(c_h):
                            horizon_corrs.append(c_h)
                wandb.log({
                    f"eval/cross_horizon_corr/o{origin}_h{h+1}_h{h+2}": float(np.mean(horizon_corrs)) if horizon_corrs else 0.0,
                })
        print(f"  [scenario_diversity] Logged for {H_dim} horizons")
    
    # ─── NEW: Energy Score + Variogram Score (multivariate proper scoring rules) ───
    if key in variant_raw_results:
        y_levels_es = variant_raw_results[key]["y_levels"]  # [N, S, H]
        base_vals_es = variant_raw_results[key]["base_val"]  # [N]
        accts_es = variant_raw_results[key]["accts"]
        N_es_total = y_levels_es.shape[0]
        idx_es = np.random.choice(N_es_total, min(500, N_es_total), replace=False)
        
        for h_es in [1, min(3, MAX_HORIZON), MAX_HORIZON]:
            eyr_es = origin + h_es
            if eyr_es not in actual_vals:
                continue
            future_v_es = actual_vals[eyr_es]
            
            # Collect valid parcels: both actual and base must exist
            valid_idx, actuals_list, base_list = [], [], []
            for ii in idx_es:
                acct_s = str(accts_es[ii]).strip()
                av = future_v_es.get(acct_s)
                bv = base_vals_es[ii]
                if bv > 0 and av is not None and av > 0:
                    valid_idx.append(ii)
                    actuals_list.append(av)
                    base_list.append(bv)
            
            if len(valid_idx) < 30:
                continue
            
            valid_idx = np.array(valid_idx)
            actuals_arr = np.array(actuals_list)
            base_arr = np.array(base_list)
            
            # Compute in log-return space for scale invariance
            # X: [S, n_parcels] scenario log-returns
            # y: [n_parcels] actual log-returns
            X = (y_levels_es[valid_idx, :, h_es - 1].T - np.log(base_arr)[None, :])  # [S, n]
            y = (np.log(actuals_arr) - np.log(base_arr))  # [n]
            S_es = X.shape[0]
            
            # Energy score: E||X_s - y||_2 - 0.5 * E||X_s - X_s'||_2
            term1 = float(np.mean(np.linalg.norm(X - y[None, :], axis=1)))
            # Pairwise term (subsample if S is large)
            if S_es <= 200:
                D = np.linalg.norm(X[:, None, :] - X[None, :, :], axis=2)  # [S, S]
                term2 = float(np.mean(D))
            else:
                _i1 = np.random.randint(0, S_es, 500)
                _i2 = np.random.randint(0, S_es, 500)
                term2 = float(np.mean(np.linalg.norm(X[_i1] - X[_i2], axis=1)))
            energy = term1 - 0.5 * term2
            
            # Variogram score (order 1): pairwise spatial differences
            n_parcels = len(valid_idx)
            n_pairs = min(500, n_parcels * (n_parcels - 1) // 2)
            pair_a = np.random.randint(0, n_parcels, n_pairs)
            pair_b = np.random.randint(0, n_parcels, n_pairs)
            actual_diffs = np.abs(y[pair_a] - y[pair_b])  # [n_pairs]
            scen_diffs = np.abs(X[:, pair_a] - X[:, pair_b])  # [S, n_pairs]
            scen_diff_mean = scen_diffs.mean(axis=0)  # [n_pairs]
            vario = float(np.mean((actual_diffs - scen_diff_mean) ** 2))
            
            wandb.log({
                f"eval/energy_score/o{origin}_h{h_es}": energy,
                f"eval/variogram_score/o{origin}_h{h_es}": vario,
            })
            print(f"  [coherence] h={h_es} energy={energy:.4f} variogram={vario:.6f} (n={n_parcels})")
    
    # ─── NEW: Per-Token Diagnostics ───
    if key in variant_raw_results:
        phi_vec = token_persistence.get_phi()
        phi_vals = torch.sigmoid(phi_vec).detach().cpu().numpy()
        K_tok = len(phi_vals)
        for k in range(K_tok):
            wandb.log({f"eval/phi_k/o{origin}_k{k}": float(phi_vals[k])})
        
        # Alpha (gating weights) per token — average across sample parcels
        try:
            samp_n = min(500, n_valid)
            with torch.no_grad():
                _h = torch.as_tensor(ctx["hist_y"][:samp_n], dtype=torch.float32, device=_device)
                _n = torch.as_tensor(ctx["cur_num"][:samp_n], dtype=torch.float32, device=_device)
                _c = torch.as_tensor(ctx["cur_cat"][:samp_n], dtype=torch.long, device=_device)
                _r = torch.as_tensor(ctx["region_id"][:samp_n], dtype=torch.long, device=_device)
                _o = torch.full((samp_n,), origin, dtype=torch.long, device=_device)
                alpha = gating_net(_h, _n, _c, _r, _o)  # [N, K]
            alpha_np = alpha.cpu().numpy()
            alpha_mean = alpha_np.mean(axis=0)  # [K]
            alpha_std = alpha_np.std(axis=0)
            for k in range(K_tok):
                wandb.log({
                    f"eval/alpha_mean/o{origin}_k{k}": float(alpha_mean[k]),
                    f"eval/alpha_std/o{origin}_k{k}": float(alpha_std[k]),
                })
            # Effective number of tokens (inverse HHI of mean alpha)
            eff_k = float(1.0 / np.sum(alpha_mean**2)) if np.sum(alpha_mean**2) > 0 else 0
            wandb.log({f"eval/eff_k/o{origin}": eff_k})
            print(f"  [tokens] phi_k={[f'{p:.3f}' for p in phi_vals]} alpha_mean={[f'{a:.3f}' for a in alpha_mean]} eff_k={eff_k:.2f}")
        except Exception as e:
            print(f"  [tokens] Alpha computation failed: {e}")
    
    # ─── NEW: Conditional Metrics (by property characteristics) ───
    if key in variant_raw_results:
        res = variant_raw_results[key]
        y_levels_cond = res["y_levels"]
        accts_cond = res["accts"]
        ya_cond = res["y_anchor"]
        base_v_cond = actual_vals.get(origin, {})
        
        # Load property metadata for conditional splits
        try:
            meta_cols = ["acct", "yr", "yr_blt", "geo_col"]
            meta_df = df_actuals.filter(pl.col("yr") == origin)
            has_yr_blt = "yr_blt" in meta_df.columns
            has_geo = "geo_col" in meta_df.columns
            if has_yr_blt or has_geo:
                meta_dict = {}
                for row in meta_df.iter_rows(named=True):
                    a = str(row.get("acct", "")).strip()
                    meta_dict[a] = row
                
                # Year-built buckets
                if has_yr_blt:
                    age_buckets = [
                        ("pre1960", 0, 1960),
                        ("1960-1990", 1960, 1990),
                        ("1990-2010", 1990, 2010),
                        ("post2010", 2010, 2100),
                    ]
                    for h in [1, min(3, MAX_HORIZON)]:
                        h_idx = h - 1
                        eyr = origin + h
                        if eyr not in actual_vals:
                            continue
                        future_v = actual_vals[eyr]
                        for age_label, age_lo, age_hi in age_buckets:
                            p_list, a_list = [], []
                            for i, acct in enumerate(accts_cond):
                                acct_s = str(acct).strip()
                                m = meta_dict.get(acct_s, {})
                                yb = m.get("yr_blt", None)
                                if yb is None or not (age_lo <= yb < age_hi):
                                    continue
                                bv = base_v_cond.get(acct_s, 0)
                                av = future_v.get(acct_s)
                                if bv <= 0 or av is None:
                                    continue
                                fan = y_levels_cond[i, :, h_idx]
                                p_list.append(float(np.expm1(np.nanmedian(fan) - ya_cond[i]) * 100))
                                a_list.append(float((av - bv) / bv * 100))
                            if len(p_list) > 20:
                                p_arr, a_arr = np.array(p_list), np.array(a_list)
                                tag_c = f"o{origin}_h{h}_{age_label}"
                                wandb.log({
                                    f"eval/cond_age/bias/{tag_c}": float(np.median(p_arr) - np.median(a_arr)),
                                    f"eval/cond_age/mdae/{tag_c}": float(np.median(np.abs(p_arr - a_arr))),
                                    f"eval/cond_age/n/{tag_c}": len(p_list),
                                })
                
                # Geography buckets (top 10 zip codes by count)
                if has_geo:
                    geo_vals = [meta_dict.get(str(a).strip(), {}).get("geo_col") for a in accts_cond]
                    from collections import Counter
                    geo_counts = Counter([g for g in geo_vals if g is not None])
                    top_geos = [g for g, _ in geo_counts.most_common(10)]
                    h = 1
                    h_idx = 0
                    eyr = origin + 1
                    if eyr in actual_vals:
                        future_v = actual_vals[eyr]
                        for geo in top_geos:
                            p_list, a_list = [], []
                            for i, acct in enumerate(accts_cond):
                                acct_s = str(acct).strip()
                                m = meta_dict.get(acct_s, {})
                                if m.get("geo_col") != geo:
                                    continue
                                bv = base_v_cond.get(acct_s, 0)
                                av = future_v.get(acct_s)
                                if bv <= 0 or av is None:
                                    continue
                                fan = y_levels_cond[i, :, h_idx]
                                p_list.append(float(np.expm1(np.nanmedian(fan) - ya_cond[i]) * 100))
                                a_list.append(float((av - bv) / bv * 100))
                            if len(p_list) > 10:
                                p_arr, a_arr = np.array(p_list), np.array(a_list)
                                tag_c = f"o{origin}_h1_geo_{geo}"
                                wandb.log({
                                    f"eval/cond_geo/bias/{tag_c}": float(np.median(p_arr) - np.median(a_arr)),
                                    f"eval/cond_geo/mdae/{tag_c}": float(np.median(np.abs(p_arr - a_arr))),
                                    f"eval/cond_geo/rho/{tag_c}": float(spearmanr(p_arr, a_arr).statistic) if len(set(p_list)) > 1 else 0.0,
                                    f"eval/cond_geo/n/{tag_c}": len(p_list),
                                })
                print(f"  [conditional] Logged age + geo breakdowns")
        except Exception as e:
            print(f"  [conditional] Skipped: {e}")
    
    # ─── NEW: Learning Curve from Checkpoint ───
    if "training_losses" in ckpt:
        losses = ckpt["training_losses"]
        for ep, loss_val in enumerate(losses):
            wandb.log({f"eval/train_loss/o{origin}_ep{ep}": float(loss_val)})
        wandb.log({f"eval/train_loss_final/o{origin}": float(losses[-1])})
        wandb.log({f"eval/train_loss_ep10/o{origin}": float(losses[min(9, len(losses)-1)])})
        print(f"  [learning_curve] {len(losses)} epochs logged")
    elif "epoch" in ckpt:
        wandb.log({f"eval/train_epochs/o{origin}": int(ckpt["epoch"])})
    if "cfg" in ckpt:
        cfg = ckpt["cfg"]
        for cfg_key in ["LR", "EPOCHS", "N_SAMPLE", "K_TOKENS", "K_ACTIVE", "DIFF_STEPS_TRAIN", "SIGMA_U_INIT"]:
            if cfg_key in cfg:
                wandb.log({f"eval/cfg/{cfg_key}/o{origin}": float(cfg[cfg_key])})

    # ══════════════════════════════════════════════════════════════════════
    # CALIBRATION PACKET DIAGNOSTICS
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n[{ts()}] ── Calibration Packet Diagnostics ──")
    
    if key in variant_raw_results:
        res = variant_raw_results[key]
        y_levels_cp = res["y_levels"]   # [N, S, H]
        ya_cp = res["y_anchor"]          # [N]
        deltas_cp = res["deltas"]        # [N, S, H]
        base_vals_cp = res["base_val"]   # [N]
        uncalibrated_medians_cp = res.get("uncalibrated_medians")
        accts_cp = res["accts"]
        N_cp, S_cp, H_cp = y_levels_cp.shape

        # ─── CP1: ANCHOR INTEGRITY ──────────────────────────────────────
        # Does forecast anchor (median at h=1) align with history value?
        print(f"\n  [CP1] Anchor Integrity")
        h1_medians = np.expm1(np.median(y_levels_cp[:, :, 0], axis=1))  # [N] dollar space
        valid_mask = base_vals_cp > 0
        if valid_mask.sum() > 0:
            log_ratios = np.abs(np.log(h1_medians[valid_mask] / base_vals_cp[valid_mask]))
            log_ratios = log_ratios[np.isfinite(log_ratios)]
            if len(log_ratios) > 0:
                implied_growth = h1_medians[valid_mask] / base_vals_cp[valid_mask] - 1
                implied_growth = implied_growth[np.isfinite(implied_growth)]
                n_bad_05 = int((log_ratios > 0.5).sum())
                n_bad_10 = int((log_ratios > 1.0).sum())
                wandb.log({
                    f"calibration/anchor_log_ratio_median/o{origin}": float(np.median(log_ratios)),
                    f"calibration/anchor_log_ratio_p95/o{origin}": float(np.percentile(log_ratios, 95)),
                    f"calibration/anchor_log_ratio_max/o{origin}": float(np.max(log_ratios)),
                    f"calibration/anchor_implied_growth_mean/o{origin}": float(np.mean(implied_growth)),
                    f"calibration/anchor_implied_growth_median/o{origin}": float(np.median(implied_growth)),
                    f"calibration/anchor_n_bad_05/o{origin}": n_bad_05,
                    f"calibration/anchor_n_bad_10/o{origin}": n_bad_10,
                    f"calibration/anchor_n_total/o{origin}": len(log_ratios),
                })
                print(f"    |log(fcst/hist)| median={np.median(log_ratios):.4f} p95={np.percentile(log_ratios, 95):.4f}")
                print(f"    implied_growth mean={np.mean(implied_growth):.4f} median={np.median(implied_growth):.4f}")
                print(f"    {n_bad_05}/{len(log_ratios)} with |ratio|>0.5, {n_bad_10} with |ratio|>1.0")

        # ─── CP2: STEP vs CUMULATIVE DISPERSION ─────────────────────────
        # Step delta std should be FLAT; cumulative std should grow ~√h
        print(f"\n  [CP2] Step vs Cumulative Dispersion")
        for h in range(H_cp):
            # Step: delta at each horizon (one year's increment)
            step_deltas = deltas_cp[:, :, h]  # [N, S]
            step_median_per_parcel = np.median(step_deltas, axis=1)  # [N]
            step_std_cross_geo = float(np.std(step_median_per_parcel))
            step_mean_cross_geo = float(np.mean(step_median_per_parcel))
            
            # Cumulative: sum of deltas from 0..h
            cum_deltas = np.cumsum(deltas_cp[:, :, :h+1], axis=2)[:, :, -1]  # [N, S]
            cum_median_per_parcel = np.median(cum_deltas, axis=1)  # [N]
            cum_std_cross_geo = float(np.std(cum_median_per_parcel))
            cum_mean_cross_geo = float(np.mean(cum_median_per_parcel))
            
            wandb.log({
                f"calibration/step_std/o{origin}_h{h+1}": step_std_cross_geo,
                f"calibration/step_mean/o{origin}_h{h+1}": step_mean_cross_geo,
                f"calibration/cum_std/o{origin}_h{h+1}": cum_std_cross_geo,
                f"calibration/cum_mean/o{origin}_h{h+1}": cum_mean_cross_geo,
            })
            print(f"    h={h+1} step_std={step_std_cross_geo:.4f} cum_std={cum_std_cross_geo:.4f}")

        # ─── CP3: HORIZON SCALING SUMMARY ────────────────────────────────
        # cum_std / cum_std_h1 should track √h
        print(f"\n  [CP3] Horizon Scaling")
        cum_stds = []
        for h in range(H_cp):
            cum_d = np.cumsum(deltas_cp[:, :, :h+1], axis=2)[:, :, -1]
            cum_med = np.median(cum_d, axis=1)
            cum_stds.append(float(np.std(cum_med)))
        
        if cum_stds[0] > 1e-10:
            for h in range(H_cp):
                ratio = cum_stds[h] / cum_stds[0]
                sqrt_h = np.sqrt(h + 1)
                wandb.log({
                    f"calibration/cum_ratio/o{origin}_h{h+1}": ratio,
                    f"calibration/sqrt_h/o{origin}_h{h+1}": sqrt_h,
                    f"calibration/ratio_vs_sqrth/o{origin}_h{h+1}": ratio / sqrt_h if sqrt_h > 0 else 0,
                })
                print(f"    h={h+1} cum/h1={ratio:.2f} √h={sqrt_h:.2f} ratio/√h={ratio/sqrt_h:.2f}")

        # ─── CP4: WITHIN-FAN vs CROSS-GEO DECOMPOSITION ─────────────────
        # within-fan: uncertainty per parcel (avg of (p90-p10)/p50)
        # cross-geo: heterogeneity across parcels (std of p50 medians)
        print(f"\n  [CP4] Within-Fan vs Cross-Geo Decomposition")
        for h in range(H_cp):
            fan_h = y_levels_cp[:, :, h]  # [N, S]
            p10s = np.percentile(fan_h, 10, axis=1)
            p50s = np.percentile(fan_h, 50, axis=1)
            p90s = np.percentile(fan_h, 90, axis=1)
            
            # Within-fan: per-parcel (p90-p10)/p50 in dollar space
            p10_dollar = np.expm1(p10s)
            p50_dollar = np.expm1(p50s)
            p90_dollar = np.expm1(p90s)
            within_fan = (p90_dollar - p10_dollar) / np.maximum(p50_dollar, 1)
            within_fan = within_fan[np.isfinite(within_fan)]
            
            # Cross-geo: std of growth rates (p50 vs anchor)
            growth_rates = p50_dollar / np.maximum(base_vals_cp, 1) - 1
            growth_rates = growth_rates[np.isfinite(growth_rates)]
            
            if len(within_fan) > 0 and len(growth_rates) > 0:
                wandb.log({
                    f"calibration/within_fan_med/o{origin}_h{h+1}": float(np.median(within_fan)),
                    f"calibration/within_fan_p95/o{origin}_h{h+1}": float(np.percentile(within_fan, 95)),
                    f"calibration/cross_geo_std/o{origin}_h{h+1}": float(np.std(growth_rates)),
                    f"calibration/cross_geo_cv/o{origin}_h{h+1}": float(np.std(growth_rates) / max(abs(np.mean(growth_rates)), 1e-6)),
                })
                print(f"    h={h+1} within_fan_med={np.median(within_fan):.4f} cross_geo_std={np.std(growth_rates):.4f}")

        # ─── CP5: BASELINE COMPARISON ────────────────────────────────────
        # Compare model MAE against persistence (no change) and random walk
        print(f"\n  [CP5] Baseline Comparison")
        
        # Historical step growth distribution (for RW baseline)
        if "yr" in df_actuals.columns and "tot_appr_val" in df_actuals.columns:
            hist_df = df_actuals.sort(["acct", "yr"])
            hist_growth = []
            for row in hist_df.group_by("acct").agg([
                pl.col("tot_appr_val").alias("vals"),
                pl.col("yr").alias("yrs"),
            ]).iter_rows(named=True):
                vals = row["vals"]
                if vals is not None and len(vals) > 1:
                    for i in range(1, len(vals)):
                        if vals[i-1] > 0 and vals[i] > 0:
                            hist_growth.append(vals[i] / vals[i-1] - 1)
            hist_growth = np.array(hist_growth)
            hist_growth = hist_growth[np.isfinite(hist_growth) & (np.abs(hist_growth) < 5)]
            hist_std = float(np.std(hist_growth)) if len(hist_growth) > 0 else 0.1
            hist_mean = float(np.mean(hist_growth)) if len(hist_growth) > 0 else 0.0
            wandb.log({
                f"calibration/hist_growth_mean/o{origin}": hist_mean,
                f"calibration/hist_growth_std/o{origin}": hist_std,
            })
        else:
            hist_std = 0.1
            hist_mean = 0.05
        
        for h in range(1, H_cp + 1):
            eyr = origin + h
            if eyr not in actual_vals:
                continue
            future_v = actual_vals[eyr]
            
            model_errs, persist_errs, rw_errs = [], [], []
            model_wins = 0
            n_compare = 0
            
            for i, acct in enumerate(accts_cp):
                acct_s = str(acct).strip()
                bv = base_vals_cp[i] if i < len(base_vals_cp) else 0
                av = future_v.get(acct_s)
                if bv <= 0 or av is None or av <= 0:
                    continue
                
                # Model prediction (uncalibrated median in dollar space to preserve point skill)
                fan = y_levels_cp[i, :, h-1]
                if uncalibrated_medians_cp is not None:
                    model_pred = float(np.expm1(uncalibrated_medians_cp[i, h-1]))
                else:
                    model_pred = float(np.expm1(np.median(fan)))
                
                # Persistence: price stays the same
                persist_pred = bv
                
                # Random walk: price + hist_mean * h years
                rw_pred = bv * (1 + hist_mean) ** h
                
                model_err = abs(model_pred - av)
                persist_err = abs(persist_pred - av)
                rw_err = abs(rw_pred - av)
                
                model_errs.append(model_err)
                persist_errs.append(persist_err)
                rw_errs.append(rw_err)
                n_compare += 1
                if model_err < persist_err:
                    model_wins += 1
            
            if n_compare > 0:
                model_mae = float(np.mean(model_errs))
                persist_mae = float(np.mean(persist_errs))
                rw_mae = float(np.mean(rw_errs))
                win_pct = model_wins / n_compare * 100
                
                wandb.log({
                    f"calibration/model_mae/o{origin}_h{h}": model_mae,
                    f"calibration/persist_mae/o{origin}_h{h}": persist_mae,
                    f"calibration/rw_mae/o{origin}_h{h}": rw_mae,
                    f"calibration/model_wins_pct/o{origin}_h{h}": win_pct,
                    f"calibration/model_vs_persist_ratio/o{origin}_h{h}": model_mae / max(persist_mae, 1),
                    f"calibration/model_vs_rw_ratio/o{origin}_h{h}": model_mae / max(rw_mae, 1),
                    f"calibration/n_compare/o{origin}_h{h}": n_compare,
                })
                print(f"    h={h} model_MAE={model_mae:,.0f} persist_MAE={persist_mae:,.0f} rw_MAE={rw_mae:,.0f} wins={win_pct:.1f}% (n={n_compare})")
                
                # Point Accuracy Guardrail
                ratio = model_mae / max(persist_mae, 1)
                if ratio > 1.10:
                    wandb.log({f"guardrail/point_accuracy_fail/o{origin}_h{h}": 1})
                    print(f"    \u26a0\ufe0f STRICT GUARDRAIL FAILED: MAE vs Persistence ratio is {ratio:.2f} > 1.10")
                else:
                    wandb.log({f"guardrail/point_accuracy_fail/o{origin}_h{h}": 0})
                    print(f"    \u2705 GUARDRAIL PASSED: MAE vs Persistence ratio {ratio:.2f} <= 1.10")

        # ─── CP6: MULTI-LEVEL COVERAGE ───────────────────────────────────
        # Coverage at multiple confidence levels (50%, 80%, 90%, 95%)
        print(f"\n  [CP6] Multi-Level PI Coverage")
        coverage_levels = [(50, 25, 75), (80, 10, 90), (90, 5, 95), (95, 2.5, 97.5)]
        for h in range(1, H_cp + 1):
            eyr = origin + h
            if eyr not in actual_vals:
                continue
            future_v = actual_vals[eyr]
            
            for cov_label, lo_pct, hi_pct in coverage_levels:
                hits, checks = 0, 0
                for i, acct in enumerate(accts_cp):
                    acct_s = str(acct).strip()
                    av = future_v.get(acct_s)
                    bv = base_vals_cp[i] if i < len(base_vals_cp) else 0
                    if bv <= 0 or av is None or av <= 0:
                        continue
                    fan = y_levels_cp[i, :, h-1]
                    lo_val = np.expm1(np.percentile(fan, lo_pct))
                    hi_val = np.expm1(np.percentile(fan, hi_pct))
                    checks += 1
                    if lo_val <= av <= hi_val:
                        hits += 1
                if checks > 0:
                    actual_cov = hits / checks * 100
                    wandb.log({
                        f"calibration/coverage_{cov_label}/o{origin}_h{h}": actual_cov,
                        f"calibration/coverage_{cov_label}_gap/o{origin}_h{h}": actual_cov - cov_label,
                    })
                    status = "OK" if abs(actual_cov - cov_label) < 10 else "LOW" if actual_cov < cov_label else "HIGH"
                    print(f"    h={h} PI{cov_label}: {actual_cov:.1f}% [{status}] (n={checks})")

        # ─── COVERAGE vs PREDICTED WIDTH DECILES ─────────────────────────
        # If coverage is low in ALL deciles → global temperature issue
        # If coverage is low only in narrow-fan deciles → heteroscedastic miss
        print(f"\n  [CovWidth] Coverage vs Predicted Width Deciles")
        for h_cw in range(1, H_cp + 1):
            eyr_cw = origin + h_cw
            if eyr_cw not in actual_vals:
                continue
            future_v_cw = actual_vals[eyr_cw]
            widths_cw, covered_cw = [], []
            for i_cw, acct_cw in enumerate(accts_cp):
                acct_s_cw = str(acct_cw).strip()
                av_cw = future_v_cw.get(acct_s_cw)
                bv_cw = base_vals_cp[i_cw] if i_cw < len(base_vals_cp) else 0
                if bv_cw <= 0 or av_cw is None or av_cw <= 0:
                    continue
                fan_cw = y_levels_cp[i_cw, :, h_cw - 1]  # log-space
                lo_log = np.percentile(fan_cw, 10)
                hi_log = np.percentile(fan_cw, 90)
                widths_cw.append(float(hi_log - lo_log))  # log-width (scale-invariant)
                covered_cw.append(1 if (lo_log <= np.log1p(av_cw) <= hi_log) else 0)
            if len(widths_cw) > 100:
                widths_arr = np.array(widths_cw)
                covered_arr = np.array(covered_cw)
                decile_edges = np.percentile(widths_arr, np.arange(0, 101, 10))
                for d_cw in range(10):
                    mask_cw = (widths_arr >= decile_edges[d_cw]) & (widths_arr < decile_edges[d_cw + 1] + 1e-10)
                    if mask_cw.sum() > 5:
                        d_cov = float(covered_arr[mask_cw].mean() * 100)
                        d_width = float(widths_arr[mask_cw].mean())
                        wandb.log({
                            f"calibration/cov_by_width_d{d_cw}/o{origin}_h{h_cw}": d_cov,
                            f"calibration/width_d{d_cw}_mean/o{origin}_h{h_cw}": d_width,
                        })
                overall_cov = float(covered_arr.mean() * 100)
                print(f"    h={h_cw} overall_cov={overall_cov:.1f}% log_width_med={np.median(widths_arr):.4f} (n={len(widths_cw)})")

        # ─── EXCEEDANCE CALIBRATION (tail quantiles) ─────────────────────
        # Separates "tails too thin" from "median shifted"
        print(f"\n  [Exceedance] Tail Calibration")
        for h_ex in range(1, H_cp + 1):
            eyr_ex = origin + h_ex
            if eyr_ex not in actual_vals:
                continue
            future_v_ex = actual_vals[eyr_ex]
            tail_counts = {"below_p01": 0, "below_p05": 0, "above_p95": 0, "above_p99": 0}
            total_ex = 0
            for i_ex, acct_ex in enumerate(accts_cp):
                acct_s_ex = str(acct_ex).strip()
                av_ex = future_v_ex.get(acct_s_ex)
                bv_ex = base_vals_cp[i_ex] if i_ex < len(base_vals_cp) else 0
                if bv_ex <= 0 or av_ex is None or av_ex <= 0:
                    continue
                fan_ex = y_levels_cp[i_ex, :, h_ex - 1]  # log-space
                log_av = np.log1p(av_ex)
                total_ex += 1
                if log_av <= np.percentile(fan_ex, 1):
                    tail_counts["below_p01"] += 1
                if log_av <= np.percentile(fan_ex, 5):
                    tail_counts["below_p05"] += 1
                if log_av >= np.percentile(fan_ex, 95):
                    tail_counts["above_p95"] += 1
                if log_av >= np.percentile(fan_ex, 99):
                    tail_counts["above_p99"] += 1
            if total_ex > 50:
                for tail_name, tail_cnt in tail_counts.items():
                    empirical = tail_cnt / total_ex * 100
                    nominal = {"below_p01": 1, "below_p05": 5, "above_p95": 5, "above_p99": 1}[tail_name]
                    wandb.log({
                        f"calibration/exceed_{tail_name}/o{origin}_h{h_ex}": empirical,
                        f"calibration/exceed_{tail_name}_gap/o{origin}_h{h_ex}": empirical - nominal,
                    })
                print(f"    h={h_ex} below_p01={tail_counts['below_p01']/total_ex*100:.1f}%(nom 1%) "
                      f"below_p05={tail_counts['below_p05']/total_ex*100:.1f}%(nom 5%) "
                      f"above_p95={tail_counts['above_p95']/total_ex*100:.1f}%(nom 5%) "
                      f"above_p99={tail_counts['above_p99']/total_ex*100:.1f}%(nom 1%) (n={total_ex})")

        # ─── CP7: DISTRIBUTIONAL SHAPE AUDIT ─────────────────────────────
        # Compare forecast growth distribution against historical growth
        print(f"\n  [CP7] Distributional Shape Audit")
        from scipy.stats import skew as scipy_skew, kurtosis as scipy_kurtosis
        from scipy.stats import ks_2samp, wasserstein_distance, iqr as scipy_iqr
        
        # Compute historical growth rates per horizon
        if "yr" in df_actuals.columns and "tot_appr_val" in df_actuals.columns:
            hist_sorted = df_actuals.sort(["acct", "yr"])
            hist_growths_by_h = {}
            for lag in [1, 2, 3, 4, 5]:
                growths = []
                for row in hist_sorted.group_by("acct").agg([
                    pl.col("tot_appr_val").alias("vals"),
                ]).iter_rows(named=True):
                    vals = row["vals"]
                    if vals is not None and len(vals) > lag:
                        for i in range(lag, len(vals)):
                            if vals[i-lag] > 0 and vals[i] > 0:
                                growths.append(vals[i] / vals[i-lag] - 1)
                g = np.array(growths)
                g = g[np.isfinite(g) & (np.abs(g) < 10)]
                hist_growths_by_h[lag] = g
        
            # Forecast growth per horizon
            for h in range(1, min(H_cp + 1, 6)):
                hist_g = hist_growths_by_h.get(h, np.array([]))
                if len(hist_g) < 30:
                    continue
                
                # Forecast growth: median scenario per parcel → distribution
                fan_h = y_levels_cp[:, :, h-1]  # [N, S]
                fcst_median = np.median(fan_h, axis=1)  # [N]
                fcst_g = np.expm1(fcst_median) / np.maximum(base_vals_cp, 1) - 1
                fcst_g = fcst_g[np.isfinite(fcst_g) & (np.abs(fcst_g) < 10)]
                
                if len(fcst_g) < 30:
                    continue
                
                # Shape statistics
                ks_stat, ks_p = ks_2samp(hist_g, fcst_g)
                wd = wasserstein_distance(hist_g, fcst_g)
                var_ratio = np.var(fcst_g) / max(np.var(hist_g), 1e-10)
                iqr_std_hist = scipy_iqr(hist_g) / max(np.std(hist_g), 1e-10)
                iqr_std_fcst = scipy_iqr(fcst_g) / max(np.std(fcst_g), 1e-10)
                skew_hist = float(scipy_skew(hist_g))
                skew_fcst = float(scipy_skew(fcst_g))
                kurt_hist = float(scipy_kurtosis(hist_g))
                kurt_fcst = float(scipy_kurtosis(fcst_g))
                
                # Tail ratios
                h_p99, h_p90, h_p75, h_p25, h_p10, h_p01 = np.percentile(hist_g, [99,90,75,25,10,1])
                f_p99, f_p90, f_p75, f_p25, f_p10, f_p01 = np.percentile(fcst_g, [99,90,75,25,10,1])
                up_tail_hist = (h_p99 - h_p90) / max(h_p90 - h_p75, 1e-10)
                up_tail_fcst = (f_p99 - f_p90) / max(f_p90 - f_p75, 1e-10)
                
                wandb.log({
                    f"calibration/shape_ks/o{origin}_h{h}": ks_stat,
                    f"calibration/shape_ks_p/o{origin}_h{h}": ks_p,
                    f"calibration/shape_wasserstein/o{origin}_h{h}": wd,
                    f"calibration/shape_var_ratio/o{origin}_h{h}": var_ratio,
                    f"calibration/shape_iqr_std_hist/o{origin}_h{h}": iqr_std_hist,
                    f"calibration/shape_iqr_std_fcst/o{origin}_h{h}": iqr_std_fcst,
                    f"calibration/shape_skew_hist/o{origin}_h{h}": skew_hist,
                    f"calibration/shape_skew_fcst/o{origin}_h{h}": skew_fcst,
                    f"calibration/shape_kurt_hist/o{origin}_h{h}": kurt_hist,
                    f"calibration/shape_kurt_fcst/o{origin}_h{h}": kurt_fcst,
                    f"calibration/shape_up_tail_hist/o{origin}_h{h}": up_tail_hist,
                    f"calibration/shape_up_tail_fcst/o{origin}_h{h}": up_tail_fcst,
                    f"calibration/shape_frac_gt50_hist/o{origin}_h{h}": float(np.mean(hist_g > 0.5)),
                    f"calibration/shape_frac_gt50_fcst/o{origin}_h{h}": float(np.mean(fcst_g > 0.5)),
                    f"calibration/shape_frac_lt30_hist/o{origin}_h{h}": float(np.mean(hist_g < -0.3)),
                    f"calibration/shape_frac_lt30_fcst/o{origin}_h{h}": float(np.mean(fcst_g < -0.3)),
                })
                vr_tag = "[wide]" if var_ratio > 2 else "[narrow]" if var_ratio < 0.3 else "[ok]"
                print(f"    h={h} KS={ks_stat:.4f} Wass={wd:.5f} VarR={var_ratio:.3f}{vr_tag} "
                      f"skew={skew_fcst:+.3f}(hist:{skew_hist:+.3f}) "
                      f"kurt={kurt_fcst:+.1f}(hist:{kurt_hist:+.1f})")

        # ─── B6: MODALITY (Hartigan's dip test) ──────────────────────────
        # Detect multi-modal distributions in forecast vs historical growth
        print(f"\n  [B6] Modality (dip test)")
        try:
            from diptest import diptest as dip_test
            for h_b6 in range(1, min(H_cp + 1, 6)):
                hist_g_b6 = hist_growths_by_h.get(h_b6, np.array([]))
                if len(hist_g_b6) < 50:
                    continue
                fan_h_b6 = y_levels_cp[:, :, h_b6 - 1]
                fcst_med_b6 = np.median(fan_h_b6, axis=1)
                fcst_g_b6 = np.expm1(fcst_med_b6) / np.maximum(base_vals_cp, 1) - 1
                fcst_g_b6 = fcst_g_b6[np.isfinite(fcst_g_b6) & (np.abs(fcst_g_b6) < 10)]
                if len(fcst_g_b6) < 50:
                    continue
                # Subsample for speed
                hist_sub = np.random.choice(hist_g_b6, min(2000, len(hist_g_b6)), replace=False)
                fcst_sub = np.random.choice(fcst_g_b6, min(2000, len(fcst_g_b6)), replace=False)
                dip_fcst, p_fcst = dip_test(fcst_sub)
                dip_hist, p_hist = dip_test(hist_sub)
                wandb.log({
                    f"calibration/shape_dip_fcst/o{origin}_h{h_b6}": float(dip_fcst),
                    f"calibration/shape_dip_hist/o{origin}_h{h_b6}": float(dip_hist),
                    f"calibration/shape_dip_p_fcst/o{origin}_h{h_b6}": float(p_fcst),
                    f"calibration/shape_dip_p_hist/o{origin}_h{h_b6}": float(p_hist),
                })
                uni_tag_f = "uni" if p_fcst > 0.05 else "MULTI"
                uni_tag_h = "uni" if p_hist > 0.05 else "MULTI"
                print(f"    h={h_b6} fcst dip={dip_fcst:.4f}(p={p_fcst:.3f})[{uni_tag_f}] "
                      f"hist dip={dip_hist:.4f}(p={p_hist:.3f})[{uni_tag_h}]")
        except ImportError:
            print("    diptest not installed, skipping B6 modality check")
        except Exception as e_b6:
            print(f"    B6 error: {e_b6}")

    print(f"\n[{ts()}] ── Calibration Packet Complete ──")

    # Return per-origin scaler data for cross-origin comparison in main()
    return {
        "origin": int(origin),
        "n_scaler_mean": _raw_n_mean.tolist(),
        "n_scaler_scale": _raw_n_scale.tolist(),
    }

@app.local_entrypoint()
def main(
    jurisdiction: str = "sf_ca",
    bucket_name: str = "properlytic-raw-data",
    sample_size: int = 20_000,
    scenarios: int = 128,
    origins: str = "2021,2022,2023",
    version_tag: str = "v11",
):
    origin_list = [int(o.strip()) for o in origins.split(",")]
    
    # Grid search parameters — Stage 3: fix under-dispersion via eta
    # eta controls DDIM stochasticity (0=deterministic, 1=full DDPM)
    # Stage 2 showed eta=0.0 gives VarRatio 0.007-0.175 [narrow]
    etas = [0.3, 0.5, 0.8, 1.0]
    steps_list = [20]
    taus = [1.0]
    
    print(f"\U0001f680 Launching sweep {version_tag} on Modal across {len(origin_list)} origins and {len(etas)*len(steps_list)*len(taus)} hyperparams")
    print(f"   Jurisdiction: {jurisdiction}")
    print(f"   Origins: {origin_list}")
    
    results = []
    # Outer loop: hyperparameter combos. Inner loop: origins in chronological order.
    # This ensures each (eta, steps, tau) config gets a proper OOT calibration chain:
    # 2021 → fit calibrator → 2022 (apply 2021 calibrator) → fit → 2023 (apply 2022 calibrator)
    for _eta in etas:
        for _steps in steps_list:
            for _tau in taus:
                print(f"\n{'='*60}")
                print(f"  Sweep config: eta={_eta}, steps={_steps}, tau={_tau}")
                print(f"  Origins (sequential): {sorted(origin_list)}")
                print(f"{'='*60}")
                for origin in sorted(origin_list):
                    res = evaluate_checkpoints.remote(
                        jurisdiction, bucket_name, origin, sample_size,
                        scenarios, version_tag, _eta, _steps, _tau
                    )
                    results.append(res)

    # ─── A2: Cross-origin scaler integrity comparison ───
    scaler_data = [r for r in results if r is not None and isinstance(r, dict)]
    if len(scaler_data) >= 2:
        import numpy as np
        origins_found = [d["origin"] for d in scaler_data]
        mus = np.array([d["n_scaler_mean"] for d in scaler_data])   # [O, F]
        stds = np.array([d["n_scaler_scale"] for d in scaler_data]) # [O, F]
        
        # Per-feature ratios across origins
        std_min = np.maximum(stds.min(axis=0), 1e-8)
        std_max = stds.max(axis=0)
        std_ratio_per_feat = std_max / std_min
        std_ratio_max = float(std_ratio_per_feat.max())
        
        mu_span_per_feat = mus.max(axis=0) - mus.min(axis=0)
        mean_shift_max = float(mu_span_per_feat.max())
        
        print(f"\n📊 A2: Cross-origin scaler integrity (origins={origins_found})")
        print(f"   std_ratio_max = {std_ratio_max:.3f} (feature_idx={int(std_ratio_per_feat.argmax())})")
        print(f"   mean_span_max = {mean_shift_max:.4f} (feature_idx={int(mu_span_per_feat.argmax())})")
        
        if std_ratio_max > 1.25:
            j = int(std_ratio_per_feat.argmax())
            print(f"   ⚠️ A2 ANOMALY: max std ratio across origins = {std_ratio_max:.2f} at feature_idx={j}")
            # Log top-5 offending features
            top5 = np.argsort(std_ratio_per_feat)[-5:][::-1]
            for rank, fi in enumerate(top5):
                print(f"      #{rank+1} feature_idx={fi} ratio={std_ratio_per_feat[fi]:.3f} "
                      f"scales={[f'{stds[o, fi]:.4f}' for o in range(len(origins_found))]}")
        else:
            print(f"   ✅ A2 OK: all feature scales consistent across origins")
    else:
        print(f"\n📊 A2: Only {len(scaler_data)} origin(s) available, skipping cross-origin comparison")

    print(f"\n\u2705 Parallel Evaluation complete!")
