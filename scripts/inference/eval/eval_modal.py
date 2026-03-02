"""
Modal evaluation wrapper for Properlytic world model v11.
Runs inference across all checkpoints and computes backtest metrics logged to W&B.

Usage:
    modal run scripts/eval_modal.py --jurisdiction sf_ca
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

app = modal.App(f"eval-{_jur}")

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
    )
)

gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
wandb_secret = modal.Secret.from_name("wandb-creds", required_keys=["WANDB_API_KEY"])


@app.function(
    image=inference_image,
    gpu="A10G",  # Inference can use smaller GPUs
    timeout=7200,
    secrets=[gcs_secret, wandb_secret],
    volumes={"/output": modal.Volume.from_name("properlytic-checkpoints")},
)
def evaluate_checkpoints(
    jurisdiction: str = "sf_ca",
    bucket_name: str = "properlytic-raw-data",
    origin: int = 2019,
    sample_size: int = 20_000,
    scenarios: int = 128,
):
    import json, time, tempfile, glob, pickle
    import numpy as np
    import polars as pl
    import torch
    from scipy.stats import kstest, spearmanr
    import wandb
    
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

    # ─── 1. Download WorldModel Code ───
    print(f"[{ts()}] Downloading worldmodel logic...")
    wm_blob = bucket.blob("code/worldmodel.py")
    if not wm_blob.exists():
        raise FileNotFoundError("code/worldmodel.py not found in GCS. Run training first to upload.")
    
    wm_blob.download_to_filename("/tmp/worldmodel.py")
    
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
    modified_wm_code = patch_code + "\n" + wm_code.replace("if __name__ == '__main__' or globals().get('__colab__'):", "if False:")
    # Force the local path override to bypass os.path.exists checks at import time
    modified_wm_code = modified_wm_code.replace('PANEL_PATH = PANEL_PATH_LOCAL if os.path.exists(PANEL_PATH_LOCAL) else PANEL_PATH_DRIVE', 'PANEL_PATH = "/tmp/panel_actuals.parquet"')
    
    # Force bypassing the explicit check on line 299-301 in worldmodel
    modified_wm_code = modified_wm_code.replace('if not os.path.exists(PANEL_PATH):\n    raise FileNotFoundError("Panel not found")', 'pass')
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

    # ─── Pre-rename: Coalesce missing columns before mapping ───
    # Ensure property_value exists (fallback from sale_price or assessed_value)
    if "property_value" not in df_actuals.columns:
        if "sale_price" in df_actuals.columns:
            df_actuals = df_actuals.with_columns(pl.col("sale_price").alias("property_value"))
            print(f"[{ts()}] Derived property_value from sale_price")
        elif "assessed_value" in df_actuals.columns:
            df_actuals = df_actuals.with_columns(pl.col("assessed_value").alias("property_value"))
            print(f"[{ts()}] Derived property_value from assessed_value")
        elif "tot_appr_val" in df_actuals.columns:
            df_actuals = df_actuals.with_columns(pl.col("tot_appr_val").alias("property_value"))
            print(f"[{ts()}] Using existing tot_appr_val as property_value")
    
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
    leaky_cols = ["sale_price", "property_value", "assessed_value", "land_value", "improvement_value"]
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

    run_group = f"eval_v11_{jurisdiction}"
    
    wandb.init(
        project="homecastr",
        entity="dhardestylewis-columbia-university",
        name=f"{run_group}_{origin}",
        group=run_group,
        tags=["eval", "v11", jurisdiction],
        config={"sample_size": sample_size, "scenarios": scenarios, "origin": origin},
        job_type="evaluation"
    )

    ckpt_dir = f"/output/{jurisdiction}_v11"
    
    import glob
    candidates = glob.glob(os.path.join(ckpt_dir, f"ckpt_v11_origin_{origin}*.pt")) + \
                 glob.glob(os.path.join(ckpt_dir, f"ckpt_origin_{origin}*.pt"))
    if not candidates:
        print(f"[{ts()}] ⚠️ Checkpoint not found for origin {origin} in {ckpt_dir}, skipping")
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
    gating_net = create_gating_network(hist_len=hist_len, num_dim=num_dim, n_cat=n_cat)
    if "gating_state_dict" in ckpt:
        gating_net.load_state_dict(_strip(ckpt["gating_state_dict"]))
    elif "gating_net_state_dict" in ckpt:
        gating_net.load_state_dict(_strip(ckpt["gating_net_state_dict"]))
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
    model._n_scaler = SimpleScaler(mean=np.array(ckpt["n_scaler_mean"]), scale=np.array(ckpt["n_scaler_scale"]))
    model._t_scaler = SimpleScaler(mean=np.array(ckpt["t_scaler_mean"]), scale=np.array(ckpt["t_scaler_scale"]))
    global_medians = ckpt.get("global_medians", {})

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
    
    # Pad cur_num if checkpoint expects more features than the panel provides
    if ctx["cur_num"].shape[1] < num_dim:
        n_pad = num_dim - ctx["cur_num"].shape[1]
        pad = torch.zeros(ctx["cur_num"].shape[0], n_pad, dtype=ctx["cur_num"].dtype, device=ctx["cur_num"].device)
        ctx["cur_num"] = torch.cat([ctx["cur_num"], pad], dim=1)
        print(f"[{ts()}] Padded cur_num: {ctx['cur_num'].shape[1] - n_pad} → {ctx['cur_num'].shape[1]} (added {n_pad} zero cols)")
    
    n_valid = len(ctx["acct"])
    print(f"[{ts()}] Built context for {n_valid:,} valid parcels")

    # Scenarios
    sched = Scheduler(int(_cfg.get("DIFF_STEPS_TRAIN", 128)), device=_device)
    
    # Sample shared latent shock paths
    phi_vec = token_persistence.get_phi()
    Z_tokens = sample_token_paths(K=int(_cfg.get("K_TOKENS", 8)), H=H, phi_vec=phi_vec, S=scenarios, device=_device)

    # Inference chunking
    batch_size = min(256, n_valid)
    all_deltas = []
    for b_start in range(0, n_valid, batch_size):
        b_end = min(b_start + batch_size, n_valid)
        b_deltas = sample_ddim(
            model=model, gating_net=gating_net, sched=sched,
            hist_y_b=ctx["hist_y"][b_start:b_end], cur_num_b=ctx["cur_num"][b_start:b_end],
            cur_cat_b=ctx["cur_cat"][b_start:b_end], region_id_b=ctx["region_id"][b_start:b_end],
            Z_tokens=Z_tokens, coh_scale=coh_scale, device=_device
        )
        all_deltas.append(b_deltas)

    deltas = np.concatenate(all_deltas, axis=0)
    accts = ctx["acct"]
    ya = ctx["y_anchor"]
    y_levels = ya[:, None, None] + np.cumsum(deltas, axis=2)  # [N, S, H] log-space
    base_v = actual_vals.get(origin, {})
    base_vals_arr = np.array([base_v.get(str(a).strip(), 0) for a in accts])
    variant_raw_results[("v11", origin)] = {
        "accts": list(accts),
        "y_anchor": ya,
        "y_levels": y_levels,
        "base_val": base_vals_arr,
        "deltas": deltas,  # [N, S, H] raw deltas for scenario diversity
    }

    # ─── 5. W&B Logging Metrics Engine ───
    print(f"\n[{ts()}] ── Computing and Logging Metrics to W&B ──")
    
    key = ("v11", origin)
    if key in variant_raw_results:
        res = variant_raw_results[key]
        y_levels = res["y_levels"]
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
                    pred_growth = float(np.expm1(np.nanmedian(fan) - y_anchor[i]) * 100)
                    actual_growth = float((av - bv) / bv * 100)
                    preds.append(pred_growth)
                    acts.append(actual_growth)
                    
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
                    
                    # PIT histogram (10 bins) — for calibration visualization
                    pit_hist, _ = np.histogram(pits, bins=10, range=(0, 1))
                    pit_hist_norm = pit_hist / max(pit_hist.sum(), 1)  # normalize
                    # Reliability = sum of squared deviations from uniform
                    pit_reliability = float(np.sum((pit_hist_norm - 0.1)**2) * 10)  # 0=perfect
                    
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
                    for b_idx in range(10):
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
        print(f"  [scenario_diversity] Logged for {H_dim} horizons")
    
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
                alpha = gating_net(
                    ctx["hist_y"][:samp_n].to(_device),
                    ctx["cur_num"][:samp_n].to(_device),
                    ctx["cur_cat"][:samp_n].to(_device),
                    ctx["region_id"][:samp_n].to(_device)
                )  # [N, K]
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

    wandb.finish()
    print(f"\n[{ts()}] ✅ Evaluation Complete.")

@app.local_entrypoint()
def main(
    jurisdiction: str = "sf_ca",
    bucket_name: str = "properlytic-raw-data",
    sample_size: int = 20_000,
    scenarios: int = 128,
):
    origins = [2019, 2020, 2021, 2022, 2023, 2024]
    print(f"🚀 Launching parallel v11 evaluation on Modal across {len(origins)} origins")
    print(f"   Jurisdiction: {jurisdiction}")
    print(f"   Origins: {origins}")
    
    # Map across multiple origins concurrently
    params = [(jurisdiction, bucket_name, o, sample_size, scenarios) for o in origins]
    
    results = list(evaluate_checkpoints.starmap(params))

    print(f"\n✅ Parallel Evaluation complete!")
