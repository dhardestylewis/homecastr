"""
Modal training wrapper for Properlytic world model v12_sb (Schrödinger Bridge).
Runs on Modal's serverless A100 GPUs, pulls panel from GCS.

Usage:
    modal run scripts/pipeline/training/train_modal_sb.py --jurisdiction hcad_houston

This is a copy of train_modal.py adapted for v12_sb:
  - Points to scripts/inference/v12_sb/worldmodel_sb.py
  - Checkpoint naming uses _sb suffix
  - W&B tags include v12_sb
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

app = modal.App(f"train-sb-{_jur}-o{_ori}")

training_image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "polars>=0.20",
        "pyarrow>=14.0",
        "numpy>=1.24",
        "pandas>=2.0",
        "torch>=2.1",
        "wandb>=0.16",
        "google-cloud-storage>=2.10",
        "scikit-learn>=1.3",
        "huggingface_hub>=0.20",
        "POT>=0.9",  # Python Optimal Transport for SF2M minibatch OT coupling
    )
    .add_local_dir("scripts", remote_path="/scripts")
)

gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
wandb_secret = modal.Secret.from_name("wandb-creds", required_keys=["WANDB_API_KEY"])


@app.function(
    image=training_image,
    gpu="A100",
    timeout=7200,
    retries=modal.Retries(max_retries=3, backoff_coefficient=1.0, initial_delay=10.0),
    secrets=[gcs_secret, wandb_secret, modal.Secret.from_name("hf-token")],
    volumes={"/output": modal.Volume.from_name("properlytic-checkpoints", create_if_missing=True)},
)
def train_worldmodel_sb(
    jurisdiction: str = "sf_ca",
    bucket_name: str = "properlytic-raw-data",
    epochs: int = 60,
    sample_size: int = 500_000,
    origin: int = 2019,
    panel_gcs_path: str = "",
):
    """Download panel from GCS, adapt schema, train v12_sb bridge model."""
    import json, time, tempfile, shutil, io
    import numpy as np
    import polars as pl

    ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")

    # ─── Set up GCS credentials ───
    creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON", "")
    if creds_json:
        creds_path = "/tmp/gcs_creds.json"
        with open(creds_path, "w") as f:
            f.write(creds_json)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path

    # ─── Download panel from GCS ───
    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # GCS path overrides for jurisdictions with non-standard panel locations
    PANEL_GCS_OVERRIDES = {
        "florida_dor": "panels/florida_dor_panel.parquet",
    }

    # Same panel download logic as train_modal.py (v11)
    if jurisdiction == "all":
        panel_blobs = [b for b in bucket.list_blobs(prefix="panel/jurisdiction=") if b.name.endswith("/part.parquet")]
        if not panel_blobs:
            raise FileNotFoundError(f"No panel partitions found in gs://{bucket_name}/panel/")
        frames = []
        for blob in panel_blobs:
            jur = blob.name.split("jurisdiction=")[1].split("/")[0]
            local = f"/tmp/panel_{jur}.parquet"
            blob.download_to_filename(local)
            df_j = pl.read_parquet(local)
            if "jurisdiction" not in df_j.columns:
                df_j = df_j.with_columns(pl.lit(jur).alias("jurisdiction"))
            if "parcel_id" in df_j.columns:
                df_j = df_j.with_columns(
                    (pl.col("jurisdiction") + "_" + pl.col("parcel_id").cast(pl.Utf8)).alias("parcel_id"))
            frames.append(df_j)
        NUMERIC_COLS = {"property_value", "sale_price", "assessed_value", "land_value",
                        "improvement_value", "sqft", "land_area", "year_built",
                        "bedrooms", "bathrooms", "stories", "lat", "lon",
                        "lehd_total_jobs", "lehd_retail_jobs", "lehd_finance_jobs",
                        "fema_disaster_count", "year"}
        STRING_COLS = {"parcel_id", "jurisdiction", "dwelling_type", "address", "sale_date"}
        harmonized = []
        for f in frames:
            casts = {}
            for c in f.columns:
                if c in NUMERIC_COLS: casts[c] = pl.Float64
                elif c in STRING_COLS: casts[c] = pl.Utf8
            harmonized.append(f.cast(casts))
        df = pl.concat(harmonized, how="diagonal")
        panel_local = "/tmp/panel_all.parquet"
        df.write_parquet(panel_local)
        jurisdiction_suffix = "grand_panel"
    else:
        blob_path = (panel_gcs_path if panel_gcs_path
                     else PANEL_GCS_OVERRIDES.get(jurisdiction,
                          f"panel/jurisdiction={jurisdiction}/part.parquet"))
        if jurisdiction == "nyc":
            blob_path = "panel/jurisdiction=nyc/nyc_panel_h3.parquet"
        blob = bucket.blob(blob_path)
        if not blob.exists():
            available = [b.name for b in bucket.list_blobs(prefix="panel/")]
            raise FileNotFoundError(f"Panel not found at gs://{bucket_name}/{blob_path}\nAvailable: {available}")
        panel_local = f"/tmp/panel_{jurisdiction}.parquet"
        blob.download_to_filename(panel_local)
        df = pl.read_parquet(panel_local)
        jurisdiction_suffix = jurisdiction

    # ─── Clean Census suppression ───
    for col in df.columns:
        if df[col].dtype in (pl.Float64, pl.Float32, pl.Int64, pl.Int32):
            df = df.with_columns(
                pl.when(pl.col(col) < -600_000_000).then(None).otherwise(pl.col(col)).alias(col))

    # ─── Schema adaptation (same as v11) ───
    rename_map = {
        "parcel_id": "acct", "year": "yr", "sqft": "living_area", "land_area": "land_ar",
        "year_built": "yr_blt", "bedrooms": "bed_cnt", "bathrooms": "full_bath",
        "stories": "nbr_story", "lat": "gis_lat", "lon": "gis_lon",
    }
    actual_renames = {k: v for k, v in rename_map.items() if k in df.columns}
    drop_targets = [v for k, v in actual_renames.items() if v in df.columns]
    if drop_targets: df = df.drop(drop_targets)
    df = df.rename(actual_renames)

    available_val_cols = [c for c in ["sale_price", "property_value", "assessed_value",
                                       "median_home_value", "market_value", "value",
                                       "total_appraised_value"] if c in df.columns]
    if available_val_cols and "tot_appr_val" not in df.columns:
        df = df.with_columns(pl.coalesce([pl.col(c) for c in available_val_cols]).alias("tot_appr_val"))
    elif "tot_appr_val" not in df.columns:
        raise ValueError("Panel contains no usable target valuation column.")

    if "yr" in df.columns and df["yr"].dtype == pl.Utf8:
        df = df.with_columns(pl.col("yr").cast(pl.Int64, strict=False))
    if "tot_appr_val" in df.columns and df["tot_appr_val"].dtype == pl.Utf8:
        df = df.with_columns(pl.col("tot_appr_val").cast(pl.Float64, strict=False))

    # Year gap fill
    if "yr" in df.columns:
        existing_years = sorted(int(y) for y in df["yr"].drop_nulls().unique().to_list() if y > 1900)
        if existing_years:
            yr_min, yr_max = existing_years[0], existing_years[-1]
            missing_years = sorted(set(range(yr_min, yr_max + 1)) - set(existing_years))
            for gap_yr in missing_years:
                prior = max(y for y in existing_years if y < gap_yr)
                df = pl.concat([df, df.filter(pl.col("yr") == prior).with_columns(
                    pl.lit(gap_yr).cast(pl.Int64).alias("yr"))])
            if missing_years: df = df.sort(["acct", "yr"])

    # Drop leaky columns
    leaky_cols = ["sale_price", "property_value", "assessed_value", "land_value",
                  "improvement_value", "median_home_value", "market_value", "value",
                  "total_value", "prior_value", "growth_pct", "CURMKTLAND",
                  "CURMKTTOT", "CURACTTOT", "FINACTTOT", "total_appraised_value"]
    drop_leaks = [c for c in leaky_cols if c in df.columns]
    if drop_leaks: df = df.drop(drop_leaks)

    # Macro enrichment (same as v11)
    try:
        import pandas as _pd
        MACRO_SERIES = {
            "macro/fred/MORTGAGE30US.csv": "macro_mortgage30",
            "macro/fred/FEDFUNDS.csv": "macro_fedfunds",
            "macro/fred/DGS10.csv": "macro_10yr_treasury",
            "macro/fred/CPIAUCSL.csv": "macro_cpi_us",
            "macro/fred/DCOILWTICO.csv": "macro_oil_price",
            "macro/fred/UNRATE.csv": "macro_unemployment_us",
            "macro/fred/VIXCLS.csv": "macro_vix",
            "macro/fred/GEPUCURRENT.csv": "macro_global_epu",
        }
        macro_frames = {}
        for gcs_path, col_name in MACRO_SERIES.items():
            try:
                blob_m = bucket.blob(gcs_path)
                if not blob_m.exists(): continue
                raw = _pd.read_csv(io.BytesIO(blob_m.download_as_bytes()), on_bad_lines="skip")
                if len(raw.columns) < 2: continue
                date_col, val_col = raw.columns[0], raw.columns[1]
                raw[date_col] = _pd.to_datetime(raw[date_col], errors="coerce")
                raw[val_col] = _pd.to_numeric(raw[val_col], errors="coerce")
                raw = raw.dropna(subset=[date_col, val_col])
                raw["_year"] = raw[date_col].dt.year
                annual = raw.groupby("_year")[val_col].mean().reset_index()
                annual.columns = ["_year", col_name]
                macro_frames[col_name] = annual
            except Exception: pass
        if macro_frames:
            merged = None
            for name, frame in macro_frames.items():
                merged = frame if merged is None else merged.merge(frame, on="_year", how="outer")
            macro_pl = pl.from_pandas(merged.sort_values("_year")).rename({"_year": "yr"}).cast({"yr": pl.Int64})
            df = df.join(macro_pl, on="yr", how="left")
    except Exception as e:
        print(f"[{ts()}] ⚠️ Macro enrichment failed (non-fatal): {e}")

    df = df.filter(pl.col("tot_appr_val").is_not_null() & (pl.col("tot_appr_val") > 0))

    # Winsorize
    _vals = df["tot_appr_val"].to_numpy()
    import numpy as _np
    _p_lo = float(_np.nanpercentile(_vals, 2.0))
    _p_hi = float(_np.nanpercentile(_vals, 98.0))
    df = df.with_columns(pl.col("tot_appr_val").clip(_p_lo, _p_hi))

    for col in df.columns:
        if df[col].dtype == pl.Utf8:
            df = df.with_columns(pl.col(col).fill_null("UNKNOWN"))

    df = df.with_columns([pl.col("acct").cast(pl.Utf8), pl.col("yr").cast(pl.Int64),
                           pl.col("tot_appr_val").cast(pl.Float64)])

    # Drop all-null columns
    null_counts = df.null_count()
    n_rows = len(df)
    all_null_cols = [c for c in df.columns if null_counts[c][0] == n_rows and c not in ("acct", "yr", "tot_appr_val")]
    if all_null_cols: df = df.drop(all_null_cols)

    adapted_path = f"/tmp/panel_{jurisdiction_suffix}_adapted.parquet"
    df.write_parquet(adapted_path)

    yr_min = int(df["yr"].min())
    yr_max = int(df["yr"].max())
    n_accts = df["acct"].n_unique()
    print(f"[{ts()}] Adapted: {len(df):,} rows, {n_accts:,} parcels, years {yr_min}-{yr_max}")

    # ─── Environment setup ───
    os.environ["WM_MAX_ACCTS"] = str(sample_size)
    os.environ["WM_SAMPLE_FRACTION"] = "1.0"
    os.environ["SWEEP_EPOCHS"] = str(epochs)
    os.environ["BACKTEST_MIN_ORIGIN"] = str(origin)
    os.environ["FORECAST_ORIGIN_YEAR"] = str(origin)

    out_dir = f"/output/{jurisdiction_suffix}_v12sb"
    os.makedirs(out_dir, exist_ok=True)

    # ─── Step 1: Load v11 worldmodel for data plumbing ───
    # v11's worldmodel.py exec's worldmodel_inference.py, which provides:
    #   build_master_training_shards_v102_local, derive_origin_shards_from_master,
    #   fit_scalers_from_shards_v102_robust_y, write_scaled_shards_v102,
    #   create_gating_network, create_token_persistence, create_coherence_scale,
    #   create_mu_backbone, init_wandb, copy_small_artifacts_to_drive, etc.
    wm_v11_path = "/scripts/inference/worldmodel.py"
    print(f"[{ts()}] Loading v11 worldmodel (data plumbing) from {wm_v11_path}")

    globals()['PANEL_PATH'] = adapted_path
    globals()['PANEL_PATH_DRIVE'] = adapted_path
    globals()['PANEL_PATH_LOCAL'] = adapted_path
    globals()['MIN_YEAR'] = yr_min
    globals()['MAX_YEAR'] = yr_max
    globals()['SEAM_YEAR'] = yr_max
    globals()['OUT_DIR'] = out_dir

    with open(wm_v11_path, "r") as f:
        wm_v11_source = f.read()
    globals()['__file__'] = wm_v11_path
    exec(wm_v11_source, globals())

    # ─── Step 2: Overlay v12_sb SF2M code ───
    # This replaces v11's denoiser/diffusion with SF2MNetwork/train_sf2m_v12
    wm_sb_path = "/scripts/inference/v12_sb/worldmodel_sb.py"
    print(f"[{ts()}] Loading v12_sb SF2M overlay from {wm_sb_path}")

    with open(wm_sb_path, "r") as f:
        wm_sb_source = f.read()
    globals()['__file__'] = wm_sb_path
    exec(wm_sb_source, globals())

    globals()['work_dirs'] = {
        'OUT_DIR_DRIVE': out_dir,
        'SCRATCH_ROOT': '/tmp/wm_scratch',
        'RAW_SHARD_ROOT': '/tmp/wm_scratch/train_shards_raw',
        'SCALED_SHARD_ROOT': '/tmp/wm_scratch/train_shards_scaled',
    }
    os.makedirs('/tmp/wm_scratch/train_shards_raw', exist_ok=True)
    os.makedirs('/tmp/wm_scratch/train_shards_scaled', exist_ok=True)

    print(f"[{ts()}] v12_sb worldmodel loaded. Ready for training.")
    print(f"[{ts()}] Training config: origin={origin}, epochs={epochs}, sample={sample_size:,}")
    sys.stdout.flush()

    # ─── Build training shards ───
    import polars as pl
    import torch
    import torch.nn as nn
    import copy

    _device = "cuda" if torch.cuda.is_available() else "cpu"
    _lf = pl.scan_parquet(adapted_path)
    _all_accts = (
        _lf.select(pl.col("acct").cast(pl.Utf8)).unique()
        .collect()["acct"].to_list()
    )
    _num_use = globals().get("num_use", [])
    _cat_use = globals().get("cat_use", [])
    _num_dim = globals().get("NUM_DIM", len(_num_use))
    _n_cat = globals().get("N_CAT", len(_cat_use))
    _full_horizon_only = globals().get("FULL_HORIZON_ONLY", True)
    _cfg = globals().get("cfg", {})
    _work_dirs = globals().get("work_dirs", {})
    _H = int(_cfg.get("H", 5))
    _hist_len = int(_cfg.get("FULL_HIST_LEN", 21))

    # Sample accounts (random, deterministic seed)
    import numpy as _np
    _rng = _np.random.default_rng(42 + origin * 100)
    _n_sample = min(sample_size, len(_all_accts))
    if len(_all_accts) > _n_sample:
        _idx = _rng.choice(len(_all_accts), size=_n_sample, replace=False)
        sweep_accts = [_all_accts[i] for i in _idx]
    else:
        sweep_accts = list(_all_accts)
    print(f"[{ts()}] Sampled {len(sweep_accts):,} accounts")

    # Build master shards
    variant_tag = "SF500K"
    variant_scratch = os.path.join(
        _work_dirs.get("SCRATCH_ROOT", "/tmp/wm_scratch"),
        f"sweep_{variant_tag}"
    )
    os.makedirs(variant_scratch, exist_ok=True)
    variant_work_dirs = copy.deepcopy(_work_dirs)
    variant_work_dirs["RAW_SHARD_ROOT"] = os.path.join(variant_scratch, "raw")
    variant_work_dirs["SCALED_SHARD_ROOT"] = os.path.join(variant_scratch, "scaled")
    os.makedirs(variant_work_dirs["RAW_SHARD_ROOT"], exist_ok=True)
    os.makedirs(variant_work_dirs["SCALED_SHARD_ROOT"], exist_ok=True)

    master_result = build_master_training_shards_v102_local(
        lf=_lf,
        accts=sweep_accts,
        num_use_local=_num_use,
        cat_use_local=_cat_use,
        max_origin=origin,
        full_horizon_only=_full_horizon_only,
        work_dirs=variant_work_dirs,
    )
    _global_medians = master_result["global_medians"]
    master_shards = master_result["shards"]

    # Derive origin-specific shards
    origin_result = derive_origin_shards_from_master(
        master_shard_paths=master_shards,
        origin=origin,
        full_horizon_only=_full_horizon_only,
        work_dirs=variant_work_dirs,
    )
    origin_shards = origin_result["shards"]
    n_train = origin_result["n_train"]
    print(f"[{ts()}] Origin {origin}: {n_train:,} training rows across {len(origin_shards)} shards")

    if n_train == 0:
        raise ValueError(f"No training data for origin {origin}")

    # ─── Create v12_sb model components ───
    sf2m_net = create_sf2m_network(
        target_dim=_H, hist_len=_hist_len,
        num_dim=_num_dim, n_cat=_n_cat,
    ).to(_device)

    gating_net = create_gating_network(
        hist_len=_hist_len, num_dim=_num_dim, n_cat=_n_cat,
    ).to(_device)

    token_persistence = create_token_persistence().to(_device)
    coh_scale = create_coherence_scale().to(_device)
    ot_coupler = create_ot_coupler()

    _ab1_enabled = bool(globals().get("AB1_ENABLED", False))
    mu_backbone = None
    if _ab1_enabled:
        mu_backbone = create_mu_backbone(
            hist_len=_hist_len, num_dim=_num_dim, n_cat=_n_cat, H_dim=_H,
        ).to(_device)
        _bb_params = sum(p.numel() for p in mu_backbone.parameters())
        print(f"[{ts()}] AB1: mu_backbone ({_bb_params:,} params), FREEZE_AB1={FREEZE_AB1}")

    _sf2m_params = sum(p.numel() for p in sf2m_net.parameters())
    _gate_params = sum(p.numel() for p in gating_net.parameters())
    print(f"[{ts()}] SF2M: sf2m_net ({_sf2m_params:,}p) + gating ({_gate_params:,}p)")
    print(f"[{ts()}] OT: method={ot_coupler.method} reg={ot_coupler.reg:.2f} "
          f"micro={ot_coupler.microbatch} cond_w={ot_coupler.cond_weight:.3f}")

    # ─── Initialize W&B ───
    try:
        init_wandb(
            name=f"v12sb-{jurisdiction}-o{origin}",
            tags=["v12_sb", "sf2m", f"origin_{origin}", jurisdiction],
            extra_config={
                "variant": variant_tag, "origin": origin,
                "sample_size": sample_size, "n_train": n_train,
                "arch": "sf2m", "freeze_ab1": FREEZE_AB1,
                "ot_microbatch": OT_MICROBATCH, "ot_cond_weight": OT_COND_WEIGHT,
                "sigma_max": BRIDGE_SIGMA_MAX,
            },
        )
    except Exception as e:
        print(f"[{ts()}] ⚠️ W&B init: {e}")

    # ── Check for resume checkpoint in Modal Volume ──
    resume_state = None
    resume_ckpt_path = os.path.join(out_dir, f"ckpt_v12sb_resume_o{origin}_{variant_tag}.pt")
    if os.path.exists(resume_ckpt_path):
        try:
            resume_state = torch.load(resume_ckpt_path, map_location=_device, weights_only=False)
            _resume_ep = resume_state.get("epoch", "?")
            print(f"[{ts()}] ♻️ Found resume checkpoint at epoch {_resume_ep}: {resume_ckpt_path}")
        except Exception as e:
            print(f"[{ts()}] ⚠️ Failed to load resume checkpoint (starting fresh): {e}")
            resume_state = None
    else:
        print(f"[{ts()}] No resume checkpoint found, starting fresh")

    # ── Checkpoint callback: write to Modal Volume every N epochs ──
    _vol = modal.Volume.from_name("properlytic-checkpoints")
    def _checkpoint_callback(epoch, state_dict):
        """Save resume checkpoint to Modal Volume."""
        torch.save(state_dict, resume_ckpt_path)
        _vol.commit()  # persist to durable storage

    # ─── Train SF2M ───
    print(f"[{ts()}] 🧪 Training SF2M v12_sb ({epochs} epochs, {n_train:,} rows)")
    sys.stdout.flush()

    y_scaler, n_scaler, t_scaler, losses, _ = train_sf2m_v12(
        shard_paths=origin_shards,
        origin=origin,
        epochs=epochs,
        sf2m_net=sf2m_net,
        gating_net=gating_net,
        token_persistence=token_persistence,
        coh_scale=coh_scale,
        device=_device,
        num_dim=_num_dim,
        n_cat=_n_cat,
        work_dirs=variant_work_dirs,
        mu_backbone=mu_backbone,
        ot_coupler=ot_coupler,
        resume_state=resume_state,
        checkpoint_callback=_checkpoint_callback,
        checkpoint_every=5,
    )

    # ─── Save checkpoint (v12sb naming — does NOT overwrite v11 checkpoints) ───
    ckpt_name = f"ckpt_v12sb_origin_{origin}_{variant_tag}.pt"
    ckpt_data = {
        "sf2m_net_state_dict": sf2m_net.state_dict(),
        "gating_net_state_dict": gating_net.state_dict(),
        "token_persistence_state_dict": token_persistence.state_dict(),
        "coh_scale_state_dict": coh_scale.state_dict(),
        "y_scaler_mean": y_scaler.mean_.tolist(),
        "y_scaler_scale": y_scaler.scale_.tolist(),
        "n_scaler_mean": n_scaler.mean_.tolist(),
        "n_scaler_scale": n_scaler.scale_.tolist(),
        "t_scaler_mean": t_scaler.mean_.tolist(),
        "t_scaler_scale": t_scaler.scale_.tolist(),
        "global_medians": _global_medians,
        "cfg": _cfg,
        "arch": "sf2m_v12sb",
        "mu_backbone_state_dict": mu_backbone.state_dict() if mu_backbone is not None else None,
        "num_use": list(_num_use),
        "cat_use": list(_cat_use),
        "sweep": {
            "variant": variant_tag,
            "sample_size": sample_size,
            "n_train": n_train,
            "epochs": epochs,
            "final_loss": losses[-1] if losses else None,
            "phi_k_final": token_persistence.get_phi_list(),
            "sigma_u_final": coh_scale.get_sigma(),
            "freeze_ab1": FREEZE_AB1,
            "ot_method": ot_coupler.method,
            "ot_microbatch": ot_coupler.microbatch,
            "ot_cond_weight": ot_coupler.cond_weight,
            "sigma_max": BRIDGE_SIGMA_MAX,
            "inference_mode": INFERENCE_MODE,
        },
    }
    local_ckpt = os.path.join(variant_scratch, ckpt_name)
    torch.save(ckpt_data, local_ckpt)
    final_path = copy_small_artifacts_to_drive(local_ckpt, out_dir)
    print(f"[{ts()}] 💾 Saved: {final_path} ({os.path.getsize(final_path)/1e6:.0f}MB)")

    # ── Clean up resume checkpoint (training completed successfully) ──
    if os.path.exists(resume_ckpt_path):
        try:
            os.remove(resume_ckpt_path)
            _vol.commit()
            print(f"[{ts()}] 🧹 Cleaned up resume checkpoint")
        except Exception as e:
            print(f"[{ts()}] ⚠️ Failed to clean up resume checkpoint: {e}")

    print(f"[{ts()}] ✅ SF2M v12_sb training complete.")
    sys.stdout.flush()

    return {
        "jurisdiction": jurisdiction,
        "origin": origin,
        "epochs": epochs,
        "output_dir": out_dir,
        "model_version": "v12_sb",
    }


@app.local_entrypoint()
def main(
    jurisdiction: str = "sf_ca",
    epochs: int = 60,
    sample_size: int = 500_000,
    origin: int = 2019,
    panel_gcs_path: str = "",
):
    """Use: modal run --detach scripts/pipeline/training/train_modal_sb.py --jurisdiction hcad_houston"""
    print(f"🚀 Training v12_sb (Schrödinger Bridge): {jurisdiction} o={origin} epochs={epochs}")
    result = train_worldmodel_sb.remote(
        jurisdiction=jurisdiction, epochs=epochs, sample_size=sample_size,
        origin=origin, panel_gcs_path=panel_gcs_path)
    print(f"✅ Done: {result}")
