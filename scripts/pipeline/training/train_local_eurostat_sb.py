import os
import sys
import time
import torch
import polars as pl
import numpy as np

sys.stdout.reconfigure(encoding='utf-8')
ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")

def main():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
    # Use enriched data if available, fallback to cleaned
    enriched_path = os.path.join(base_dir, "_scratch", "data", "eurostat_enriched.parquet")
    cleaned_path = os.path.join(base_dir, "_scratch", "data", "eurostat_apri_lprc_cleaned.parquet")
    data_path = enriched_path if os.path.exists(enriched_path) else cleaned_path
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Missing {data_path}")
        
    print(f"[{ts()}] Loading Eurostat data from {data_path}")
    df = pl.read_parquet(data_path)
    
    # ── Schema adaptation ──
    df = df.with_columns(
        (pl.col("geo") + "_" + pl.col("agriprod")).alias("acct"),
        pl.col("year").cast(pl.Int64).alias("yr"),
        pl.col("price_eur_per_hectare").cast(pl.Float64).alias("tot_appr_val")
    )
    
    # Filter nulls
    df = df.filter(pl.col("tot_appr_val").is_not_null() & (pl.col("tot_appr_val") > 0))
    
    # Year gap fill
    existing_years = sorted(int(y) for y in df["yr"].drop_nulls().unique().to_list())
    yr_min, yr_max = existing_years[0], existing_years[-1]
    missing_years = sorted(set(range(yr_min, yr_max + 1)) - set(existing_years))
    for gap_yr in missing_years:
        prior = max(y for y in existing_years if y < gap_yr)
        df = pl.concat([df, df.filter(pl.col("yr") == prior).with_columns(
            pl.lit(gap_yr).cast(pl.Int64).alias("yr"))])
    
    if missing_years: 
        df = df.sort(["acct", "yr"])
        
    # Auto-discover macro numeric columns
    macro_num_cols = sorted([c for c in df.columns if c.startswith("macro_") or c.startswith("wdi_")])
    cat_use_local = ['geo', 'agriprod']
    num_use_local = macro_num_cols  # all macro indicators become numeric features
    
    print(f"[{ts()}] Discovered {len(num_use_local)} numeric covariates: {num_use_local}")
    print(f"[{ts()}] Categorical covariates: {cat_use_local}")
    
    # ── Deterministic Geoholdout (80/20) ──
    import hashlib
    def is_holdout(acct: str) -> bool:
        # returns True for ~20% of accts deterministically based on hash
        hex_hash = hashlib.md5(acct.encode("utf-8")).hexdigest()
        return int(hex_hash[:8], 16) % 100 < 20
        
    all_accts = df["acct"].unique().to_list()
    holdout_accts = {a for a in all_accts if is_holdout(a)}
    train_accts = {a for a in all_accts if a not in holdout_accts}
    
    df = df.with_columns(
        pl.col("acct").is_in(list(train_accts)).alias("is_train")
    )
    
    print(f"[{ts()}] Geoholdout Split: {len(train_accts)} Train (80%) / {len(holdout_accts)} Eval (20%)")
    
    # Save the holdout list explicitly for eval script
    holdout_path = os.path.join(base_dir, "_scratch", "data", "eurostat_holdout_accts.txt")
    with open(holdout_path, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(holdout_accts)))
    
    adapted_path = os.path.join(base_dir, "_scratch", "data", "eurostat_adapted.parquet")
    df.write_parquet(adapted_path)
    print(f"[{ts()}] Adapted: {len(df):,} rows, {df['acct'].n_unique():,} parcels, years {yr_min}-{yr_max}")

    # Set environment variables for worldmodel.py
    origins = [2019, 2020, 2021, 2022, 2023, 2024]
    epochs = 40
    
    # Reduce CUDA memory overhead if torch available
    if torch.cuda.is_available():
        torch.backends.cudnn.benchmark = True
    
    os.environ["WM_MAX_ACCTS"] = "500000"
    os.environ["WM_SAMPLE_FRACTION"] = "1.0"
    os.environ["SWEEP_EPOCHS"] = str(epochs)
    
    out_dir = os.path.join(base_dir, "output", "eurostat_v12sb")
    os.makedirs(out_dir, exist_ok=True)
    out_dir_local = os.path.abspath(out_dir)

    # ── Load v11 and v12_sb modules into dict ──
    wm_v11_path = os.path.join(base_dir, "scripts", "inference", "worldmodel.py")
    wm_sb_path = os.path.join(base_dir, "scripts", "inference", "v12_sb", "worldmodel_sb.py")
    
    wm_globals = globals().copy()
    wm_globals['PANEL_PATH'] = adapted_path
    wm_globals['PANEL_PATH_DRIVE'] = adapted_path
    wm_globals['PANEL_PATH_LOCAL'] = adapted_path
    wm_globals['MIN_YEAR'] = yr_min
    wm_globals['MAX_YEAR'] = yr_max
    wm_globals['SEAM_YEAR'] = yr_max
    wm_globals['OUT_DIR'] = out_dir_local
    
    with open(wm_v11_path, "r", encoding="utf-8") as f:
        wm_v11_source = f.read()
    wm_globals['__file__'] = wm_v11_path
    exec(wm_v11_source, wm_globals)

    with open(wm_sb_path, "r", encoding="utf-8") as f:
        wm_sb_source = f.read()
    wm_globals['__file__'] = wm_sb_path
    exec(wm_sb_source, wm_globals)
    
    wm_globals['num_use'] = num_use_local
    wm_globals['cat_use'] = cat_use_local
    wm_globals['NUM_DIM'] = len(num_use_local)
    wm_globals['N_CAT'] = len(cat_use_local)
    
    scratch_dir = os.path.join(base_dir, "local_scratch")
    wm_globals['work_dirs'] = {
        'OUT_DIR_DRIVE': out_dir_local,
        'SCRATCH_ROOT': scratch_dir,
        'RAW_SHARD_ROOT': os.path.join(scratch_dir, 'train_shards_raw'),
        'SCALED_SHARD_ROOT': os.path.join(scratch_dir, 'train_shards_scaled'),
    }
    os.makedirs(os.path.join(scratch_dir, 'train_shards_raw'), exist_ok=True)
    os.makedirs(os.path.join(scratch_dir, 'train_shards_scaled'), exist_ok=True)

    _device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"Using device: {_device}")
    
    for origin in origins:
        os.environ["BACKTEST_MIN_ORIGIN"] = str(origin)
        os.environ["FORECAST_ORIGIN_YEAR"] = str(origin)
        wm_globals['BACKTEST_MIN_ORIGIN'] = origin
        
        print(f"\n==============================================")
        print(f"[{ts()}] Starting training for origin: {origin}")
        print(f"==============================================")
        
        variant_tag = f"eurostat_o{origin}"
        variant_scratch = os.path.join(scratch_dir, f"sweep_{variant_tag}")
        os.makedirs(variant_scratch, exist_ok=True)
        
        variant_work_dirs = wm_globals['work_dirs'].copy()
        variant_work_dirs["RAW_SHARD_ROOT"] = os.path.join(variant_scratch, "raw")
        variant_work_dirs["SCALED_SHARD_ROOT"] = os.path.join(variant_scratch, "scaled")
        os.makedirs(variant_work_dirs["RAW_SHARD_ROOT"], exist_ok=True)
        os.makedirs(variant_work_dirs["SCALED_SHARD_ROOT"], exist_ok=True)
        
        _lf = pl.scan_parquet(adapted_path).filter(pl.col("is_train") == True)
        _train_all_accts = _lf.select(pl.col("acct").cast(pl.Utf8)).unique().collect()["acct"].to_list()
        
        master_result = wm_globals['build_master_training_shards_v102_local'](
            lf=_lf,
            accts=_train_all_accts,
            num_use_local=num_use_local,
            cat_use_local=cat_use_local,
            max_origin=origin,
            full_horizon_only=wm_globals.get("FULL_HORIZON_ONLY", True),
            work_dirs=variant_work_dirs,
        )
        
        _global_medians = master_result["global_medians"]
        master_shards = master_result["shards"]
        
        origin_result = wm_globals['derive_origin_shards_from_master'](
            master_shard_paths=master_shards,
            origin=origin,
            full_horizon_only=wm_globals.get("FULL_HORIZON_ONLY", True),
            work_dirs=variant_work_dirs,
        )
        origin_shards = origin_result["shards"]
        n_train = origin_result["n_train"]
        print(f"[{ts()}] Origin {origin}: {n_train:,} training rows across {len(origin_shards)} shards")
        
        if n_train == 0:
            print(f"WARN: No training data for origin {origin}. Skipping.")
            continue
            
        _hist_len = int(wm_globals['cfg'].get("FULL_HIST_LEN", 21))
        try:
            _first_shard_z = np.load(origin_shards[0], allow_pickle=True)
            _actual_hist_len = int(_first_shard_z["hist_y"].shape[1])
            if _actual_hist_len != _hist_len:
                print(f"[{ts()}] ⚠️  hist_len mismatch: cfg says {_hist_len}, shard has {_actual_hist_len}. Overriding.")
                _hist_len = _actual_hist_len
        except Exception as e:
            print(f"[{ts()}] ⚠️ Could not peek shard for hist_len ({e}). Using cfg value {_hist_len}.")
            
        _H = int(wm_globals['cfg'].get("H", 5))
        _num_dim = len(num_use_local)
        _n_cat = len(cat_use_local)
        
        sf2m_net = wm_globals['create_sf2m_network'](
            target_dim=_H, hist_len=_hist_len,
            num_dim=_num_dim, n_cat=_n_cat,
        ).to(_device)
        
        gating_net = wm_globals['create_gating_network'](
            hist_len=_hist_len, num_dim=_num_dim, n_cat=_n_cat,
        ).to(_device)
        
        token_persistence = wm_globals['create_token_persistence']().to(_device)
        coh_scale = wm_globals['create_coherence_scale']().to(_device)
        ot_coupler = wm_globals['create_ot_coupler']()
        
        mu_backbone = None
            
        print(f"[{ts()}] 🧪 Training SF2M v12_sb for origin {origin} ({epochs} epochs, {n_train:,} rows)")
        
        y_scaler, n_scaler, t_scaler, losses, _ = wm_globals['train_sf2m_v12'](
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
            resume_state=None,
            checkpoint_callback=None,
            checkpoint_every=5,
        )
        
        ckpt_name = f"ckpt_v12sb_eurostat_origin_{origin}.pt"
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
            "cfg": wm_globals['cfg'],
            "arch": "sf2m_v12sb",
            "mu_backbone_state_dict": None,
            "num_use": num_use_local,
            "cat_use": cat_use_local,
            "sweep": {
                "variant": "eurostat",
                "origin": origin,
                "n_train": n_train,
                "epochs": epochs,
                "final_loss": losses[-1] if losses else None,
            },
        }
        out_ckpt = os.path.join(out_dir_local, ckpt_name)
        torch.save(ckpt_data, out_ckpt)
        print(f"[{ts()}] 💾 Saved: {out_ckpt} ({os.path.getsize(out_ckpt)/1e6:.0f}MB)")

if __name__ == "__main__":
    main()
