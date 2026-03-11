"""
train_global_panel_sb.py
========================
Trains the V12 SB world model on the unified global panel.
Adapts the global_panel.parquet to the worldmodel schema and
supports source-holdout cross-validation.

Usage:
  python train_global_panel_sb.py                     # Train on all data
  python train_global_panel_sb.py --holdout usda      # Hold out USDA, train on rest
  python train_global_panel_sb.py --holdout australia  # Hold out Australia, train on rest
"""

import os, sys, time, hashlib, argparse
import torch
import polars as pl
import numpy as np

sys.stdout.reconfigure(encoding='utf-8')
ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")


def adapt_global_panel(base_dir, holdout_group=None):
    """Load global panel and adapt to worldmodel schema."""
    panel_path = os.path.join(base_dir, "_scratch", "data", "global_panel.parquet")
    if not os.path.exists(panel_path):
        raise FileNotFoundError(f"Missing {panel_path}. Run build_global_panel.py first.")
    
    df = pl.read_parquet(panel_path)
    print(f"[{ts()}] Loaded global panel: {len(df):,} rows, {df['iso2'].n_unique()} countries")
    
    # Create unique account ID from iso2 + region_name
    df = df.with_columns(
        (pl.col("iso2") + "_" + pl.col("region_name")).alias("acct"),
        pl.col("price_usd_per_ha").cast(pl.Float64).alias("tot_appr_val"),
        pl.col("iso2").alias("geo"),
        pl.col("source_group").alias("agriprod"),  # reuse agriprod slot for source_group
    )
    
    # Filter nulls
    df = df.filter(pl.col("tot_appr_val").is_not_null() & (pl.col("tot_appr_val") > 0))
    
    # ── Join WDI covariates from the enrichment pipeline ──
    wdi_path = os.path.join(base_dir, "_scratch", "data", "wdi_covariates.parquet")
    if os.path.exists(wdi_path):
        wdi = pl.read_parquet(wdi_path)
        df = df.join(wdi, on=["iso2", "yr"], how="left")
        print(f"[{ts()}] Joined WDI covariates from cache")
    else:
        # Fetch WDI covariates inline (minimal set)
        print(f"[{ts()}] Fetching WDI covariates for {df['iso2'].n_unique()} countries...")
        df = _fetch_and_join_wdi(df, base_dir)
    
    # ── Join FAO producer prices as covariate ──
    fao_path = os.path.join(base_dir, "_scratch", "data", "fao_producer_prices.parquet")
    if os.path.exists(fao_path):
        fao = pl.read_parquet(fao_path).select(["iso2", "yr", "median_crop_price_usd_tonne"])
        fao = fao.rename({"median_crop_price_usd_tonne": "fao_crop_price"})
        # Fix FAO iso2 casing (was mixed case)
        fao = fao.with_columns(pl.col("iso2").str.to_uppercase())
        df = df.join(fao, on=["iso2", "yr"], how="left")
        print(f"[{ts()}] Joined FAO crop price covariate")
    
    # ── Join FRED macro series (country-independent) ──
    fred_path = os.path.join(base_dir, "_scratch", "data", "fred_macro.parquet")
    if os.path.exists(fred_path):
        fred = pl.read_parquet(fred_path)
        df = df.join(fred, on="yr", how="left")
        print(f"[{ts()}] Joined FRED macro series")
    
    # Fill NaN in numeric covariates with 0
    num_cols = [c for c in df.columns if c.startswith("wdi_") or c.startswith("macro_") or c == "fao_crop_price"]
    for c in num_cols:
        df = df.with_columns(pl.col(c).fill_null(0.0))
    
    # Year gap fill — add missing years within each account's range
    yr_min = int(df["yr"].min())
    yr_max = int(df["yr"].max())
    existing_years = sorted(int(y) for y in df["yr"].unique().to_list())
    all_years = list(range(yr_min, yr_max + 1))
    missing_years = sorted(set(all_years) - set(existing_years))
    for gap_yr in missing_years:
        prior_years = [y for y in existing_years if y < gap_yr]
        if not prior_years:
            continue
        prior = max(prior_years)
        df = pl.concat([df, df.filter(pl.col("yr") == prior).with_columns(
            pl.lit(gap_yr).cast(pl.Int64).alias("yr"))])
    df = df.sort(["acct", "yr"])
    
    # ── Source-holdout split ──
    if holdout_group:
        holdout_accts = set(df.filter(pl.col("source_group") == holdout_group)["acct"].unique().to_list())
        train_accts = set(df["acct"].unique().to_list()) - holdout_accts
        df = df.with_columns(
            pl.col("acct").is_in(list(train_accts)).alias("is_train")
        )
        print(f"[{ts()}] Source holdout '{holdout_group}': {len(train_accts)} train / {len(holdout_accts)} eval accounts")
    else:
        # Default: 80/20 geoholdout
        all_accts = df["acct"].unique().to_list()
        holdout_accts = {a for a in all_accts if int(hashlib.md5(a.encode("utf-8")).hexdigest()[:8], 16) % 100 < 20}
        train_accts = {a for a in all_accts if a not in holdout_accts}
        df = df.with_columns(
            pl.col("acct").is_in(list(train_accts)).alias("is_train")
        )
        print(f"[{ts()}] Geoholdout: {len(train_accts)} train / {len(holdout_accts)} eval accounts")
    
    # Save holdout list
    holdout_path = os.path.join(base_dir, "_scratch", "data", "global_holdout_accts.txt")
    with open(holdout_path, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(holdout_accts)))
    
    # Auto-discover numeric columns
    num_use_local = sorted([c for c in df.columns if c.startswith("macro_") or c.startswith("wdi_") or c == "fao_crop_price"])
    cat_use_local = ['geo', 'agriprod']
    
    adapted_path = os.path.join(base_dir, "_scratch", "data", "global_adapted.parquet")
    df.write_parquet(adapted_path)
    print(f"[{ts()}] Adapted: {len(df):,} rows, {df['acct'].n_unique():,} accounts, years {yr_min}-{yr_max}")
    print(f"[{ts()}] Numeric covariates ({len(num_use_local)}): {num_use_local}")
    
    return adapted_path, num_use_local, cat_use_local, yr_min, yr_max, holdout_accts


def _fetch_and_join_wdi(df, base_dir):
    """Fetch WDI indicators for countries in the panel and join."""
    import urllib.request, json
    
    countries = sorted(df["iso2"].unique().to_list())
    
    WDI_INDICATORS = {
        "NY.GDP.PCAP.CD": "wdi_gdp_per_capita",
        "FR.INR.RINR": "wdi_real_interest_rate",
        "SP.URB.TOTL.IN.ZS": "wdi_urban_pct",
        "EN.POP.DNST": "wdi_pop_density",
        "AG.LND.AGRI.ZS": "wdi_ag_land_pct",
        "FP.CPI.TOTL.ZG": "wdi_cpi_inflation",
    }
    
    wdi_records = []
    batch_size = 20
    
    for i in range(0, len(countries), batch_size):
        batch = countries[i:i+batch_size]
        country_str = ";".join(batch)
        
        for indicator_code, col_name in WDI_INDICATORS.items():
            url = f"https://api.worldbank.org/v2/country/{country_str}/indicator/{indicator_code}?format=json&per_page=10000&date=1995:2024"
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                
                if len(data) > 1 and data[1]:
                    for entry in data[1]:
                        iso2 = entry.get("country", {}).get("id", "")
                        yr = int(entry.get("date", "0"))
                        val = entry.get("value")
                        if val is not None and yr >= 1995:
                            wdi_records.append({"iso2": iso2, "yr": yr, col_name: float(val)})
            except Exception:
                pass
    
    if wdi_records:
        wdi_df = pl.from_pandas(__import__("pandas").DataFrame(wdi_records))
        # Aggregate: group by iso2+yr, take first non-null for each indicator
        wdi_cols = list(WDI_INDICATORS.values())
        wdi_agg = wdi_df.group_by(["iso2", "yr"]).agg([
            pl.col(c).drop_nulls().first().alias(c) for c in wdi_cols if c in wdi_df.columns
        ])
        
        # Cache for future runs
        wdi_cache = os.path.join(base_dir, "_scratch", "data", "wdi_covariates.parquet")
        wdi_agg.write_parquet(wdi_cache)
        print(f"[{ts()}] Cached WDI: {len(wdi_agg):,} country-year records")
        
        df = df.join(wdi_agg, on=["iso2", "yr"], how="left", suffix="_wdi")
    
    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--holdout", type=str, default=None, 
                        help="Source group to hold out (e.g., usda, australia, latam)")
    args = parser.parse_args()
    
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
    
    adapted_path, num_use_local, cat_use_local, yr_min, yr_max, holdout_accts = \
        adapt_global_panel(base_dir, holdout_group=args.holdout)
    
    # Training config
    origins = list(range(max(yr_min + 3, 2015), yr_max + 1))  # Need at least 3 years of history
    epochs = int(os.environ.get("SWEEP_EPOCHS", "40"))
    
    if torch.cuda.is_available():
        torch.backends.cudnn.benchmark = True
    
    os.environ["WM_MAX_ACCTS"] = "500000"
    os.environ["WM_SAMPLE_FRACTION"] = "1.0"
    os.environ["SWEEP_EPOCHS"] = str(epochs)
    
    holdout_tag = args.holdout or "geo20"
    out_dir = os.path.join(base_dir, "output", f"global_v12sb_{holdout_tag}")
    os.makedirs(out_dir, exist_ok=True)
    
    # Load worldmodel modules
    wm_v11_path = os.path.join(base_dir, "scripts", "inference", "worldmodel.py")
    wm_sb_path = os.path.join(base_dir, "scripts", "inference", "v12_sb", "worldmodel_sb.py")
    
    wm_globals = globals().copy()
    wm_globals['PANEL_PATH'] = adapted_path
    wm_globals['PANEL_PATH_DRIVE'] = adapted_path
    wm_globals['PANEL_PATH_LOCAL'] = adapted_path
    wm_globals['MIN_YEAR'] = yr_min
    wm_globals['MAX_YEAR'] = yr_max
    wm_globals['SEAM_YEAR'] = yr_max
    wm_globals['OUT_DIR'] = out_dir
    
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
    
    scratch_dir = os.path.join(base_dir, "local_scratch", f"global_{holdout_tag}")
    wm_globals['work_dirs'] = {
        'OUT_DIR_DRIVE': out_dir,
        'SCRATCH_ROOT': scratch_dir,
        'RAW_SHARD_ROOT': os.path.join(scratch_dir, 'train_shards_raw'),
        'SCALED_SHARD_ROOT': os.path.join(scratch_dir, 'train_shards_scaled'),
    }
    os.makedirs(os.path.join(scratch_dir, 'train_shards_raw'), exist_ok=True)
    os.makedirs(os.path.join(scratch_dir, 'train_shards_scaled'), exist_ok=True)

    _device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"[{ts()}] Device: {_device}, Origins: {origins}, Epochs: {epochs}")
    
    for origin in origins:
        os.environ["BACKTEST_MIN_ORIGIN"] = str(origin)
        os.environ["FORECAST_ORIGIN_YEAR"] = str(origin)
        wm_globals['BACKTEST_MIN_ORIGIN'] = origin
        
        print(f"\n==============================================")
        print(f"[{ts()}] Training GLOBAL origin: {origin} (holdout={holdout_tag})")
        print(f"==============================================")
        
        variant_tag = f"global_{holdout_tag}_o{origin}"
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
                _hist_len = _actual_hist_len
        except Exception:
            pass
            
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
        
        print(f"[{ts()}] Training SF2M v12_sb GLOBAL origin {origin} ({epochs} epochs, {n_train:,} rows)")
        
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
            mu_backbone=None,
            ot_coupler=ot_coupler,
            resume_state=None,
            checkpoint_callback=None,
            checkpoint_every=5,
        )
        
        ckpt_name = f"ckpt_v12sb_global_{holdout_tag}_origin_{origin}.pt"
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
                "variant": f"global_{holdout_tag}",
                "origin": origin,
                "n_train": n_train,
                "epochs": epochs,
                "final_loss": losses[-1] if losses else None,
                "holdout_group": holdout_tag,
            },
        }
        out_ckpt = os.path.join(out_dir, ckpt_name)
        torch.save(ckpt_data, out_ckpt)
        print(f"[{ts()}] Saved: {out_ckpt} ({os.path.getsize(out_ckpt)/1e6:.0f}MB)")

if __name__ == "__main__":
    main()
