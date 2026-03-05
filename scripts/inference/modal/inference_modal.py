"""
Grand Inference Pipeline — Modal wrapper.
Downloads panel + checkpoints from GCS, runs worldmodel.py + inference_pipeline.py,
streams results to Supabase.

Usage:
    modal run scripts/inference_modal.py --jurisdiction hcad_houston [--origin 2025]
"""
import modal, os, sys, json

# Parse CLI args at module level for Modal dashboard naming
_jur = "hcad_houston"
_origin = "2025"
for i, a in enumerate(sys.argv):
    if a == "--jurisdiction" and i + 1 < len(sys.argv):
        _jur = sys.argv[i + 1]
    if a == "--origin" and i + 1 < len(sys.argv):
        _origin = sys.argv[i + 1]

app = modal.App(f"inference-{_jur}-o{_origin}")

# GPU image with all dependencies
image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "google-cloud-storage",
        "torch",
        "numpy",
        "pandas",
        "polars",
        "pyarrow",
        "psycopg2-binary",
        "wandb",
        "scipy",
        "scikit-learn",
    )
)

# Secrets
gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
supabase_secret = modal.Secret.from_name("supabase-creds", required_keys=["SUPABASE_DB_URL"])
wandb_secret = modal.Secret.from_name("wandb-creds", required_keys=["WANDB_API_KEY"])

# Volume for checkpoint & output persistence
output_vol = modal.Volume.from_name("inference-outputs", create_if_missing=True)
ckpt_vol = modal.Volume.from_name("properlytic-checkpoints", create_if_missing=True)


@app.function(
    image=image,
    secrets=[gcs_secret, supabase_secret, wandb_secret],
    gpu="A100",
    timeout=86400,    # 24 hours — full 1.2M acct inference needs ~13h
    memory=32768,     # 32 GB RAM
    volumes={"/output": output_vol, "/checkpoints": ckpt_vol},
)
def run_inference(jurisdiction: str, origin_year: int, backtest: bool = False):
    """Run grand inference for one jurisdiction at one origin year."""
    import time
    t0 = time.time()

    # ─── 1. Download panel from GCS ───────────────────────────────────
    from google.cloud import storage
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")

    # Panel
    panel_blob = bucket.blob(f"panel/jurisdiction={jurisdiction}/part.parquet")
    panel_path = f"/tmp/panel_{jurisdiction}.parquet"
    panel_blob.download_to_filename(panel_path)
    print(f"[{_ts()}] Downloaded panel: {os.path.getsize(panel_path) / 1e6:.1f} MB")

    # ─── 2. Download worldmodel.py from GCS ──────────────────────────
    wm_blob = bucket.blob("code/worldmodel.py")
    wm_source = wm_blob.download_as_text()
    print(f"[{_ts()}] Downloaded worldmodel.py: {len(wm_source)} chars")

    # ─── 3. Find checkpoints (Modal volume first, then GCS) ──────────
    ckpt_dir = f"/output/{jurisdiction}_v11"
    os.makedirs(ckpt_dir, exist_ok=True)

    # Check Modal volume (training saves here)
    modal_ckpt_dir = f"/checkpoints/{jurisdiction}_v11"
    import glob as _glob
    modal_ckpts = _glob.glob(os.path.join(modal_ckpt_dir, "*.pt")) if os.path.isdir(modal_ckpt_dir) else []
    if modal_ckpts:
        import shutil
        for src in modal_ckpts:
            dst = os.path.join(ckpt_dir, os.path.basename(src))
            if not os.path.exists(dst):
                shutil.copy2(src, dst)
                print(f"[{_ts()}] Copied checkpoint from volume: {os.path.basename(src)}")
            else:
                print(f"[{_ts()}] Checkpoint cached: {os.path.basename(src)}")
    
    # Also try GCS
    ckpt_prefix = f"checkpoints/{jurisdiction}/"
    blobs = list(bucket.list_blobs(prefix=ckpt_prefix))
    ckpt_files = [b for b in blobs if b.name.endswith(".pt")]
    if not ckpt_files:
        ckpt_prefix2 = f"output/{jurisdiction}_v11/"
        blobs2 = list(bucket.list_blobs(prefix=ckpt_prefix2))
        ckpt_files = [b for b in blobs2 if b.name.endswith(".pt")]
    for blob in ckpt_files:
        fname = blob.name.split("/")[-1]
        local_path = os.path.join(ckpt_dir, fname)
        if not os.path.exists(local_path):
            blob.download_to_filename(local_path)
            print(f"[{_ts()}] Downloaded checkpoint from GCS: {fname}")

    all_ckpts = _glob.glob(os.path.join(ckpt_dir, "*.pt"))
    print(f"[{_ts()}] {len(all_ckpts)} checkpoints in {ckpt_dir}")

    # ─── 4. Download inference_pipeline.py from GCS ──────────────────
    inf_blob = bucket.blob("code/inference_pipeline.py")
    if inf_blob.exists():
        inf_source = inf_blob.download_as_text()
    else:
        # Use local mount as fallback
        with open("/root/inference_pipeline.py", "r") as f:
            inf_source = f.read()
    print(f"[{_ts()}] Downloaded inference_pipeline.py: {len(inf_source)} chars")

    # ─── 5. Set up globals and config patches ────────────────────────
    import torch

    # Patch config before exec'ing worldmodel.py
    config_patches = {
        "PANEL_PATH": panel_path,
        "JURISDICTION": jurisdiction,
        "CKPT_DIR": ckpt_dir,
        "OUT_DIR": ckpt_dir,
        "FORECAST_ORIGIN_YEAR": origin_year,
        "SUPABASE_DB_URL": os.environ.get("SUPABASE_DB_URL", ""),
        "TARGET_SCHEMA": "forecast_20260220_7f31c6e4",  # Shared schema for all jurisdictions (differentiated by jurisdiction column)
        "CKPT_VARIANT_SUFFIX": "SF500K",
        "RUN_FULL_BACKTEST": backtest,
        "H": 5,
        "S_SCENARIOS": 256,
    }

    # Override OUT_ROOT to use local filesystem instead of Google Drive
    config_patches["OUT_ROOT"] = f"/output/{jurisdiction}_inference"
    os.makedirs(config_patches["OUT_ROOT"], exist_ok=True)

    # Inject config into globals
    for k, v in config_patches.items():
        globals()[k] = v

    # ─── 5b. Preprocess panel: rename columns to worldmodel's expected schema ──
    # worldmodel.py expects HCAD column names (acct, yr, tot_appr_val, etc.)
    # Multi-jurisdiction panels use canonical names (parcel_id, year, property_value)
    # This mapping mirrors eval_modal.py lines 132-174.
    import polars as pl
    print(f"[{_ts()}] Preprocessing panel columns...")
    _df = pl.read_parquet(panel_path)
    print(f"[{_ts()}] Panel loaded: {len(_df):,} rows, columns: {_df.columns[:15]}")

    # Clean Census suppression values (-666666666)
    for _col in _df.columns:
        if _df[_col].dtype in (pl.Float64, pl.Float32, pl.Int64, pl.Int32):
            _df = _df.with_columns(
                pl.when(pl.col(_col) < -600_000_000).then(None).otherwise(pl.col(_col)).alias(_col)
            )

    # Derive property_value if missing (support ACS median_home_value)
    if "property_value" not in _df.columns:
        if "median_home_value" in _df.columns:
            _df = _df.with_columns(pl.col("median_home_value").alias("property_value"))
            print(f"[{_ts()}] Derived property_value from median_home_value (ACS)")
        elif "sale_price" in _df.columns:
            _df = _df.with_columns(pl.col("sale_price").alias("property_value"))
        elif "assessed_value" in _df.columns:
            _df = _df.with_columns(pl.col("assessed_value").alias("property_value"))
        elif "tot_appr_val" in _df.columns:
            _df = _df.with_columns(pl.col("tot_appr_val").alias("property_value"))

    # Derive parcel_id from geoid if missing (ACS uses geoid as tract identifier)
    if "parcel_id" not in _df.columns and "acct" not in _df.columns:
        if "geoid" in _df.columns:
            _df = _df.with_columns(pl.col("geoid").alias("parcel_id"))
            print(f"[{_ts()}] Using geoid as parcel_id (ACS)")

    # Derive year if missing
    if "year" not in _df.columns and "yr" not in _df.columns:
        if "sale_date" in _df.columns:
            _df = _df.with_columns(
                pl.col("sale_date").cast(pl.Utf8).str.slice(0, 4).cast(pl.Int64, strict=False).alias("year")
            )

    # Rename to worldmodel canonical schema
    _rename_map = {
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
    _actual_renames = {k: v for k, v in _rename_map.items() if k in _df.columns}
    # Drop clashing target columns
    _drop = [v for k, v in _actual_renames.items() if v in _df.columns]
    if _drop:
        _df = _df.drop(_drop)
    _df = _df.rename(_actual_renames)
    print(f"[{_ts()}] Renamed columns: {_actual_renames}")

    # Drop original leaky columns after rename
    _leaky = [c for c in ["median_home_value", "sale_price", "assessed_value", "land_value", "improvement_value"] if c in _df.columns]
    if _leaky:
        _df = _df.drop(_leaky)
        print(f"[{_ts()}] Dropped leaky columns: {_leaky}")

    # Filter: need acct, yr, tot_appr_val
    if "tot_appr_val" in _df.columns:
        _df = _df.filter(pl.col("tot_appr_val").is_not_null() & (pl.col("tot_appr_val") > 0))
    _df.write_parquet(panel_path)
    print(f"[{_ts()}] Panel preprocessed: {len(_df):,} rows → {panel_path}")
    del _df

    _gpu_props = torch.cuda.get_device_properties(0)
    _gpu_mem = getattr(_gpu_props, 'total_memory', getattr(_gpu_props, 'total_mem', 0))
    print(f"[{_ts()}] Config: jurisdiction={jurisdiction}, origin={origin_year}")
    print(f"[{_ts()}] GPU: {torch.cuda.get_device_name(0)}, {_gpu_mem // 1024**2} MiB")

    # ─── 6. Execute worldmodel.py to define all functions ────────────
    print(f"[{_ts()}] Executing worldmodel.py...")
    exec(wm_source, globals())
    print(f"[{_ts()}] worldmodel.py loaded")

    # ─── 7. Patch inference_pipeline.py config and exec ──────────────
    # Replace Colab-specific paths
    inf_source_patched = inf_source.replace(
        '/content/drive/MyDrive/data_backups/world_model_v10_2_fullpanel/live_inference_runs/',
        f'/output/{jurisdiction}_inference/'
    )

    # Re-inject globals (exec may have cleared them)
    globals()["FORECAST_ORIGIN_YEAR"] = origin_year
    globals()["JURISDICTION"] = jurisdiction
    globals()["CKPT_DIR"] = ckpt_dir

    # Debug: list what's actually in ckpt_dir
    import glob as _glob2
    actual_ckpts = _glob2.glob(os.path.join(ckpt_dir, "*.pt"))
    print(f"[{_ts()}] Checkpoint dir {ckpt_dir} contains: {[os.path.basename(f) for f in actual_ckpts]}")
    # Also debug the volume mount
    if os.path.isdir("/checkpoints"):
        vol_contents = []
        for root, dirs, files in os.walk("/checkpoints"):
            for f in files:
                if f.endswith(".pt"):
                    vol_contents.append(os.path.join(root, f))
        print(f"[{_ts()}] /checkpoints volume .pt files: {vol_contents}")

    print(f"[{_ts()}] Executing inference_pipeline.py...")
    exec(inf_source_patched, globals())
    print(f"[{_ts()}] inference_pipeline.py loaded")

    # ─── 8. Run the grand inference ──────────────────────────────────
    # At this point, all functions from inference_pipeline.py are in globals()
    # The pipeline will auto-discover checkpoints, load panel, and stream to Supabase

    # Get checkpoint list
    ckpt_pairs = _get_checkpoint_paths(ckpt_dir)  # noqa: F821
    print(f"[{_ts()}] Found checkpoints: {ckpt_pairs}")

    if not ckpt_pairs:
        return json.dumps({"status": "error", "error": "No checkpoints found"})

    # Run production forecast
    ckpt_origin, ckpt_path = _pick_ckpt_for_origin(ckpt_pairs, origin_year)  # noqa: F821
    print(f"[{_ts()}] Using checkpoint: origin={ckpt_origin}, path={ckpt_path}")

    # Load checkpoint
    ckpt = _load_ckpt_into_live_objects(ckpt_path)  # noqa: F821
    print(f"[{_ts()}] Checkpoint loaded")

    # Discover all accounts
    all_accts = globals().get("all_accts_g") or globals().get("all_accts")
    if all_accts is None:
        import polars as pl
        lf = pl.scan_parquet(panel_path)
        all_accts = lf.select("acct").unique().collect().to_series().to_list()
        globals()["all_accts_g"] = all_accts
    print(f"[{_ts()}] Total accounts: {len(all_accts):,}")

    # Run inference
    print(f"\n{'='*60}")
    print(f"GRAND INFERENCE: {jurisdiction} origin={origin_year}")
    print(f"  Accounts: {len(all_accts):,}")
    print(f"  Checkpoint: {ckpt_path}")
    print(f"  Streaming to: {os.environ.get('SUPABASE_DB_URL', 'N/A')[:30]}...")
    print(f"{'='*60}\n")

    result = _run_one_origin(  # noqa: F821
        schema=config_patches["TARGET_SCHEMA"],
        all_accts_prod=all_accts,
        origin_year=origin_year,
        mode="forecast",
        ckpt_origin=ckpt_origin,
        ckpt_path=ckpt_path,
        out_dir=os.path.join(config_patches["OUT_ROOT"], "production"),
        variant_id="__forecast_v2__",  # v2: per-horizon loss norm + anchor enforcement + fail-fast gates
        write_history_series=True,
    )

    elapsed = time.time() - t0
    print(f"\n[{_ts()}] Done in {elapsed/60:.1f} min")
    output_vol.commit()

    # ─── GCS backup: upload all output parquet chunks ────────────────
    print(f"[{_ts()}] Backing up output to GCS...")
    _gcs_backup_count = 0
    _out_root = config_patches["OUT_ROOT"]
    for dirpath, dirnames, filenames in os.walk(_out_root):
        for fname in filenames:
            if fname.endswith((".parquet", ".csv.gz", ".json")):
                local_fp = os.path.join(dirpath, fname)
                rel_path = os.path.relpath(local_fp, _out_root)
                gcs_key = f"inference_output/{jurisdiction}/{rel_path.replace(os.sep, '/')}"
                try:
                    blob = bucket.blob(gcs_key)
                    blob.upload_from_filename(local_fp)
                    _gcs_backup_count += 1
                except Exception as _gcs_err:
                    print(f"[{_ts()}] ⚠️  GCS upload failed for {gcs_key}: {_gcs_err}")
    print(f"[{_ts()}] GCS backup complete: {_gcs_backup_count} files → gs://properlytic-raw-data/inference_output/{jurisdiction}/")

    return json.dumps({
        "status": "ok",
        "jurisdiction": jurisdiction,
        "origin": origin_year,
        "accounts": len(all_accts),
        "elapsed_min": round(elapsed / 60, 1),
    })


def _ts():
    from datetime import datetime
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


@app.local_entrypoint()
def main(jurisdiction: str = "hcad_houston", origin: int = 2025, backtest: bool = False):
    print(f"🚀 Inference: {jurisdiction} o={origin} backtest={backtest}")
    result = run_inference.remote(jurisdiction, origin, backtest)
    print(f"✅ Done: {result}")

