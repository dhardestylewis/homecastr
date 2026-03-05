"""
retrain_panel.py — Train v11 world model on canonical panel from GCS
====================================================================

Downloads the panel parquet from GCS, adapts column names to match
worldmodel.py expectations, then runs the full training pipeline.

Usage (Colab):
    !python retrain_panel.py --jurisdiction sf_ca

Usage (local with GPU):
    python retrain_panel.py --jurisdiction sf_ca --local

Environment:
    GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json  (for GCS)
    WANDB_API_KEY=...  (for W&B tracking)
"""
import os, sys, argparse, time

def main():
    parser = argparse.ArgumentParser(description="Train v11 world model on canonical panel")
    parser.add_argument("--jurisdiction", default="sf_ca",
                       help="Jurisdiction to train on (default: sf_ca)")
    parser.add_argument("--bucket", default="properlytic-raw-data",
                       help="GCS bucket name")
    parser.add_argument("--local", action="store_true",
                       help="Run locally instead of Colab")
    parser.add_argument("--epochs", type=int, default=60,
                       help="Training epochs")
    parser.add_argument("--sample-size", type=int, default=500_000,
                       help="Max parcels to sample for training")
    parser.add_argument("--origin", type=int, default=2022,
                       help="Forecast origin year")
    parser.add_argument("--out-dir", default=None,
                       help="Output directory for checkpoints")
    args = parser.parse_args()

    ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")

    # ─── Step 1: Download panel from GCS ───
    print(f"[{ts()}] Downloading panel for {args.jurisdiction} from gs://{args.bucket}/panel/")

    panel_local = f"/tmp/panel_{args.jurisdiction}.parquet"

    if not os.path.exists(panel_local):
        from google.cloud import storage
        client = storage.Client()
        bucket = client.bucket(args.bucket)
        blob_path = f"panel/jurisdiction={args.jurisdiction}/part.parquet"
        blob = bucket.blob(blob_path)

        if not blob.exists():
            print(f"ERROR: Panel not found at gs://{args.bucket}/{blob_path}")
            print(f"Available panels:")
            for b in bucket.list_blobs(prefix="panel/"):
                print(f"  {b.name}")
            sys.exit(1)

        blob.download_to_filename(panel_local)
        size_mb = os.path.getsize(panel_local) / 1e6
        print(f"[{ts()}] Downloaded {size_mb:.1f} MB to {panel_local}")
    else:
        size_mb = os.path.getsize(panel_local) / 1e6
        print(f"[{ts()}] Using cached panel: {panel_local} ({size_mb:.1f} MB)")

    # ─── Step 2: Adapt canonical → HCAD schema ───
    print(f"[{ts()}] Adapting canonical schema → worldmodel.py schema")

    try:
        import polars as pl
    except ImportError:
        os.system(f"{sys.executable} -m pip install -q polars pyarrow")
        import polars as pl

    df = pl.read_parquet(panel_local)
    print(f"[{ts()}] Raw panel: {len(df):,} rows, {len(df.columns)} columns")
    print(f"[{ts()}] Columns: {df.columns}")
    print(f"[{ts()}] Year range: {df['year'].min()} - {df['year'].max()}")
    print(f"[{ts()}] property_value non-null: {df['property_value'].drop_nulls().len():,}")

    # Rename canonical → HCAD columns expected by worldmodel.py
    rename_map = {
        "parcel_id": "acct",
        "year": "yr",
        "property_value": "tot_appr_val",
    }

    # Map structural features to worldmodel numeric features
    # worldmodel.py auto-discovers numeric features — these will be picked up
    numeric_renames = {
        "sqft": "living_area",
        "land_area": "land_ar",
        "year_built": "yr_blt",
        "bedrooms": "bed_cnt",
        "bathrooms": "full_bath",
        "stories": "nbr_story",
        "lat": "gis_lat",
        "lon": "gis_lon",
    }

    # Categorical features — worldmodel hashes these
    cat_renames = {
        "dwelling_type": "state_class",
    }

    all_renames = {**rename_map, **numeric_renames, **cat_renames}
    # Only rename columns that actually exist
    actual_renames = {k: v for k, v in all_renames.items() if k in df.columns}
    df = df.rename(actual_renames)

    # Drop rows with no target value
    df = df.filter(pl.col("tot_appr_val").is_not_null() & (pl.col("tot_appr_val") > 0))

    # Ensure required columns exist
    if "acct" not in df.columns:
        print("ERROR: No parcel_id/acct column in panel")
        sys.exit(1)
    if "yr" not in df.columns:
        print("ERROR: No year/yr column in panel")
        sys.exit(1)
    if "tot_appr_val" not in df.columns:
        print("ERROR: No property_value/tot_appr_val column in panel")
        sys.exit(1)

    # Cast types
    df = df.with_columns([
        pl.col("acct").cast(pl.Utf8),
        pl.col("yr").cast(pl.Int32),
        pl.col("tot_appr_val").cast(pl.Float64),
    ])

    # Save adapted panel
    adapted_path = f"/tmp/panel_{args.jurisdiction}_adapted.parquet"
    df.write_parquet(adapted_path)

    n_accts = df["acct"].n_unique()
    yr_min = int(df["yr"].min())
    yr_max = int(df["yr"].max())
    print(f"[{ts()}] Adapted panel: {len(df):,} rows, {n_accts:,} unique parcels")
    print(f"[{ts()}] Year range: {yr_min}-{yr_max}")
    print(f"[{ts()}] Columns: {df.columns}")

    # ─── Step 3: Configure and run worldmodel.py ───
    print(f"\n[{ts()}] Configuring worldmodel.py for {args.jurisdiction}")

    # Set environment variables that worldmodel.py reads
    os.environ["WM_MAX_ACCTS"] = str(args.sample_size)
    os.environ["WM_SAMPLE_FRACTION"] = "1.0"  # use all data, rely on MAX_ACCTS
    os.environ["SWEEP_EPOCHS"] = str(args.epochs)
    os.environ["BACKTEST_MIN_ORIGIN"] = str(args.origin)
    os.environ["FORECAST_ORIGIN_YEAR"] = str(args.origin)

    # Override worldmodel.py's panel path
    os.environ["PANEL_PATH_OVERRIDE"] = adapted_path

    out_dir = args.out_dir or f"/tmp/worldmodel_retrain_{args.jurisdiction}"
    os.makedirs(out_dir, exist_ok=True)
    os.environ["OUT_DIR_OVERRIDE"] = out_dir

    # Adjust time contract based on data range
    os.environ["MIN_YEAR_OVERRIDE"] = str(yr_min)
    os.environ["MAX_YEAR_OVERRIDE"] = str(yr_max)

    print(f"[{ts()}] Panel: {adapted_path}")
    print(f"[{ts()}] Output: {out_dir}")
    print(f"[{ts()}] Origin: {args.origin}")
    print(f"[{ts()}] Epochs: {args.epochs}")
    print(f"[{ts()}] Sample: {args.sample_size:,}")
    print(f"[{ts()}] Year contract: {yr_min}-{yr_max}")

    # Instead of exec'ing worldmodel.py (which is Colab-centric),
    # print instructions for running in Colab or trigger locally
    if args.local:
        print(f"\n{'='*70}")
        print(f"LOCAL TRAINING MODE")
        print(f"{'='*70}")
        print(f"Panel ready at: {adapted_path}")
        print(f"To train, open scripts/inference/worldmodel.py and modify:")
        print(f"  PANEL_PATH_LOCAL = '{adapted_path}'")
        print(f"  OUT_DIR = '{out_dir}'")
        print(f"  MIN_YEAR = {yr_min}")
        print(f"  MAX_YEAR = {yr_max}")
        print(f"Then run in order:")
        print(f"  1. exec(open('scripts/inference/worldmodel.py').read())")
        print(f"  2. exec(open('scripts/retrain_sample_sweep.py').read())")
    else:
        print(f"\n{'='*70}")
        print(f"COLAB TRAINING MODE")
        print(f"{'='*70}")
        print(f"Upload {adapted_path} to Google Drive, then in Colab:")
        print(f"  1. Mount Drive")
        print(f"  2. Set PANEL_PATH_LOCAL = '/content/drive/MyDrive/panel_{args.jurisdiction}_adapted.parquet'")
        print(f"  3. Set MIN_YEAR = {yr_min}, MAX_YEAR = {yr_max}")
        print(f"  4. Run Cell 1 (worldmodel.py)")
        print(f"  5. Run Cell 1.5 (retrain_sample_sweep.py)")

    # ─── Step 4: Upload adapted panel to GCS for Colab access ───
    print(f"\n[{ts()}] Uploading adapted panel to GCS...")
    try:
        from google.cloud import storage
        client = storage.Client()
        bucket = client.bucket(args.bucket)
        gcs_adapted_path = f"panel/adapted/jurisdiction={args.jurisdiction}/adapted.parquet"
        blob = bucket.blob(gcs_adapted_path)
        blob.upload_from_filename(adapted_path)
        print(f"[{ts()}] Uploaded to gs://{args.bucket}/{gcs_adapted_path}")
        print(f"\nIn Colab:")
        print(f"  !gsutil cp gs://{args.bucket}/{gcs_adapted_path} /content/local_panel.parquet")
    except Exception as e:
        print(f"[{ts()}] GCS upload failed: {e}")
        print(f"Manually upload from: {adapted_path}")


if __name__ == "__main__":
    main()
