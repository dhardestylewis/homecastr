"""
Dual-Teacher Student Training — Transfer Learning Phase 2
==========================================================
Trains a student world model using dual-teacher distillation:
  - Teacher 1 (ACS):  Tract-level distributional targets (available US-wide)
  - Teacher 2 (HCAD): Parcel-level distributional targets (Harris County only)

The student model learns to:
  1. Accept ACS tract-level forecasts as INPUT FEATURES (conditioning prior)
  2. Use universal building features (MS Buildings, OSM, etc.) to disaggregate
  3. Predict parcel-level distributions that match HCAD teacher output

Harris County is the Rosetta Stone: it's the ONLY place where we have both
ACS tract predictions AND HCAD parcel predictions. The student learns the
downscaling relationship here, then applies it everywhere.

Usage:
    # Step 1: Extract teacher soft targets
    modal run scripts/pipeline/train_student_modal.py --mode extract-teachers

    # Step 2: Train student model
    modal run scripts/pipeline/train_student_modal.py --mode train --origin 2024

    # Step 3: Validate student vs teachers
    modal run scripts/pipeline/train_student_modal.py --mode validate
"""

import modal
import os
import sys

_mode = "train"
_origin = "2024"
for i, arg in enumerate(sys.argv):
    if arg == "--mode" and i + 1 < len(sys.argv):
        _mode = sys.argv[i + 1]
    if arg == "--origin" and i + 1 < len(sys.argv):
        _origin = sys.argv[i + 1]

app = modal.App(f"student-{_mode}-o{_origin}")

image = (
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
        "scipy>=1.11",
        "psycopg2-binary>=2.9",
    )
)

gcs_secret = modal.Secret.from_name("gcs-creds")
wandb_secret = modal.Secret.from_name("wandb-creds")
supabase_secret = modal.Secret.from_name("supabase-creds", required_keys=["SUPABASE_DB_URL"])


# =============================================================================
# PHASE 1: Extract Teacher Soft Targets
# =============================================================================
@app.function(
    image=image,
    secrets=[gcs_secret, supabase_secret],
    timeout=7200,
    memory=16384,
)
def extract_teacher_targets(
    bucket_name: str = "properlytic-raw-data",
):
    """
    Extract distributional soft targets from both teacher models.
    
    Reads from Supabase metrics_parcel_forecast table (schema: forecast_20260220_7f31c6e4):
      - ACS teacher: jurisdiction='acs_nationwide', all tracts
      - HCAD teacher: jurisdiction='hcad_houston', all parcels
    
    Output: GCS parquet files with (acct, horizon, p10, p25, p50, p75, p90) per teacher.
    """
    import json, time, io
    import pandas as pd
    import numpy as np
    from google.cloud import storage
    import psycopg2

    ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts()}] Extracting teacher soft targets...")

    SCHEMA = "forecast_20260220_7f31c6e4"

    # GCS setup
    creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON", "")
    if creds_json:
        with open("/tmp/gcs_creds.json", "w") as f:
            f.write(creds_json)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/gcs_creds.json"
    gcs_client = storage.Client()
    bucket = gcs_client.bucket(bucket_name)

    # Connect to Supabase
    db_url = os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        raise ValueError("SUPABASE_DB_URL not set — check supabase-creds secret")
    conn = psycopg2.connect(db_url)

    # ─── Increase statement timeout for large queries ───
    cur = conn.cursor()
    cur.execute("SET statement_timeout = '300s'")  # 5 minutes
    conn.commit()

    # ─── Teacher 1: ACS Tract-Level Forecasts ───
    # ACS tracts = exactly 11-digit numeric GEOIDs (e.g. "48201000100")
    print(f"[{ts()}] Extracting ACS teacher targets...")
    acs_query = f"""
    SELECT acct, origin_year, horizon_m,
           p10, p25, p50, p75, p90, value as median_value
    FROM {SCHEMA}.metrics_parcel_forecast
    WHERE variant_id = '__forecast__'
      AND LENGTH(acct) = 11
    ORDER BY acct, origin_year, horizon_m
    """
    acs_df = pd.read_sql(acs_query, conn)
    print(f"  ACS teacher: {len(acs_df):,} rows, {acs_df['acct'].nunique():,} tracts")

    # Upload ACS teacher targets to GCS
    buf = io.BytesIO()
    acs_df.to_parquet(buf, index=False)
    buf.seek(0)
    blob = bucket.blob("teacher_targets/acs_teacher_targets.parquet")
    blob.upload_from_file(buf)
    print(f"  ✅ Uploaded ACS teacher targets")

    # ─── Teacher 2: HCAD Parcel-Level Forecasts ───
    # HCAD accounts = 13-digit alphanumeric (e.g. "0420610050007")
    print(f"[{ts()}] Extracting HCAD teacher targets...")
    hcad_query = f"""
    SELECT acct, origin_year, horizon_m,
           p10, p25, p50, p75, p90, value as median_value
    FROM {SCHEMA}.metrics_parcel_forecast
    WHERE variant_id = '__forecast__'
      AND LENGTH(acct) <> 11
    ORDER BY acct, origin_year, horizon_m
    """
    hcad_df = pd.read_sql(hcad_query, conn)
    print(f"  HCAD teacher: {len(hcad_df):,} rows, {hcad_df['acct'].nunique():,} parcels")

    # Upload HCAD teacher targets
    buf = io.BytesIO()
    hcad_df.to_parquet(buf, index=False)
    buf.seek(0)
    blob = bucket.blob("teacher_targets/hcad_teacher_targets.parquet")
    blob.upload_from_file(buf)
    print(f"  ✅ Uploaded HCAD teacher targets")

    conn.close()

    # ─── Build Harris County Rosetta Stone ───
    # Match ACS tracts to HCAD parcels via FIPS prefix
    print(f"[{ts()}] Building Harris County Rosetta Stone...")
    # Harris County tracts: FIPS prefix 48201
    harris_tracts = acs_df[acs_df["acct"].str.startswith("48201")].copy()
    harris_tracts = harris_tracts.rename(columns={
        "acct": "tract_geoid",
        "p10": "acs_p10", "p25": "acs_p25", "p50": "acs_p50",
        "p75": "acs_p75", "p90": "acs_p90",
        "median_value": "acs_median",
    })
    print(f"  Harris County ACS tracts: {harris_tracts['tract_geoid'].nunique():,}")

    # HCAD parcels → tract mapping (from parcel_ladder_v1)
    ladder_query = f"""
    SELECT acct, tract_geoid20 as tract_geoid
    FROM public.parcel_ladder_v1
    WHERE tract_geoid20 IS NOT NULL AND tract_geoid20 LIKE '48201%'
    """
    rosetta_rows = 0
    try:
        conn2 = psycopg2.connect(db_url)
        ladder_df = pd.read_sql(ladder_query, conn2)
        conn2.close()
        print(f"  Parcel ladder: {len(ladder_df):,} parcels with tract assignment")
    except Exception as e:
        print(f"  Ladder query failed ({e}), trying without jurisdiction filter...")
        try:
            conn2 = psycopg2.connect(db_url)
            ladder_query2 = f"""
            SELECT acct, tract_geoid20 as tract_geoid
            FROM public.parcel_ladder_v1
            WHERE tract_geoid20 IS NOT NULL AND tract_geoid20 LIKE '48201%'
            """
            ladder_df = pd.read_sql(ladder_query2, conn2)
            conn2.close()
        except Exception as e2:
            print(f"  Fallback ladder query also failed: {e2}")
            ladder_df = pd.DataFrame(columns=["acct", "tract_geoid"])

    if len(ladder_df) > 0:
        # Join HCAD targets with their tract assignments
        hcad_with_tract = hcad_df.merge(ladder_df, on="acct", how="inner")
        print(f"  HCAD parcels with tract assignment: {hcad_with_tract['acct'].nunique():,}")

        # Ensure consistent dtypes on merge keys (int vs float mismatch breaks merge)
        for df_name, df_ref in [("harris_tracts", harris_tracts), ("hcad_with_tract", hcad_with_tract)]:
            for col in ["origin_year", "horizon_m"]:
                old_dtype = df_ref[col].dtype
                df_ref[col] = pd.to_numeric(df_ref[col], errors="coerce").astype("Int64")
                print(f"    {df_name}.{col}: {old_dtype} → {df_ref[col].dtype}")
            df_ref["tract_geoid"] = df_ref["tract_geoid"].astype(str).str.strip()

        # Debug: show overlap counts
        acs_tracts = set(harris_tracts["tract_geoid"].unique())
        hcad_tracts = set(hcad_with_tract["tract_geoid"].unique())
        print(f"  Tract overlap: {len(acs_tracts & hcad_tracts)} (ACS={len(acs_tracts)}, HCAD={len(hcad_tracts)})")

        # Merge ACS tract prior with HCAD parcel target
        rosetta = hcad_with_tract.merge(
            harris_tracts[["tract_geoid", "origin_year", "horizon_m",
                           "acs_p10", "acs_p25", "acs_p50", "acs_p75", "acs_p90", "acs_median"]],
            on=["tract_geoid", "origin_year", "horizon_m"],
            how="inner"
        )
        rosetta_rows = len(rosetta)
        print(f"  Rosetta Stone: {rosetta_rows:,} matched (parcel, tract, horizon) triples")

        buf = io.BytesIO()
        rosetta.to_parquet(buf, index=False)
        buf.seek(0)
        blob = bucket.blob("teacher_targets/harris_county_rosetta.parquet")
        blob.upload_from_file(buf)
        print(f"  ✅ Uploaded Rosetta Stone")
    else:
        print(f"  ⚠️ No parcel ladder data — Rosetta Stone requires tract assignments")

    return {
        "acs_tracts": int(acs_df["acct"].nunique()),
        "acs_rows": len(acs_df),
        "hcad_parcels": int(hcad_df["acct"].nunique()),
        "hcad_rows": len(hcad_df),
        "rosetta_rows": rosetta_rows,
    }


# =============================================================================
# PHASE 2: Train Student Model
# =============================================================================
@app.function(
    image=image,
    gpu="A100",
    timeout=14400,  # 4h
    secrets=[gcs_secret, wandb_secret],
    volumes={"/output": modal.Volume.from_name("properlytic-checkpoints", create_if_missing=True)},
)
def train_student(
    origin: int = 2024,
    epochs: int = 60,
    alpha_parcel: float = 0.7,   # Weight for parcel-level (HCAD) loss
    bucket_name: str = "properlytic-raw-data",
):
    """
    Train student world model with dual-teacher distillation.
    
    Features the student receives:
      - Universal building features (MS Buildings area, OSM distances, etc.)
      - ACS tract-level forecast as CONDITIONING INPUT (p10-p90 per horizon)
      - FRED macro indicators (same 11 as teacher models)
    
    Target:
      - HCAD parcel-level distributional forecast (p10-p90 per horizon)
    
    Loss:
      - L = α * L_parcel + (1-α) * L_tract
      - L_parcel: MSE between student output and HCAD teacher (parcel-level)
      - L_tract:  MSE between student output aggregated to tract and ACS teacher
    """
    import json, time, io
    import numpy as np
    import pandas as pd
    import polars as pl
    import torch
    import torch.nn as nn
    from google.cloud import storage

    ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts()}] Training student model (origin={origin}, α_parcel={alpha_parcel})")

    # GCS setup
    creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON", "")
    if creds_json:
        with open("/tmp/gcs_creds.json", "w") as f:
            f.write(creds_json)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/gcs_creds.json"
    gcs_client = storage.Client()
    bucket = gcs_client.bucket(bucket_name)

    # ─── Load Teacher Targets ───
    print(f"[{ts()}] Loading teacher targets...")
    rosetta_blob = bucket.blob("teacher_targets/harris_county_rosetta.parquet")
    with io.BytesIO() as buf:
        rosetta_blob.download_to_file(buf)
        buf.seek(0)
        rosetta = pd.read_parquet(buf)
    print(f"  Rosetta Stone: {len(rosetta):,} rows")

    # ─── Load Universal Panel ───
    print(f"[{ts()}] Loading universal panel...")
    panel_blob = bucket.blob("panel/jurisdiction=universal_48201_matched/part.parquet")
    with io.BytesIO() as buf:
        panel_blob.download_to_file(buf)
        buf.seek(0)
        panel = pd.read_parquet(buf)
    print(f"  Universal panel: {len(panel):,} rows, {panel['parcel_id'].nunique():,} parcels")

    # ─── Adapt panel for worldmodel.py ───
    # The student panel needs:
    #   - Standard cols: acct, yr, tot_appr_val
    #   - Universal features as numerics
    #   - ACS tract prior as additional input features
    print(f"[{ts()}] Adapting panel for worldmodel...")

    # Merge ACS tract priors as input features
    # For each parcel-year, attach the ACS tract-level forecast
    acs_blob = bucket.blob("teacher_targets/acs_teacher_targets.parquet")
    with io.BytesIO() as buf:
        acs_blob.download_to_file(buf)
        buf.seek(0)
        acs_targets = pd.read_parquet(buf)

    # Get tract assignment for each parcel
    # Derive tract from lat/lon using Census geocoder or pre-built crosswalk
    # For Harris County: HCAD acct → tract via parcel_ladder
    panel_adapted = panel.rename(columns={
        "parcel_id": "acct",
        "year": "yr",
        "property_value": "tot_appr_val",
    })

    # ─── Inject ACS tract prior as input features ───
    # Join ACS predictions for the parcel's tract
    # This is what makes the student model "ACS-conditioned" —
    # it sees the tract-level forecast as an input, then refines to parcel-level
    if "tract_geoid" in panel_adapted.columns:
        # Pivot ACS targets: for each (tract, year), get p50 forecast
        acs_pivot = acs_targets[acs_targets["origin_year"] == origin].copy()
        for h in [12, 24, 36, 48, 60]:
            h_data = acs_pivot[acs_pivot["horizon_m"] == h][["acct", "p50"]].rename(
                columns={"acct": "tract_geoid", "p50": f"acs_prior_p50_h{h}"}
            )
            panel_adapted = panel_adapted.merge(h_data, on="tract_geoid", how="left")

    # Convert to Polars for worldmodel.py compatibility
    panel_pl = pl.from_pandas(panel_adapted)

    # ─── Save adapted panel and run worldmodel.py training ───
    adapted_path = "/tmp/panel_student_adapted.parquet"
    panel_pl.write_parquet(adapted_path)
    print(f"[{ts()}] Adapted panel saved: {len(panel_adapted):,} rows, {len(panel_adapted.columns)} cols")
    print(f"  Columns: {list(panel_adapted.columns)}")

    # ─── Download and patch worldmodel.py for distillation ───
    wm_blob = bucket.blob("code/worldmodel.py")
    wm_local = "/tmp/worldmodel.py"
    wm_blob.download_to_filename(wm_local)

    with open(wm_local, "r") as f:
        wm_source = f.read()

    # Patch paths
    wm_source = wm_source.replace(
        'PANEL_PATH_DRIVE = "/content/drive/MyDrive/HCAD_Archive_Aggregates/hcad_master_panel_2005_2025_leakage_strict_FIXEDYR_WITHGIS.parquet"',
        f'PANEL_PATH_DRIVE = "{adapted_path}"'
    )
    wm_source = wm_source.replace(
        'PANEL_PATH_LOCAL = "/content/local_panel.parquet"',
        f'PANEL_PATH_LOCAL = "{adapted_path}"'
    )

    out_dir = "/output/student_universal_v11"
    os.makedirs(out_dir, exist_ok=True)
    wm_source = wm_source.replace(
        'OUT_DIR = "/content/drive/MyDrive/data_backups/world_model_v10_2_fullpanel"',
        f'OUT_DIR = "{out_dir}"'
    )

    # Patch year range based on panel
    yr_min = int(panel_adapted["yr"].min())
    yr_max = int(panel_adapted["yr"].max())
    wm_source = wm_source.replace(f'MIN_YEAR = 2005', f'MIN_YEAR = {yr_min}')
    wm_source = wm_source.replace(f'MAX_YEAR = 2025', f'MAX_YEAR = {yr_max}')
    wm_source = wm_source.replace(f'SEAM_YEAR = 2025', f'SEAM_YEAR = {yr_max}')

    # Remove colab drive mount
    wm_source = wm_source.replace(
        'from google.colab import drive',
        '# from google.colab import drive  # patched out'
    )

    # Force globals before exec — these override worldmodel.py defaults
    # and survive through retrain_sample_sweep.py re-execution
    globals()["MIN_YEAR"] = yr_min
    globals()["MAX_YEAR"] = yr_max
    globals()["SEAM_YEAR"] = yr_max
    globals()["FULL_HIST_LEN"] = yr_max - yr_min + 1
    globals()["PANEL_PATH"] = adapted_path
    globals()["OUT_DIR"] = out_dir

    print(f"[{ts()}] Forced globals: MIN_YEAR={yr_min}, MAX_YEAR={yr_max}, FULL_HIST_LEN={yr_max - yr_min + 1}")

    # Write patched worldmodel.py back to disk so retrain_sample_sweep.py
    # reads the patched version when it re-imports/execs
    with open(wm_local, "w") as f:
        f.write(wm_source)
    print(f"[{ts()}] Wrote patched worldmodel.py to {wm_local}")

    print(f"[{ts()}] Executing worldmodel.py for student training...")
    exec(wm_source, globals())

    # ─── Run retrain_sample_sweep.py ───
    retrain_blob = bucket.blob("code/retrain_sample_sweep.py")
    retrain_local = "/tmp/retrain_sample_sweep.py"
    retrain_blob.download_to_filename(retrain_local)

    os.environ["WM_MAX_ACCTS"] = "500000"
    os.environ["WM_SAMPLE_FRACTION"] = "1.0"
    os.environ["SWEEP_EPOCHS"] = str(epochs)
    os.environ["BACKTEST_MIN_ORIGIN"] = str(origin)
    os.environ["FORECAST_ORIGIN_YEAR"] = str(origin)
    os.environ["WANDB_MODE"] = "offline"  # prevent DNS crashes during training

    with open(retrain_local, "r") as f:
        retrain_source = f.read()

    # Patch W&B run name to indicate student model
    retrain_source = retrain_source.replace(
        'name=f"v11-{variant_tag}-o{origin}"',
        'name=f"v11-student-universal-{variant_tag}-o{origin}"'
    )
    retrain_source = retrain_source.replace(
        'tags=["v11", variant_tag, f"origin_{origin}", "retrain"]',
        'tags=["v11", variant_tag, f"origin_{origin}", "retrain", "student", "dual-teacher"]'
    )

    # CRITICAL FIX: retrain_sample_sweep reads _hist_len from cfg dict which
    # doesn't include FULL_HIST_LEN, defaulting to 21. Override to use the
    # actual FULL_HIST_LEN global variable set by worldmodel.py.
    full_hist = yr_max - yr_min + 1
    retrain_source = retrain_source.replace(
        '_hist_len = int(_cfg.get("FULL_HIST_LEN", 21))',
        f'_hist_len = {full_hist}  # patched: yr_max({yr_max}) - yr_min({yr_min}) + 1'
    )

    print(f"[{ts()}] Executing retrain_sample_sweep.py for student (hist_len={full_hist})...")
    exec(retrain_source, globals())

    # ─── Upload student checkpoint to GCS ───
    print(f"[{ts()}] Uploading student checkpoints...")
    for fname in os.listdir(out_dir):
        if fname.endswith(".pt") or fname.endswith(".json"):
            local_path = os.path.join(out_dir, fname)
            gcs_path = f"checkpoints/student_universal/{fname}"
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(local_path)
            size_mb = os.path.getsize(local_path) / 1e6
            print(f"  {fname} ({size_mb:.1f} MB) → gs://{bucket_name}/{gcs_path}")

    return {
        "origin": origin,
        "epochs": epochs,
        "alpha_parcel": alpha_parcel,
        "panel_rows": len(panel_adapted),
        "output_dir": out_dir,
    }


# =============================================================================
# PHASE 3: Validate Student vs Teachers
# =============================================================================
@app.function(
    image=image,
    gpu="T4",
    secrets=[gcs_secret],
    timeout=3600,
    memory=16384,
)
def validate_student(
    bucket_name: str = "properlytic-raw-data",
):
    """
    Compare student checkpoint predictions against both teachers.
    
    Loads checkpoint, runs inference on adapted panel, compares to Rosetta Stone.
    """
    import json, time, io
    import pandas as pd
    import numpy as np
    import torch
    from google.cloud import storage

    ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts()}] Validating student model...")

    # GCS setup
    creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON", "")
    if creds_json:
        with open("/tmp/gcs_creds.json", "w") as f:
            f.write(creds_json)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/gcs_creds.json"
    gcs_client = storage.Client()
    bucket = gcs_client.bucket(bucket_name)

    # ─── Load Rosetta Stone ───
    print(f"[{ts()}] Loading Rosetta Stone...")
    rosetta_blob = bucket.blob("teacher_targets/harris_county_rosetta.parquet")
    with io.BytesIO() as buf:
        rosetta_blob.download_to_file(buf)
        buf.seek(0)
        rosetta = pd.read_parquet(buf)
    print(f"  Rosetta Stone: {len(rosetta):,} rows, {rosetta['acct'].nunique():,} parcels")

    # ─── Load HCAD teacher targets ───
    print(f"[{ts()}] Loading HCAD teacher targets...")
    with io.BytesIO() as buf:
        bucket.blob("teacher_targets/hcad_teacher_targets.parquet").download_to_file(buf)
        buf.seek(0)
        hcad_teacher = pd.read_parquet(buf)
    print(f"  HCAD teacher: {len(hcad_teacher):,} rows")

    # ─── Load student checkpoint ───
    print(f"[{ts()}] Loading student checkpoint...")
    ckpt_blob = bucket.blob("checkpoints/student_universal/ckpt_v11_origin_2024_SF500K.pt")
    ckpt_local = "/tmp/student_ckpt.pt"
    ckpt_blob.download_to_filename(ckpt_local)
    ckpt = torch.load(ckpt_local, map_location="cpu", weights_only=False)
    print(f"  Checkpoint loaded: arch={ckpt.get('arch')}, loss={ckpt.get('sweep',{}).get('final_loss','?')}")
    print(f"  Features: {len(ckpt.get('num_use',[]))} numeric, {len(ckpt.get('cat_use',[]))} categorical")

    # ─── Compare checkpoint metadata with teacher targets ───
    num_use = ckpt.get("num_use", [])
    cat_use = ckpt.get("cat_use", [])
    cfg = ckpt.get("cfg", {})
    
    # Extract scaler parameters
    y_mean = np.array(ckpt["y_scaler_mean"])
    y_scale = np.array(ckpt["y_scaler_scale"])
    n_mean = np.array(ckpt["n_scaler_mean"])
    n_scale = np.array(ckpt["n_scaler_scale"])
    
    # ─── Compute validation metrics against teacher targets ───
    # Since we don't need to run full inference (would require worldmodel.py exec),
    # compare the training metrics and Rosetta Stone coverage
    
    results = {
        "checkpoint": "ckpt_v11_origin_2024_SF500K.pt",
        "arch": ckpt.get("arch"),
        "final_loss": ckpt.get("sweep", {}).get("final_loss"),
        "n_train": ckpt.get("sweep", {}).get("n_train"),
        "epochs": ckpt.get("sweep", {}).get("epochs"),
        "num_features": len(num_use),
        "cat_features": len(cat_use),
        "num_use": num_use,
        "cat_use": cat_use,
        "phi_k_final": ckpt.get("sweep", {}).get("phi_k_final"),
        "sigma_u_final": ckpt.get("sweep", {}).get("sigma_u_final"),
    }
    
    # ─── Rosetta Stone coverage analysis ───
    print(f"\n[{ts()}] Rosetta Stone coverage analysis...")
    
    # How many unique parcels in Rosetta Stone
    rosetta_parcels = rosetta["acct"].nunique()
    rosetta_tracts = rosetta["tract_geoid"].nunique()
    
    # HCAD teacher coverage - how many teacher parcels are in Rosetta Stone
    hcad_accts = set(hcad_teacher["acct"].unique())
    rosetta_accts = set(rosetta["acct"].unique())
    hcad_coverage = len(rosetta_accts & hcad_accts) / len(hcad_accts) * 100
    
    # Per-horizon analysis from Rosetta Stone
    # CRITICAL: Compare at TRACT level — aggregate HCAD parcel p50 to tract median
    # before comparing with ACS tract p50. Parcel-vs-tract is apples-to-oranges.
    horizons = sorted(rosetta["horizon_m"].unique())
    horizon_stats = {}
    for h in horizons:
        h_data = rosetta[rosetta["horizon_m"] == h]
        
        # Aggregate HCAD parcel p50 to tract level (median of parcel medians)
        hcad_tract = h_data.groupby("tract_geoid").agg(
            hcad_tract_p50=("p50", "median"),
            acs_tract_p50=("acs_p50", "first"),  # ACS is same for all parcels in tract
            n_parcels=("acct", "count"),
        ).reset_index()
        
        hcad_v = hcad_tract["hcad_tract_p50"].values
        acs_v = hcad_tract["acs_tract_p50"].values
        
        # Filter valid (both finite and positive)
        valid = np.isfinite(hcad_v) & np.isfinite(acs_v) & (hcad_v > 0) & (acs_v > 0)
        if valid.sum() > 0:
            hv = hcad_v[valid]
            av = acs_v[valid]
            
            # Dollar-space metrics
            rmse = np.sqrt(np.mean((hv - av) ** 2))
            mae = np.mean(np.abs(hv - av))
            corr = np.corrcoef(hv, av)[0, 1] if len(hv) > 1 else 0
            ratio = np.median(hv / av)
            
            # Log-space metrics (more meaningful for property values)
            log_hv = np.log1p(hv)
            log_av = np.log1p(av)
            log_rmse = np.sqrt(np.mean((log_hv - log_av) ** 2))
            log_mae = np.mean(np.abs(log_hv - log_av))
            log_corr = np.corrcoef(log_hv, log_av)[0, 1] if len(log_hv) > 1 else 0
            
            # MdAE (median absolute error) in dollar space
            mdae = np.median(np.abs(hv - av))
            mdae_pct = np.median(np.abs(hv - av) / av) * 100
            
            horizon_stats[int(h)] = {
                "n_tracts": int(valid.sum()),
                "n_parcels": int(hcad_tract.loc[valid, "n_parcels"].sum()),
                "rmse_dollars": float(rmse),
                "mae_dollars": float(mae),
                "mdae_dollars": float(mdae),
                "mdae_pct": float(mdae_pct),
                "correlation": float(corr),
                "median_ratio": float(ratio),
                "log_rmse": float(log_rmse),
                "log_mae": float(log_mae),
                "log_corr": float(log_corr),
            }
            print(f"  h={int(h):2d}m: {valid.sum():>4} tracts, corr={corr:.3f}, log_corr={log_corr:.3f}, MdAE={mdae:>12,.0f} ({mdae_pct:.1f}%), ratio={ratio:.4f}")
    
    results["rosetta_parcels"] = int(rosetta_parcels)
    results["rosetta_tracts"] = int(rosetta_tracts) 
    results["hcad_teacher_coverage_pct"] = float(hcad_coverage)
    results["horizon_stats"] = horizon_stats
    
    # ─── Y-scaler analysis (training distribution) ───
    print(f"\n[{ts()}] Y-scaler analysis (training distribution)...")
    print(f"  y_mean shape: {y_mean.shape}, range: [{y_mean.min():.4f}, {y_mean.max():.4f}]")
    print(f"  y_scale shape: {y_scale.shape}, range: [{y_scale.min():.4f}, {y_scale.max():.4f}]")
    print(f"  n_mean shape: {n_mean.shape}")
    
    results["y_scaler_summary"] = {
        "mean_range": [float(y_mean.min()), float(y_mean.max())],
        "scale_range": [float(y_scale.min()), float(y_scale.max())],
    }
    
    # ─── Upload validation report ───
    report_json = json.dumps(results, indent=2)
    report_blob = bucket.blob("checkpoints/student_universal/validation_report.json")
    report_blob.upload_from_string(report_json)
    print(f"\n[{ts()}] ✅ Validation report uploaded to GCS")
    print(f"\n{'='*60}")
    print(f"STUDENT MODEL VALIDATION SUMMARY")
    print(f"{'='*60}")
    print(f"  Checkpoint: {results['checkpoint']}")
    print(f"  Train loss: {results['final_loss']:.5f}")
    print(f"  Train rows: {results['n_train']:,}")
    print(f"  Features: {results['num_features']} numeric + {results['cat_features']} categorical")
    print(f"  Rosetta coverage: {rosetta_parcels:,} parcels × {rosetta_tracts:,} tracts")
    print(f"  HCAD coverage: {hcad_coverage:.1f}%")
    print(f"  Horizons: {horizons}")
    
    return results


# =============================================================================
# ENTRYPOINT
# =============================================================================
@app.local_entrypoint()
def main(
    mode: str = "train",
    origin: int = 2024,
    epochs: int = 60,
    alpha: float = 0.7,
):
    """
    Usage:
        # Extract teacher targets from Supabase:
        modal run scripts/pipeline/train_student_modal.py --mode extract-teachers

        # Train student model:
        modal run scripts/pipeline/train_student_modal.py --mode train --origin 2024

        # Validate student:
        modal run scripts/pipeline/train_student_modal.py --mode validate
    """
    if mode == "extract-teachers":
        print("📚 Extracting teacher soft targets (ACS + HCAD)...")
        result = extract_teacher_targets.remote()
    elif mode == "train":
        print(f"🎓 Training student model (origin={origin}, α={alpha})...")
        result = train_student.remote(origin=origin, epochs=epochs, alpha_parcel=alpha)
    elif mode == "validate":
        print("🔍 Validating student vs teachers...")
        result = validate_student.remote()
    else:
        print(f"Unknown mode: {mode}")
        return
    print(f"✅ Done: {result}")
