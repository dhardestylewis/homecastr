"""
Parallel Grand Inference Pipeline — Modal wrapper.
===================================================
Fans out inference across N A100 containers simultaneously using
Modal's map() primitive. Each container handles a shard of accounts
and writes results directly to Supabase.

Single-container serial: ~1h for 108K ACS accounts
Parallel (N_SHARDS=6):   ~12min (6× speedup, one A100 per shard)

Usage:
    modal run scripts/inference/inference_modal_parallel.py --jurisdiction acs_nationwide --origin 2024
    modal run scripts/inference/inference_modal_parallel.py --jurisdiction acs_nationwide --origin 2024 --n-shards 10
"""
import modal, os, sys, json

# Parse CLI args at module level for Modal dashboard naming
_jur = "hcad_houston"
_origin = "2024"
_model = "v11"
for i, a in enumerate(sys.argv):
    if a == "--jurisdiction" and i + 1 < len(sys.argv):
        _jur = sys.argv[i + 1]
    if a == "--origin" and i + 1 < len(sys.argv):
        _origin = sys.argv[i + 1]
    if a == "--model" and i + 1 < len(sys.argv):
        _model = sys.argv[i + 1]

_model_tag = "sb" if _model == "v12_sb" else "v11"
app = modal.App(f"inference-{_model_tag}-{_jur}-o{_origin}")

image = (
    modal.Image.from_registry(
        "pytorch/pytorch:2.3.0-cuda12.1-cudnn8-runtime",
        add_python="3.11",
    )
    # gcc is required by torch.compile / inductor's Triton autotuner.
    # Without it every matmul falls back to eager, costing ~5 min per shard.
    .apt_install(["gcc"])
    .pip_install(
        "google-cloud-storage",
        "numpy",
        "pandas",
        "polars",
        "pyarrow",
        "psycopg2-binary",
        "wandb",
        "scipy",
        "scikit-learn",
        "POT>=0.9",  # Python Optimal Transport for SF2M (v12_sb)
    )
    # Mount local scripts so worldmodel.py + inference_pipeline.py always match
    # the local codebase (prevents stale GCS code/ downloads)
    .add_local_dir("scripts", remote_path="/scripts")
)

gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
supabase_secret = modal.Secret.from_name("supabase-creds", required_keys=["SUPABASE_DB_URL"])
wandb_secret = modal.Secret.from_name("wandb-creds", required_keys=["WANDB_API_KEY"])

output_vol = modal.Volume.from_name("inference-outputs", create_if_missing=True)
ckpt_vol = modal.Volume.from_name("properlytic-checkpoints", create_if_missing=True)

# ── Jurisdiction-specific panel path resolution ──────────────────────────────
# Must match the overrides in train_modal_sb.py so inference uses the same
# panel that training used (prevents feature/year mismatches).
_PANEL_OVERRIDES = {
    "nyc": "panel/jurisdiction=nyc/nyc_panel_h3.parquet",
    "florida_dor": "panels/florida_dor_panel.parquet",
}

def _resolve_panel_gcs_path(jurisdiction: str, panel_gcs_path: str = "") -> str:
    """Return the GCS blob path for a jurisdiction's panel.
    Priority: explicit panel_gcs_path > jurisdiction override > default."""
    if panel_gcs_path:
        return panel_gcs_path
    return _PANEL_OVERRIDES.get(jurisdiction, f"panel/jurisdiction={jurisdiction}/part.parquet")


def _ts():
    from datetime import datetime
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


@app.function(
    image=image,
    secrets=[gcs_secret, supabase_secret, wandb_secret],
    gpu="A100",
    timeout=21600,      # 6h max per shard (bumped from 2h — HCAD/FL timeout at ~2h)
    memory=32768,
    volumes={"/output": output_vol, "/checkpoints": ckpt_vol},
)
def run_inference_shard(
    jurisdiction: str,
    origin_year: int,
    shard_accts: list,          # subset of account IDs for this shard
    run_id: str,                # shared run_id so all shards write under same run
    shard_idx: int,
    n_shards: int,
    suite_id: str,
    schema: str,
    panel_gcs_path: str = "",
    model: str = "v11",         # "v11" or "v12_sb"
):
    """Run inference for a single shard of accounts on its own A100."""
    import time, glob as _glob, shutil
    import polars as pl
    t0 = time.time()

    print(f"[{_ts()}] ═══ SHARD {shard_idx+1}/{n_shards}: {len(shard_accts):,} accounts ═══")

    # ─── 1. Download panel ───────────────────────────────────────────────────
    from google.cloud import storage
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")

    panel_path = f"/tmp/panel_{jurisdiction}_shard{shard_idx}.parquet"
    _panel_blob_path = _resolve_panel_gcs_path(jurisdiction, panel_gcs_path)
    blob = bucket.blob(_panel_blob_path)
    for attempt in range(3):
        try:
            blob.download_to_filename(panel_path, timeout=600)
            break
        except Exception as e:
            print(f"[{_ts()}] Panel download attempt {attempt+1} failed: {e}")
            if attempt == 2:
                raise
            time.sleep(5 * (attempt + 1))
    print(f"[{_ts()}] Downloaded panel: {os.path.getsize(panel_path) / 1e6:.1f} MB")

    # ─── 2. Load worldmodel from local mount (matches training codebase) ──
    _use_sb = (model == "v12_sb")
    if _use_sb:
        # v12_sb: load the SB-specific worldmodel (includes data plumbing + SF2M architecture)
        with open("/scripts/inference/v12_sb/worldmodel_inference_sb.py", "r") as _f:
            wm_source = _f.read()
        print(f"[{_ts()}] Loaded worldmodel_inference_sb.py (v12_sb) from local mount")
    else:
        with open("/scripts/inference/worldmodel.py", "r") as _f:
            wm_source = _f.read()
        print(f"[{_ts()}] Loaded worldmodel.py (v11) from local mount")

    # ─── 3. Get checkpoint (always copy from volume to pick up retrains) ─────
    _ckpt_suffix = "v12sb" if _use_sb else "v11"
    ckpt_dir = f"/output/{jurisdiction}_{_ckpt_suffix}"
    os.makedirs(ckpt_dir, exist_ok=True)
    modal_ckpt_dir = f"/checkpoints/{jurisdiction}_{_ckpt_suffix}"
    modal_ckpts = _glob.glob(os.path.join(modal_ckpt_dir, "*.pt")) if os.path.isdir(modal_ckpt_dir) else []
    if modal_ckpts:
        for src in modal_ckpts:
            dst = os.path.join(ckpt_dir, os.path.basename(src))
            shutil.copy2(src, dst)
            print(f"[{_ts()}] Copied checkpoint: {os.path.basename(src)}")
    # Also copy calibrator .pkl files from the volume (produced by sweep_stage1_calibration.py)
    modal_calibs = _glob.glob(os.path.join(modal_ckpt_dir, "calibrators_*.pkl")) if os.path.isdir(modal_ckpt_dir) else []
    # Also check the output volume path (sweep writes to /output/{jur}_{suffix}/)
    modal_calibs += _glob.glob(os.path.join(ckpt_dir, "calibrators_*.pkl"))
    for src in modal_calibs:
        dst = os.path.join(ckpt_dir, os.path.basename(src))
        if src != dst:
            shutil.copy2(src, dst)
            print(f"[{_ts()}] Copied calibrator: {os.path.basename(src)}")
    # Fallback: GCS (only download checkpoints matching the model variant)
    _ckpt_prefix = "ckpt_v12sb_" if _use_sb else "ckpt_v11_"
    for blob in [b for b in bucket.list_blobs(prefix=f"checkpoints/{jurisdiction}/") if b.name.endswith(".pt")]:
        fname = blob.name.split("/")[-1]
        # Skip checkpoints from wrong model variant
        if _use_sb and not fname.startswith("ckpt_v12sb_"):
            continue
        if not _use_sb and fname.startswith("ckpt_v12sb_"):
            continue
        local = os.path.join(ckpt_dir, fname)
        if not os.path.exists(local):
            blob.download_to_filename(local)
            print(f"[{_ts()}] Downloaded checkpoint from GCS: {fname}")
    # GCS fallback for calibrator .pkl files
    for blob in [b for b in bucket.list_blobs(prefix=f"checkpoints/{jurisdiction}/") if b.name.endswith(".pkl")]:
        fname = blob.name.split("/")[-1]
        local = os.path.join(ckpt_dir, fname)
        if not os.path.exists(local):
            blob.download_to_filename(local)
            print(f"[{_ts()}] Downloaded calibrator from GCS: {fname}")

    # Clean stale checkpoints from wrong variant (Modal volume may persist across runs)
    for _f in os.listdir(ckpt_dir):
        if not _f.endswith(".pt"):
            continue
        if _use_sb and not _f.startswith("ckpt_v12sb_"):
            os.remove(os.path.join(ckpt_dir, _f))
            print(f"[{_ts()}] Removed stale non-SB checkpoint: {_f}")
        elif not _use_sb and _f.startswith("ckpt_v12sb_"):
            os.remove(os.path.join(ckpt_dir, _f))
            print(f"[{_ts()}] Removed stale SB checkpoint: {_f}")

    # ─── 4. Load inference_pipeline.py from local mount ──────────────────────
    with open("/scripts/inference/inference_pipeline.py", "r") as _f:
        inf_source = _f.read()
    inf_source = inf_source.replace(
        '/content/drive/MyDrive/data_backups/world_model_v10_2_fullpanel/live_inference_runs/',
        f'/output/{jurisdiction}_inference/'
    )
    print(f"[{_ts()}] Loaded inference_pipeline.py from local mount")

    # ─── 5. Preprocess panel ─────────────────────────────────────────────────
    import polars as pl
    _df = pl.read_parquet(panel_path)
    for _col in _df.columns:
        if _df[_col].dtype in (pl.Float64, pl.Float32, pl.Int64, pl.Int32):
            _df = _df.with_columns(
                pl.when(pl.col(_col) < -600_000_000).then(None).otherwise(pl.col(_col)).alias(_col)
            )
    if "property_value" not in _df.columns:
        # Match training's coalesce order (train_modal_sb.py)
        _val_fallbacks = ["total_appraised_value", "median_home_value", "assessed_value"]
        _found_val = next((c for c in _val_fallbacks if c in _df.columns), None)
        if _found_val:
            _df = _df.with_columns(pl.col(_found_val).alias("property_value"))
            print(f"[{_ts()}] Derived property_value from {_found_val}")
    if "parcel_id" not in _df.columns and "acct" not in _df.columns:
        if "global_parcel_id" in _df.columns:
            _df = _df.with_columns(pl.col("global_parcel_id").alias("parcel_id"))
        elif "geoid" in _df.columns:
            _df = _df.with_columns(pl.col("geoid").alias("parcel_id"))
    _rename_map = {"parcel_id": "acct", "year": "yr", "property_value": "tot_appr_val",
                   "sqft": "living_area", "land_area": "land_ar", "year_built": "yr_blt",
                   "building_area_sqft": "living_area", "land_area_sqft": "land_ar",
                   "bedrooms": "bed_cnt", "bathrooms": "full_bath", "stories": "nbr_story",
                   "lat": "gis_lat", "lon": "gis_lon"}
    _actual = {k: v for k, v in _rename_map.items() if k in _df.columns}
    _drop = [v for k, v in _actual.items() if v in _df.columns]
    if _drop:
        _df = _df.drop(_drop)
    _df = _df.rename(_actual)
    # Drop leaky columns AFTER deriving property_value
    # Must match train_modal_sb.py leaky_cols list to prevent feature leakage
    for c in ["median_home_value", "sale_price", "assessed_value", "land_value",
              "improvement_value", "total_appraised_value",
              "CURMKTLAND", "CURMKTTOT", "CURACTTOT", "FINACTTOT"]:
        if c in _df.columns:
            _df = _df.drop(c)
    # Cast columns to match the checkpoint's feature architecture:
    #   Numeric (13): living_area, land_ar + 11 macro features
    #   Categorical (3): county_id, yr_blt, global_parcel_id (stay as Utf8)
    # yr_blt must NOT be cast — it's categorical in the training checkpoint.
    _numeric_casts = {"tot_appr_val": pl.Float64, "yr": pl.Int64,
                      "living_area": pl.Float64, "land_ar": pl.Float64}
    for _nc, _ndt in _numeric_casts.items():
        if _nc in _df.columns and _df[_nc].dtype == pl.Utf8:
            print(f"[{_ts()}] Casting {_nc} from Utf8 -> {_ndt}")
            _df = _df.with_columns(pl.col(_nc).cast(_ndt, strict=False))
    if "tot_appr_val" in _df.columns:
        _df = _df.filter(pl.col("tot_appr_val").is_not_null() & (pl.col("tot_appr_val") > 0))
    _df.write_parquet(panel_path)
    del _df

    # ─── 5b. Macro enrichment: add FRED features to match training's 34-feature panel ─
    # Training merges 11 macro indicators (unemployment, CPI, VIX, etc.) from GCS FRED CSVs.
    # Without this, panel has 23 features but checkpoint expects 34 → shape mismatch.
    print(f"[{_ts()}] Adding macro features to match training panel...")
    try:
        import pandas as _pd
        MACRO_SERIES = {
            "macro/fred/MORTGAGE30US.csv":            "macro_mortgage30",
            "macro/fred/FEDFUNDS.csv":                "macro_fedfunds",
            "macro/fred/DGS10.csv":                   "macro_10yr_treasury",
            "macro/fred/CPIAUCSL.csv":                "macro_cpi_us",
            "macro/fred/CP0000EZ19M086NEST.csv":      "macro_cpi_eurozone",
            "macro/fred/DCOILWTICO.csv":              "macro_oil_price",
            "macro/fred/UNRATE.csv":                  "macro_unemployment_us",
            "macro/fred/LRHUTTTTEZM156S.csv":         "macro_unemployment_eurozone",
            "macro/fred/VIXCLS.csv":                  "macro_vix",
            "macro/fred/GEPUCURRENT.csv":             "macro_global_epu",
            "macro/fred/IR3TIB01EZM156N.csv":         "macro_euribor_3m",
        }
        import io as _io
        macro_frames = {}

        # ── Fast path: try pre-staged annual lookup parquet first ──
        # Run scripts/inference/utils/stage_macro_lookup.py once to build it.
        _lookup_blob = bucket.blob("macro/macro_annual_lookup.parquet")
        _used_fast_path = False
        try:
            if _lookup_blob.exists():
                import io as _io2
                _lookup_bytes = _lookup_blob.download_as_bytes(timeout=60)
                macro_pl = pl.read_parquet(_io2.BytesIO(_lookup_bytes))
                if "yr" in macro_pl.columns:
                    macro_pl = macro_pl.cast({"yr": pl.Int32})
                    _df2 = pl.read_parquet(panel_path)
                    _df2 = _df2.join(macro_pl, on="yr", how="left")
                    _df2.write_parquet(panel_path)
                    n_macro_cols = len([c for c in macro_pl.columns if c != "yr"])
                    print(f"[{_ts()}] Macro enrichment (fast path): {n_macro_cols} features from "
                          f"macro_annual_lookup.parquet → {len(_df2.columns)} total cols")
                    del _df2
                    _used_fast_path = True
        except Exception as _fast_err:
            print(f"[{_ts()}] ⚠️ Macro fast path failed ({_fast_err}), falling back to per-CSV loop")

        # ── Slow path: per-CSV download and join (fallback) ──
        if not _used_fast_path:
            for gcs_path, col_name in MACRO_SERIES.items():
                try:
                    blob = bucket.blob(gcs_path)
                    if not blob.exists():
                        continue
                    raw = _pd.read_csv(_io.BytesIO(blob.download_as_bytes()), on_bad_lines="skip")
                    if len(raw.columns) < 2:
                        continue
                    date_col, val_col = raw.columns[0], raw.columns[1]
                    raw[date_col] = _pd.to_datetime(raw[date_col], errors="coerce")
                    raw[val_col] = _pd.to_numeric(raw[val_col], errors="coerce")
                    raw = raw.dropna(subset=[date_col, val_col])
                    raw["_year"] = raw[date_col].dt.year
                    annual = raw.groupby("_year")[val_col].mean().reset_index()
                    annual.columns = ["_year", col_name]
                    macro_frames[col_name] = annual
                except Exception as _me:
                    print(f"[{_ts()}] ⚠️ Macro {gcs_path}: {_me}")
            if macro_frames:
                merged = None
                for _name, _frame in macro_frames.items():
                    merged = _frame if merged is None else merged.merge(_frame, on="_year", how="outer")
                merged = merged.sort_values("_year")
                macro_pl = pl.from_pandas(merged).rename({"_year": "yr"}).cast({"yr": pl.Int32})
                _df2 = pl.read_parquet(panel_path)
                _df2 = _df2.join(macro_pl, on="yr", how="left")
                _df2.write_parquet(panel_path)
                print(f"[{_ts()}] Macro enrichment (slow path): {len(macro_frames)} features added → {len(_df2.columns)} total cols")
                del _df2
    except Exception as _ee:
        print(f"[{_ts()}] ⚠️ Macro enrichment failed (non-fatal): {_ee}")


    # ─── 6. Set globals and exec worldmodel.py ───────────────────────────────
    import torch
    out_root = f"/output/{jurisdiction}_inference/{suite_id}"
    os.makedirs(out_root, exist_ok=True)

    g = globals()
    g.update({
        "PANEL_PATH": panel_path,
        "JURISDICTION": jurisdiction,
        "CKPT_DIR": ckpt_dir,
        "OUT_DIR": ckpt_dir,
        "FORECAST_ORIGIN_YEAR": origin_year,
        "SUPABASE_DB_URL": os.environ.get("SUPABASE_DB_URL", ""),
        "TARGET_SCHEMA": schema,
        "CKPT_VARIANT_SUFFIX": "" if _use_sb else "SF500K",  # SB trained on all property types — skip SF filter
        "MODEL_VARIANT": model,
        "APPLY_CALIBRATION": False,  # disabled globally per user request
        "RUN_FULL_BACKTEST": False,
        "H": 5 if jurisdiction in ["florida_dor", "nyc"] else 6,  # NYC was trained with H=5
        "S_SCENARIOS": 256,
        "OUT_ROOT": out_root,
        "SUITE_ID": suite_id,
        # Disable per-shard final aggregate refresh.
        # In parallel mode each shard would DELETE all aggregates for
        # (origin_year, series_kind, variant_id) — not scoped to its run_id —
        # then re-INSERT only its own 1/N parcel rows.  The last shard to
        # finish wins and wipes all other shards' aggregated data.
        # Use rebuild_aggregates.py as a single post-inference pass instead.
        "RUN_FINAL_EXACT_AGG_REFRESH": False,
        # Skip per-chunk aggregate writes in parallel mode.
        # Each shard only has 1/N of the accounts but ALL geographies overlap
        # across shards (the same ZCTA can have parcels in shard 1 AND shard 3).
        # With DO NOTHING, only the first shard to write a geo row wins and
        # subsequent shards' parcel rows are silently excluded from that geo's average.
        # Solution: skip agg writes entirely during inference; run rebuild_aggregates.py
        # once after all shards complete to get a clean, complete aggregation.
        "SKIP_AGG_CHUNK_WRITES": True,
        # Disable torch.compile in Modal — Triton's inductor backend has
        # filesystem race conditions in containerized /tmp dirs, causing
        # FileNotFoundError on triton_.json.tmp files.  Eager mode is ~30%
        # slower per shard but avoids crashes entirely.
        "USE_TORCH_COMPILE": False,
        # Raise statement timeout to 20 min for large parcel history/forecast inserts
        # (default 5 min was too short for 500K+ row chunks hitting index rebuild)
        "PG_STATEMENT_TIMEOUT_MS": 1200_000,  # 20 minutes
        # ── Large-jurisdiction history write bypass ──────────────────────────────
        # For FL DOR (and other large jurisdictions), history chunks are ~2.7M rows
        # each.  The Supabase upsert causes a 10-minute stall between chunks and
        # crashes the run at exactly 13.87%.  Set SKIP_HISTORY_DB_WRITE=True to
        # write only to GCS (parquets still saved by watchdog).  Run
        # upload_vol_to_supabase_modal.py post-inference to replay history to Supabase.
        "SKIP_HISTORY_DB_WRITE": "florida" in jurisdiction or "fl_" in jurisdiction,
        # ── GCS-based resume skip-scan ───────────────────────────────────────────
        # Provides a second skip-source alongside Supabase: scans already-written
        # GCS forecast parquets in the resumed suite so accounts whose DB write
        # failed (but whose parquet landed) are not re-inferred.
        "GCS_SKIP_BUCKET": "properlytic-raw-data",
        "GCS_SKIP_SUITE_ID": suite_id,  # same suite_id used for resume
    })

    # Patch DB URL to raise statement_timeout from Supabase's default (~30s)
    # to 5 min so large history-chunk inserts don't get killed mid-write.
    # psycopg2 accepts ?options=-c%20statement_timeout%3D300000 in the DSN.
    _db_url = os.environ.get("SUPABASE_DB_URL", "")
    if _db_url and "statement_timeout" not in _db_url:
        import urllib.parse as _up
        _sep = "&" if "?" in _db_url else "?"
        _db_url_patched = _db_url + _sep + "options=" + _up.quote("-c statement_timeout=1200000")
        os.environ["SUPABASE_DB_URL"] = _db_url_patched
        g["SUPABASE_DB_URL"] = _db_url_patched
        print(f"[{_ts()}] DB statement_timeout patched to 1200s (20min) for shard {shard_idx+1}")

    exec(wm_source, g)
    print(f"[{_ts()}] {'worldmodel_inference_sb' if _use_sb else 'worldmodel'}.py loaded")

    # For v12_sb: also load the SF2M training code overlay (needed for model class defs)
    if _use_sb:
        _wm_sb_path = "/scripts/inference/v12_sb/worldmodel_sb.py"
        with open(_wm_sb_path, "r") as _f:
            wm_sb_source = _f.read()
        g["__file__"] = _wm_sb_path  # so worldmodel_sb.py resolves worldmodel_inference_sb.py
        exec(wm_sb_source, g)
        print(f"[{_ts()}] worldmodel_sb.py overlay loaded")

    # ── torch.compile safety net ──────────────────────────────────────────────
    # Even with USE_TORCH_COMPILE=False, some code paths may trigger a compile.
    # Give each shard its own Triton cache dir to prevent /tmp race conditions
    # (FileNotFoundError on triton_.cubin.tmp) and suppress any compile errors
    # so they fall back to eager rather than crashing the container.
    _shard_tmp = f"/tmp/torchinductor_shard{shard_idx}"
    os.makedirs(_shard_tmp, exist_ok=True)
    os.environ["TORCHINDUCTOR_CACHE_DIR"] = _shard_tmp
    try:
        import torch._dynamo as _dynamo
        _dynamo.config.suppress_errors = True
    except Exception:
        pass

    # inference_pipeline.py expects `all_accts` in globals (set by worldmodel.py at module level).
    # In parallel mode, each shard only has its subset — inject it before exec.
    g["all_accts"] = shard_accts

    # v12_sb: worldmodel_sb.py does not create `lf` (panel DataFrame) like v11 worldmodel.py does.
    # inference_pipeline.py references `lf` for building inference context — inject it.
    if _use_sb and "lf" not in g:
        g["lf"] = pl.scan_parquet(panel_path)
        _lf_schema = g["lf"].collect_schema()
        print(f"[{_ts()}] Injected 'lf' panel LazyFrame: {len(_lf_schema)} cols")

    exec(inf_source, g)
    print(f"[{_ts()}] inference_pipeline.py loaded")

    # ─── 7. Load checkpoint ───────────────────────────────────────────────────
    ckpt_pairs = g["_get_checkpoint_paths"](ckpt_dir)
    ckpt_origin, ckpt_path = g["_pick_ckpt_for_origin"](ckpt_pairs, origin_year)
    print(f"[{_ts()}] Using checkpoint: origin={ckpt_origin} H={g.get('H')} path={os.path.basename(ckpt_path)}")

    import torch as _torch
    _ckpt = _torch.load(ckpt_path, map_location="cuda" if _torch.cuda.is_available() else "cpu")
    _ckpt_H = _ckpt.get("cfg", {}).get("H", 5)
    print(f"[{_ts()}] Checkpoint H={_ckpt_H} (config H={g.get('H')})")

    g["_load_ckpt_into_live_objects"](ckpt_path)
    print(f"[{_ts()}] Checkpoint loaded")

    # ─── 8. Run inference for this shard only ─────────────────────────────────
    print(f"\n{'='*60}")
    print(f"SHARD {shard_idx+1}/{n_shards}: {jurisdiction} origin={origin_year}")
    print(f"  Accounts in shard: {len(shard_accts):,}")
    print(f"  run_id: {run_id}")
    print(f"  suite_id: {suite_id}")
    print(f"{'='*60}\n")

    # ─── GCS streaming watchdog for this shard ────────────────────────
    # Polls the shard's output dir every 60s and uploads new files to GCS
    # as chunks land, rather than waiting until the shard finishes.
    import threading
    _gcs_uploaded = set()
    _stop_watchdog = threading.Event()

    def _gcs_watchdog():
        _interval = 60
        _count = 0
        while not _stop_watchdog.is_set():
            try:
                for dp, _, fnames in os.walk(out_root):
                    for fn in fnames:
                        if not fn.endswith((".parquet", ".csv.gz", ".json")):
                            continue
                        fp = os.path.join(dp, fn)
                        rel = os.path.relpath(fp, out_root)
                        gcs_key = f"inference_output/{jurisdiction}/{suite_id}/{rel.replace(os.sep, '/')}"
                        if gcs_key in _gcs_uploaded:
                            continue
                        try:
                            bucket.blob(gcs_key).upload_from_filename(fp)
                            _gcs_uploaded.add(gcs_key)
                            _count += 1
                            print(f"[{_ts()}] 📤 GCS shard{shard_idx+1}: {os.path.basename(fp)} (total={_count})")
                        except Exception as _e:
                            print(f"[{_ts()}] ⚠️ GCS watchdog shard{shard_idx+1} failed {gcs_key}: {_e}")
            except Exception as _scan_err:
                print(f"[{_ts()}] ⚠️ GCS watchdog shard{shard_idx+1} scan error: {_scan_err}")
            _stop_watchdog.wait(_interval)
        print(f"[{_ts()}] GCS watchdog shard{shard_idx+1} stopped. Total streamed: {_count} files.")

    _watchdog_thread = threading.Thread(target=_gcs_watchdog, daemon=True, name=f"gcs-watchdog-shard{shard_idx}")
    _watchdog_thread.start()
    print(f"[{_ts()}] GCS watchdog started for shard {shard_idx+1}/{n_shards}")

    result = g["_run_one_origin"](
        schema=g["TARGET_SCHEMA"],
        all_accts_prod=shard_accts,
        origin_year=origin_year,
        mode="forecast",
        ckpt_origin=ckpt_origin,
        ckpt_path=ckpt_path,
        out_dir=os.path.join(out_root, "production"),
        variant_id="__forecast__",
        write_history_series=True,
        resume_run_id=run_id,   # all shards share run_id = no double-writes
    )

    # Stop watchdog and let it finish its last scan
    _stop_watchdog.set()
    _watchdog_thread.join(timeout=120)

    elapsed = time.time() - t0
    print(f"[{_ts()}] Shard {shard_idx+1}/{n_shards} done in {elapsed/60:.1f} min")
    output_vol.commit()

    # Final straggler sweep (catches any files written in the last 60s window)
    _final_count = 0
    for dp, _, fnames in os.walk(out_root):
        for fn in fnames:
            if not fn.endswith((".parquet", ".csv.gz", ".json")):
                continue
            fp = os.path.join(dp, fn)
            rel = os.path.relpath(fp, out_root)
            gcs_key = f"inference_output/{jurisdiction}/{suite_id}/{rel.replace(os.sep, '/')}"
            if gcs_key in _gcs_uploaded:
                continue
            try:
                bucket.blob(gcs_key).upload_from_filename(fp)
                _final_count += 1
                print(f"[{_ts()}] 📤 GCS straggler shard{shard_idx+1}: {gcs_key}")
            except Exception as e:
                print(f"[{_ts()}] ⚠️ GCS straggler upload failed: {gcs_key}: {e}")
    _total = len(_gcs_uploaded) + _final_count
    print(f"[{_ts()}] GCS shard{shard_idx+1} sync complete: {_total} files ({len(_gcs_uploaded)} live + {_final_count} stragglers)")

    return {"shard": shard_idx, "accounts": len(shard_accts), "elapsed_min": round(elapsed / 60, 1)}


@app.function(
    image=image,
    secrets=[gcs_secret],
    timeout=900,
    memory=16384,
)
def _load_accounts(jurisdiction: str, panel_gcs_path: str = "") -> list:
    """Load all account IDs from GCS panel — runs in Modal with cloud creds.

    Fast path: if acct_lists/{jurisdiction}_acct_list.parquet exists in GCS and
    was written within the last 30 days, download only that tiny file instead of
    the full 500MB+ panel.  Falls back to full panel download and writes a fresh
    cache on completion.
    """
    import json, polars as pl, time as _time
    from google.cloud import storage
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")

    # ── Fast path: GCS acct-list cache ──────────────────────────────────────
    _cache_key = f"acct_lists/{jurisdiction}_acct_list.parquet"
    _cache_ttl_days = 30
    _cache_blob = bucket.blob(_cache_key)
    try:
        _cache_blob.reload()  # fetch metadata
        from datetime import timezone, timedelta
        _age = (_time.time() -
                _cache_blob.updated.replace(tzinfo=timezone.utc).timestamp()) / 86400
        if _age < _cache_ttl_days:
            import io as _io
            import pyarrow.parquet as _pq
            _raw = _cache_blob.download_as_bytes(timeout=120)
            _tbl = _pq.read_table(_io.BytesIO(_raw), columns=["acct"])
            all_accts = [str(a) for a in _tbl["acct"].to_pylist()]
            print(f"[_load_accounts] ⚡ Cache HIT: loaded {len(all_accts):,} accts from "
                  f"{_cache_key} (age={_age:.1f}d)")
            return all_accts
        else:
            print(f"[_load_accounts] Cache stale ({_age:.1f}d > {_cache_ttl_days}d), refreshing from panel")
    except Exception as _ce:
        print(f"[_load_accounts] Cache miss or error ({_ce}), loading from panel")

    # ── Slow path: download full panel ───────────────────────────────────────
    panel_path = "/tmp/panel_split.parquet"
    _panel_blob_path = _resolve_panel_gcs_path(jurisdiction, panel_gcs_path)
    blob = bucket.blob(_panel_blob_path)
    # Retry download up to 3 times — large panels can fail with PARTIAL_CONTENT
    for attempt in range(3):
        try:
            blob.download_to_filename(panel_path, timeout=600)
            break
        except Exception as e:
            print(f"[_load_accounts] Download attempt {attempt+1} failed: {e}")
            if attempt == 2:
                raise
            _time.sleep(5 * (attempt + 1))
    schema = pl.scan_parquet(panel_path).collect_schema().names()
    acct_col = "acct" if "acct" in schema else ("parcel_id" if "parcel_id" in schema else "geoid")
    # Only read the acct column to minimize memory for large panels
    all_accts = pl.scan_parquet(panel_path).select(acct_col).unique().collect().to_series().to_list()
    all_accts = [str(a) for a in all_accts]
    print(f"[_load_accounts] Loaded {len(all_accts):,} unique accounts ({acct_col}) from {jurisdiction}")

    # ── Write cache for next resume ──────────────────────────────────────────
    try:
        import pyarrow as _pa
        import pyarrow.parquet as _pq
        import io as _io
        _tbl = _pa.table({"acct": _pa.array(all_accts, type=_pa.string())})
        _buf = _io.BytesIO()
        _pq.write_table(_tbl, _buf)
        bucket.blob(_cache_key).upload_from_string(_buf.getvalue(), content_type="application/octet-stream")
        print(f"[_load_accounts] ✅ Wrote acct cache to GCS: {_cache_key} ({len(all_accts):,} accts)")
    except Exception as _we:
        print(f"[_load_accounts] ⚠️ Failed to write acct cache (non-fatal): {_we}")

    return all_accts


@app.function(
    image=image,
    secrets=[gcs_secret, supabase_secret],
    timeout=21600,
    memory=16384,
)
def _dispatch_and_run(
    jurisdiction: str,
    origin: int,
    n_shards: int,
    run_id: str,
    suite_id: str,
    schema: str,
    panel_gcs_path: str,
    model: str,
):
    """Split accounts into shards and fan out run_inference_shard — all inside Modal.

    By running this as a Modal function rather than in the @local_entrypoint, the
    17M+ account ID list is never serialized across the local↔Modal wire.  The list
    is loaded, shuffled, split, and dispatched entirely within the Modal network.
    """
    import uuid, time, random, json, io
    import polars as pl
    from google.cloud import storage

    creds  = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")

    # ── Read account list from GCS cache (written by _load_accounts) ──────────
    _cache_key = f"acct_lists/{jurisdiction}_acct_list.parquet"
    _cache_blob = bucket.blob(_cache_key)
    try:
        import pyarrow.parquet as _pq
        _raw = _cache_blob.download_as_bytes(timeout=120)
        _tbl = _pq.read_table(io.BytesIO(_raw), columns=["acct"])
        all_accts = [str(a) for a in _tbl["acct"].to_pylist()]
        print(f"[dispatch] Loaded {len(all_accts):,} accts from cache {_cache_key}")
    except Exception as _ce:
        # Fallback: scan the panel directly
        print(f"[dispatch] Cache miss ({_ce}), loading from panel")
        panel_path = "/tmp/panel_dispatch.parquet"
        from inference_modal_parallel import _resolve_panel_gcs_path
        blob = bucket.blob(_resolve_panel_gcs_path(jurisdiction, panel_gcs_path))
        blob.download_to_filename(panel_path, timeout=600)
        schema_names = pl.scan_parquet(panel_path).collect_schema().names()
        acct_col = "acct" if "acct" in schema_names else ("parcel_id" if "parcel_id" in schema_names else "geoid")
        all_accts = [str(a) for a in pl.scan_parquet(panel_path).select(acct_col).unique().collect().to_series().to_list()]
        print(f"[dispatch] Loaded {len(all_accts):,} accts from panel")

    # ── Shard and dispatch ─────────────────────────────────────────────────────
    random.seed(42)
    random.shuffle(all_accts)
    chunk_size = max(1, len(all_accts) // n_shards)
    shards = []
    for i in range(n_shards):
        start = i * chunk_size
        end = start + chunk_size if i < n_shards - 1 else len(all_accts)
        shards.append(all_accts[start:end])

    print(f"[dispatch] Dispatching {n_shards} shards:")
    for i, s in enumerate(shards):
        print(f"  Shard {i+1}: {len(s):,} accounts")

    t0 = time.time()
    inputs = [
        (jurisdiction, origin, shard, run_id, i, n_shards, suite_id, schema, panel_gcs_path, model)
        for i, shard in enumerate(shards)
    ]

    results = list(run_inference_shard.starmap(inputs))

    elapsed = time.time() - t0
    print(f"\n✅ All {n_shards} shards complete in {elapsed/60:.1f} min")
    for r in results:
        print(f"  Shard {r['shard']+1}: {r['accounts']:,} accts in {r['elapsed_min']} min")
    total = sum(r['accounts'] for r in results)
    print(f"\nTotal accounts processed: {total:,}")
    return {"total_accounts": total, "elapsed_min": round(elapsed / 60, 1), "n_shards": n_shards}


@app.local_entrypoint()
def main(
    jurisdiction: str = "acs_nationwide",
    origin: int = 2024,
    n_shards: int = 6,
    resume_run_id: str = "forecast_2025_20260309T022308Z_cc38ca113e00434681196b3375cb4d47",   # FL DOR cancelled @13.87% — pass empty string to start fresh
    suite_id_override: str = "suite_20260309T022308Z_2a1dd29f6dd446c59f1353b07c329e72",  # FL DOR suite — pass empty string for a new suite
    schema: str = "forecast_queue",
    panel_gcs_path: str = "",  # override GCS blob path for non-standard panels
    model: str = "v11",        # "v11" or "v12_sb" — which worldmodel to use
):
    """Fan out inference across N parallel A100 containers.

    Resume a cancelled run:
        modal run ... --resume-run-id RUN_ID --suite-id-override SUITE_ID

    NOTE: shard-splitting runs inside Modal (_dispatch_and_run) so large account
    lists are never serialized across the local↔Modal wire.
    """
    import uuid, time

    print(f"🚀 Parallel inference ({model}): {jurisdiction} o={origin} n_shards={n_shards}")
    if resume_run_id:
        print(f"🔄 RESUME mode: run_id={resume_run_id} suite_id={suite_id_override or '(new suite)'}")

    # Ensure acct list is cached in GCS before dispatch (Modal function with GCS creds)
    _load_accounts.remote(jurisdiction, panel_gcs_path)

    # Build shared run/suite IDs locally (just strings — cheap to pass)
    suite_id = suite_id_override if suite_id_override else f"suite_{time.strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:16]}"
    run_id   = resume_run_id    if resume_run_id    else f"forecast_{origin}_{time.strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:16]}"
    print(f"  suite_id : {suite_id}")
    print(f"  run_id   : {run_id}")

    # Dispatch: all heavy work (shuffle, split, starmap) happens inside Modal
    result = _dispatch_and_run.remote(
        jurisdiction=jurisdiction,
        origin=origin,
        n_shards=n_shards,
        run_id=run_id,
        suite_id=suite_id,
        schema=schema,
        panel_gcs_path=panel_gcs_path,
        model=model,
    )
    print(f"\n✅ Done: {result['total_accounts']:,} accounts in {result['elapsed_min']} min")

