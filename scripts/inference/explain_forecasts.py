"""
Post-Hoc Explainability Script (Modal Parallel)
===============================================
Computes surrogate SHAP attributions for the FT-Transformer's deterministic
mu_backbone step and stores them in a new attribution table.
"""
import modal, os, sys, json
import time, math
from datetime import datetime

_model = "v11"
_n_shards = 1

_model_tag = "sb" if _model == "v12_sb" else "v11"
# App name will be updated dynamically inside main if needed, or just keep generic
app = modal.App("explain-forecasts")

image = (
    modal.Image.from_registry(
        "pytorch/pytorch:2.3.0-cuda12.1-cudnn8-runtime",
        add_python="3.11",
    )
    .pip_install(
        "google-cloud-storage",
        "numpy",
        "pandas",
        "polars",
        "pyarrow",
        "psycopg2-binary",
        "scipy",
        "POT>=0.9",
        "captum",
    )
    .add_local_dir("scripts", remote_path="/scripts")
)

supabase_secret = modal.Secret.from_name("supabase-creds", required_keys=["SUPABASE_DB_URL"])
gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])

output_vol = modal.Volume.from_name("inference-outputs", create_if_missing=True)
ckpt_vol = modal.Volume.from_name("properlytic-checkpoints", create_if_missing=True)

_PANEL_OVERRIDES = {
    "nyc": "panel/jurisdiction=nyc/nyc_panel_h3.parquet",
    "florida_dor": "panels/florida_dor_panel.parquet",
}

def _resolve_panel_gcs_path(jurisdiction: str) -> str:
    return _PANEL_OVERRIDES.get(jurisdiction, f"panel/jurisdiction={jurisdiction}/part.parquet")

def _ts():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

@app.function(
    image=image,
    secrets=[gcs_secret, supabase_secret],
    gpu="A10G",
    timeout=3600,
    memory=16384,
    volumes={"/output": output_vol, "/checkpoints": ckpt_vol},
)
def run_explain_shard(
    jurisdiction: str,
    origin_year: int,
    shard_accts: list,
    run_id: str,
    schema: str,
    model: str = "v11",
):
    import polars as pl
    import psycopg2
    from psycopg2.extras import execute_values
    import glob as _glob, shutil
    import torch
    import traceback
    
    try:
        t0 = time.time()
        print(f"[{_ts()}] ═══ EXPLAIN SHARD: {len(shard_accts):,} accounts ═══")

        # 1. Download panel
        from google.cloud import storage
        creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
        client = storage.Client.from_service_account_info(creds)
        bucket = client.bucket("properlytic-raw-data")

        panel_path = f"/tmp/panel_{jurisdiction}_explain.parquet"
        _panel_blob_path = _resolve_panel_gcs_path(jurisdiction)
        blob = bucket.blob(_panel_blob_path)
        blob.download_to_filename(panel_path)
        print(f"[{_ts()}] Downloaded panel: {os.path.getsize(panel_path) / 1e6:.1f} MB")

        # 2. Setup Worldmodel Env
        _use_sb = (model == "v12_sb")
        if _use_sb:
            with open("/scripts/inference/v12_sb/worldmodel_inference_sb.py", "r") as _f:
                wm_source = _f.read()
        else:
            with open("/scripts/inference/worldmodel.py", "r") as _f:
                wm_source = _f.read()

        _ckpt_suffix = "v12sb" if _use_sb else "v11"
        ckpt_dir = f"/output/{jurisdiction}_{_ckpt_suffix}"
        os.makedirs(ckpt_dir, exist_ok=True)
        modal_ckpt_dir = f"/checkpoints/{jurisdiction}_{_ckpt_suffix}"
        modal_ckpts = _glob.glob(os.path.join(modal_ckpt_dir, "*.pt")) if os.path.isdir(modal_ckpt_dir) else []
        for src in modal_ckpts:
            shutil.copy2(src, os.path.join(ckpt_dir, os.path.basename(src)))

        # Evaluate worldmodel environment
        g = globals().copy()
        g.update({
            "PANEL_PATH": panel_path,
            "JURISDICTION": jurisdiction,
            "CKPT_DIR": ckpt_dir,
            "OUT_DIR": ckpt_dir,
            "FORECAST_ORIGIN_YEAR": origin_year,
            "SUPABASE_DB_URL": os.environ.get("SUPABASE_DB_URL", ""),
            "TARGET_SCHEMA": schema,
            "CKPT_VARIANT_SUFFIX": "" if _use_sb else "SF500K",
            "MODEL_VARIANT": model,
            "H": 6,
            "USE_TORCH_COMPILE": False,
            "APPLY_CALIBRATION": False,
        })
        
        # Needs to process panel macro enrichment to match features (13 numeric + 11 macro)
        _df = pl.read_parquet(panel_path)
        if "property_value" not in _df.columns:
            _val_fallbacks = ["total_appraised_value", "median_home_value", "assessed_value"]
            _found_val = next((c for c in _val_fallbacks if c in _df.columns), None)
            if _found_val: _df = _df.with_columns(pl.col(_found_val).alias("property_value"))
        if "parcel_id" not in _df.columns and "acct" not in _df.columns:
            if "global_parcel_id" in _df.columns: _df = _df.with_columns(pl.col("global_parcel_id").alias("parcel_id"))
            elif "geoid" in _df.columns: _df = _df.with_columns(pl.col("geoid").alias("parcel_id"))
        _actual = {k: v for k, v in {"parcel_id": "acct", "year": "yr", "property_value": "tot_appr_val", "sqft": "living_area", "land_area": "land_ar", "year_built": "yr_blt"}.items() if k in _df.columns}
        _drop = [v for k, v in _actual.items() if v in _df.columns]
        if _drop: _df = _df.drop(_drop)
        _df = _df.rename(_actual)
        _df.write_parquet(panel_path)
        del _df
        
        try:
            # We execute worldmodel source so it builds `lf` and parses checkpoints
            exec(wm_source, g)
            if _use_sb:
                _wm_sb_path = "/scripts/inference/v12_sb/worldmodel_sb.py"
                with open(_wm_sb_path, "r") as _f:
                    wm_sb_source = _f.read()
                g["__file__"] = _wm_sb_path
                exec(wm_sb_source, g)
        except Exception as e:
            import traceback
            err_msg = traceback.format_exc()
            return f"ERROR_SETUP: {str(e)}\n{err_msg}"
            
        mu_backbone = g.get("mu_backbone")
        if mu_backbone is None:
            return "ERROR: mu_backbone is None. Checkpoint might not contain weights."
            
        build_ctx_fn = g.get("build_inference_context_chunked_v102")
        if build_ctx_fn is None:
            return "ERROR: build_inference_context_chunked_v102 not generated by env."

        lf_ref = g.get("lf")
        num_use = g.get("num_use", [])
        cat_use = g.get("cat_use", [])
        global_medians = g.get("global_medians", {})
        
        print(f"[{_ts()}] Features: {len(num_use)} numeric, {len(cat_use)} categorical")
        
        device = "cuda" if torch.cuda.is_available() else "cpu"
        mu_backbone = mu_backbone.to(device)
        mu_backbone.eval()
        
        # Build batch extraction function using PyTorch AutoGrad
        def extract_shap(mu_bb, _hist_y, _cur_num, _cur_cat, _region_id, _origin_year):
            _cur_num = _cur_num.to(device)
            _cur_num.requires_grad_(True)
            
            B = _hist_y.shape[0]
            chunk_size = 512
            mu_parts = []
            
            # Forward pass (grad enabled)
            with torch.enable_grad():
                for i in range(0, B, chunk_size):
                    j = min(i + chunk_size, B)
                    oy_c = _origin_year[i:j].to(device) if _origin_year is not None else None
                    mu_c, _ = mu_bb(
                        _hist_y[i:j].to(device),
                        _cur_num[i:j],
                        _cur_cat[i:j].to(device),
                        _region_id[i:j].to(device),
                        origin_year=oy_c
                    )
                    mu_parts.append(mu_c)
                    
                mu_hat = torch.cat(mu_parts, dim=0)

                # mu_hat is [B, H_dim]
                # SHAP surrogate: feature * sum(grad(mu_last))
                # We care about exactly how it impacts the last target Delta year
                last_targets = mu_hat[:, -1]
                last_targets.sum().backward()
                
                grad_base = _cur_num.grad * _cur_num  # [B, NUM_DIM]
                return grad_base.detach().cpu().numpy()
                
        # Process accounts in batches to avoid OOM
        batch_size = 2048
        results_to_upsert = []
        
        print(f"[{_ts()}] Context builder extraction beginning...")
        for i in range(0, len(shard_accts), batch_size):
            accts_batch = shard_accts[i : i + batch_size]
            ctx = build_ctx_fn(
                lf=lf_ref,
                accts=accts_batch,
                num_use_local=num_use,
                cat_use_local=cat_use,
                global_medians=global_medians,
                anchor_year=int(origin_year),
            )
            if ctx is None or "acct" not in ctx: continue
            
            # Valid length might be smaller due to dedup logic in ctx building
            a_list = ctx["acct"]
            b_len = len(a_list)
            
            shap_vals = extract_shap(mu_backbone, ctx["hist_y"], ctx["cur_num"], ctx["cur_cat"], ctx["region_id"], None)
            
            for b_idx in range(b_len):
                acct_val = str(a_list[b_idx])
                shap_vec = shap_vals[b_idx].tolist()
                
                # Form dict mapping feature name -> shap value
                attr_dict = {feat_name: round(val, 6) for feat_name, val in zip(num_use, shap_vec)}
                
                # Format: (acct, origin_year, variant_id, attributions JSON)
                results_to_upsert.append((
                    acct_val, int(origin_year), "baseline", json.dumps(attr_dict)
                ))

        # Save to Postgres
        if results_to_upsert:
            db_url = os.environ.get("SUPABASE_DB_URL")
            with psycopg2.connect(db_url) as conn:
                with conn.cursor() as cur:
                    # Ensure table exists
                    cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {schema}.metrics_parcel_attribution (
                        acct VARCHAR(50) NOT NULL,
                        origin_year INT NOT NULL,
                        variant_id VARCHAR(50) NOT NULL,
                        attributions JSONB,
                        updated_at TIMESTAMP,
                        PRIMARY KEY (acct, origin_year, variant_id)
                    );
                    """)
                    
                    insert_sql = f"""
                    INSERT INTO {schema}.metrics_parcel_attribution (acct, origin_year, variant_id, attributions, updated_at)
                    VALUES %s
                    ON CONFLICT (acct, origin_year, variant_id)
                    DO UPDATE SET attributions = EXCLUDED.attributions, updated_at = NOW();
                    """
                    execute_values(cur, insert_sql, results_to_upsert, page_size=2000)
        
        print(f"[{_ts()}] Processed and saved SHAP for {len(results_to_upsert)} accounts.")
        return f"SUCCESS: Processed {len(results_to_upsert)} accounts."
    except Exception as e:
        err_msg = traceback.format_exc()
        return f"ERROR_EXECUTION: {str(e)}\n{err_msg}"


@app.local_entrypoint()
def main(
    run_id: str,
    schema: str = "forecast_tx",
    jurisdiction: str = "hcad_houston",
    origin: str = "2025",
    model: str = "v11",
    n_shards: int = 1,
):
    print(f"[{_ts()}] EXPLAIN FORECASTS")
    print(f"  Jurisdiction: {jurisdiction}")
    print(f"  Schema: {schema}")
    print(f"  Run ID: {run_id}")
    import psycopg2
    
    # Fetch distinct accounts from the target run id
    db_url = os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        print("Set SUPABASE_DB_URL for local orchestrator.")
        return
        
    print(f"[{_ts()}] Querying DB for accounts in run_id = {run_id}...")
    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT distinct acct FROM {schema}.metrics_parcel_forecast WHERE run_id = %s LIMIT 2000;", (run_id,))
            rows = cur.fetchall()
            
    accts = [r[0] for r in rows]
    print(f"[{_ts()}] Found {len(accts):,} accounts.")
    if len(accts) == 0:
        return
        
    chunk_size = math.ceil(len(accts) / n_shards)
    shards = [accts[i : i + chunk_size] for i in range(0, len(accts), chunk_size)]
    
    print(f"[{_ts()}] Dispatching {len(shards)} shards to Modal...")
    results = list(run_explain_shard.map([jurisdiction]*len(shards), [int(origin)]*len(shards), shards, [run_id]*len(shards), [schema]*len(shards), [model]*len(shards)))
    print(f"[{_ts()}] ALL SHARDS COMPLETE")
    for idx, r in enumerate(results):
        print(f"--- SHARD {idx} RESULT ---")
        print(r)
