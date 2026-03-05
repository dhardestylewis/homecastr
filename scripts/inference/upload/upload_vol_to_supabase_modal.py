"""
upload_vol_to_supabase_modal.py
================================
Reads inference parquet chunks from the Modal volume ('inference-outputs')
and upserts them into Supabase IN PARALLEL.

Forecasts are uploaded FIRST (map visibility), then history.

Usage:
    python -m modal run scripts/inference/upload/upload_vol_to_supabase_modal.py \
        --jurisdiction hcad_houston

    # Dry-run (just list what's there):
    python -m modal run ... --jurisdiction hcad_houston --dry-run

    # Also copy to GCS while uploading:
    python -m modal run ... --jurisdiction hcad_houston --copy-to-gcs
"""
import modal
import os
import sys

_jur = "hcad_houston"
for i, a in enumerate(sys.argv):
    if a == "--jurisdiction" and i + 1 < len(sys.argv):
        _jur = sys.argv[i + 1]

app = modal.App(f"vol-upload-{_jur}")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "google-cloud-storage",
        "psycopg2-binary",
        "pandas",
        "pyarrow",
    )
)

gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
supabase_secret = modal.Secret.from_name("supabase-creds", required_keys=["SUPABASE_DB_URL"])

GCS_BUCKET = "properlytic-raw-data"
PG_BATCH_ROWS = 2000

output_vol = modal.Volume.from_name("inference-outputs", create_if_missing=False)


# ─────────────────────────────────────────────────────────────────────────────
# Worker — one per parquet file, runs in its own container
# ─────────────────────────────────────────────────────────────────────────────
@app.function(
    image=image,
    secrets=[gcs_secret, supabase_secret],
    timeout=1800,
    memory=8192,
    max_containers=6,   # cap parallel DB connections to avoid pooler saturation
    volumes={"/output": output_vol},
)
def upload_one_chunk(
    fpath: str,
    kind: str,           # "forecast" or "history"
    jurisdiction: str,
    schema: str,
    copy_to_gcs: bool,
    vol_root: str,
) -> dict:
    """Upload a single parquet chunk to Supabase (and optionally GCS)."""
    import json, time
    import pandas as pd
    import psycopg2
    from psycopg2.extras import execute_values
    from datetime import datetime
    import urllib.parse as _up

    def _ts():
        return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    fname = os.path.basename(fpath)

    # ── DB connection ─────────────────────────────────────────────────────────
    db_url = os.environ["SUPABASE_DB_URL"]
    if "statement_timeout" not in db_url:
        sep = "&" if "?" in db_url else "?"
        db_url = db_url + sep + "options=" + _up.quote("-c statement_timeout=1200000")

    def _q(name: str) -> str:
        return f'"{name}"'

    t0 = time.time()
    df = pd.read_parquet(fpath)
    n = len(df)

    if "jurisdiction" not in df.columns:
        df["jurisdiction"] = jurisdiction
    now_iso = datetime.utcnow().isoformat()
    if "updated_at" not in df.columns:
        df["updated_at"] = now_iso
    if "inserted_at" not in df.columns:
        df["inserted_at"] = now_iso

    # Deduplicate
    if kind == "forecast":
        conflict_cols = ["acct", "origin_year", "horizon_m", "series_kind", "variant_id"]
        update_cols = [
            "forecast_year", "value", "p10", "p25", "p50", "p75", "p90",
            "run_id", "backtest_id", "model_version", "as_of_date", "n_scenarios",
            "is_backtest", "updated_at"
        ]
        if "n" in df.columns:
            update_cols.insert(7, "n")
        table = "metrics_parcel_forecast"
    else:
        conflict_cols = ["acct", "year", "series_kind", "variant_id"]
        update_cols = ["value", "p50", "n", "run_id", "backtest_id",
                       "model_version", "as_of_date", "updated_at"]
        table = "metrics_parcel_history"

    ck = [c for c in conflict_cols if c in df.columns]
    if ck:
        df = df.drop_duplicates(subset=ck, keep="last")

    cols = list(df.columns)
    rows = [tuple(r) for r in df.itertuples(index=False, name=None)]

    col_sql = ", ".join(_q(c) for c in cols)
    conflict_sql = ", ".join(_q(c) for c in conflict_cols)
    update_sql = ", ".join(f"{_q(c)} = EXCLUDED.{_q(c)}" for c in update_cols)

    sql = f"""
        INSERT INTO "{schema}".{_q(table)} ({col_sql})
        VALUES %s
        ON CONFLICT ({conflict_sql})
        DO UPDATE SET {update_sql}
    """

    upserted = 0
    max_retries = 5
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(db_url)
            conn.autocommit = False
            with conn.cursor() as cur:
                execute_values(cur, sql, rows, page_size=PG_BATCH_ROWS)
            conn.commit()
            upserted = len(rows)
            conn.close()
            break
        except Exception as e:
            print(f"[{_ts()}] ⚠️ {kind.upper()} {fname} attempt {attempt+1}/{max_retries} failed: {e}")
            try:
                conn.close()
            except:
                pass
            if attempt == max_retries - 1:
                print(f"[{_ts()}] ❌ {kind.upper()} {fname} FAILED after {max_retries} retries")
                return {"file": fname, "kind": kind, "rows": n, "upserted": 0, "gcs": False, "error": str(e)}
            import time as _time
            _time.sleep(5 * (attempt + 1))

    elapsed = time.time() - t0
    print(f"[{_ts()}] ✅ {kind.upper()} {fname} → {upserted:,} rows ({elapsed:.1f}s)", flush=True)

    # Copy to GCS
    gcs_ok = False
    if copy_to_gcs:
        try:
            from google.cloud import storage
            creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
            gcs_client = storage.Client.from_service_account_info(creds)
            bucket = gcs_client.bucket(GCS_BUCKET)
            rel = os.path.relpath(fpath, vol_root)
            gcs_key = f"inference_output/{jurisdiction}/{rel.replace(os.sep, '/')}"
            bucket.blob(gcs_key).upload_from_filename(fpath)
            gcs_ok = True
            print(f"[{_ts()}] 📤 GCS: {gcs_key}", flush=True)
        except Exception as e:
            print(f"[{_ts()}] ⚠️ GCS upload failed: {e}", flush=True)

    return {"file": fname, "kind": kind, "rows": n, "upserted": upserted, "gcs": gcs_ok, "elapsed": round(elapsed, 1)}


# ─────────────────────────────────────────────────────────────────────────────
# Scanner — discovers all parquet files on the volume, organized by suite
# ─────────────────────────────────────────────────────────────────────────────
@app.function(
    image=image,
    timeout=120,
    memory=512,
    volumes={"/output": output_vol},
)
def scan_volume(jurisdiction: str) -> dict:
    """Scan the Modal volume and return parquet file paths organized by suite."""
    import glob
    from collections import defaultdict

    vol_root = f"/output/{jurisdiction}_inference"
    if not os.path.isdir(vol_root):
        avail = sorted(os.listdir("/output"))
        return {"error": f"Not found: {vol_root}", "available": avail, "vol_root": vol_root, "suites": {}}

    all_parquets = sorted(glob.glob(os.path.join(vol_root, "**/*.parquet"), recursive=True))

    # Organize by suite → kind → files
    suites = defaultdict(lambda: {"fc": [], "hist": []})
    for f in all_parquets:
        # Extract suite ID from path
        parts = f.split("/")
        suite_id = None
        for p in parts:
            if p.startswith("suite_"):
                suite_id = p
                break
        if not suite_id:
            continue

        if "/forecast_chunks/" in f:
            suites[suite_id]["fc"].append(f)
        elif "/history_chunks/" in f:
            suites[suite_id]["hist"].append(f)

    # Convert to regular dict for serialization, sorted by suite ID (= chronological)
    suites_sorted = dict(sorted(suites.items()))

    return {
        "vol_root": vol_root,
        "suites": suites_sorted,
    }


@app.local_entrypoint()
def main(
    jurisdiction: str = "hcad_houston",
    schema: str = "forecast_queue",
    dry_run: bool = False,
    copy_to_gcs: bool = False,
):
    """
    Upload inference parquet chunks from Modal volume to Supabase.

    Suites are processed SEQUENTIALLY from oldest to newest so the most recent
    data always wins the ON CONFLICT upsert.  Within each suite, chunks are
    parallelized (concurrency_limit=6 on the worker).  Forecasts before history.

    Examples:
        python -m modal run scripts/inference/upload/upload_vol_to_supabase_modal.py \\
            --jurisdiction hcad_houston --dry-run

        python -m modal run scripts/inference/upload/upload_vol_to_supabase_modal.py \\
            --jurisdiction hcad_houston --copy-to-gcs
    """
    print(f"🚀 Volume-to-Supabase upload: {jurisdiction}")
    print(f"   schema={schema} dry_run={dry_run} copy_to_gcs={copy_to_gcs}")

    # 1. Scan the volume
    scan = scan_volume.remote(jurisdiction)

    if "error" in scan:
        print(f"❌ {scan['error']}")
        if "available" in scan:
            print("Available directories:")
            for d in scan["available"]:
                print(f"  {d}")
        return

    vol_root = scan["vol_root"]
    suites = scan["suites"]

    if not suites:
        print("⚠️  No suites found on volume.")
        return

    total_fc = sum(len(s["fc"]) for s in suites.values())
    total_hist = sum(len(s["hist"]) for s in suites.values())

    print(f"\n📊 Volume scan results:")
    print(f"   Suites: {len(suites)} (oldest → newest)")
    for i, (sid, files) in enumerate(suites.items()):
        # Parse timestamp from suite ID for human-readable display
        ts_part = sid.split("_")[1] if "_" in sid else sid
        print(f"   {i+1}. {sid}")
        print(f"      forecast: {len(files['fc'])} chunks  |  history: {len(files['hist'])} chunks")
    print(f"   TOTAL: {total_fc} forecast + {total_hist} history = {total_fc + total_hist} chunks")

    if dry_run:
        print("\n🔎 DRY RUN — no DB writes.")
        return

    # 2. Process suites sequentially: oldest → newest (most recent wins upsert)
    grand_fc_rows = 0
    grand_hist_rows = 0
    grand_fc_errors = 0
    grand_hist_errors = 0

    for suite_idx, (suite_id, files) in enumerate(suites.items()):
        fc_files = files["fc"]
        hist_files = files["hist"]
        n_files = len(fc_files) + len(hist_files)

        print(f"\n{'═'*60}")
        print(f"📦 SUITE {suite_idx+1}/{len(suites)}: {suite_id}")
        print(f"   {len(fc_files)} forecast + {len(hist_files)} history chunks")
        print(f"{'═'*60}")

        # Phase A: Forecasts first (parallel within suite)
        if fc_files:
            print(f"\n  📈 Forecasts ({len(fc_files)} chunks, 6 parallel workers)...")
            fc_inputs = [
                (fpath, "forecast", jurisdiction, schema, copy_to_gcs, vol_root)
                for fpath in fc_files
            ]
            fc_results = list(upload_one_chunk.starmap(fc_inputs))
            fc_rows = sum(r.get("upserted", 0) for r in fc_results if r)
            fc_errs = sum(1 for r in fc_results if r and r.get("error"))
            grand_fc_rows += fc_rows
            grand_fc_errors += fc_errs
            print(f"  ✅ Forecasts: {fc_rows:,} rows ({fc_errs} errors)")

        # Phase B: History (parallel within suite)
        if hist_files:
            print(f"\n  📜 History ({len(hist_files)} chunks, 6 parallel workers)...")
            hist_inputs = [
                (fpath, "history", jurisdiction, schema, copy_to_gcs, vol_root)
                for fpath in hist_files
            ]
            hist_results = list(upload_one_chunk.starmap(hist_inputs))
            hist_rows = sum(r.get("upserted", 0) for r in hist_results if r)
            hist_errs = sum(1 for r in hist_results if r and r.get("error"))
            grand_hist_rows += hist_rows
            grand_hist_errors += hist_errs
            print(f"  ✅ History: {hist_rows:,} rows ({hist_errs} errors)")

    # 3. Grand summary
    print(f"\n{'='*60}")
    print(f"🏁 GRAND TOTAL across {len(suites)} suites")
    print(f"{'='*60}")
    print(f"  Forecasts: {grand_fc_rows:,} rows ({total_fc} files, {grand_fc_errors} errors)")
    print(f"  History:   {grand_hist_rows:,} rows ({total_hist} files, {grand_hist_errors} errors)")
    print(f"  TOTAL:     {grand_fc_rows + grand_hist_rows:,} rows")
    if grand_fc_errors + grand_hist_errors > 0:
        print(f"  ⚠️  {grand_fc_errors + grand_hist_errors} chunks failed — re-run to retry")

