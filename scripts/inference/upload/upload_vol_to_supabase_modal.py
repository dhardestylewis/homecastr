"""
upload_vol_to_supabase_modal.py
================================
Reads inference parquet chunks from the Modal volume ('inference-outputs')
and upserts them into Supabase using THREAD-LEVEL parallelism inside a
single container (avoids Modal autoscaler bottleneck).

Suites processed oldest→newest so most recent data wins.
Forecasts uploaded before history for faster map visibility.

Usage:
    python -m modal run scripts/inference/upload/upload_vol_to_supabase_modal.py \
        --jurisdiction hcad_houston --copy-to-gcs

    python -m modal run ... --jurisdiction hcad_houston --dry-run
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
# Heavy worker — processes ALL chunks for a suite using thread-level parallelism
# ─────────────────────────────────────────────────────────────────────────────
@app.function(
    image=image,
    secrets=[gcs_secret, supabase_secret],
    timeout=7200,
    memory=16384,
    volumes={"/output": output_vol},
)
def upload_suite(
    suite_id: str,
    fc_files: list,
    hist_files: list,
    jurisdiction: str,
    schema: str,
    copy_to_gcs: bool,
    vol_root: str,
    n_threads: int = 6,
) -> dict:
    """Upload all chunks for one suite using thread-pool parallelism."""
    import json, time, io
    import pandas as pd
    import psycopg2
    from psycopg2.extras import execute_values
    from datetime import datetime
    import urllib.parse as _up
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _ts():
        return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    print(f"[{_ts()}] ═══ SUITE {suite_id} ═══")
    print(f"[{_ts()}]   {len(fc_files)} forecast + {len(hist_files)} history chunks, {n_threads} threads")

    # ── DB URL with extended timeout ──────────────────────────────────────────
    db_url = os.environ["SUPABASE_DB_URL"]
    if "statement_timeout" not in db_url:
        sep = "&" if "?" in db_url else "?"
        db_url = db_url + sep + "options=" + _up.quote("-c statement_timeout=1200000")

    # ── GCS client ────────────────────────────────────────────────────────────
    gcs_bucket = None
    if copy_to_gcs:
        from google.cloud import storage
        creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
        gcs_bucket = storage.Client.from_service_account_info(creds).bucket(GCS_BUCKET)

    def _q(name: str) -> str:
        return f'"{name}"'

    def _upload_one(fpath: str, kind: str) -> dict:
        """Upload a single parquet chunk — runs in a thread."""
        fname = os.path.basename(fpath)
        t0 = time.time()

        try:
            df = pd.read_parquet(fpath)
        except Exception as e:
            print(f"[{_ts()}] ❌ {kind.upper()} {fname}: read error: {e}")
            return {"file": fname, "kind": kind, "rows": 0, "upserted": 0, "error": str(e)}

        n = len(df)
        if "jurisdiction" not in df.columns:
            df["jurisdiction"] = jurisdiction
        now_iso = datetime.utcnow().isoformat()
        if "updated_at" not in df.columns:
            df["updated_at"] = now_iso
        if "inserted_at" not in df.columns:
            df["inserted_at"] = now_iso

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
            conn = None
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
                if conn:
                    try: conn.close()
                    except: pass
                if attempt == max_retries - 1:
                    print(f"[{_ts()}] ❌ {kind.upper()} {fname} FAILED after {max_retries} retries: {e}")
                    return {"file": fname, "kind": kind, "rows": n, "upserted": 0, "error": str(e)}
                time.sleep(5 * (attempt + 1))

        elapsed = time.time() - t0
        print(f"[{_ts()}] ✅ {kind.upper()} {fname} → {upserted:,} rows ({elapsed:.1f}s)", flush=True)

        # Copy to GCS
        if gcs_bucket:
            try:
                rel = os.path.relpath(fpath, vol_root)
                gcs_key = f"inference_output/{jurisdiction}/{rel.replace(os.sep, '/')}"
                gcs_bucket.blob(gcs_key).upload_from_filename(fpath)
            except Exception as e:
                print(f"[{_ts()}] ⚠️ GCS {fname}: {e}", flush=True)

        return {"file": fname, "kind": kind, "rows": n, "upserted": upserted, "elapsed": round(elapsed, 1)}

    # ── Phase 1: Forecasts (parallel threads) ─────────────────────────────────
    fc_results = []
    if fc_files:
        print(f"\n[{_ts()}] 📈 FORECASTS: {len(fc_files)} chunks × {n_threads} threads")
        with ThreadPoolExecutor(max_workers=n_threads) as pool:
            futures = {pool.submit(_upload_one, f, "forecast"): f for f in fc_files}
            for fut in as_completed(futures):
                fc_results.append(fut.result())

    fc_rows = sum(r.get("upserted", 0) for r in fc_results)
    fc_errs = sum(1 for r in fc_results if r.get("error"))
    print(f"[{_ts()}] ✅ FORECASTS DONE: {fc_rows:,} rows ({fc_errs} errors)")

    # ── Phase 2: History (parallel threads) ───────────────────────────────────
    hist_results = []
    if hist_files:
        print(f"\n[{_ts()}] 📜 HISTORY: {len(hist_files)} chunks × {n_threads} threads")
        with ThreadPoolExecutor(max_workers=n_threads) as pool:
            futures = {pool.submit(_upload_one, f, "history"): f for f in hist_files}
            for fut in as_completed(futures):
                hist_results.append(fut.result())

    hist_rows = sum(r.get("upserted", 0) for r in hist_results)
    hist_errs = sum(1 for r in hist_results if r.get("error"))
    print(f"[{_ts()}] ✅ HISTORY DONE: {hist_rows:,} rows ({hist_errs} errors)")

    return {
        "suite_id": suite_id,
        "fc_rows": fc_rows, "fc_errs": fc_errs, "fc_files": len(fc_files),
        "hist_rows": hist_rows, "hist_errs": hist_errs, "hist_files": len(hist_files),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Scanner
# ─────────────────────────────────────────────────────────────────────────────
@app.function(image=image, timeout=120, memory=512, volumes={"/output": output_vol})
def scan_volume(jurisdiction: str) -> dict:
    """Scan the Modal volume and return parquet file paths organized by suite."""
    import glob
    from collections import defaultdict

    vol_root = f"/output/{jurisdiction}_inference"
    if not os.path.isdir(vol_root):
        avail = sorted(os.listdir("/output"))
        return {"error": f"Not found: {vol_root}", "available": avail, "vol_root": vol_root, "suites": {}}

    all_parquets = sorted(glob.glob(os.path.join(vol_root, "**/*.parquet"), recursive=True))

    suites = defaultdict(lambda: {"fc": [], "hist": []})
    for f in all_parquets:
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

    return {"vol_root": vol_root, "suites": dict(sorted(suites.items()))}


@app.local_entrypoint()
def main(
    jurisdiction: str = "hcad_houston",
    schema: str = "forecast_queue",
    dry_run: bool = False,
    copy_to_gcs: bool = False,
    n_threads: int = 6,
):
    """
    Upload inference results from Modal volume to Supabase.

    Suites processed oldest→newest (most recent wins upsert).
    Within each suite: forecasts first, then history.
    Parallelism via Python threads inside a single container (bypasses Modal autoscaler).
    """
    print(f"🚀 Volume-to-Supabase upload: {jurisdiction}")
    print(f"   schema={schema} dry_run={dry_run} copy_to_gcs={copy_to_gcs} threads={n_threads}")

    scan = scan_volume.remote(jurisdiction)

    if "error" in scan:
        print(f"❌ {scan['error']}")
        if "available" in scan:
            for d in scan["available"]:
                print(f"  {d}")
        return

    vol_root = scan["vol_root"]
    suites = scan["suites"]

    if not suites:
        print("⚠️  No suites found.")
        return

    total_fc = sum(len(s["fc"]) for s in suites.values())
    total_hist = sum(len(s["hist"]) for s in suites.values())

    print(f"\n📊 {len(suites)} suites (oldest → newest):")
    for i, (sid, files) in enumerate(suites.items()):
        print(f"   {i+1}. {sid}")
        print(f"      fc: {len(files['fc'])}  |  hist: {len(files['hist'])}")
    print(f"   TOTAL: {total_fc} fc + {total_hist} hist = {total_fc + total_hist} chunks")

    if dry_run:
        print("\n🔎 DRY RUN — done.")
        return

    # Process suites SEQUENTIALLY oldest→newest (1 container at a time, 6 threads each)
    # This keeps DB connections to ~6 max, well within Supabase pooler limits.
    results = []
    for suite_idx, (suite_id, files) in enumerate(suites.items()):
        print(f"\n📦 Suite {suite_idx+1}/{len(suites)}: {suite_id}")
        r = upload_suite.remote(
            suite_id, files["fc"], files["hist"],
            jurisdiction, schema, copy_to_gcs, vol_root, n_threads,
        )
        results.append(r)
        print(f"   ✅ fc={r['fc_rows']:,} hist={r['hist_rows']:,} errs={r['fc_errs']+r['hist_errs']}")

    # Grand summary
    grand_fc = sum(r["fc_rows"] for r in results)
    grand_hist = sum(r["hist_rows"] for r in results)
    grand_errs = sum(r["fc_errs"] + r["hist_errs"] for r in results)
    print(f"\n{'='*60}")
    print(f"🏁 GRAND TOTAL across {len(suites)} suites")
    print(f"{'='*60}")
    print(f"  Forecasts: {grand_fc:,} rows")
    print(f"  History:   {grand_hist:,} rows")
    print(f"  TOTAL:     {grand_fc + grand_hist:,} rows")
    if grand_errs > 0:
        print(f"  ⚠️  {grand_errs} chunks failed — re-run to retry")

