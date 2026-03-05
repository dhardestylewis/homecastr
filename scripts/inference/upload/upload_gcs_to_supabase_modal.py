"""
upload_gcs_to_supabase_modal.py
================================
Reads inference parquet chunks from GCS and upserts them into Supabase.

Target tables (in --schema, default forecast_queue):
  - metrics_parcel_history  (history_chunks/)
  - metrics_parcel_forecast (forecast_chunks/)

GCS key layout (set by the inference_modal_parallel.py watchdog):
  inference_output/{jurisdiction}/{suite_id}/production/
    forecast_origin_{year}_{run_id}/
      history_chunks/metrics_parcel_history_chunk_*.parquet
      forecast_chunks/metrics_parcel_forecast_chunk_*.parquet

Usage — upload a specific suite:
    modal run scripts/inference/upload/upload_gcs_to_supabase_modal.py \\
        --jurisdiction acs_nationwide \\
        --suite-id suite_20260304T221620Z_183790204e904e2c

Dry-run (print what would be uploaded, no DB writes):
    modal run ... --dry-run

Scope to a single run_id (one origin):
    modal run ... --suite-id <suite_id> --run-id-filter forecast_2024_20260304T221620Z_ea61f9aa8c88404e
"""
import modal
import os
import sys

# ── CLI arg parsing for app name ──────────────────────────────────────────────
_jur = "acs_nationwide"
_suite = "unknown"
for i, a in enumerate(sys.argv):
    if a == "--jurisdiction" and i + 1 < len(sys.argv):
        _jur = sys.argv[i + 1]
    if a == "--suite-id" and i + 1 < len(sys.argv):
        _suite = sys.argv[i + 1][:16]  # truncate for app name

app = modal.App(f"gcs-upload-{_jur}-{_suite}")

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


# ─────────────────────────────────────────────────────────────────────────────
# Upload worker — one Modal container per run_id directory
# ─────────────────────────────────────────────────────────────────────────────

@app.function(
    image=image,
    secrets=[gcs_secret, supabase_secret],
    timeout=3600,
    memory=8192,
)
def upload_run(
    jurisdiction: str,
    suite_id: str,
    run_prefix: str,   # GCS prefix for this run_id, e.g. inference_output/.../production/forecast_origin_2024_.../
    schema: str,
    dry_run: bool,
    run_id_filter: str,
):
    """
    Download and upsert all history_chunks + forecast_chunks parquets
    from a single run_id directory in GCS.
    """
    import json, io, time
    import pandas as pd
    import psycopg2
    from psycopg2.extras import execute_values
    from google.cloud import storage

    def _ts():
        from datetime import datetime
        return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # ── GCS client ────────────────────────────────────────────────────────────
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    gcs = storage.Client.from_service_account_info(creds)
    bucket = gcs.bucket(GCS_BUCKET)

    # ── DB connection ─────────────────────────────────────────────────────────
    db_url = os.environ["SUPABASE_DB_URL"]
    # Raise statement timeout to 20 min for large inserts
    import urllib.parse as _up
    if "statement_timeout" not in db_url:
        sep = "&" if "?" in db_url else "?"
        db_url = db_url + sep + "options=" + _up.quote("-c statement_timeout=1200000")

    conn = None
    if not dry_run:
        conn = psycopg2.connect(db_url)
        conn.autocommit = False

    def _q(name: str) -> str:
        return f'"{name}"'

    def _upsert(df: pd.DataFrame, table: str, conflict_cols: list, update_cols: list):
        if df is None or df.empty:
            return 0
        # Deduplicate on conflict key before insert
        ck = [c for c in conflict_cols if c in df.columns]
        if ck:
            df = df.drop_duplicates(subset=ck, keep="last")

        cols = list(df.columns)
        rows = [tuple(r) for r in df.itertuples(index=False, name=None)]
        if not rows:
            return 0

        col_sql = ", ".join(_q(c) for c in cols)
        conflict_sql = ", ".join(_q(c) for c in conflict_cols)
        update_sql = ", ".join(f"{_q(c)} = EXCLUDED.{_q(c)}" for c in update_cols)

        sql = f"""
            INSERT INTO "{schema}".{_q(table)} ({col_sql})
            VALUES %s
            ON CONFLICT ({conflict_sql})
            DO UPDATE SET {update_sql}
        """
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=PG_BATCH_ROWS)
        conn.commit()
        return len(rows)

    # ── List blobs under this run prefix ─────────────────────────────────────
    all_blobs = list(bucket.list_blobs(prefix=run_prefix))
    hist_blobs = [b for b in all_blobs if "/history_chunks/" in b.name and b.name.endswith(".parquet")]
    fc_blobs   = [b for b in all_blobs if "/forecast_chunks/" in b.name and b.name.endswith(".parquet")]

    # Apply run_id filter if requested
    if run_id_filter:
        hist_blobs = [b for b in hist_blobs if run_id_filter in b.name]
        fc_blobs   = [b for b in fc_blobs   if run_id_filter in b.name]

    print(f"[{_ts()}] run_prefix={run_prefix}")
    print(f"[{_ts()}]   history chunks : {len(hist_blobs)}")
    print(f"[{_ts()}]   forecast chunks: {len(fc_blobs)}")
    if dry_run:
        print(f"[{_ts()}] DRY RUN — no DB writes.")

    hist_rows_total = 0
    fc_rows_total = 0

    # ── Upload history chunks ─────────────────────────────────────────────────
    for idx, blob in enumerate(hist_blobs):
        t0 = time.time()
        data = blob.download_as_bytes()
        df = pd.read_parquet(io.BytesIO(data))

        # Ensure jurisdiction
        if "jurisdiction" not in df.columns:
            df["jurisdiction"] = jurisdiction

        # Ensure updated_at
        from datetime import datetime as _dt
        now_iso = _dt.utcnow().isoformat()
        if "updated_at" not in df.columns:
            df["updated_at"] = now_iso
        if "inserted_at" not in df.columns:
            df["inserted_at"] = now_iso

        n = len(df)
        print(f"[{_ts()}] [{idx+1}/{len(hist_blobs)}] HISTORY {blob.name.split('/')[-1]} → {n:,} rows", flush=True)
        print(f"           cols: {df.columns.tolist()}", flush=True)

        if not dry_run:
            upserted = _upsert(
                df=df,
                table="metrics_parcel_history",
                conflict_cols=["acct", "year", "series_kind", "variant_id"],
                update_cols=["value", "p50", "n", "run_id", "backtest_id",
                             "model_version", "as_of_date", "updated_at"],
            )
            hist_rows_total += upserted
            print(f"           ✅ upserted {upserted:,} rows ({time.time()-t0:.1f}s)", flush=True)

    # ── Upload forecast chunks ────────────────────────────────────────────────
    for idx, blob in enumerate(fc_blobs):
        t0 = time.time()
        data = blob.download_as_bytes()
        df = pd.read_parquet(io.BytesIO(data))

        if "jurisdiction" not in df.columns:
            df["jurisdiction"] = jurisdiction

        from datetime import datetime as _dt
        now_iso = _dt.utcnow().isoformat()
        if "updated_at" not in df.columns:
            df["updated_at"] = now_iso
        if "inserted_at" not in df.columns:
            df["inserted_at"] = now_iso

        n = len(df)
        print(f"[{_ts()}] [{idx+1}/{len(fc_blobs)}] FORECAST {blob.name.split('/')[-1]} → {n:,} rows", flush=True)
        print(f"           cols: {df.columns.tolist()}", flush=True)

        if not dry_run:
            # Determine whether the parquet has an "n" column (optional field)
            fc_update_cols = [
                "forecast_year", "value", "p10", "p25", "p50", "p75", "p90",
                "run_id", "backtest_id", "model_version", "as_of_date", "n_scenarios",
                "is_backtest", "updated_at"
            ]
            if "n" in df.columns:
                fc_update_cols.insert(7, "n")

            upserted = _upsert(
                df=df,
                table="metrics_parcel_forecast",
                conflict_cols=["acct", "origin_year", "horizon_m", "series_kind", "variant_id"],
                update_cols=fc_update_cols,
            )
            fc_rows_total += upserted
            print(f"           ✅ upserted {upserted:,} rows ({time.time()-t0:.1f}s)", flush=True)

    if conn:
        conn.close()

    return {
        "run_prefix": run_prefix,
        "hist_chunks": len(hist_blobs),
        "fc_chunks": len(fc_blobs),
        "hist_rows": hist_rows_total,
        "fc_rows": fc_rows_total,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Dispatcher — discovers all run_id dirs under the suite and fans out workers
# ─────────────────────────────────────────────────────────────────────────────

@app.local_entrypoint()
def main(
    jurisdiction: str = "acs_nationwide",
    suite_id: str = "",
    all_suites: bool = False,   # auto-discover ALL suites in GCS for this jurisdiction
    schema: str = "forecast_queue",
    dry_run: bool = False,
    run_id_filter: str = "",  # optional: only process chunks matching this run_id substring
):
    """
    Fan out one upload_run worker per run_id directory found under the suite(s).

    Examples:
        # Upload a specific suite
        modal run scripts/inference/upload/upload_gcs_to_supabase_modal.py \\
            --jurisdiction acs_nationwide \\
            --suite-id suite_20260304T221620Z_183790204e904e2c

        # Upload ALL suites found in GCS for this jurisdiction
        modal run scripts/inference/upload/upload_gcs_to_supabase_modal.py \\
            --jurisdiction acs_nationwide --all-suites

        # Dry-run
        modal run ... --all-suites --dry-run
    """
    if not suite_id and not all_suites:
        print("❌ Provide --suite-id <id> or --all-suites to upload everything.")
        return

    # Resolve suite IDs to process
    if all_suites:
        print(f"🔍 Discovering all suites under inference_output/{jurisdiction}/ ...")
        suite_ids = _list_all_suite_ids.remote(jurisdiction=jurisdiction)
        if not suite_ids:
            print("⚠️  No suites found in GCS.")
            return
        print(f"✅ Found {len(suite_ids)} suite(s):")
        for s in suite_ids:
            print(f"  {s}")
    else:
        suite_ids = [suite_id]

    all_results = []

    for sid in suite_ids:
        print(f"\n{'─'*60}")
        print(f"🔍 Scanning run dirs under suite_id={sid} ...")
        run_prefixes = _list_run_prefixes.remote(jurisdiction=jurisdiction, suite_id=sid)

        if not run_prefixes:
            print(f"  ⚠️  No run directories found — skipping.")
            continue

        print(f"  ✅ {len(run_prefixes)} run dir(s):")
        for p in run_prefixes:
            print(f"    {p.rstrip('/').split('/')[-1]}")

        if dry_run:
            print("  🔎 DRY RUN — no DB writes.")

        inputs = [
            (jurisdiction, sid, prefix, schema, dry_run, run_id_filter)
            for prefix in run_prefixes
        ]
        results = list(upload_run.starmap(inputs))
        all_results.extend(results)

        for r in results:
            if r:
                print(f"  {r['run_prefix'].rstrip('/').split('/')[-1]}")
                print(f"    history:  {r['hist_chunks']} chunks → {r['hist_rows']:,} rows")
                print(f"    forecast: {r['fc_chunks']} chunks → {r['fc_rows']:,} rows")

    print("\n" + "="*60)
    print("GRAND TOTAL")
    print("="*60)
    total_hist = sum(r["hist_rows"] for r in all_results if r)
    total_fc   = sum(r["fc_rows"]   for r in all_results if r)
    print(f"  {total_hist + total_fc:,} rows ({total_hist:,} history + {total_fc:,} forecast)")
    if dry_run:
        print("  (dry-run — nothing was written)")


@app.function(image=image, secrets=[gcs_secret], timeout=120, memory=512)
def _list_all_suite_ids(jurisdiction: str) -> list:
    """Return all suite IDs found under inference_output/{jurisdiction}/."""
    import json
    from google.cloud import storage

    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    gcs = storage.Client.from_service_account_info(creds)
    bucket = gcs.bucket(GCS_BUCKET)

    prefix = f"inference_output/{jurisdiction}/"
    blobs = list(bucket.list_blobs(prefix=prefix, max_results=10000))

    suite_ids = set()
    for b in blobs:
        rel = b.name[len(prefix):]   # e.g. suite_xxx/production/.../file.parquet
        parts = rel.split("/")
        if parts and parts[0].startswith("suite_"):
            suite_ids.add(parts[0])

    return sorted(suite_ids)


@app.function(image=image, secrets=[gcs_secret], timeout=120, memory=512)
def _list_run_prefixes(jurisdiction: str, suite_id: str) -> list:
    """Return the list of per-run_id GCS prefixes under a suite."""
    import json
    from google.cloud import storage

    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    gcs = storage.Client.from_service_account_info(creds)
    bucket = gcs.bucket(GCS_BUCKET)

    suite_prefix = f"inference_output/{jurisdiction}/{suite_id}/production/"
    blobs = list(bucket.list_blobs(prefix=suite_prefix))

    # Collect unique run-level prefixes (one level below production/)
    run_prefixes = set()
    for b in blobs:
        rel = b.name[len(suite_prefix):]  # e.g. forecast_origin_2024_.../history_chunks/file.parquet
        parts = rel.split("/")
        if len(parts) >= 2:
            run_prefixes.add(suite_prefix + parts[0] + "/")

    return sorted(run_prefixes)
