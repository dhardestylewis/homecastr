"""Comprehensive status audit — queries GCS + Supabase, writes verified YAML."""
import yaml, psycopg2
from google.cloud import storage
from datetime import datetime

DB_URL = os.environ["SUPABASE_DB_URL"]
SCHEMA = "forecast_20260220_7f31c6e4"
GCS_BUCKET = "properlytic-raw-data"

def main():
    status = {"generated_at": datetime.now().isoformat()}

    # ── GCS ──
    print("Checking GCS...")
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    gcs = {}
    for prefix in ["hcad/", "nyc/", "sf/", "cook/", "france/", "seattle/", "wa/", "txgio/"]:
        blobs = list(bucket.list_blobs(prefix=prefix))
        if blobs:
            gcs[prefix.rstrip("/")] = {
                "file_count": len(blobs),
                "total_mb": round(sum(b.size for b in blobs) / 1e6, 1),
                "sample_files": [f"{b.name} ({b.size/1e6:.1f}MB)" for b in blobs[:5]],
            }
        else:
            gcs[prefix.rstrip("/")] = {"file_count": 0, "total_mb": 0}
    status["gcs"] = gcs

    # ── Supabase ──
    print("Checking Supabase...")
    supabase = {}
    try:
        conn = psycopg2.connect(DB_URL)
        conn.autocommit = True
        cur = conn.cursor()

        # Find all schemas with our tables
        cur.execute("""
            SELECT schemaname, tablename FROM pg_tables
            WHERE tablename IN ('metrics_parcel_forecast','metrics_tract_forecast',
                               'metrics_zcta_forecast','parcel_ladder_v1',
                               'tract_geometry','zcta_geometry')
            ORDER BY schemaname, tablename
        """)
        tables = cur.fetchall()
        supabase["tables_found"] = [f"{r[0]}.{r[1]}" for r in tables]

        # Count per jurisdiction in forecast schema
        cur.execute(f"SET search_path TO {SCHEMA}, public")
        for table in ["metrics_parcel_forecast", "metrics_tract_forecast", "metrics_zcta_forecast"]:
            try:
                cur.execute(f"SELECT jurisdiction, count(*) FROM {table} GROUP BY jurisdiction ORDER BY count DESC")
                rows = cur.fetchall()
                supabase[table] = {r[0]: r[1] for r in rows}
            except Exception as e:
                supabase[table] = f"ERROR: {e}"
                conn.rollback()

        # parcel_ladder_v1
        try:
            cur.execute("SELECT jurisdiction, count(*) FROM parcel_ladder_v1 GROUP BY jurisdiction ORDER BY count DESC")
            rows = cur.fetchall()
            supabase["parcel_ladder_v1"] = {r[0]: r[1] for r in rows}
        except Exception as e:
            supabase["parcel_ladder_v1"] = f"ERROR: {e}"

        conn.close()
    except Exception as e:
        supabase["connection_error"] = str(e)

    status["supabase"] = supabase

    # ── Local logs ──
    import os, json
    logs = {}
    log_dir = os.path.join(os.path.dirname(__file__), "..", "inference")
    for f in ["entity_backtest_model_vs.json", "entity_backtest_results.json", "entity_backtest_1to1.json"]:
        path = os.path.join(os.path.dirname(__file__), "..", "logs", f)
        if os.path.exists(path):
            try:
                with open(path) as fh:
                    data = json.load(fh)
                logs[f] = {"exists": True, "record_count": len(data)}
            except:
                logs[f] = {"exists": True, "parse_error": True}
        else:
            logs[f] = {"exists": False}

    # Training logs
    train_dir = os.path.join(os.path.dirname(__file__), "..", "logs", "train")
    if os.path.exists(train_dir):
        train_files = os.listdir(train_dir)
        jurisdictions = set(f.split("_o")[0] for f in train_files if "_o" in f)
        for j in sorted(jurisdictions):
            j_files = [f for f in train_files if f.startswith(j + "_")]
            logs[f"training_{j}"] = sorted(j_files)

    status["local_logs"] = logs

    # Write YAML
    out = os.path.join(os.path.dirname(__file__), "..", "logs", "status_audit.yaml")
    with open(out, "w") as f:
        yaml.dump(status, f, default_flow_style=False, sort_keys=False)
    print(f"\n✅ Written to {out}")
    print(yaml.dump(status, default_flow_style=False, sort_keys=False))

if __name__ == "__main__":
    main()
