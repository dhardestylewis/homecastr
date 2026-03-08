"""
Quick diagnostic: compare nyc_panel_h3.parquet columns on GCS
vs checkpoint feature lists on Modal volume.
"""
import modal, os, io, json

app = modal.App("diag-nyc-panel")
image = modal.Image.debian_slim(python_version="3.11").pip_install(
    "google-cloud-storage", "pyarrow", "pandas", "torch", "numpy"
)

@app.function(
    image=image,
    secrets=[modal.Secret.from_name("gcs-creds")],
    volumes={"/output": modal.Volume.from_name("properlytic-checkpoints")},
    timeout=600,
    memory=16384,
)
def diagnose():
    import torch, numpy as np, pandas as pd, pyarrow.parquet as pq, sys
    from google.cloud import storage

    # Tee stdout to file
    log_path = "/output/diag_nyc_panel.log"
    class Tee:
        def __init__(self, *streams):
            self.streams = streams
        def write(self, data):
            for s in self.streams:
                s.write(data)
                s.flush()
        def flush(self):
            for s in self.streams:
                s.flush()
    log_file = open(log_path, "w")
    sys.stdout = Tee(sys.__stdout__, log_file)

    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")

    # ── 1. Inspect GCS panel ──
    panel_path = "panel/jurisdiction=nyc/nyc_panel_h3.parquet"
    blob = bucket.blob(panel_path)
    blob.reload()  # fetch metadata
    print(f"=== GCS Panel: {panel_path} ===")
    if not blob.exists():
        print("  ❌ FILE DOES NOT EXIST ON GCS")
        return
    size = blob.size or 0
    print(f"  Size: {size/1e6:.1f} MB")

    # Download to local file then read schema + sample
    local = "/tmp/nyc_panel.parquet"
    blob.download_to_filename(local)
    pf = pq.ParquetFile(local)
    schema = pf.schema_arrow
    print(f"  Schema ({len(schema)}): {schema.names}")
    print(f"  Num row groups: {pf.metadata.num_row_groups}")
    print(f"  Total rows: {pf.metadata.num_rows:,}")

    # Read just first 5 rows for sample
    df_sample = pf.read_row_group(0).to_pandas().head(5)
    print(f"  Sample (first 5 rows):\n{df_sample.to_string()}")

    panel_cols = set(schema.names)

    # Check key RPAD columns
    rpad_cols = ["BBL", "BORO", "BLOCK", "LOT", "BLDG_CLASS", "CURTAXCLASS",
                 "ZIP_CODE", "OWNER", "FY", "CURMKTTOT", "CURMKTLAND",
                 "lat", "lon", "h3_12", "parcel_id", "year", "total_appraised_value"]
    present = [c for c in rpad_cols if c in panel_cols]
    missing = [c for c in rpad_cols if c not in panel_cols]
    print(f"\n  Key columns PRESENT: {present}")
    print(f"  Key columns MISSING: {missing}")

    # ── 2. Inspect checkpoint feature lists ──
    ckpt_dirs = ["/output/nyc_v12sb", "/output/nyc_v11"]
    for ckpt_dir in ckpt_dirs:
        print(f"\n=== Checkpoint dir: {ckpt_dir} ===")
        if not os.path.isdir(ckpt_dir):
            print("  ❌ Directory not found")
            continue
        files = os.listdir(ckpt_dir)
        print(f"  Files: {files}")
        pt_files = sorted([f for f in files if f.endswith(".pt")])
        for pt in pt_files[:3]:
            path = os.path.join(ckpt_dir, pt)
            size_mb = os.path.getsize(path) / 1e6
            print(f"\n  ── {pt} ({size_mb:.1f} MB) ──")
            ckpt = torch.load(path, map_location="cpu", weights_only=False)
            num_use = ckpt.get("num_use", [])
            cat_use = ckpt.get("cat_use", [])
            cfg = ckpt.get("cfg", {})
            print(f"    num_use ({len(num_use)}): {num_use}")
            print(f"    cat_use ({len(cat_use)}): {cat_use}")
            print(f"    cfg keys: {list(cfg.keys())[:10]}")
            # Check which checkpoint features are in the panel
            all_feats = list(num_use) + list(cat_use)
            in_panel = [f for f in all_feats if f in panel_cols]
            not_in_panel = [f for f in all_feats if f not in panel_cols]
            print(f"    Features IN panel: {in_panel}")
            print(f"    Features NOT in panel: {not_in_panel}")

    log_file.close()
    sys.stdout = sys.__stdout__
    print(f"Diagnostic log saved to {log_path}")

@app.local_entrypoint()
def main():
    diagnose.remote()
