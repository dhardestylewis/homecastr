"""
Export the v11 checkpoint from dhardestylewis Modal volume to GCS.
This makes it available for the homecastr workspace.

Run with: python -m modal run scripts/inference/upload/export_ckpt_dhardestylewis.py
(Must be run while dhardestylewis is the active profile OR with token env vars)
"""
import modal, os, json

app = modal.App("export-ckpt-dhardestylewis")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install("google-cloud-storage")
)

gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
ckpt_vol = modal.Volume.from_name("properlytic-checkpoints", create_if_missing=False)


@app.function(
    image=image,
    secrets=[gcs_secret],
    volumes={"/checkpoints": ckpt_vol},
    timeout=600,
    memory=4096,
)
def export_checkpoints(jurisdiction: str) -> list:
    """List all checkpoints in the volume and upload them to GCS."""
    import glob
    from google.cloud import storage

    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")

    uploaded = []
    print("=== Scanning /checkpoints volume ===")
    for dp, _, fnames in os.walk("/checkpoints"):
        for fn in fnames:
            if not fn.endswith(".pt"):
                continue
            fp = os.path.join(dp, fn)
            size_mb = os.path.getsize(fp) / 1e6
            # Derive GCS key from path
            # /checkpoints/hcad_houston_v11/ckpt_origin_2025_SF500K.pt
            # → checkpoints/hcad_houston/ckpt_origin_2025_SF500K.pt
            rel = os.path.relpath(fp, "/checkpoints")
            parts = rel.replace("\\", "/").split("/")
            # Strip _v11 suffix from dir name
            dir_name = parts[0].replace("_v11", "").replace("_v10", "").replace("_v9", "")
            fname_only = parts[-1]
            gcs_key = f"checkpoints/{dir_name}/{fname_only}"

            # Only upload for target jurisdiction
            if jurisdiction not in dir_name:
                print(f"  Skipping {fp} (not {jurisdiction})")
                continue

            print(f"  Uploading {fp} ({size_mb:.1f} MB) → {gcs_key}")
            bucket.blob(gcs_key).upload_from_filename(fp)
            uploaded.append({"gcs_key": gcs_key, "size_mb": round(size_mb, 1)})
            print(f"  ✅ Done")

    if not uploaded:
        print(f"\nNo checkpoints found for {jurisdiction}. Full volume listing:")
        for dp, _, fnames in os.walk("/checkpoints"):
            for fn in fnames:
                print(f"  {os.path.join(dp, fn)}")

    return uploaded


@app.local_entrypoint()
def main(jurisdiction: str = "hcad_houston"):
    print(f"Exporting {jurisdiction} checkpoints from Modal volume → GCS...")
    result = export_checkpoints.remote(jurisdiction)
    if result:
        print(f"\n✅ Exported {len(result)} checkpoint(s):")
        for r in result:
            print(f"  gs://properlytic-raw-data/{r['gcs_key']}  ({r['size_mb']} MB)")
    else:
        print("\n⚠️  Nothing was exported.")
