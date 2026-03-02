"""
Copy checkpoint(s) from the Modal properlytic-checkpoints volume to GCS.
Run on dhardestylewis to export checkpoints so homecastr can use them.

Usage:
    modal run scripts/inference/upload/copy_ckpt_to_gcs.py --jurisdiction hcad_houston
"""
import modal, os, sys, json

_jur = "hcad_houston"
for i, a in enumerate(sys.argv):
    if a == "--jurisdiction" and i + 1 < len(sys.argv):
        _jur = sys.argv[i + 1]

app = modal.App("copy-ckpt-to-gcs")

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
    timeout=300,
)
def copy_to_gcs(jurisdiction: str) -> list:
    import glob, shutil
    from google.cloud import storage

    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")

    ckpt_dir = f"/checkpoints/{jurisdiction}_v11"
    pts = glob.glob(os.path.join(ckpt_dir, "*.pt"))
    if not pts:
        # Try without _v11 suffix
        ckpt_dir = f"/checkpoints/{jurisdiction}"
        pts = glob.glob(os.path.join(ckpt_dir, "*.pt"))

    if not pts:
        print(f"No .pt files found in /checkpoints/ for {jurisdiction}")
        # List what's there
        for dp, _, fnames in os.walk("/checkpoints"):
            for fn in fnames:
                print(f"  Found: {os.path.join(dp, fn)}")
        return []

    uploaded = []
    for pt in pts:
        fname = os.path.basename(pt)
        gcs_key = f"checkpoints/{jurisdiction}/{fname}"
        print(f"Uploading {pt} → gs://properlytic-raw-data/{gcs_key}")
        bucket.blob(gcs_key).upload_from_filename(pt)
        size_mb = os.path.getsize(pt) / 1e6
        print(f"  ✅ Uploaded {fname} ({size_mb:.1f} MB)")
        uploaded.append(gcs_key)
    return uploaded


@app.local_entrypoint()
def main(jurisdiction: str = "hcad_houston"):
    result = copy_to_gcs.remote(jurisdiction)
    if result:
        print(f"\n✅ Uploaded {len(result)} checkpoint(s) to GCS:")
        for r in result:
            print(f"  gs://properlytic-raw-data/{r}")
    else:
        print("\n⚠️  No checkpoints uploaded — check volume contents above.")
