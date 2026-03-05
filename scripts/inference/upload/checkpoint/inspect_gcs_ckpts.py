"""Inspect GCS checkpoint files for a jurisdiction."""
import modal, os, sys, json

app = modal.App("inspect-gcs-ckpts")
image = modal.Image.debian_slim(python_version="3.11").pip_install("google-cloud-storage", "torch")
gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
ckpt_vol = modal.Volume.from_name("properlytic-checkpoints", create_if_missing=False)

@app.function(image=image, secrets=[gcs_secret], volumes={"/checkpoints": ckpt_vol}, timeout=300, memory=8192)
def inspect(jurisdiction: str) -> dict:
    import glob, torch
    from google.cloud import storage

    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")

    result = {"gcs": [], "volume": []}

    # List GCS checkpoints
    print("=== GCS checkpoints ===")
    for blob in bucket.list_blobs(prefix=f"checkpoints/{jurisdiction}/"):
        if blob.name.endswith(".pt"):
            print(f"  {blob.name}  ({blob.size/1e6:.1f} MB)")
            result["gcs"].append({"name": blob.name, "size_mb": round(blob.size/1e6, 1)})

    # List volume checkpoints
    print("\n=== Volume checkpoints ===")
    for dp, _, fnames in os.walk("/checkpoints"):
        for fn in fnames:
            if fn.endswith(".pt"):
                fp = os.path.join(dp, fn)
                result["volume"].append(fp)
                print(f"  {fp}  ({os.path.getsize(fp)/1e6:.1f} MB)")

    # Try loading first GCS checkpoint to check its cfg
    if result["gcs"]:
        first_blob = result["gcs"][0]["name"]
        local = f"/tmp/check_{jurisdiction}.pt"
        print(f"\nDownloading {first_blob} to inspect cfg...")
        bucket.blob(first_blob).download_to_filename(local)
        ckpt = torch.load(local, map_location="cpu")
        cfg = ckpt.get("cfg", {})
        print(f"  cfg = {cfg}")
        result["cfg"] = cfg

    return result

@app.local_entrypoint()
def main(jurisdiction: str = "hcad_houston"):
    result = inspect.remote(jurisdiction)
    print(f"\n=== Summary ===")
    print(f"GCS: {len(result.get('gcs', []))} checkpoints")
    print(f"Volume: {len(result.get('volume', []))} checkpoints")
    cfg = result.get("cfg", {})
    if cfg:
        print(f"First checkpoint cfg: {cfg}")
