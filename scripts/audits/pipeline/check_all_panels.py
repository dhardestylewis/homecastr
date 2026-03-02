"""Check ALL jurisdiction panels on GCS and list what's available."""
import modal, os

app = modal.App("check-all-panels")
image = modal.Image.debian_slim(python_version="3.11").pip_install("google-cloud-storage")
gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])

@app.function(image=image, secrets=[gcs_secret], timeout=120)
def check() -> str:
    import json
    from google.cloud import storage
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")

    # List ALL blobs under panel/ and raw_data/
    results = {"panels": {}, "raw_data": [], "checkpoints": {}}
    
    # Check panels
    for blob in bucket.list_blobs(prefix="panel/jurisdiction="):
        jur = blob.name.split("jurisdiction=")[1].split("/")[0]
        if jur not in results["panels"]:
            results["panels"][jur] = {"files": [], "total_bytes": 0}
        results["panels"][jur]["files"].append(blob.name)
        results["panels"][jur]["total_bytes"] += blob.size
    
    # Check raw data sources
    for blob in bucket.list_blobs(prefix="raw_data/", delimiter="/"):
        pass  # just get prefixes
    prefixes = list(bucket.list_blobs(prefix="raw_data/", delimiter="/"))
    # List top-level dirs under raw_data/
    raw_dirs = set()
    for blob in bucket.list_blobs(prefix="raw_data/"):
        parts = blob.name.replace("raw_data/", "").split("/")
        if parts[0]:
            raw_dirs.add(parts[0])
    results["raw_data"] = sorted(raw_dirs)
    
    # Check checkpoints on Modal volume
    # List checkpoint blobs
    ckpt_dirs = set()
    for blob in bucket.list_blobs(prefix="checkpoints/"):
        parts = blob.name.replace("checkpoints/", "").split("/")
        if parts[0]:
            ckpt_dirs.add(parts[0])
    results["checkpoints_gcs"] = sorted(ckpt_dirs)
    
    return json.dumps(results, indent=2)

@app.local_entrypoint()
def main():
    result = check.remote()
    with open("scripts/logs/all_panels_status.json", "w") as f:
        f.write(result)
    print(result)
