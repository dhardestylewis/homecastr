"""Check what raw data exists on GCS for all jurisdictions."""
import modal, os

app = modal.App("check-raw-data")
image = modal.Image.debian_slim(python_version="3.11").pip_install("google-cloud-storage")
gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])

@app.function(image=image, secrets=[gcs_secret], timeout=120)
def check() -> str:
    import json
    from google.cloud import storage
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")
    
    # List ALL top-level prefixes in the bucket
    results = {}
    seen_prefixes = set()
    for blob in bucket.list_blobs():
        prefix = blob.name.split("/")[0]
        if prefix not in seen_prefixes:
            seen_prefixes.add(prefix)
    
    # For each prefix, count files and total size
    for prefix in sorted(seen_prefixes):
        files = []
        total_bytes = 0
        for blob in bucket.list_blobs(prefix=f"{prefix}/", max_results=20):
            files.append({"name": blob.name, "size_mb": round(blob.size / 1e6, 1)})
            total_bytes += blob.size
        results[prefix] = {
            "file_count": len(files),
            "total_mb": round(total_bytes / 1e6, 1),
            "sample_files": [f["name"] for f in files[:5]]
        }
    
    return json.dumps(results, indent=2)

@app.local_entrypoint()
def main():
    result = check.remote()
    with open("scripts/logs/raw_data_inventory.json", "w") as f:
        f.write(result)
    print(result)
