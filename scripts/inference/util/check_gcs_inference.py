"""Check GCS for inference output files."""
import modal, os, json

app = modal.App("check-gcs-inference")
image = modal.Image.debian_slim(python_version="3.11").pip_install("google-cloud-storage")
gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])

@app.function(image=image, secrets=[gcs_secret], timeout=60)
def check():
    from google.cloud import storage
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")
    results = []
    for prefix in ["inference_output/hcad_houston/", "inference_output/", "output/hcad_houston"]:
        blobs = list(bucket.list_blobs(prefix=prefix, max_results=30))
        for b in blobs:
            results.append(f"{b.name}  ({b.size/1e6:.1f} MB, {b.updated})")
    if not results:
        results.append("NO inference output found in GCS yet")
    return "\n".join(results)

@app.local_entrypoint()
def main():
    result = check.remote()
    # Write to local file to avoid Modal log truncation
    out_path = os.path.join(os.path.dirname(__file__), "gcs_check_result.txt")
    with open(out_path, "w") as f:
        f.write(result)
    print(result)
