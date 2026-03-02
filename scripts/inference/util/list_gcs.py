"""List GCS panel and checkpoint files."""
import modal, os, json

app = modal.App("list-gcs-panels")
image = modal.Image.debian_slim(python_version="3.11").pip_install("google-cloud-storage")
gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])

@app.function(image=image, secrets=[gcs_secret], timeout=60)
def list_files():
    from google.cloud import storage
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")
    results = []
    for prefix in ["panel/", "enriched_panels/", "checkpoints/"]:
        blobs = list(bucket.list_blobs(prefix=prefix, max_results=100))
        for b in blobs:
            results.append(f"{b.name}  ({b.size/1e6:.1f} MB)")
    return "\n".join(results)

@app.local_entrypoint()
def main():
    print(list_files.remote())
