"""Download the SF feature audit results from GCS."""
import modal, os

app = modal.App("download-audit")
image = modal.Image.debian_slim(python_version="3.11").pip_install("google-cloud-storage")
gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])

@app.function(image=image, secrets=[gcs_secret], timeout=30)
def download() -> str:
    import json
    from google.cloud import storage
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")
    blob = bucket.blob("diagnostics/sf_feature_audit.json")
    return blob.download_as_text()

@app.local_entrypoint()
def main():
    result = download.remote()
    # Write locally
    with open("scripts/logs/sf_feature_audit.json", "w") as f:
        f.write(result)
    print("Saved to scripts/logs/sf_feature_audit.json")
