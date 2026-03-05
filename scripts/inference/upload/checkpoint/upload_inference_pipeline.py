"""Upload inference_pipeline.py to GCS."""
import modal, os, json

app = modal.App("upload-inference-pipeline")
image = modal.Image.debian_slim(python_version="3.11").pip_install("google-cloud-storage")
gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])

@app.function(image=image, secrets=[gcs_secret], timeout=60)
def upload(content: str) -> str:
    from google.cloud import storage
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")
    blob = bucket.blob("code/inference_pipeline.py")
    blob.upload_from_string(content.encode("utf-8"), content_type="text/x-python")
    return f"OK: {blob.size} bytes"

@app.local_entrypoint()
def main():
    path = os.path.join(os.path.dirname(__file__), "inference_pipeline.py")
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    print(f"[LOCAL] Read {len(content)} chars")
    result = upload.remote(content)
    print(f"[UPLOAD] {result}")
