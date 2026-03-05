"""Upload inference_pipeline.py to GCS code/ bucket via Modal (no local GCS key needed)."""
import modal, os, json

app = modal.App("upload-pipeline-code")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install("google-cloud-storage")
)
gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])

# upload_pipeline_code.py lives at scripts/inference/util/upload_pipeline_code.py
# so: util/ -> inference/ -> scripts/ — we need inference/ = one dirname up
_INFERENCE_DIR = os.path.dirname(os.path.abspath(__file__))  # .../scripts/inference/util
_INFERENCE_DIR = os.path.dirname(_INFERENCE_DIR)              # .../scripts/inference
_FILE_PATH = os.path.join(_INFERENCE_DIR, "inference_pipeline.py")

@app.function(image=image, secrets=[gcs_secret], timeout=120)
def upload_code(file_bytes: bytes, filename: str):
    from google.cloud import storage
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")
    gcs_path = f"code/{filename}"
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(file_bytes)
    size_kb = len(file_bytes) / 1024
    msg = f"✅ Uploaded gs://properlytic-raw-data/{gcs_path}  ({size_kb:.1f} KB)"
    print(msg)
    return msg


@app.local_entrypoint()
def main():
    with open(_FILE_PATH, "rb") as f:
        data = f.read()
    print(f"Uploading inference_pipeline.py ({len(data)/1024:.1f} KB) ...")
    result = upload_code.remote(data, "inference_pipeline.py")
    print(result)
    print("\nDone — Modal containers will pick up the new code on next inference run.")
