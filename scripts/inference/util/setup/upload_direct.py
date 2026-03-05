"""Direct upload script: reads GCS creds from Modal secret and uploads inference_pipeline.py."""
import os, sys, json
import modal

app = modal.App("upload-pipeline-code-v2")
image = modal.Image.debian_slim(python_version="3.11").pip_install("google-cloud-storage")
gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])

@app.function(image=image, secrets=[gcs_secret], timeout=120)
def upload_bytes(file_bytes: bytes, gcs_path: str):
    from google.cloud import storage as gcs
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = gcs.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(file_bytes)
    return f"✅ gs://properlytic-raw-data/{gcs_path}  ({len(file_bytes)/1024:.1f} KB)"


with app.run():
    here = os.path.dirname(os.path.abspath(__file__))
    # inference_pipeline.py is two levels up: util -> inference -> here (inference)
    inf_dir = os.path.dirname(here)  # scripts/inference
    fp = os.path.join(inf_dir, "inference_pipeline.py")
    with open(fp, "rb") as f:
        data = f.read()
    print(f"Uploading inference_pipeline.py ({len(data)/1024:.1f} KB)...")
    result = upload_bytes.remote(data, "code/inference_pipeline.py")
    print(result)
    print("Done.")
