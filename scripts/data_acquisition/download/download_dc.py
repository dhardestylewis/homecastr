"""
Download DC (Washington) property data from Open Data DC.
DC reassesses annually to market value — good data for our model.

Usage:
    modal run scripts/download_dc.py
"""
import modal, os

app = modal.App("download-dc")
image = modal.Image.debian_slim(python_version="3.11").pip_install(
    "google-cloud-storage", "requests", "pandas", "pyarrow"
)
gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])


@app.function(image=image, secrets=[gcs_secret], timeout=1800, memory=8192)
def download_and_upload() -> str:
    """Download DC property data and upload to GCS."""
    import json, requests, pandas as pd, io
    from google.cloud import storage

    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")

    # DC Open Data: Computer Assisted Mass Appraisal (CAMA) dataset
    # https://opendata.dc.gov/datasets/computer-assisted-mass-appraisal-residential
    # Also: Integrated Tax System Public Extract
    
    sources = {
        "cama_residential": {
            "url": "https://opendata.dc.gov/api/download/v1/items/c5fb3fbe11c5461796a2960962ad9a3d/csv?layers=0",
            "desc": "CAMA Residential - property characteristics",
        },
        "cama_commercial": {
            "url": "https://opendata.dc.gov/api/download/v1/items/e53572ef8f124631b965709da8200c67/csv?layers=0",
            "desc": "CAMA Commercial",
        },
        "tax_assessment": {
            "url": "https://opendata.dc.gov/api/download/v1/items/014f4b4f94ea461498bfeba877d92319/csv?layers=0",
            "desc": "Integrated Tax System - assessed values",
        },
    }
    
    results = {}
    for name, src in sources.items():
        gcs_path = f"dc/{name}.csv"
        print(f"[DC] Downloading {name}: {src['desc']}")
        try:
            resp = requests.get(src["url"], timeout=300)
            resp.raise_for_status()
            blob = bucket.blob(gcs_path)
            blob.upload_from_string(resp.content, content_type="text/csv")
            
            # Quick stats
            df = pd.read_csv(io.BytesIO(resp.content), nrows=5, low_memory=False)
            results[name] = {
                "status": "ok",
                "size_mb": round(len(resp.content) / 1e6, 1),
                "columns": list(df.columns),
                "gcs_path": gcs_path,
            }
            print(f"  -> {results[name]['size_mb']}MB, {len(df.columns)} cols: {list(df.columns)[:10]}...")
        except Exception as e:
            results[name] = {"status": "error", "error": str(e)}
            print(f"  -> Error: {e}")
    
    return json.dumps(results, indent=2)


@app.local_entrypoint()
def main():
    result = download_and_upload.remote()
    print(f"\n[RESULT] {result}")
    with open("scripts/logs/dc_download.json", "w") as f:
        f.write(result)
