"""
Download SF sale price data from public sources:
1. DataSF "Real Property Transactions" (transfer tax records with sale prices)
2. SF Recorder's Office deed data

Usage: modal run scripts/download_sf_sales.py
"""
import modal, os

app = modal.App("download-sf-sales")
image = modal.Image.debian_slim(python_version="3.11").pip_install(
    "google-cloud-storage", "requests", "pandas", "pyarrow"
)
gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])


@app.function(image=image, secrets=[gcs_secret], timeout=1800, memory=8192)
def download_and_upload() -> str:
    """Download SF sale price data and upload to GCS."""
    import json, requests, pandas as pd, io
    from google.cloud import storage

    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")
    results = {}

    # 1. DataSF Real Property Transactions (Assessor-Recorder)
    # https://data.sfgov.org/Housing-and-Buildings/Real-Property-Tax-Transfers/5dah-bh6a
    # This dataset has actual sale prices from transfer tax declarations
    print("[SF-SALES] Downloading Real Property Tax Transfers from DataSF...")
    datasf_url = "https://data.sfgov.org/api/views/5dah-bh6a/rows.csv?accessType=DOWNLOAD"
    try:
        resp = requests.get(datasf_url, timeout=300)
        resp.raise_for_status()
        gcs_path = "sf/real_property_transfers.csv"
        blob = bucket.blob(gcs_path)
        blob.upload_from_string(resp.content, content_type="text/csv")
        
        df = pd.read_csv(io.BytesIO(resp.content), nrows=1000, low_memory=False)
        results["transfers"] = {
            "status": "ok",
            "size_mb": round(len(resp.content) / 1e6, 1),
            "rows_total": resp.content.count(b'\n'),
            "columns": list(df.columns),
            "gcs_path": gcs_path,
        }
        # Show value columns
        val_cols = [c for c in df.columns if any(kw in c.lower() for kw in 
            ["price", "value", "amount", "consider", "sale"])]
        results["transfers"]["value_columns"] = val_cols
        if val_cols:
            results["transfers"]["value_samples"] = df[val_cols].head(5).to_dict(orient="records")
        print(f"  -> {results['transfers']['size_mb']}MB, {len(df.columns)} cols")
        print(f"  Value columns: {val_cols}")
        print(f"  All columns: {list(df.columns)}")
    except Exception as e:
        results["transfers"] = {"status": "error", "error": str(e)}
        print(f"  -> Error: {e}")

    # 2. DataSF Assessor Historical Secured Property Tax Rolls
    # https://data.sfgov.org/Housing-and-Buildings/Assessor-Historical-Secured-Property-Tax-Rolls/wv5m-vpq2
    # This is the same secured roll data but may have more years
    print("\n[SF-SALES] Downloading Historical Secured Property Tax Rolls...")
    hist_url = "https://data.sfgov.org/api/views/wv5m-vpq2/rows.csv?accessType=DOWNLOAD"
    try:
        resp = requests.get(hist_url, timeout=600)
        resp.raise_for_status()
        gcs_path = "sf/assessor_historical_rolls.csv"
        blob = bucket.blob(gcs_path)
        blob.upload_from_string(resp.content, content_type="text/csv")
        
        df = pd.read_csv(io.BytesIO(resp.content), nrows=100, low_memory=False)
        results["historical_rolls"] = {
            "status": "ok",
            "size_mb": round(len(resp.content) / 1e6, 1),
            "columns": list(df.columns),
            "gcs_path": gcs_path,
        }
        print(f"  -> {results['historical_rolls']['size_mb']}MB, {len(df.columns)} cols")
    except Exception as e:
        results["historical_rolls"] = {"status": "error", "error": str(e)}
        print(f"  -> Error: {e}")

    # 3. SF Recorder deed records  
    # https://data.sfgov.org/City-Management-and-Ethics/Recorded-Documents/dtcx-m9ts
    print("\n[SF-SALES] Downloading Recorded Documents (deeds)...")
    deeds_url = "https://data.sfgov.org/api/views/dtcx-m9ts/rows.csv?accessType=DOWNLOAD"
    try:
        resp = requests.get(deeds_url, timeout=600)
        resp.raise_for_status()
        gcs_path = "sf/recorder_documents.csv"
        blob = bucket.blob(gcs_path)
        blob.upload_from_string(resp.content, content_type="text/csv")
        
        df = pd.read_csv(io.BytesIO(resp.content), nrows=100, low_memory=False)
        results["recorder_deeds"] = {
            "status": "ok",
            "size_mb": round(len(resp.content) / 1e6, 1),
            "columns": list(df.columns),
            "gcs_path": gcs_path,
        }
        # Check for price/value columns
        val_cols = [c for c in df.columns if any(kw in c.lower() for kw in 
            ["price", "value", "amount", "consider"])]
        results["recorder_deeds"]["value_columns"] = val_cols
        print(f"  -> {results['recorder_deeds']['size_mb']}MB, {len(df.columns)} cols")
        print(f"  Value columns: {val_cols}")
    except Exception as e:
        results["recorder_deeds"] = {"status": "error", "error": str(e)}
        print(f"  -> Error: {e}")

    return json.dumps(results, indent=2, default=str)


@app.local_entrypoint()
def main():
    result = download_and_upload.remote()
    with open("scripts/logs/sf_sales_download.json", "w") as f:
        f.write(result)
    print(f"\n[RESULT] {result}")
