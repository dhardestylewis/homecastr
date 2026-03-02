"""Check which jurisdiction panels exist on GCS."""
import modal, os

app = modal.App("check-panels")
image = modal.Image.debian_slim(python_version="3.11").pip_install("google-cloud-storage")
gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])

@app.function(image=image, secrets=[gcs_secret], timeout=60)
def check():
    import json
    from google.cloud import storage
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")

    jurisdictions = ["hcad_houston", "sf_ca", "cook_county_il", "nyc", "philly",
                     "uk_ppd", "france_dvf"]
    
    for j in jurisdictions:
        blob = bucket.blob(f"panel/jurisdiction={j}/part.parquet")
        if blob.exists():
            blob.reload()
            size_mb = blob.size / 1e6
            print(f"  OK: {j:20s} {size_mb:8.1f} MB")
        else:
            print(f"  MISSING: {j}")
    
    # Grand panel
    gp = bucket.blob("panel/grand_panel/part.parquet")
    if gp.exists():
        gp.reload()
        print(f"  OK: {'grand_panel':20s} {gp.size/1e6:8.1f} MB")
    else:
        print(f"  MISSING: grand_panel")
