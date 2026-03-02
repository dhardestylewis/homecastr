"""
Download data for additional jurisdictions from public open data portals.
Covers: UK PPD, Maricopa AZ, Boston/Suffolk MA, Chicago (supplement),
        TXGIO bulk download, and more.

Usage: modal run scripts/download_more_jurisdictions.py
"""
import modal, os

app = modal.App("download-more-jurisdictions")
image = modal.Image.debian_slim(python_version="3.11").pip_install(
    "google-cloud-storage", "requests", "pandas", "pyarrow"
)
gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])


@app.function(image=image, secrets=[gcs_secret], timeout=3600, memory=16384)
def download_jurisdiction(name: str) -> str:
    """Download data for one jurisdiction."""
    import json, requests, pandas as pd, io
    from google.cloud import storage

    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")

    JURISDICTIONS = {
        "uk_ppd": {
            "desc": "UK Price Paid Data (Land Registry) — ALL sales since 1995",
            "url": "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv",
            "gcs_path": "uk_ppd/pp-complete.csv",
        },
        "boston_ma": {
            "desc": "Boston Property Assessment (annual, market value)",
            "url": "https://data.boston.gov/dataset/e02c44d2-3c64-459c-8fe2-e1ce5f38a035/resource/fd351943-c2c6-4630-992d-3f895360febd/download/ast2024full.csv",
            "gcs_path": "boston_ma/assessments_2024.csv",
        },
        "la_county": {
            "desc": "LA County Assessor — property characteristics",
            "url": "https://data.lacounty.gov/api/views/9trm-uz8i/rows.csv?accessType=DOWNLOAD",
            "gcs_path": "la_county/assessor_parcels.csv",
        },
        "seattle_wa": {
            "desc": "King County (Seattle) Assessor — Real Property Sales",
            "url": "https://aqua.kingcounty.gov/extranet/assessor/Real%20Property%20Sales.zip",
            "gcs_path": "seattle_wa/real_property_sales.zip",
        },
        "denver_co": {
            "desc": "Denver Property Assessment",
            "url": "https://data.denvergov.org/api/views/3tm8-jxpa/rows.csv?accessType=DOWNLOAD",
            "gcs_path": "denver_co/property_assessment.csv",
        },
        "portland_or": {
            "desc": "Multnomah County (Portland) Property Data",
            "url": "https://data.oregon.gov/api/views/u9cx-5fqn/rows.csv?accessType=DOWNLOAD",
            "gcs_path": "portland_or/property_data.csv",
        },
        "miami_fl": {
            "desc": "Miami-Dade Property Appraiser — annual values",
            "url": "https://opendata.arcgis.com/api/v3/datasets/0e5bcfcc6884465e98be47ad7db6fc49_0/downloads/data?format=csv&spatialRefId=4326",
            "gcs_path": "miami_fl/property_appraiser.csv",
        },
    }

    if name not in JURISDICTIONS:
        return json.dumps({"error": f"Unknown jurisdiction: {name}"})

    cfg = JURISDICTIONS[name]
    print(f"[{name}] {cfg['desc']}")
    print(f"  URL: {cfg['url']}")

    try:
        # Download
        resp = requests.get(cfg["url"], timeout=600, stream=True, 
                          headers={"User-Agent": "Mozilla/5.0 Properlytic/1.0"})
        resp.raise_for_status()
        content = resp.content
        
        # Upload to GCS
        blob = bucket.blob(cfg["gcs_path"])
        blob.upload_from_string(content)
        
        size_mb = round(len(content) / 1e6, 1)
        print(f"  Downloaded {size_mb}MB -> gs://properlytic-raw-data/{cfg['gcs_path']}")
        
        # Quick schema check if CSV
        result = {
            "status": "ok",
            "size_mb": size_mb,
            "gcs_path": cfg["gcs_path"],
        }
        
        if cfg["gcs_path"].endswith(".csv"):
            try:
                df = pd.read_csv(io.BytesIO(content), nrows=10, low_memory=False)
                result["columns"] = list(df.columns)
                result["rows_sample"] = len(content.split(b'\n'))
                val_cols = [c for c in df.columns if any(kw in c.lower() for kw in 
                    ["price", "value", "amount", "sale", "assess", "market", "apprais"])]
                result["value_columns"] = val_cols
                print(f"  Columns ({len(df.columns)}): {list(df.columns)[:15]}...")
                print(f"  Value columns: {val_cols}")
            except Exception as e:
                result["parse_note"] = str(e)
        
        return json.dumps(result, default=str)
    except Exception as e:
        print(f"  Error: {e}")
        return json.dumps({"status": "error", "error": str(e)})


@app.local_entrypoint()
def main():
    import json
    
    jurisdictions = [
        "uk_ppd",
        "boston_ma", 
        "la_county",
        "seattle_wa",
        "denver_co",
        "portland_or",
        "miami_fl",
    ]
    
    print(f"Downloading {len(jurisdictions)} jurisdictions in parallel...")
    results = list(download_jurisdiction.map(jurisdictions))
    
    combined = {}
    for jur, result in zip(jurisdictions, results):
        combined[jur] = json.loads(result)
        status = "✅" if combined[jur].get("status") == "ok" else "❌"
        size = combined[jur].get("size_mb", "?")
        print(f"  {status} {jur}: {size}MB")
    
    with open("scripts/logs/more_jurisdictions_download.json", "w") as f:
        json.dump(combined, f, indent=2)
    
    print(f"\nDone! Results saved to scripts/logs/more_jurisdictions_download.json")
