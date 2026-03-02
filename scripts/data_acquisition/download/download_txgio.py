"""
Download Texas-wide property data from TXGIO (Texas Geographic Information Office).
TXGIO provides statewide parcel data with owner, value, land use, location for all 254 counties.
Texas mandates annual market-value reassessment, so assessed_value = market value.

Usage:
    modal run scripts/download_txgio.py
"""
import modal, os

app = modal.App("download-txgio")
image = modal.Image.debian_slim(python_version="3.11").pip_install(
    "google-cloud-storage", "requests", "pandas", "pyarrow"
)
gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])


@app.function(image=image, secrets=[gcs_secret], timeout=3600, memory=16384)
def download_and_upload() -> str:
    """Download TXGIO statewide parcel data and upload to GCS."""
    import json, requests, pandas as pd, io, zipfile
    from google.cloud import storage

    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")

    # TXGIO DataHub REST API endpoint for land parcels
    # The TXGIO Bulk Downloader uses this API:
    #   https://data.tnris.org/collection?c=...
    # Direct downloads are available per county or statewide
    
    # Step 1: Discover available datasets via TNRIS API
    print("[TXGIO] Querying TNRIS DataHub for land parcel collections...")
    api_url = "https://api.tnris.org/api/v1/collections"
    params = {"search": "land parcels", "limit": 50}
    
    try:
        resp = requests.get(api_url, params=params, timeout=30)
        resp.raise_for_status()
        collections = resp.json()
        
        results = {"collections": []}
        for item in collections.get("results", []):
            info = {
                "name": item.get("name"),
                "collection_id": item.get("collection_id"),
                "source": item.get("source_name"),
                "category": item.get("category"),
                "acquisition_date": item.get("acquisition_date"),
                "description": str(item.get("description", ""))[:200],
            }
            results["collections"].append(info)
            print(f"  Found: {info['name']} ({info['source']}) - {info['acquisition_date']}")
        
        # Step 2: For each parcel collection, get download links
        for coll in results["collections"]:
            cid = coll.get("collection_id")
            if cid:
                resources_url = f"https://api.tnris.org/api/v1/resources?collection_id={cid}&limit=50"
                try:
                    rresp = requests.get(resources_url, timeout=30)
                    rresp.raise_for_status()
                    resources = rresp.json()
                    coll["resources"] = []
                    for r in resources.get("results", []):
                        rinfo = {
                            "resource": r.get("resource"),
                            "area_type": r.get("area_type"),
                            "filesize": r.get("filesize"),
                        }
                        coll["resources"].append(rinfo)
                    print(f"    {cid}: {len(coll['resources'])} resources")
                except Exception as e:
                    print(f"    Error fetching resources for {cid}: {e}")
        
        # Save inventory to GCS
        inventory_json = json.dumps(results, indent=2)
        blob = bucket.blob("txgio/inventory.json")
        blob.upload_from_string(inventory_json, content_type="application/json")
        print(f"\n[TXGIO] Saved inventory to gs://properlytic-raw-data/txgio/inventory.json")
        
        # Step 3: Download actual parcel data files
        downloaded = 0
        for coll in results["collections"]:
            for res in coll.get("resources", []):
                url = res.get("resource")
                if url and (".zip" in url.lower() or ".csv" in url.lower() or ".geojson" in url.lower()):
                    fname = url.split("/")[-1]
                    gcs_path = f"txgio/{coll['collection_id']}/{fname}"
                    
                    # Check if already on GCS
                    existing = bucket.blob(gcs_path)
                    if existing.exists():
                        print(f"  [SKIP] {gcs_path} already exists")
                        continue
                    
                    print(f"  [DOWNLOAD] {url} -> {gcs_path}")
                    try:
                        dresp = requests.get(url, timeout=300, stream=True)
                        dresp.raise_for_status()
                        blob = bucket.blob(gcs_path)
                        blob.upload_from_string(dresp.content)
                        downloaded += 1
                        print(f"    Uploaded {len(dresp.content)/1e6:.1f}MB")
                    except Exception as e:
                        print(f"    Error: {e}")
        
        return json.dumps({
            "status": "ok",
            "collections_found": len(results["collections"]),
            "files_downloaded": downloaded,
        })
        
    except Exception as e:
        return json.dumps({"error": str(e)})


@app.local_entrypoint()
def main():
    result = download_and_upload.remote()
    print(f"\n[RESULT] {result}")
    with open("scripts/logs/txgio_download.json", "w") as f:
        f.write(result)
