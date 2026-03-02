"""Download TxGIO statewide Texas land parcels (2.8GB) and upload to GCS."""
import urllib.request
import os
import json
import time
from google.cloud import storage

S3_URL = "https://tnris-data-warehouse.s3.us-east-1.amazonaws.com/LCD/collection/stratmap-2025-land-parcels/stratmap25-landparcels_48_lp.zip"
LOCAL_PATH = "/tmp/txgio_land_parcels_48.zip"
GCS_PATH = "txgio/stratmap25-landparcels_48_lp.zip"

def progress_hook(count, block_size, total_size):
    pct = count * block_size * 100 / total_size
    mb = count * block_size / 1e6
    total_mb = total_size / 1e6
    if count % 500 == 0:
        print(f"  {mb:.0f}/{total_mb:.0f} MB ({pct:.1f}%)")

# Download
print(f"=== Downloading TxGIO statewide parcels ===")
print(f"  URL: {S3_URL}")
print(f"  Local: {LOCAL_PATH}")
t0 = time.time()
urllib.request.urlretrieve(S3_URL, LOCAL_PATH, reporthook=progress_hook)
elapsed = time.time() - t0
size_mb = os.path.getsize(LOCAL_PATH) / 1e6
print(f"\n  ✅ Downloaded {size_mb:.0f} MB in {elapsed:.0f}s ({size_mb/elapsed:.1f} MB/s)")

# Upload to GCS
print(f"\n=== Uploading to gs://properlytic-raw-data/{GCS_PATH} ===")
creds = json.load(open("scripts/.gcs-key.json"))
client = storage.Client.from_service_account_info(creds)
bucket = client.bucket("properlytic-raw-data")
blob = bucket.blob(GCS_PATH)
t0 = time.time()
blob.upload_from_filename(LOCAL_PATH, timeout=1800)
print(f"  ✅ Uploaded to GCS in {time.time()-t0:.0f}s")

# Clean up
os.remove(LOCAL_PATH)
print("\nDone!")
