"""Generic TxGIO parcel downloader — pass year as CLI arg.
Usage: python download_txgio.py 2022
"""
import urllib.request
import xml.etree.ElementTree as ET
import os, json, time, zipfile, sys, shutil

YEAR = int(sys.argv[1]) if len(sys.argv) > 1 else 2022
S3_BUCKET = "https://tnris-data-warehouse.s3.us-east-1.amazonaws.com"
S3_PREFIX = f"LCD/collection/stratmap-{YEAR}-land-parcels/"
LOCAL_DIR = f"/tmp/txgio_{YEAR}"
GCS_PATH = f"txgio/stratmap{str(YEAR)[2:]}-landparcels_48_lp.zip"

os.makedirs(LOCAL_DIR, exist_ok=True)

# List ALL files
print(f"=== Listing TxGIO {YEAR} files ===")
all_files = []
marker = ""
while True:
    url = f"{S3_BUCKET}?prefix={S3_PREFIX}&max-keys=1000"
    if marker:
        url += f"&marker={marker}"
    data = urllib.request.urlopen(url).read()
    root = ET.fromstring(data)
    ns = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
    contents = root.findall('.//s3:Contents', ns)
    for c in contents:
        key = c.find('s3:Key', ns).text
        size = int(c.find('s3:Size', ns).text)
        if size > 0:
            all_files.append((key, size))
    is_truncated = root.find('.//s3:IsTruncated', ns)
    if is_truncated is not None and is_truncated.text == 'true':
        marker = all_files[-1][0]
    else:
        break

total_size = sum(f[1] for f in all_files)
print(f"  Found {len(all_files)} files, {total_size/1e6:.0f} MB total")

# Download all files
print(f"\n=== Downloading to {LOCAL_DIR} ===")
t0 = time.time()
downloaded = 0
for i, (key, size) in enumerate(all_files):
    rel_path = key[len(S3_PREFIX):]
    if not rel_path:
        continue
    local_path = os.path.join(LOCAL_DIR, rel_path)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    try:
        urllib.request.urlretrieve(f"{S3_BUCKET}/{key}", local_path)
    except Exception as e:
        print(f"  ERROR {rel_path}: {e}")
        continue
    downloaded += size
    if (i+1) % 100 == 0:
        elapsed = time.time() - t0
        rate = downloaded / elapsed / 1e6 if elapsed > 0 else 0
        eta = (total_size - downloaded) / (rate * 1e6) / 60 if rate > 0 else 0
        print(f"  {i+1}/{len(all_files)} files, {downloaded/1e6:.0f}/{total_size/1e6:.0f} MB ({rate:.1f}MB/s, ETA {eta:.0f}min)")

elapsed = time.time() - t0
print(f"\n  ✅ Downloaded {downloaded/1e6:.0f} MB in {elapsed:.0f}s ({downloaded/elapsed/1e6:.1f} MB/s)")

# Zip
print(f"\n=== Zipping ===")
zip_path = f"/tmp/txgio_{YEAR}.zip"
t0 = time.time()
with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
    for root_dir, dirs, files in os.walk(LOCAL_DIR):
        for f in files:
            full = os.path.join(root_dir, f)
            arcname = os.path.relpath(full, LOCAL_DIR)
            zf.write(full, arcname)
zip_size = os.path.getsize(zip_path)
print(f"  ✅ Zipped: {zip_size/1e6:.0f} MB in {time.time()-t0:.0f}s")

# Upload to GCS
print(f"\n=== Uploading to gs://properlytic-raw-data/{GCS_PATH} ===")
from google.cloud import storage
creds = json.load(open("scripts/.gcs-key.json"))
client = storage.Client.from_service_account_info(creds)
bucket = client.bucket("properlytic-raw-data")
blob = bucket.blob(GCS_PATH)
t0 = time.time()
blob.upload_from_filename(zip_path, timeout=3600)
print(f"  ✅ Uploaded to GCS in {time.time()-t0:.0f}s")

# Cleanup
shutil.rmtree(LOCAL_DIR, ignore_errors=True)
os.remove(zip_path)
print(f"\n✅ TxGIO {YEAR} done!")
