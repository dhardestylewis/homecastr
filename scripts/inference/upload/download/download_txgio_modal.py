"""
TxGIO Parcel Downloader — Modal + parallel file downloads.
Each year runs in its own container. Within each container,
files are downloaded concurrently with ThreadPoolExecutor.

Usage: python -m modal run scripts/inference/upload/download_txgio_modal.py --years 2019,2021,2022,2023,2024
"""
import modal
import os

app = modal.App("txgio-downloader")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install("google-cloud-storage")
)

gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])


@app.function(
    image=image,
    secrets=[gcs_secret],
    timeout=7200,
    memory=32768,
    cpu=4,
)
def download_one_year(year: int):
    """Download all TxGIO files for one year from S3 using parallel downloads."""
    import urllib.request
    import xml.etree.ElementTree as ET
    import json, time, zipfile, shutil
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading

    S3_BUCKET = "https://tnris-data-warehouse.s3.us-east-1.amazonaws.com"
    S3_PREFIX = f"LCD/collection/stratmap-{year}-land-parcels/"
    LOCAL_DIR = f"/tmp/txgio_{year}"
    GCS_PATH = f"txgio/stratmap{str(year)[2:]}-landparcels_48_lp.zip"
    WORKERS = 32  # concurrent download threads

    os.makedirs(LOCAL_DIR, exist_ok=True)

    # List ALL files
    print(f"[{year}] Listing files...")
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
    print(f"[{year}] Found {len(all_files)} files, {total_size/1e6:.0f} MB — downloading with {WORKERS} threads")

    # Parallel download
    counter = [0, 0, 0]  # [files_done, bytes_done, errors]
    lock = threading.Lock()
    t0 = time.time()

    def download_one(args):
        key, size = args
        rel_path = key[len(S3_PREFIX):]
        if not rel_path:
            return
        local_path = os.path.join(LOCAL_DIR, rel_path)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        try:
            urllib.request.urlretrieve(f"{S3_BUCKET}/{key}", local_path)
            with lock:
                counter[0] += 1
                counter[1] += size
        except Exception:
            with lock:
                counter[2] += 1

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(download_one, f): f for f in all_files}
        last_print = time.time()
        for future in as_completed(futures):
            future.result()  # propagate exceptions
            now = time.time()
            if now - last_print > 10:  # print every 10s
                with lock:
                    files_done, bytes_done, errors = counter
                elapsed = now - t0
                rate = bytes_done / elapsed / 1e6 if elapsed > 0 else 0
                eta = (total_size - bytes_done) / (rate * 1e6) / 60 if rate > 0 else 0
                print(f"[{year}] {files_done}/{len(all_files)} files, {bytes_done/1e6:.0f}/{total_size/1e6:.0f} MB ({rate:.1f}MB/s, ETA {eta:.0f}min, {errors} errors)")
                last_print = now

    elapsed = time.time() - t0
    with lock:
        files_done, bytes_done, errors = counter
    print(f"[{year}] ✅ Downloaded {bytes_done/1e6:.0f} MB in {elapsed:.0f}s ({bytes_done/elapsed/1e6:.1f} MB/s, {errors} errors)")

    # Zip
    print(f"[{year}] Zipping...")
    zip_path = f"/tmp/txgio_{year}.zip"
    t0 = time.time()
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for root_dir, dirs, files in os.walk(LOCAL_DIR):
            for f in files:
                full = os.path.join(root_dir, f)
                arcname = os.path.relpath(full, LOCAL_DIR)
                zf.write(full, arcname)
    zip_size = os.path.getsize(zip_path)
    print(f"[{year}] Zipped: {zip_size/1e6:.0f} MB in {time.time()-t0:.0f}s")

    # Upload to GCS
    print(f"[{year}] Uploading to gs://properlytic-raw-data/{GCS_PATH}")
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    from google.cloud import storage
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")
    blob = bucket.blob(GCS_PATH)
    t0 = time.time()
    blob.upload_from_filename(zip_path, timeout=3600)
    print(f"[{year}] ✅ Uploaded to GCS in {time.time()-t0:.0f}s")

    # Cleanup
    shutil.rmtree(LOCAL_DIR, ignore_errors=True)
    os.remove(zip_path)

    return {"year": year, "files": len(all_files), "size_mb": total_size/1e6, "errors": errors}


@app.local_entrypoint()
def main(years: str = "2019,2021,2022,2023,2024"):
    year_list = [int(y.strip()) for y in years.split(",")]
    print(f"🚀 Downloading TxGIO parcels for {len(year_list)} years on Modal (32 threads per container)")
    print(f"   Each year runs in its own container in AWS us-east-1\n")

    results = list(download_one_year.map(year_list))
    
    print(f"\n{'='*60}")
    print(f"✅ All downloads complete!")
    for r in results:
        print(f"  {r['year']}: {r['files']} files, {r['size_mb']:.0f} MB, {r['errors']} errors")
