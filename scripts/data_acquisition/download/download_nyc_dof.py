"""
Download ALL NYC DOF Assessment Roll ZIPs (FY09-FY26) to GCS.
URLs extracted from https://www.nyc.gov/site/finance/property/property-assessment-roll-archives.page
Also downloads the record layout (data dictionary).
"""
import requests, zipfile, io, os, json, time
from google.cloud import storage

GCS_BUCKET = "properlytic-raw-data"
BASE = "https://www.nyc.gov/assets/finance/downloads/tar"
OUT = os.path.dirname(__file__)

# Exact URLs from the DOF archives page (naming varies by year)
DOWNLOADS = {
    # FY26 (latest final roll)
    "fy26_tc1": f"{BASE}/fy26_tc1.zip",
    "fy26_tc234": f"{BASE}/fy26_tc234.zip",
    "fy26_avroll1234": f"{BASE}/fy26_avroll1234.zip",
    "fy26_reuc": f"{BASE}/fy26_reuc.zip",
    # FY25
    "fy25_tc1": f"{BASE}/fy25_tc1.zip",
    "fy25_tc234": f"{BASE}/fy25_tc234.zip",
    "fy25_avroll1234": f"{BASE}/fy25_avroll1234.zip",
    "fy25_reuc": f"{BASE}/fy25_reuc.zip",
    # FY24
    "fy24_tc1": f"{BASE}/fy24_tc1.zip",
    "fy24_tc234": f"{BASE}/fy24_tc234.zip",
    "fy24_avroll1234": f"{BASE}/fy24_avroll1234.zip",
    "fy24_reuc": f"{BASE}/fy24_reuc.zip",
    # FY23 (different naming)
    "fy23_tc1": f"{BASE}/final_tc1_2023.zip",
    "fy23_tc234": f"{BASE}/final_tc234_2023.zip",
    "fy23_avroll1234": f"{BASE}/final_tc1234_2023.zip",
    "fy23_reuc": f"{BASE}/final_reuc_2023.zip",
    # FY22
    "fy22_tc1": f"{BASE}/tc1_22.zip",
    "fy22_tc234": f"{BASE}/tc234_22.zip",
    "fy22_tc2": f"{BASE}/tc2_22.zip",       # TC2 separate for FY22
    "fy22_reuc": f"{BASE}/reuc_22.zip",
    # FY21
    "fy21_tc1": f"{BASE}/tc1_21.zip",
    "fy21_tc234": f"{BASE}/tc234_21.zip",
    "fy21_reuc": f"{BASE}/reuc_21.zip",
    # FY20
    "fy20_tc1": f"{BASE}/tc1_20.zip",
    "fy20_tc24": f"{BASE}/tc24_20.zip",     # TC24 combined for FY20
    # FY19
    "fy19_tc1": f"{BASE}/tc1_19.zip",
    "fy19_tc234": f"{BASE}/tc234_19.zip",
    # FY18
    "fy18_tc1": f"{BASE}/tc1_18.zip",
    "fy18_tc234": f"{BASE}/tc234_18.zip",
    # FY17
    "fy17_tc1": f"{BASE}/tc1_17.zip",
    "fy17_tc234": f"{BASE}/tc234_17.zip",
    # FY16
    "fy16_tc1": f"{BASE}/tc1_16.zip",
    "fy16_tc234": f"{BASE}/tc234_16.zip",
    # FY15
    "fy15_tc1": f"{BASE}/tc1_15.zip",
    "fy15_tc234": f"{BASE}/tc234_15.zip",
    # FY14
    "fy14_tc1": f"{BASE}/tc1_14.zip",
    "fy14_tc234": f"{BASE}/tc234_14.zip",
    # FY13
    "fy13_tc1": f"{BASE}/tc1_13.zip",
    "fy13_tc234": f"{BASE}/tc234_13.zip",
    # FY12
    "fy12_tc1": f"{BASE}/tc1_12.zip",
    "fy12_tc234": f"{BASE}/tc234_12.zip",
    # FY11
    "fy11_tc1": f"{BASE}/tc1_11.zip",
    "fy11_tc234": f"{BASE}/tc234_11.zip",
    # FY10
    "fy10_tc1": f"{BASE}/tc1_10.zip",
    "fy10_tc234": f"{BASE}/tc234_10.zip",
    # FY09 (oldest, no year suffix)
    "fy09_tc1": f"{BASE}/tc1.zip",
    "fy09_tc234": f"{BASE}/tc234.zip",
}

# Also grab the record layout
EXTRAS = {
    "layout": f"{BASE}/layout-pts-property-master.xlsx",
    "data_dictionary": f"{BASE}/tarfieldcodes.pdf",
}


def inspect_sample():
    """Download FY26 TC1 locally and inspect contents."""
    print("=== Inspecting FY26 TC1 sample ===")
    url = DOWNLOADS["fy26_tc1"]
    try:
        r = requests.get(url, timeout=60)
        print(f"  Status: {r.status_code}, Size: {len(r.content)/1e6:.1f}MB")
        if r.status_code != 200:
            return

        z = zipfile.ZipFile(io.BytesIO(r.content))
        print(f"  Files: {z.namelist()}")

        for name in z.namelist():
            with z.open(name) as f:
                raw = f.read(8000)
                text = raw.decode('utf-8', errors='replace')
                lines = text.split('\n')
                print(f"\n  File: {name}")
                print(f"  First line ({len(lines[0])} chars): {lines[0][:300]}")
                if len(lines) > 1:
                    print(f"  Second line: {lines[1][:300]}")
                if len(lines) > 2:
                    print(f"  Third line: {lines[2][:300]}")

                # Try to detect delimiter
                for delim_name, delim in [('comma', ','), ('tab', '\t'), ('pipe', '|')]:
                    n = lines[0].count(delim)
                    if n > 5:
                        print(f"  Likely delimiter: {delim_name} ({n} occurrences)")
                        cols = lines[0].split(delim)
                        print(f"  Columns ({len(cols)}): {cols[:20]}")
                        break
                else:
                    # Fixed-width?
                    print(f"  May be fixed-width format")
                    print(f"  Line length: {len(lines[0])}")
            break
    except Exception as e:
        print(f"  Error: {e}")


def download_all():
    """Download all ZIPs to GCS."""
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)

    results = {}
    downloaded = 0
    skipped = 0
    failed = 0

    for name, url in DOWNLOADS.items():
        gcs_path = f"nyc/dof_assessment/{name}.zip"
        blob = bucket.blob(gcs_path)

        if blob.exists():
            blob.reload()
            size_mb = (blob.size or 0) / 1e6
            print(f"  {name}: cached ({size_mb:.1f}MB)")
            results[name] = {"status": "cached", "size_mb": round(size_mb, 1)}
            skipped += 1
            continue

        try:
            r = requests.get(url, stream=True, timeout=120)
            if r.status_code == 200:
                size = int(r.headers.get('Content-Length', 0))
                print(f"  {name}: downloading ({size/1e6:.0f}MB)...")
                blob.upload_from_file(r.raw, content_type='application/zip',
                                      size=size if size > 0 else None)
                blob.reload()
                actual = (blob.size or 0) / 1e6
                print(f"    -> {actual:.1f}MB")
                results[name] = {"status": "downloaded", "size_mb": round(actual, 1)}
                downloaded += 1
            else:
                print(f"  {name}: HTTP {r.status_code}")
                results[name] = {"status": f"http_{r.status_code}"}
                failed += 1
        except Exception as e:
            print(f"  {name}: {e}")
            results[name] = {"status": "error", "error": str(e)[:100]}
            failed += 1

    # Download extras (layout, data dict)
    for name, url in EXTRAS.items():
        ext = url.rsplit('.', 1)[-1]
        gcs_path = f"nyc/dof_assessment/{name}.{ext}"
        blob = bucket.blob(gcs_path)
        if not blob.exists():
            try:
                r = requests.get(url, timeout=30)
                if r.status_code == 200:
                    blob.upload_from_string(r.content)
                    print(f"  {name}: {len(r.content)/1024:.0f}KB")
            except Exception as e:
                print(f"  {name}: {e}")

    print(f"\nDone: {downloaded} downloaded, {skipped} cached, {failed} failed")
    return results


def main():
    print("=" * 60)
    print("NYC DOF ASSESSMENT ROLL — COMPLETE DOWNLOAD")
    print(f"Total files to download: {len(DOWNLOADS)} ZIPs + {len(EXTRAS)} extras")
    print("=" * 60)

    inspect_sample()

    print("\n" + "=" * 60)
    print("DOWNLOADING ALL TO GCS")
    print("=" * 60)

    results = download_all()

    with open(os.path.join(OUT, '..', 'nyc_dof_download_status.json'), 'w') as f:
        json.dump(results, f, indent=2)
    print("Saved: nyc_dof_download_status.json")


if __name__ == '__main__':
    main()
