"""Download only the LATEST MapPLUTO version per year to GCS.
Only need one snapshot per year for the temporal panel — earlier versions
within the same year are just bug fixes/corrections.
"""
import requests
from google.cloud import storage

GCS_BUCKET = "properlytic-raw-data"
BASE = "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/mappluto"

# Latest version per year only (from DCP archive JSON)
LATEST_PER_YEAR = {
    # 2025: 25v3.1 is latest
    "mappluto_25v3.1": f"{BASE}/nyc_mappluto_25v3_1_arc_fgdb.zip",
    # 2024: 24v4.1 is latest
    "mappluto_24v4.1": f"{BASE}/nyc_mappluto_24v4_1_arc_fgdb.zip",
    # 2023: 23v3.1 is latest
    "mappluto_23v3.1": f"{BASE}/nyc_mappluto_23v3_1_arc_fgdb.zip",
    # 2022: 22v3 is latest
    "mappluto_22v3": f"{BASE}/nyc_mappluto_22v3_arc_fgdb.zip",
    # 2021: 21v3 is latest
    "mappluto_21v3": f"{BASE}/nyc_mappluto_21v3_arc_fgdb.zip",
    # 2020: 20v8 is latest
    "mappluto_20v8": f"{BASE}/nyc_mappluto_20v8_arc_fgdb.zip",
    # 2019: 19v2 is latest
    "mappluto_19v2": f"{BASE}/nyc_mappluto_19v2_arc_fgdb.zip",
    # 2018: 18v2.1 is latest
    "mappluto_18v2.1": f"{BASE}/nyc_mappluto_18v2_1_arc_fgdb.zip",
    # 2017 and earlier: combined format (no _arc_fgdb suffix)
    "mappluto_17v1.1": f"{BASE}/mappluto_17v1_1.zip?r=2",
    "mappluto_16v2": f"{BASE}/mappluto_16v2.zip",
    "mappluto_15v1": f"{BASE}/mappluto_15v1.zip",
    "mappluto_14v2": f"{BASE}/mappluto_14v2.zip",
    "mappluto_13v2": f"{BASE}/mappluto_13v2.zip",
    "mappluto_12v2": f"{BASE}/mappluto_12v2.zip",
    "mappluto_11v2": f"{BASE}/mappluto_11v2.zip",
    "mappluto_10v2": f"{BASE}/mappluto_10v2.zip",
    "mappluto_09v2": f"{BASE}/mappluto_09v2.zip",
    "mappluto_08b": f"{BASE}/mappluto_08b.zip",
    "mappluto_07c": f"{BASE}/mappluto_07c.zip",
    "mappluto_06c": f"{BASE}/mappluto_06c.zip",
    "mappluto_05d": f"{BASE}/mappluto_05d.zip",
    "mappluto_04c": f"{BASE}/mappluto_04c.zip",
    "mappluto_03c": f"{BASE}/mappluto_03c.zip",
    "mappluto_02b": f"{BASE}/mappluto_02b.zip",
}


def main():
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)

    cached = 0
    downloaded = 0
    failed = 0

    print(f"Downloading {len(LATEST_PER_YEAR)} MapPLUTO releases (latest per year, 2002-2025)")
    print()

    for name, url in LATEST_PER_YEAR.items():
        gcs_path = f"nyc/mappluto/{name}.zip"
        blob = bucket.blob(gcs_path)

        if blob.exists():
            blob.reload()
            size_mb = (blob.size or 0) / 1e6
            print(f"  {name}: cached ({size_mb:.1f}MB)")
            cached += 1
            continue

        try:
            r = requests.get(url, stream=True, timeout=300)
            if r.status_code == 200:
                size = int(r.headers.get('Content-Length', 0))
                print(f"  {name}: downloading ({size//1000000}MB)...")
                blob.upload_from_file(r.raw, content_type='application/zip',
                                      size=size if size > 0 else None)
                blob.reload()
                print(f"    done ({(blob.size or 0)/1e6:.1f}MB)")
                downloaded += 1
            else:
                print(f"  {name}: HTTP {r.status_code}")
                failed += 1
        except Exception as e:
            print(f"  {name}: ERROR {str(e)[:80]}")
            failed += 1

    print(f"\nDone: {downloaded} downloaded, {cached} cached, {failed} failed")
    print(f"Total releases: {len(LATEST_PER_YEAR)}")


if __name__ == '__main__':
    main()
