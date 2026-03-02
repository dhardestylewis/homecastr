"""Download ALL NYC MapPLUTO releases (FGDB format, includes geometry) to GCS.
MapPLUTO = PLUTO + tax lot geometry. Goes back to 2002.
"""
import requests, os, tempfile
from google.cloud import storage

GCS_BUCKET = "properlytic-raw-data"

# MapPLUTO FGDB releases — URL pattern from NYC DCP
# https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/mappluto/
# Format: nyc_mappluto_{version}_fgdb.zip  (sometimes _unclipped)
# We want ALL versions going back as far as possible

BASE = "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes"

# Known releases (mapped from NYC DCP archives)
RELEASES = {
    # 2025
    "mappluto_25v3.1": f"{BASE}/mappluto/nyc_mappluto_25v3_1_fgdb.zip",
    "mappluto_25v3": f"{BASE}/mappluto/nyc_mappluto_25v3_fgdb.zip",
    "mappluto_25v2": f"{BASE}/mappluto/nyc_mappluto_25v2_fgdb.zip",
    "mappluto_25v1": f"{BASE}/mappluto/nyc_mappluto_25v1_fgdb.zip",
    # 2024
    "mappluto_24v4": f"{BASE}/mappluto/nyc_mappluto_24v4_fgdb.zip",
    "mappluto_24v3": f"{BASE}/mappluto/nyc_mappluto_24v3_fgdb.zip",
    "mappluto_24v2": f"{BASE}/mappluto/nyc_mappluto_24v2_fgdb.zip",
    "mappluto_24v1": f"{BASE}/mappluto/nyc_mappluto_24v1_fgdb.zip",
    # 2023
    "mappluto_23v3": f"{BASE}/mappluto/nyc_mappluto_23v3_fgdb.zip",
    "mappluto_23v2": f"{BASE}/mappluto/nyc_mappluto_23v2_fgdb.zip",
    "mappluto_23v1": f"{BASE}/mappluto/nyc_mappluto_23v1_fgdb.zip",
    # 2022
    "mappluto_22v3": f"{BASE}/mappluto/nyc_mappluto_22v3_fgdb.zip",
    "mappluto_22v2": f"{BASE}/mappluto/nyc_mappluto_22v2_fgdb.zip",
    "mappluto_22v1": f"{BASE}/mappluto/nyc_mappluto_22v1_fgdb.zip",
    # 2021
    "mappluto_21v3": f"{BASE}/mappluto/nyc_mappluto_21v3_fgdb.zip",
    "mappluto_21v2": f"{BASE}/mappluto/nyc_mappluto_21v2_fgdb.zip",
    "mappluto_21v1": f"{BASE}/mappluto/nyc_mappluto_21v1_fgdb.zip",
    # 2020
    "mappluto_20v8": f"{BASE}/mappluto/nyc_mappluto_20v8_fgdb.zip",
    "mappluto_20v1": f"{BASE}/mappluto/nyc_mappluto_20v1_fgdb.zip",
    # 2019-2002 (older naming conventions)
    "mappluto_19v2": f"{BASE}/mappluto/nyc_mappluto_19v2_fgdb.zip",
    "mappluto_19v1": f"{BASE}/mappluto/nyc_mappluto_19v1_fgdb.zip",
    "mappluto_18v2": f"{BASE}/mappluto/nyc_mappluto_18v2_fgdb.zip",
    "mappluto_18v1": f"{BASE}/mappluto/nyc_mappluto_18v1_fgdb.zip",
    "mappluto_17v1": f"{BASE}/mappluto/nyc_mappluto_17v1_fgdb.zip",
    "mappluto_16v2": f"{BASE}/mappluto/nyc_mappluto_16v2_fgdb.zip",
    "mappluto_16v1": f"{BASE}/mappluto/nyc_mappluto_16v1_fgdb.zip",
    "mappluto_15v1": f"{BASE}/mappluto/nyc_mappluto_15v1_fgdb.zip",
    "mappluto_14v2": f"{BASE}/mappluto/nyc_mappluto_14v2_fgdb.zip",
    "mappluto_14v1": f"{BASE}/mappluto/nyc_mappluto_14v1_fgdb.zip",
    "mappluto_13v2": f"{BASE}/mappluto/nyc_mappluto_13v2_fgdb.zip",
    "mappluto_13v1": f"{BASE}/mappluto/nyc_mappluto_13v1_fgdb.zip",
    "mappluto_12v2": f"{BASE}/mappluto/nyc_mappluto_12v2_fgdb.zip",
    "mappluto_12v1": f"{BASE}/mappluto/nyc_mappluto_12v1_fgdb.zip",
    "mappluto_11v2": f"{BASE}/mappluto/nyc_mappluto_11v2_fgdb.zip",
    "mappluto_11v1": f"{BASE}/mappluto/nyc_mappluto_11v1_fgdb.zip",
    "mappluto_10v1": f"{BASE}/mappluto/nyc_mappluto_10v1_fgdb.zip",
    "mappluto_09v2": f"{BASE}/mappluto/nyc_mappluto_09v2_fgdb.zip",
    "mappluto_09v1": f"{BASE}/mappluto/nyc_mappluto_09v1_fgdb.zip",
    # Very old — may use different URL pattern
    "mappluto_07c": f"{BASE}/mappluto/nyc_mappluto_07c_fgdb.zip",
    "mappluto_06c": f"{BASE}/mappluto/nyc_mappluto_06c_fgdb.zip",
    "mappluto_05d": f"{BASE}/mappluto/nyc_mappluto_05d_fgdb.zip",
    "mappluto_04c": f"{BASE}/mappluto/nyc_mappluto_04c_fgdb.zip",
    "mappluto_03c": f"{BASE}/mappluto/nyc_mappluto_03c_fgdb.zip",
    "mappluto_02a": f"{BASE}/mappluto/nyc_mappluto_02a_fgdb.zip",
}


def main():
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)

    downloaded = 0
    skipped = 0
    failed = 0

    for name, url in RELEASES.items():
        gcs_path = f"nyc/mappluto/{name}.zip"

        blob = bucket.blob(gcs_path)
        if blob.exists():
            blob.reload()
            print(f"  {name}: Already in GCS ({(blob.size or 0)/1e6:.1f}MB)")
            skipped += 1
            continue

        try:
            r = requests.get(url, stream=True, timeout=120)
            if r.status_code != 200:
                print(f"  {name}: ❌ Status {r.status_code}")
                failed += 1
                continue

            size = int(r.headers.get("Content-Length", 0))
            if size > 0 and size < 500000:
                print(f"  {name}: ❌ Too small ({size} bytes)")
                failed += 1
                continue

            print(f"  {name}: Streaming to GCS ({size/1e6:.0f}MB)...")
            blob.upload_from_file(r.raw, content_type="application/zip", size=size if size > 0 else None)
            blob.reload()
            print(f"    ✅ Done ({blob.size/1e6:.1f}MB)")
            downloaded += 1

        except Exception as e:
            print(f"  {name}: ❌ {e}")
            failed += 1

    print(f"\n✅ Done! Downloaded: {downloaded}, Skipped: {skipped}, Failed: {failed}")


if __name__ == "__main__":
    main()
