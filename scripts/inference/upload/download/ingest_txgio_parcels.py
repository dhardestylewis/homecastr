"""
Transfer TxGIO Land Parcels from TNRIS S3 to GCS.

Downloads Texas statewide parcel geometry from the public TNRIS S3 bucket
and uploads to our GCS bucket. This enables extending our model to ALL
of Texas, not just Harris County.

Available years: 2019, 2021, 2022, 2023, 2024, 2025

S3 structure per year:
  LCD/collection/stratmap-{year}-land-parcels/
    fgdb/   - FileGDB format (per-county .gdb folders)
    shp/    - Shapefile format (per-county loose .shp/.dbf/.prj files)

Usage:
  # List available files for 2024
  python scripts/inference/ingest_txgio_parcels.py --list-only --years 2024

  # Transfer FGDB for 2024 and 2025 to GCS
  python scripts/inference/ingest_txgio_parcels.py --years 2024 2025

  # Transfer only Harris County (48201)
  python scripts/inference/ingest_txgio_parcels.py --years 2024 --county-fips 48201
"""

import os
import sys
import argparse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor

# ── Config ──
TNRIS_BUCKET = "tnris-data-warehouse"
GCS_BUCKET = os.environ.get("GCS_BUCKET", "properlytic-raw-data")
GCS_PREFIX = "geo/txgio_land_parcels"
AVAILABLE_YEARS = [2019, 2021, 2022, 2023, 2024, 2025]
S3_NS = "http://s3.amazonaws.com/doc/2006-03-01/"


def s3_list(prefix: str, delimiter: str = "", max_keys: int = 1000) -> dict:
    """List objects in public S3 bucket. Returns {keys: [...], prefixes: [...]}."""
    all_keys = []
    all_prefixes = []
    continuation = ""

    while True:
        url = (f"https://{TNRIS_BUCKET}.s3.us-east-1.amazonaws.com/"
               f"?list-type=2&prefix={prefix}&max-keys={max_keys}")
        if delimiter:
            url += f"&delimiter={delimiter}"
        if continuation:
            from urllib.parse import quote
            url += f"&continuation-token={quote(continuation, safe='')}"

        with urllib.request.urlopen(urllib.request.Request(url), timeout=30) as resp:
            root = ET.fromstring(resp.read())

        for c in root.findall(f".//{{{S3_NS}}}Contents"):
            k = c.find(f"{{{S3_NS}}}Key")
            s = c.find(f"{{{S3_NS}}}Size")
            if k is not None:
                all_keys.append({"key": k.text, "size": int(s.text) if s is not None else 0})

        for cp in root.findall(f".//{{{S3_NS}}}CommonPrefixes"):
            p = cp.find(f"{{{S3_NS}}}Prefix")
            if p is not None:
                all_prefixes.append(p.text)

        # Check for truncation
        is_truncated = root.find(f"{{{S3_NS}}}IsTruncated")
        if is_truncated is not None and is_truncated.text == "true":
            next_token = root.find(f"{{{S3_NS}}}NextContinuationToken")
            if next_token is not None:
                continuation = next_token.text
                continue
        break

    return {"keys": all_keys, "prefixes": all_prefixes}


def download_s3(key: str, dest_path: str):
    """Download a file from public S3."""
    url = f"https://{TNRIS_BUCKET}.s3.us-east-1.amazonaws.com/{key}"
    urllib.request.urlretrieve(url, dest_path)
    return os.path.getsize(dest_path)


def upload_gcs(local_path: str, gcs_key: str):
    """Upload a file to GCS."""
    from google.cloud import storage as gcs_storage
    client = gcs_storage.Client()
    blob = client.bucket(GCS_BUCKET).blob(gcs_key)
    blob.upload_from_filename(local_path)


def transfer_file(s3_key: str, gcs_key: str, tmpdir: str) -> tuple:
    """Download from S3 and upload to GCS. Returns (filename, size_mb, success)."""
    fname = os.path.basename(s3_key)
    tmp_path = os.path.join(tmpdir, fname)
    try:
        size = download_s3(s3_key, tmp_path)
        upload_gcs(tmp_path, gcs_key)
        return (fname, size / 1024 / 1024, True)
    except Exception as e:
        return (fname, 0, False, str(e))
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def process_year(year: int, county_fips: str = None, list_only: bool = False):
    """Process one year of TxGIO land parcels."""
    prefix = f"LCD/collection/stratmap-{year}-land-parcels/"

    print(f"\n{'='*60}")
    print(f"TxGIO Land Parcels {year}")
    print(f"  s3://{TNRIS_BUCKET}/{prefix}")
    print(f"{'='*60}")

    # List top-level structure
    result = s3_list(prefix, delimiter="/")
    print(f"  Subdirectories: {[p.split('/')[-2] for p in result['prefixes']]}")

    # Use SHP format (cleaner: ~8 files per county vs hundreds of tiny GDB internals)
    shp_prefix = f"{prefix}shp/"
    result = s3_list(shp_prefix)

    if not result["keys"]:
        # Fallback to FGDB
        print("  No SHP found, checking FGDB...")
        shp_prefix = f"{prefix}fgdb/"
        result = s3_list(shp_prefix)

    files = result["keys"]

    if county_fips:
        files = [f for f in files if county_fips in f["key"]]
        print(f"  Filtered to county {county_fips}: {len(files)} files")
    else:
        print(f"  Total files: {len(files)}")

    total_mb = sum(f["size"] for f in files) / (1024 * 1024)
    print(f"  Total size: {total_mb:,.0f} MB ({total_mb/1024:.1f} GB)")

    if list_only:
        # Show summary by county
        counties = {}
        for f in files:
            parts = os.path.basename(f["key"]).split("_")
            county_id = "_".join(parts[1:3]) if len(parts) >= 3 else "unknown"
            counties.setdefault(county_id, {"count": 0, "size": 0})
            counties[county_id]["count"] += 1
            counties[county_id]["size"] += f["size"]

        print(f"  Counties with data: {len(counties)}")
        for cid, info in sorted(counties.items())[:10]:
            print(f"    {cid}: {info['count']} files ({info['size']/1024/1024:.0f} MB)")
        if len(counties) > 10:
            print(f"    ... and {len(counties)-10} more")
        return

    # Transfer files
    import tempfile
    tmpdir = tempfile.mkdtemp()
    transferred = 0
    failed = 0

    for i, f in enumerate(files):
        s3_key = f["key"]
        fname = os.path.basename(s3_key)
        # Preserve folder structure in GCS
        rel_path = s3_key.replace(prefix, "")
        gcs_key = f"{GCS_PREFIX}/{year}/{rel_path}"

        try:
            size = download_s3(s3_key, os.path.join(tmpdir, fname))
            upload_gcs(os.path.join(tmpdir, fname), gcs_key)
            transferred += 1
            size_mb = size / 1024 / 1024
            if (i + 1) % 50 == 0 or size_mb > 100:
                print(f"  [{i+1}/{len(files)}] {fname} ({size_mb:.1f} MB)")
        except Exception as e:
            failed += 1
            print(f"  ❌ [{i+1}/{len(files)}] {fname}: {e}")
        finally:
            tmp = os.path.join(tmpdir, fname)
            if os.path.exists(tmp):
                os.remove(tmp)

    os.rmdir(tmpdir)
    print(f"\n  ✅ {year}: {transferred} transferred, {failed} failed")


def main():
    parser = argparse.ArgumentParser(description="Transfer TxGIO Land Parcels S3→GCS")
    parser.add_argument("--years", nargs="+", type=int, default=[2024],
                        help=f"Years to transfer (available: {AVAILABLE_YEARS})")
    parser.add_argument("--county-fips", type=str, default=None,
                        help="Filter to specific county FIPS (e.g. 48201 for Harris)")
    parser.add_argument("--list-only", action="store_true",
                        help="List available files without transferring")
    args = parser.parse_args()

    for year in args.years:
        if year not in AVAILABLE_YEARS:
            print(f"⚠️  Year {year} not available. Available: {AVAILABLE_YEARS}")
            continue
        process_year(year, args.county_fips, args.list_only)

    print("\n✅ Done!")


if __name__ == "__main__":
    main()
