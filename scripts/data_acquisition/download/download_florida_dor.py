"""
Download Florida DOR NAL, NAP, and SDF files and upload to GCS.
=================================================================
Source: Florida DOR PTO Data Portal (Public Records Request #20260226-1301654)
Target: gs://properlytic-raw-data/florida_dor/{NAL,NAP,SDF}/...

Three datasets:
  - NAL  (Name-Address-Legal):  2002F-2025S  (single zip per year+suffix)
  - NAP  (Name-Address-Parcel): 2008F-2025S  (per-county zips under year folders)
  - SDF  (Sale Data Files):     2009F-2025S  (single zip per year+suffix)

Usage:
  # Default: discover base URL interactively
  python download_florida_dor.py --base-url "https://DOMAIN" --gcs-bucket properlytic-raw-data

  # Dry-run to see all URLs:
  python download_florida_dor.py --base-url "https://DOMAIN" --dry-run
"""

import argparse
import os
import sys
import tempfile
import time
import requests
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed

# Optional GCS - will skip upload if not available
try:
    from google.cloud import storage as gcs_storage
    HAS_GCS = True
except ImportError:
    HAS_GCS = False

# ── Constants ─────────────────────────────────────────────────────────
PATH_PREFIX = "/property/dataportal/Documents/PTO Data Portal/~Public Records/~20260226-1301654"

GCS_PREFIX = "florida_dor"

# NAL: {year}{suffix}.zip  — years 2002-2025, suffixes vary
# 2002-2003: only F
# 2004+: F, P, S
NAL_YEARS_F_ONLY = list(range(2002, 2004))  # 2002, 2003
NAL_YEARS_FPS = list(range(2004, 2026))     # 2004-2025
NAL_SUFFIXES_FULL = ["F", "P", "S"]

# SDF: {year}{suffix}.zip — years 2009-2025, all have F, P, S
SDF_YEARS = list(range(2009, 2026))
SDF_SUFFIXES = ["F", "P", "S"]

# NAP: organized by year+suffix folders, each containing per-county zips
# From sample: 2008F/NAP{county_id}F{year}{version}.zip
# Counties range from 11 to 77 (Florida FIPS codes - 67 counties)
# Year folders to probe: 2008F through 2025S
NAP_YEARS = list(range(2008, 2026))  # discovered from sample data
NAP_SUFFIXES = ["F", "P", "S"]

# Florida county FIPS codes (01-67, but DOR uses their own numbering 11-77+)
# From the sample, we see counties 11 through 77
NAP_COUNTY_RANGE = list(range(11, 78))


def encode_url(base_url, rel_path):
    """Build full URL with proper percent-encoding of path components."""
    return f"{base_url}{quote(rel_path, safe='/:~')}"


def build_nal_urls():
    """Build list of (relative_url, gcs_path) for NAL files."""
    urls = []
    for yr in NAL_YEARS_F_ONLY:
        fname = f"{yr}F.zip"
        rel = f"{PATH_PREFIX}/NAL/{fname}"
        gcs = f"{GCS_PREFIX}/NAL/{fname}"
        urls.append((rel, gcs, fname))

    for yr in NAL_YEARS_FPS:
        for sfx in NAL_SUFFIXES_FULL:
            fname = f"{yr}{sfx}.zip"
            rel = f"{PATH_PREFIX}/NAL/{fname}"
            gcs = f"{GCS_PREFIX}/NAL/{fname}"
            urls.append((rel, gcs, fname))

    return urls


def build_sdf_urls():
    """Build list of (relative_url, gcs_path) for SDF files."""
    urls = []
    for yr in SDF_YEARS:
        for sfx in SDF_SUFFIXES:
            fname = f"{yr}{sfx}.zip"
            rel = f"{PATH_PREFIX}/SDF/{fname}"
            gcs = f"{GCS_PREFIX}/SDF/{fname}"
            urls.append((rel, gcs, fname))
    return urls


def discover_nap_folder(session, base_url, year, suffix):
    """Probe whether a NAP year folder exists by trying county 11 (first county)."""
    folder = f"{year}{suffix}"
    # Try a known pattern: NAP11{suffix}{year}01.zip
    probe_fname = f"NAP11{suffix}{year:04d}01.zip"
    probe_url = encode_url(base_url, f"{PATH_PREFIX}/NAP/{folder}/{probe_fname}")

    try:
        r = session.head(probe_url, allow_redirects=True, timeout=15)
        if r.status_code == 200:
            return folder
        # Try version 02
        probe_fname = f"NAP11{suffix}{year:04d}02.zip"
        probe_url = encode_url(base_url, f"{PATH_PREFIX}/NAP/{folder}/{probe_fname}")
        r = session.head(probe_url, allow_redirects=True, timeout=15)
        if r.status_code == 200:
            return folder
    except Exception:
        pass
    return None


def discover_nap_files_for_folder(session, base_url, folder, suffix, year):
    """
    Discover all NAP files in a given year folder by probing county numbers.
    Returns list of (relative_url, gcs_path, filename).
    """
    files = []
    for county in NAP_COUNTY_RANGE:
        # Try version suffixes 01, 02, 03 and VAB variants
        for version_suffix in ["01", "02", "03", "02VAB", "03VAB"]:
            fname = f"NAP{county:02d}{suffix}{year:04d}{version_suffix}.zip"
            rel = f"{PATH_PREFIX}/NAP/{folder}/{fname}"
            full_url = encode_url(base_url, rel)

            try:
                r = session.head(full_url, allow_redirects=True, timeout=10)
                if r.status_code == 200:
                    gcs = f"{GCS_PREFIX}/NAP/{folder}/{fname}"
                    files.append((rel, gcs, fname))
                    # Found this county, no need to try more versions
                    # Actually, some counties have both regular and VAB, so keep going
                    continue
            except Exception:
                continue

    return files


def download_and_upload(session, base_url, rel_url, gcs_path, fname,
                        bucket=None, tmp_dir=None, dry_run=False):
    """Download a file and upload to GCS. Returns (success, gcs_path, size_mb)."""
    full_url = encode_url(base_url, rel_url)

    if dry_run:
        print(f"  [DRY-RUN] {full_url} → gs://BUCKET/{gcs_path}")
        return True, gcs_path, 0

    # Check if already in GCS
    if bucket:
        blob = bucket.blob(gcs_path)
        if blob.exists():
            blob.reload()
            sz = (blob.size or 0) / 1e6
            print(f"  ✓ Already in GCS: {gcs_path} ({sz:.1f}MB)")
            return True, gcs_path, sz

    tmp_path = os.path.join(tmp_dir, fname)
    try:
        print(f"  ↓ Downloading {fname}...")
        r = session.get(full_url, stream=True, timeout=300)
        if r.status_code != 200:
            print(f"  ✗ HTTP {r.status_code} for {fname}")
            return False, gcs_path, 0

        total = 0
        with open(tmp_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
                total += len(chunk)

        size_mb = total / 1e6
        print(f"  ↓ Downloaded {fname}: {size_mb:.1f}MB")

        # Upload to GCS
        if bucket:
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(tmp_path)
            print(f"  ↑ Uploaded to gs://{bucket.name}/{gcs_path}")

        return True, gcs_path, size_mb

    except Exception as e:
        print(f"  ✗ Error {fname}: {e}")
        return False, gcs_path, 0
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except:
                pass


def main():
    parser = argparse.ArgumentParser(description="Download Florida DOR data to GCS")
    parser.add_argument("--base-url", required=True,
                        help="Base URL of the SharePoint site (e.g. https://floridarevenue.sharepoint.com)")
    parser.add_argument("--gcs-bucket", default="properlytic-raw-data",
                        help="GCS bucket name (default: properlytic-raw-data)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Just print URLs without downloading")
    parser.add_argument("--skip-nap-discovery", action="store_true",
                        help="Skip NAP probe (just download NAL + SDF)")
    parser.add_argument("--nap-only", action="store_true",
                        help="Only download NAP files")
    parser.add_argument("--nal-only", action="store_true",
                        help="Only download NAL files")
    parser.add_argument("--sdf-only", action="store_true",
                        help="Only download SDF files")
    parser.add_argument("--workers", type=int, default=3,
                        help="Number of parallel download workers (default: 3)")
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")

    # Setup GCS
    bucket = None
    if HAS_GCS and not args.dry_run:
        client = gcs_storage.Client()
        bucket = client.bucket(args.gcs_bucket)
        print(f"✅ GCS bucket: {args.gcs_bucket}")
    elif not args.dry_run:
        print("⚠️  google-cloud-storage not installed. Will download to /tmp only.")

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) properlytic-data-acquisition"
    })

    tmp_dir = tempfile.mkdtemp(prefix="florida_dor_")
    print(f"📁 Temp dir: {tmp_dir}")

    results = {"success": 0, "failed": 0, "skipped": 0, "total_mb": 0}

    # ──────────────────────────────────────────────────────────────
    # 1. NAL files
    # ──────────────────────────────────────────────────────────────
    if not args.nap_only and not args.sdf_only:
        nal_urls = build_nal_urls()
        print(f"\n{'='*60}")
        print(f"  NAL: {len(nal_urls)} files (2002F-2025S)")
        print(f"{'='*60}")

        for rel, gcs, fname in nal_urls:
            ok, _, sz = download_and_upload(
                session, base_url, rel, gcs, fname, bucket, tmp_dir, args.dry_run)
            if ok:
                results["success"] += 1
                results["total_mb"] += sz
            else:
                results["failed"] += 1

    # ──────────────────────────────────────────────────────────────
    # 2. SDF files
    # ──────────────────────────────────────────────────────────────
    if not args.nap_only and not args.nal_only:
        sdf_urls = build_sdf_urls()
        print(f"\n{'='*60}")
        print(f"  SDF: {len(sdf_urls)} files (2009F-2025S)")
        print(f"{'='*60}")

        for rel, gcs, fname in sdf_urls:
            ok, _, sz = download_and_upload(
                session, base_url, rel, gcs, fname, bucket, tmp_dir, args.dry_run)
            if ok:
                results["success"] += 1
                results["total_mb"] += sz
            else:
                results["failed"] += 1

    # ──────────────────────────────────────────────────────────────
    # 3. NAP files (discover year folders first)
    # ──────────────────────────────────────────────────────────────
    if not args.nal_only and not args.sdf_only and not args.skip_nap_discovery:
        print(f"\n{'='*60}")
        print(f"  NAP: Discovering year folders...")
        print(f"{'='*60}")

        discovered_folders = []
        for yr in NAP_YEARS:
            for sfx in NAP_SUFFIXES:
                if args.dry_run:
                    discovered_folders.append((f"{yr}{sfx}", sfx, yr))
                    continue
                folder = discover_nap_folder(session, base_url, yr, sfx)
                if folder:
                    print(f"  ✓ Found NAP folder: {folder}")
                    discovered_folders.append((folder, sfx, yr))
                else:
                    print(f"  · No NAP folder: {yr}{sfx}")

        print(f"\n  Found {len(discovered_folders)} NAP year folders")

        for folder, sfx, yr in discovered_folders:
            print(f"\n  --- NAP/{folder} ---")
            if args.dry_run:
                # In dry-run just show the pattern
                print(f"  [DRY-RUN] Would probe counties 11-77 for NAP{folder}")
                continue

            nap_files = discover_nap_files_for_folder(session, base_url, folder, sfx, yr)
            print(f"  Found {len(nap_files)} files in {folder}")

            for rel, gcs, fname in nap_files:
                ok, _, sz = download_and_upload(
                    session, base_url, rel, gcs, fname, bucket, tmp_dir, args.dry_run)
                if ok:
                    results["success"] += 1
                    results["total_mb"] += sz
                else:
                    results["failed"] += 1

    # ── Summary ───────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  SUMMARY")
    print(f"{'='*60}")
    print(f"  ✅ Success:  {results['success']}")
    print(f"  ❌ Failed:   {results['failed']}")
    print(f"  📦 Total:    {results['total_mb']:.1f} MB")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
