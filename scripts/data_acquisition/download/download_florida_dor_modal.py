"""
Download Florida DOR NAL, NAP, and SDF files to GCS via Modal.
================================================================
Fans out downloads in parallel using Modal containers for datacenter bandwidth.

Usage:
  python -m modal run --detach scripts/data_acquisition/download/download_florida_dor_modal.py

Source: Florida DOR PTO Data Portal (Public Records Request #20260226-1301654)
Target: gs://properlytic-raw-data/florida_dor/{NAL,NAP,SDF}/...
"""

import modal
import io
import json

app = modal.App("florida-dor-download")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install("requests", "google-cloud-storage")
)

# ── Constants ─────────────────────────────────────────────────────────
BASE_URL = "https://floridarevenue.com"
PATH_PREFIX = "/property/dataportal/Documents/PTO%20Data%20Portal/~Public%20Records/~20260226-1301654"
GCS_BUCKET = "properlytic-raw-data"
GCS_PREFIX = "florida_dor"


def build_all_jobs():
    """Build the complete list of (url, gcs_path, label) download jobs."""
    jobs = []

    # ── NAL: 2002-2025 ──────────────────────────────────────────
    # 2002-2003: only F
    for yr in range(2002, 2004):
        fname = f"{yr}F.zip"
        jobs.append((
            f"{BASE_URL}{PATH_PREFIX}/NAL/{fname}",
            f"{GCS_PREFIX}/NAL/{fname}",
            f"NAL/{fname}"
        ))
    # 2004-2025: F, P, S
    for yr in range(2004, 2026):
        for sfx in ["F", "P", "S"]:
            fname = f"{yr}{sfx}.zip"
            jobs.append((
                f"{BASE_URL}{PATH_PREFIX}/NAL/{fname}",
                f"{GCS_PREFIX}/NAL/{fname}",
                f"NAL/{fname}"
            ))

    # ── SDF: 2009-2025 ──────────────────────────────────────────
    for yr in range(2009, 2026):
        for sfx in ["F", "P", "S"]:
            fname = f"{yr}{sfx}.zip"
            jobs.append((
                f"{BASE_URL}{PATH_PREFIX}/SDF/{fname}",
                f"{GCS_PREFIX}/SDF/{fname}",
                f"SDF/{fname}"
            ))

    # ── NAP: per-county files under year folders ────────────────
    # Years 2008-2025, suffixes F/P/S, counties 11-77
    # We'll generate candidate URLs and let the worker skip 404s
    for yr in range(2008, 2026):
        for sfx in ["F", "P", "S"]:
            folder = f"{yr}{sfx}"
            for county in range(11, 78):
                # Try common version suffixes
                for ver in ["01", "02", "03"]:
                    fname = f"NAP{county:02d}{sfx}{yr:04d}{ver}.zip"
                    jobs.append((
                        f"{BASE_URL}{PATH_PREFIX}/NAP/{folder}/{fname}",
                        f"{GCS_PREFIX}/NAP/{folder}/{fname}",
                        f"NAP/{folder}/{fname}"
                    ))
                # Also try VAB variants
                for ver in ["02VAB", "03VAB"]:
                    fname = f"NAP{county:02d}{sfx}{yr:04d}{ver}.zip"
                    jobs.append((
                        f"{BASE_URL}{PATH_PREFIX}/NAP/{folder}/{fname}",
                        f"{GCS_PREFIX}/NAP/{folder}/{fname}",
                        f"NAP/{folder}/{fname}"
                    ))


    return jobs


gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])


@app.function(
    image=image,
    secrets=[gcs_secret],
    timeout=1800,  # 30 min per file (NAL files are 700MB+)
    max_containers=40,
)
def download_one(url: str, gcs_path: str, label: str) -> dict:
    """Download a single file from Florida DOR and upload to GCS."""
    import requests
    from google.cloud import storage
    import os, json, tempfile, time

    # Setup GCS
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket(GCS_BUCKET)

    # Check if already uploaded
    blob = bucket.blob(gcs_path)
    if blob.exists():
        blob.reload()
        sz = (blob.size or 0) / 1e6
        return {"status": "skipped", "label": label, "size_mb": sz, "reason": "already in GCS"}

    # Download
    t0 = time.time()
    try:
        r = requests.get(url, stream=True, timeout=600)
        if r.status_code == 404:
            return {"status": "not_found", "label": label, "size_mb": 0}
        if r.status_code != 200:
            return {"status": "error", "label": label, "size_mb": 0, "reason": f"HTTP {r.status_code}"}

        # Stream to temp file
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
        total = 0
        for chunk in r.iter_content(chunk_size=4 * 1024 * 1024):  # 4MB chunks
            tmp.write(chunk)
            total += len(chunk)
        tmp.close()

        dl_time = time.time() - t0
        size_mb = total / 1e6

        # Upload to GCS
        t1 = time.time()
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(tmp.name, timeout=1200)
        ul_time = time.time() - t1

        os.unlink(tmp.name)

        print(f"✅ {label}: {size_mb:.1f}MB (dl={dl_time:.0f}s, ul={ul_time:.0f}s)")
        return {"status": "ok", "label": label, "size_mb": size_mb,
                "dl_seconds": dl_time, "ul_seconds": ul_time}

    except Exception as e:
        return {"status": "error", "label": label, "size_mb": 0, "reason": str(e)[:200]}


def safe_starmap(fn, jobs, stats, phase_name, quiet_404=False):
    """Run starmap but catch per-item exceptions so one failure doesn't kill the batch."""
    try:
        for result in fn.starmap(jobs, return_exceptions=True):
            if isinstance(result, Exception):
                stats["error"] = stats.get("error", 0) + 1
                print(f"  ❌ [{phase_name}] Exception: {str(result)[:150]}")
                continue
            s = result["status"]
            stats[s] = stats.get(s, 0) + 1
            stats["total_mb"] += result.get("size_mb", 0)
            if s == "ok":
                print(f"  ✅ {result['label']}: {result['size_mb']:.1f}MB")
            elif s == "skipped":
                if not quiet_404:
                    print(f"  ⏭️  {result['label']}: already in GCS ({result['size_mb']:.1f}MB)")
            elif s == "error":
                print(f"  ❌ {result['label']}: {result.get('reason', '?')}")
            # Don't print 404s if quiet_404
    except Exception as e:
        print(f"  ⚠️  [{phase_name}] starmap crashed: {e}")
        stats["error"] = stats.get("error", 0) + 1


@app.local_entrypoint()
def main():
    """Fan out all downloads in parallel."""
    jobs = build_all_jobs()

    # Separate NAL+SDF (known to exist) from NAP (many will 404)
    nal_sdf = [j for j in jobs if not j[2].startswith("NAP/")]
    nap = [j for j in jobs if j[2].startswith("NAP/")]

    print(f"Total jobs: {len(jobs)}")
    print(f"  NAL+SDF: {len(nal_sdf)} (all should exist)")
    print(f"  NAP:     {len(nap)} candidates (many will be 404)")

    stats = {"ok": 0, "skipped": 0, "not_found": 0, "error": 0, "total_mb": 0}

    # ── Phase 1: NAL + SDF (guaranteed files) ────────────────────
    print(f"\n{'='*60}")
    print(f"  Phase 1: NAL + SDF ({len(nal_sdf)} files)")
    print(f"{'='*60}")

    safe_starmap(download_one, nal_sdf, stats, "Phase1")

    print(f"\n  Phase 1 done: {stats['ok']} uploaded, {stats['skipped']} skipped, {stats['error']} errors")

    # ── Phase 2: NAP (discovery via brute force) ─────────────────
    print(f"\n{'='*60}")
    print(f"  Phase 2: NAP ({len(nap)} candidates)")
    print(f"{'='*60}")

    nap_before = stats["ok"] + stats.get("skipped", 0)
    safe_starmap(download_one, nap, stats, "Phase2", quiet_404=True)
    nap_found = (stats["ok"] + stats.get("skipped", 0)) - nap_before

    print(f"\n  Phase 2 done: NAP files found={nap_found}")

    # ── Summary ──────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  FINAL SUMMARY")
    print(f"{'='*60}")
    print(f"  ✅ Uploaded:   {stats['ok']}")
    print(f"  ⏭️  Skipped:    {stats['skipped']}")
    print(f"  🔍 Not found:  {stats['not_found']}")
    print(f"  ❌ Errors:     {stats['error']}")
    print(f"  📦 Total data: {stats['total_mb']/1000:.1f} GB")
    print(f"{'='*60}")

