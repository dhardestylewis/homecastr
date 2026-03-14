"""
migrate_gcs_to_r2.py — Copy properlytic-raw-data from GCS → Cloudflare R2
Uses google-cloud-storage (gcloud ADC) for reads, boto3 for R2 writes.
Run: python scripts/infra/migrate_gcs_to_r2.py [--prefix geo/] [--dry-run]

Required env vars: R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY
"""
import argparse, os, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage as gcs
import boto3
from botocore.config import Config

# ── R2 configuration (from environment) ──────────────────────────────────────
R2_ACCOUNT_ID      = os.environ["R2_ACCOUNT_ID"]
R2_ACCESS_KEY_ID   = os.environ["R2_ACCESS_KEY_ID"]
R2_SECRET_KEY      = os.environ["R2_SECRET_ACCESS_KEY"]
R2_ENDPOINT        = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
R2_BUCKET          = os.environ.get("R2_BUCKET", "properlytic-raw-data")
GCS_BUCKET         = "properlytic-raw-data"

# ── Clients ───────────────────────────────────────────────────────────────────
gcs_client = gcs.Client()
gcs_bucket = gcs_client.bucket(GCS_BUCKET)

s3 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_KEY,
    region_name="auto",
    config=Config(
        retries={"max_attempts": 3, "mode": "adaptive"},
        max_pool_connections=32,
    ),
)


def r2_key_exists(key: str) -> bool:
    """Check if a key already exists in R2 (skip already-copied files)."""
    try:
        s3.head_object(Bucket=R2_BUCKET, Key=key)
        return True
    except:
        return False


def copy_blob(blob_name: str, blob_size: int, dry_run: bool) -> tuple[str, bool, str]:
    """Download from GCS, upload to R2. Returns (name, success, message)."""
    if dry_run:
        return (blob_name, True, f"DRY-RUN  {blob_name} ({blob_size/1024/1024:.1f} MB)")

    # Skip if already in R2
    if r2_key_exists(blob_name):
        return (blob_name, True, f"SKIP     {blob_name} (already in R2)")

    try:
        blob = gcs_bucket.blob(blob_name)
        data = blob.download_as_bytes()
        content_type = blob.content_type or "application/octet-stream"

        s3.put_object(
            Bucket=R2_BUCKET,
            Key=blob_name,
            Body=data,
            ContentType=content_type,
        )
        return (blob_name, True, f"COPIED   {blob_name} ({len(data)/1024/1024:.1f} MB)")
    except Exception as e:
        return (blob_name, False, f"ERROR    {blob_name}: {e}")


def main():
    parser = argparse.ArgumentParser(description="Copy GCS → R2")
    parser.add_argument("--prefix", default="", help="Only copy objects with this prefix")
    parser.add_argument("--dry-run", action="store_true", help="List objects without copying")
    parser.add_argument("--workers", type=int, default=8, help="Parallel workers (default: 8)")
    parser.add_argument("--limit", type=int, default=0, help="Max objects to copy (0=all)")
    args = parser.parse_args()

    print(f"[LIST] GCS objects in gs://{GCS_BUCKET}/{args.prefix}...")
    blobs = list(gcs_client.list_blobs(GCS_BUCKET, prefix=args.prefix or None))
    total = len(blobs)
    total_bytes = sum(b.size or 0 for b in blobs)
    print(f"   Found {total:,} objects ({total_bytes/1024/1024/1024:.1f} GB)")

    if args.limit:
        blobs = blobs[:args.limit]
        print(f"   Limited to {len(blobs)} objects")

    if args.dry_run:
        print(f"\n[DRY RUN] No data will be transferred\n")
    else:
        print(f"\n[COPY] {len(blobs):,} objects with {args.workers} workers...\n")

    copied = 0
    skipped = 0
    errors = 0
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(copy_blob, b.name, b.size or 0, args.dry_run): b
            for b in blobs
        }
        for i, future in enumerate(as_completed(futures), 1):
            name, ok, msg = future.result()
            if ok:
                if "SKIP" in msg:
                    skipped += 1
                else:
                    copied += 1
            else:
                errors += 1
            if i % 50 == 0 or not ok:
                elapsed = time.time() - t0
                print(f"  [{i:,}/{len(blobs):,}] {elapsed:.0f}s — {msg}")

    elapsed = time.time() - t0
    print(f"\n[DONE] {elapsed:.0f}s")
    print(f"   Copied:  {copied:,}")
    print(f"   Skipped: {skipped:,}")
    print(f"   Errors:  {errors:,}")

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
