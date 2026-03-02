"""Download HCAD Real_acct_owner.zip files and upload to GCS."""
import requests, os, tempfile
from google.cloud import storage

GCS_BUCKET = "properlytic-raw-data"
YEARS = list(range(2005, 2026))

# Correct HCAD URL pattern (from their download page HTML)
URL_PATTERNS = [
    "https://download.hcad.org/data/CAMA/{yr}/Real_acct_owner.zip",
]

def main():
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)

    # First, probe which URL pattern works
    working_pattern = None
    for pattern in URL_PATTERNS:
        url = pattern.format(yr=2025)
        print(f"Trying: {url}")
        try:
            r = requests.head(url, allow_redirects=True, timeout=10)
            print(f"  Status: {r.status_code}, Size: {r.headers.get('Content-Length', 'unknown')}")
            if r.status_code == 200:
                working_pattern = pattern
                break
        except Exception as e:
            print(f"  Error: {e}")

    if not working_pattern:
        # Try the main download page to discover links
        print("\nNo direct pattern worked. Checking download page...")
        r = requests.get("https://hcad.org/pdata/pdata-property-downloads.html", timeout=10)
        print(f"  Status: {r.status_code}")
        # Look for zip URLs in page
        import re
        urls = re.findall(r'https?://[^\s"]+Real_acct_owner\.zip', r.text)
        print(f"  Found URLs: {urls[:5]}")
        if urls:
            # Extract pattern
            working_pattern = urls[0].rsplit('/', 2)[0] + "/{yr}/" + urls[0].rsplit('/', 1)[1]

    if not working_pattern:
        print("\n❌ Could not find download URL pattern. Check https://hcad.org/pdata/ manually.")
        return

    print(f"\n✅ Working pattern: {working_pattern}")

    for yr in YEARS:
        url = working_pattern.format(yr=yr)
        gcs_path = f"hcad/owner/{yr}/Real_acct_owner.zip"

        # Check if already in GCS
        blob = bucket.blob(gcs_path)
        if blob.exists():
            print(f"\n{yr}: Already in GCS ({blob.size/1e6:.1f}MB)")
            continue

        print(f"\n{yr}: Downloading {url}...")
        try:
            r = requests.get(url, stream=True, timeout=60)
            if r.status_code != 200:
                print(f"  ❌ Status {r.status_code}")
                continue

            # Stream to temp file then upload
            tmp = os.path.join(tempfile.gettempdir(), f"hcad_owner_{yr}.zip")
            total = 0
            with open(tmp, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024*1024):
                    f.write(chunk)
                    total += len(chunk)
            print(f"  Downloaded: {total/1e6:.1f}MB")

            # Upload to GCS
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(tmp)
            print(f"  ✅ Uploaded to gs://{GCS_BUCKET}/{gcs_path}")
            os.remove(tmp)

        except Exception as e:
            print(f"  ❌ Error: {e}")

    print("\n✅ Done!")

if __name__ == "__main__":
    main()
