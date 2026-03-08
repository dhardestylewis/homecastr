"""
Build tract name cache — lightweight version using raw HTTP.
No external dependencies required (no supabase, no pandas).

Resolves names for census tracts missing from the ZCTA crosswalk by:
1. Querying forecast tracts from Supabase REST API
2. Downloading Census Gazetteer for centroids
3. Calling Census geocoder API for place names
4. Saving results to tract-name-cache.json and Supabase tract_name_cache table

Usage:
    python scripts/data_acquisition/build/build_tract_name_cache.py [--state XX] [--dry-run] [--limit N]
"""

import argparse, json, os, sys, time, csv, io, zipfile, tempfile
import urllib.request, urllib.parse, urllib.error
from pathlib import Path

# ── Project paths ──────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[3]
ZCTA_CROSSWALK_PATH = PROJECT_ROOT / "lib" / "publishing" / "tract-zcta-crosswalk.json"
OUTPUT_CACHE_PATH = PROJECT_ROOT / "lib" / "publishing" / "tract-name-cache.json"

# ── Load .env.local ────────────────────────────────────────────────────────
def _load_env():
    env_path = PROJECT_ROOT / ".env.local"
    if env_path.exists():
        raw = env_path.read_bytes().decode("utf-8-sig", errors="replace")
        for line in raw.splitlines():
            line = line.strip().replace("\x00", "")
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                k = k.strip().replace("\x00", "")
                v = v.strip().strip('"').strip("'").replace("\x00", "")
                if k and v:
                    os.environ.setdefault(k, v)

_load_env()
SUPABASE_URL = os.environ.get("NEXT_PUBLIC_SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")


# ── Supabase REST helpers ──────────────────────────────────────────────────
def sb_get(table: str, params: dict) -> list:
    """GET from Supabase REST API."""
    qs = urllib.parse.urlencode(params)
    url = f"{SUPABASE_URL}/rest/v1/{table}?{qs}"
    req = urllib.request.Request(url, headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Accept": "application/json",
    })
    resp = urllib.request.urlopen(req, timeout=30)
    return json.loads(resp.read())


def sb_upsert(table: str, rows: list):
    """POST upsert to Supabase REST API."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    body = json.dumps(rows).encode()
    req = urllib.request.Request(url, data=body, method="POST", headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    })
    try:
        resp = urllib.request.urlopen(req, timeout=30)
        return resp.status
    except urllib.error.HTTPError as e:
        print(f"  WARN: upsert failed: {e.code} {e.read().decode()[:200]}")
        return e.code


def sb_get_schema(schema: str, table: str, params: dict) -> list:
    """GET from Supabase REST API with custom schema."""
    qs = urllib.parse.urlencode(params)
    url = f"{SUPABASE_URL}/rest/v1/{table}?{qs}"
    req = urllib.request.Request(url, headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Accept": "application/json",
        "Accept-Profile": schema,
    })
    resp = urllib.request.urlopen(req, timeout=60)
    return json.loads(resp.read())


# ── Census Gazetteer ───────────────────────────────────────────────────────
GAZETTEER_URL = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2020_Gazetteer/2020_Gaz_tracts_national.zip"

def download_gazetteer() -> dict:
    cache_path = Path(tempfile.gettempdir()) / "2020_Gaz_tracts_national.txt"
    if cache_path.exists() and cache_path.stat().st_size > 1_000_000:
        print(f"  Using cached gazetteer at {cache_path}")
        raw = cache_path.read_text(encoding="utf-8", errors="replace")
    else:
        print(f"  Downloading from {GAZETTEER_URL}...")
        resp = urllib.request.urlopen(GAZETTEER_URL, timeout=120)
        zdata = resp.read()
        with zipfile.ZipFile(io.BytesIO(zdata)) as zf:
            name = [n for n in zf.namelist() if n.endswith(".txt")][0]
            raw = zf.read(name).decode("utf-8", errors="replace")
            cache_path.write_text(raw, encoding="utf-8")
        print(f"  Cached to {cache_path}")

    centroids = {}
    reader = csv.DictReader(io.StringIO(raw), delimiter="\t")
    for row in reader:
        geoid = row.get("GEOID", "").strip()
        lat_s = row.get("INTPTLAT", "").strip()
        lng_s = row.get("INTPTLONG", "").strip()
        if geoid and lat_s and lng_s:
            try:
                centroids[geoid] = (float(lat_s), float(lng_s))
            except ValueError:
                pass
    print(f"  Loaded {len(centroids):,} tract centroids")
    return centroids


# ── Census Geocoder ────────────────────────────────────────────────────────
def geocode(lat: float, lng: float) -> str:
    """Reverse-geocode via Census Bureau (free, no key)."""
    params = urllib.parse.urlencode({
        "x": lng, "y": lat,
        "benchmark": "Public_AR_Current",
        "vintage": "Census2020_Current",
        "format": "json",
    })
    url = f"https://geocoding.geo.census.gov/geocoder/geographies/coordinates?{params}"
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Homecastr/1.0"})
            resp = urllib.request.urlopen(req, timeout=20)
            data = json.loads(resp.read())
            geos = data.get("result", {}).get("geographies", {})
            # Prefer: Incorporated Place > County Subdivision > County
            for key in ["Incorporated Places", "County Subdivisions", "Counties"]:
                items = geos.get(key, [])
                if items and items[0].get("BASENAME"):
                    return items[0]["BASENAME"]
            return ""
        except Exception as e:
            if attempt < 2:
                time.sleep(1 + attempt)
            else:
                print(f"    WARN: geocode failed ({lat:.4f}, {lng:.4f}): {e}")
                return ""
    return ""


# ── Main ───────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Build tract name cache")
    parser.add_argument("--state", help="State FIPS filter (e.g. 01)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--schema", default="forecast_queue")
    args = parser.parse_args()

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: Set NEXT_PUBLIC_SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY")
        sys.exit(1)

    # 1. Load ZCTA crosswalk
    print("Loading ZCTA crosswalk...")
    with open(ZCTA_CROSSWALK_PATH) as f:
        tract_zcta = json.load(f)
    print(f"  {len(tract_zcta):,} tracts in crosswalk")

    # 2. Get all forecast tracts
    print(f"Querying forecast tracts from {args.schema}...")
    filter_params = {
        "select": "tract_geoid20",
        "horizon_m": "eq.12",
        "series_kind": "eq.forecast",
        "limit": "100000",
    }
    if args.state:
        filter_params["tract_geoid20"] = f"like.{args.state}*"

    all_tracts_raw = sb_get_schema(args.schema, "metrics_tract_forecast", filter_params)
    all_tracts = sorted(set(r["tract_geoid20"] for r in all_tracts_raw))
    print(f"  {len(all_tracts):,} unique forecast tracts")

    # 3. Find tracts missing from crosswalk
    missing = [t for t in all_tracts if t not in tract_zcta]
    print(f"  {len(missing):,} tracts have no ZCTA match")
    if not missing:
        print("All tracts covered! Generating JSON...")
        _write_json({})
        return

    # 4. Load existing cache (if any)
    existing = {}
    if OUTPUT_CACHE_PATH.exists():
        with open(OUTPUT_CACHE_PATH) as f:
            existing = json.load(f)
    to_process = [t for t in missing if t not in existing]
    print(f"  {len(existing):,} already cached, {len(to_process):,} to resolve")

    if args.limit:
        to_process = to_process[:args.limit]

    if not to_process:
        print("Nothing new to resolve!")
        return

    # 5. Download gazetteer
    print("\nDownloading Census Gazetteer...")
    centroids = download_gazetteer()

    # 6. Resolve names
    print(f"\nResolving {len(to_process):,} tracts via Census geocoder...")
    new_entries = {}
    upsert_rows = []

    for idx, tid in enumerate(to_process):
        centroid = centroids.get(tid) or centroids.get(tid.rstrip("RrSs"))
        if not centroid:
            print(f"  [{idx+1}/{len(to_process)}] {tid} — no centroid, skip")
            new_entries[tid] = f"Tract {tid[5:]}"
            continue

        lat, lng = centroid
        name = geocode(lat, lng)
        if not name:
            name = f"Tract {tid[5:]}"
            source = "fallback"
        else:
            source = "census_geocoder"

        new_entries[tid] = name
        upsert_rows.append({
            "tract_geoid20": tid,
            "display_name": name,
            "lat": lat,
            "lng": lng,
            "source": source,
        })

        if (idx + 1) % 5 == 0 or idx == len(to_process) - 1:
            print(f"  [{idx+1}/{len(to_process)}] {tid} → {name} ({source})")

        time.sleep(0.3)  # Rate limit

    # 7. Write to Supabase
    if not args.dry_run and upsert_rows:
        print(f"\nUpserting {len(upsert_rows)} rows to tract_name_cache...")
        for i in range(0, len(upsert_rows), 50):
            status = sb_upsert("tract_name_cache", upsert_rows[i:i+50])
            print(f"  Chunk {i}: status={status}")
        print("  Done!")

    # 8. Write JSON
    merged = {**existing, **new_entries}
    _write_json(merged)

    # Summary
    sources = {}
    for r in upsert_rows:
        s = r["source"]
        sources[s] = sources.get(s, 0) + 1
    print(f"\nResolved {len(new_entries)} tracts:")
    for s, c in sorted(sources.items()):
        print(f"  {s}: {c}")


def _write_json(data: dict):
    with open(OUTPUT_CACHE_PATH, "w") as f:
        json.dump(data, f, separators=(",", ":"), sort_keys=True)
    print(f"Wrote {len(data):,} entries to {OUTPUT_CACHE_PATH}")


if __name__ == "__main__":
    main()
