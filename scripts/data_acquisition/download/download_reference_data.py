#!/usr/bin/env python3
"""
download_reference_data.py
==========================
Downloads three missing US-wide reference datasets and uploads them to GCS/Supabase:

  1. ACS 5-year B25077 (Median Owner-Specified Home Value)
     — Block group level, 2018-2023
     — GCS: census/ACS_B25077_{year}.zip

  2. FHFA HPI (House Price Index)
     — State, MSA, and 3-digit ZIP levels
     — GCS: fhfa/HPI_AT_{state|metro|3zip}.csv

  3. TIGER Block Group geometry (US-national)
     — Supabase table: geo_bg20_us (geoid text PK, geom geometry)

Usage (local, uses gcloud ADC):
    python scripts/data_acquisition/download/download_reference_data.py

    # Or individual steps:
    python scripts/data_acquisition/download/download_reference_data.py --acs
    python scripts/data_acquisition/download/download_reference_data.py --fhfa
    python scripts/data_acquisition/download/download_reference_data.py --tiger
"""

import argparse, io, os, sys, time, zipfile
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

# ── deps ──────────────────────────────────────────────────────────────────────
try:
    import requests
except ImportError:
    os.system(f"{sys.executable} -m pip install requests -q")
    import requests

try:
    from google.cloud import storage
    from google.auth import default as gauth_default
except ImportError:
    os.system(f"{sys.executable} -m pip install google-cloud-storage google-auth -q")
    from google.cloud import storage
    from google.auth import default as gauth_default

PROJECT    = "properlytic-data"
BUCKET     = "properlytic-raw-data"
BATCH_SIZE = 500

def ts(): return time.strftime("%H:%M:%S")

def get_gcs_bucket():
    creds, _ = gauth_default()
    client = storage.Client(credentials=creds, project=PROJECT)
    return client.bucket(BUCKET)

# ── ACS B25077 ────────────────────────────────────────────────────────────────
# Table-based SF zip contains ALL geographies (block group rows identifiable
# by GEO_ID starting with "1500000US").  Each annual zip is ~5-15 MB compressed.

ACS_YEARS  = list(range(2018, 2024))   # 2018-2023, table-based SF available
ACS_TABLE  = "B25077"
ACS_LABEL  = "home_value"

def acs_url(year: int) -> str:
    return (
        f"https://www2.census.gov/programs-surveys/acs/summary_file/{year}"
        f"/table-based-SF/{year}_ACS_Detailed_Tables_Group_{ACS_TABLE}.zip"
    )

def download_acs(bucket):
    print(f"\n[{ts()}] === Downloading ACS B25077 ({ACS_YEARS[0]}-{ACS_YEARS[-1]}) ===")
    results = {}
    for year in ACS_YEARS:
        blob_name = f"census/ACS_{ACS_TABLE}_{ACS_LABEL}_{year}.zip"
        blob = bucket.blob(blob_name)
        if blob.exists():
            print(f"[{ts()}]   {year}: already on GCS, skipping")
            results[year] = "skipped"
            continue
        url = acs_url(year)
        print(f"[{ts()}]   {year}: downloading from {url}")
        try:
            r = requests.get(url, timeout=300, stream=True)
            r.raise_for_status()
            data = b"".join(r.iter_content(1024 * 1024))
            blob.upload_from_string(data, content_type="application/zip")
            size_mb = len(data) / 1e6
            print(f"[{ts()}]   {year}: ✅ uploaded {size_mb:.1f} MB → {blob_name}")
            results[year] = f"ok ({size_mb:.1f} MB)"
        except Exception as e:
            print(f"[{ts()}]   {year}: ❌ {e}")
            results[year] = f"error: {e}"
    return results

# ── FHFA HPI ──────────────────────────────────────────────────────────────────
FHFA_DATASETS = {
    "fhfa/HPI_AT_state.csv":  "https://www.fhfa.gov/DataTools/Downloads/Documents/HPI/HPI_AT_state.csv",
    "fhfa/HPI_AT_metro.csv":  "https://www.fhfa.gov/DataTools/Downloads/Documents/HPI/HPI_AT_metro.csv",
    "fhfa/HPI_AT_3zip.csv":   "https://www.fhfa.gov/DataTools/Downloads/Documents/HPI/HPI_AT_3zip.csv",
}

# Fallback URLs in case FHFA site restructures
FHFA_FALLBACKS = {
    "fhfa/HPI_AT_state.csv": [
        "https://www.fhfa.gov/DataTools/Downloads/Documents/HPI/HPI_AT_state.csv",
        "https://www.fhfa.gov/sites/default/files/2024-10/HPI_AT_state.csv",
    ],
    "fhfa/HPI_AT_metro.csv": [
        "https://www.fhfa.gov/DataTools/Downloads/Documents/HPI/HPI_AT_metro.csv",
        "https://www.fhfa.gov/sites/default/files/2024-10/HPI_AT_metro.csv",
    ],
    "fhfa/HPI_AT_3zip.csv": [
        "https://www.fhfa.gov/DataTools/Downloads/Documents/HPI/HPI_AT_3zip.csv",
        "https://www.fhfa.gov/sites/default/files/2024-10/HPI_AT_3zip.csv",
    ],
}

def download_fhfa(bucket):
    print(f"\n[{ts()}] === Downloading FHFA HPI (state/MSA/ZIP3) ===")
    results = {}
    headers = {"User-Agent": "Mozilla/5.0 Properlytic-DataBot/1.0"}
    for blob_name, primary_url in FHFA_DATASETS.items():
        blob = bucket.blob(blob_name)
        if blob.exists():
            print(f"[{ts()}]   {blob_name}: already on GCS, skipping")
            results[blob_name] = "skipped"
            continue
        urls = FHFA_FALLBACKS.get(blob_name, [primary_url])
        for url in urls:
            try:
                print(f"[{ts()}]   Trying {url}")
                r = requests.get(url, timeout=120, headers=headers)
                r.raise_for_status()
                if len(r.content) < 1000 or b"<html" in r.content[:200].lower():
                    print(f"[{ts()}]   Got HTML response, trying next URL...")
                    continue
                blob.upload_from_string(r.content, content_type="text/csv")
                size_mb = len(r.content) / 1e6
                print(f"[{ts()}]   ✅ {blob_name}: {size_mb:.2f} MB uploaded")
                results[blob_name] = f"ok ({size_mb:.2f} MB)"
                break
            except Exception as e:
                print(f"[{ts()}]   ⚠️  {url}: {e}")
                continue
        else:
            print(f"[{ts()}]   ❌ All URLs failed for {blob_name}")
            results[blob_name] = "error: all URLs failed"
    return results

# ── TIGER Block Group geometry → Supabase ─────────────────────────────────────
TIGER_BG_URL = "https://www2.census.gov/geo/tiger/TIGER2023/BG/tl_2023_us_bg.zip"
TIGER_CACHE  = Path(__file__).resolve().parents[3] / "data" / "tiger_cache"

def ensure_pg():
    try:
        import psycopg2  # noqa
    except ImportError:
        os.system(f"{sys.executable} -m pip install psycopg2-binary -q")

def ensure_geopandas():
    try:
        import geopandas  # noqa
    except ImportError:
        os.system(f"{sys.executable} -m pip install geopandas pyogrio fiona shapely -q")

def get_db_conn():
    env_path = Path(__file__).resolve().parents[3] / ".env.local"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                v = v.strip().strip('"').strip("'")
                os.environ.setdefault(k.strip(), v)
    import psycopg2
    for key in ["SUPABASE_DB_URL", "POSTGRES_URL_NON_POOLING", "POSTGRES_URL"]:
        raw = os.environ.get(key, "").strip()
        if raw:
            parts = urlsplit(raw)
            q = dict(parse_qsl(parts.query, keep_blank_values=True))
            allowed = {"sslmode": q["sslmode"]} if "sslmode" in q else {}
            db_url = urlunsplit((parts.scheme, parts.netloc, parts.path,
                                 urlencode(allowed), parts.fragment))
            conn = psycopg2.connect(db_url)
            conn.autocommit = True
            return conn
    raise RuntimeError("No DB URL in environment")

def download_tiger_bg():
    print(f"\n[{ts()}] === Downloading TIGER Block Group geometry ===")
    ensure_geopandas()
    ensure_pg()
    import geopandas as gpd
    import psycopg2.extras

    TIGER_CACHE.mkdir(parents=True, exist_ok=True)
    local = TIGER_CACHE / "tl_2023_us_bg.zip"

    if not local.exists() or local.stat().st_size < 1000:
        print(f"[{ts()}]   Downloading {TIGER_BG_URL} (~400 MB)...")
        r = requests.get(TIGER_BG_URL, stream=True, timeout=600)
        r.raise_for_status()
        with open(local, "wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                f.write(chunk)
        print(f"[{ts()}]   Downloaded {local.stat().st_size/1e6:.0f} MB")
    else:
        print(f"[{ts()}]   Using cached {local}")

    print(f"[{ts()}]   Reading shapefile...")
    gdf = gpd.read_file(f"zip://{local}")
    print(f"[{ts()}]   Loaded {len(gdf):,} block groups, columns: {list(gdf.columns)}")

    geoid_col = next((c for c in ["GEOID", "GEOID20", "GEOID_BG"] if c in gdf.columns), None)
    if not geoid_col:
        print(f"[{ts()}]   ERROR: no GEOID column found. Columns: {list(gdf.columns)}")
        return {"error": "no GEOID column"}

    if gdf.crs and gdf.crs.to_epsg() != 4326:
        print(f"[{ts()}]   Reprojecting to EPSG:4326...")
        gdf = gdf.to_crs(epsg=4326)

    gdf = gdf[gdf.geometry.notna() & gdf.geometry.is_valid].copy()
    gdf["geoid"] = gdf[geoid_col].astype(str).str.strip()
    print(f"[{ts()}]   Valid block groups: {len(gdf):,}")

    conn = get_db_conn()
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS public.geo_bg20_us CASCADE;")
        cur.execute("""
            CREATE TABLE public.geo_bg20_us (
                geoid  text PRIMARY KEY,
                statefp text,
                countyfp text,
                tractce text,
                blkgrpce text,
                geom   geometry(Geometry, 4326)
            );
        """)
        cur.execute("CREATE INDEX idx_geo_bg20_us_geom ON public.geo_bg20_us USING GIST (geom);")
        cur.execute("GRANT SELECT ON public.geo_bg20_us TO anon, authenticated, service_role;")

    inserted = 0
    batch = []
    with conn.cursor() as cur:
        for _, row in gdf.iterrows():
            batch.append((
                row["geoid"],
                str(row.get("STATEFP", ""))[:2],
                str(row.get("COUNTYFP", ""))[:3],
                str(row.get("TRACTCE", ""))[:6],
                str(row.get("BLKGRPCE", ""))[:1],
                row.geometry.wkb_hex,
            ))
            if len(batch) >= BATCH_SIZE:
                psycopg2.extras.execute_values(
                    cur,
                    """INSERT INTO public.geo_bg20_us
                       (geoid, statefp, countyfp, tractce, blkgrpce, geom)
                       VALUES %s ON CONFLICT (geoid) DO NOTHING""",
                    batch,
                    template="(%s,%s,%s,%s,%s, ST_Multi(ST_SetSRID(ST_GeomFromWKB(decode(%s,'hex')),4326)))"
                )
                inserted += len(batch)
                batch = []
                if inserted % 50000 == 0:
                    print(f"[{ts()}]   Inserted {inserted:,}/{len(gdf):,}")
        if batch:
            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO public.geo_bg20_us
                   (geoid, statefp, countyfp, tractce, blkgrpce, geom)
                   VALUES %s ON CONFLICT (geoid) DO NOTHING""",
                batch,
                template="(%s,%s,%s,%s,%s, ST_Multi(ST_SetSRID(ST_GeomFromWKB(decode(%s,'hex')),4326)))"
            )
            inserted += len(batch)
    conn.close()
    print(f"[{ts()}]   ✅ Inserted {inserted:,} block groups into geo_bg20_us")
    return {"rows": inserted}

# ── CLI ───────────────────────────────────────────────────────────────────────
def main():
    p = argparse.ArgumentParser(description="Download ACS, FHFA HPI, and TIGER block groups")
    p.add_argument("--acs",   action="store_true", help="Download ACS B25077 → GCS")
    p.add_argument("--fhfa",  action="store_true", help="Download FHFA HPI → GCS")
    p.add_argument("--tiger", action="store_true", help="Download TIGER BG geometry → Supabase")
    p.add_argument("--all",   action="store_true", help="Run all three (default)")
    args = p.parse_args()

    run_all = args.all or not any([args.acs, args.fhfa, args.tiger])

    if run_all or args.acs or args.fhfa:
        bucket = get_gcs_bucket()

    if run_all or args.acs:
        acs_res = download_acs(bucket)
        print(f"\n[{ts()}] ACS results: {acs_res}")

    if run_all or args.fhfa:
        fhfa_res = download_fhfa(bucket)
        print(f"\n[{ts()}] FHFA results: {fhfa_res}")

    if run_all or args.tiger:
        tiger_res = download_tiger_bg()
        print(f"\n[{ts()}] TIGER results: {tiger_res}")

    print(f"\n[{ts()}] 🎉 Done!")

if __name__ == "__main__":
    main()
