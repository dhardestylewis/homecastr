#!/usr/bin/env python3
"""
Nationwide Census Tract Uploader
================================
Downloads the 2020 Census Tract shapefiles for ALL 50 states + DC + PR and
appends them to public.geo_tract20_tx in Supabase.

Does NOT drop the table, so existing data (like TX or custom inserts) is preserved.
Uses ON CONFLICT (geoid) DO NOTHING.
"""
import os, sys, time, requests
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

# --- Deps ---
try:
    import psycopg2, psycopg2.extras
except ImportError:
    os.system(f"{sys.executable} -m pip install psycopg2-binary")
    import psycopg2, psycopg2.extras
try:
    import geopandas as gpd
except ImportError:
    os.system(f"{sys.executable} -m pip install geopandas pyogrio fiona")
    import geopandas as gpd

from shapely import wkb

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
TIGER_CACHE = PROJECT_ROOT / "data" / "tiger_cache"
TIGER_CACHE.mkdir(parents=True, exist_ok=True)
BATCH_SIZE = 1000

# Standard 2020 FIPS codes for 50 states + DC (11) + PR (72)
STATE_FIPS = [
    "01", "02", "04", "05", "06", "08", "09", "10", "11", "12", "13", "15",
    "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27",
    "28", "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "39",
    "40", "41", "42", "44", "45", "46", "47", "48", "49", "50", "51", "53",
    "54", "55", "56", "72"
]


def ts():
    return time.strftime("%H:%M:%S")


def get_db_connection():
    db_url = os.environ.get("SUPABASE_DB_URL", "").strip()
    if not db_url:
        raise RuntimeError("No database URL found in environment (SUPABASE_DB_URL)")

    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    return conn


def download_tiger_state(fips: str) -> Path:
    """Download a TIGER shapefile zip for a specific state FIPS."""
    url = f"https://www2.census.gov/geo/tiger/TIGER2020/TRACT/tl_2020_{fips}_tract.zip"
    name = f"tl_2020_{fips}_tract"
    local = TIGER_CACHE / f"{name}.zip"
    
    if local.exists() and local.stat().st_size > 1000:
        print(f"[{ts()}]   Cached: {local.name}")
        return local
        
    print(f"[{ts()}]   Downloading {name}...")
    resp = requests.get(url, stream=True, timeout=120)
    if resp.status_code == 404:
        print(f"[{ts()}]   URL not found (404) for FIPS {fips}")
        return None
    resp.raise_for_status()
    
    with open(local, "wb") as f:
        for chunk in resp.iter_content(1024 * 1024):
            f.write(chunk)
    return local


def upload_tracts_nationwide():
    conn = get_db_connection()
    
    # Ensure table exists first (it should, but just in case)
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.geo_tract20_tx (
                geoid text PRIMARY KEY,
                geom geometry(Geometry, 4326)
            );
        """)
        
    total_inserted = 0
    total_read = 0

    for fips in STATE_FIPS:
        print(f"\n[{ts()}] Processing FIPS {fips}...")
        shp_zip = download_tiger_state(fips)
        if not shp_zip:
            continue

        try:
            gdf = gpd.read_file(f"zip://{shp_zip}")
        except Exception as e:
            print(f"[{ts()}]   ERROR loading shapefile: {e}")
            continue
            
        geoid_col = "GEOID" if "GEOID" in gdf.columns else "GEOID20"
        if geoid_col not in gdf.columns:
            print(f"[{ts()}]   ERROR: Cannot find GEOID column ({list(gdf.columns)})")
            continue

        if gdf.crs and gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(epsg=4326)

        gdf = gdf[gdf.geometry.notna() & gdf.geometry.is_valid].copy()
        gdf["geoid"] = gdf[geoid_col].astype(str).str.strip()
        total_read += len(gdf)

        inserted_state = 0
        with conn.cursor() as cur:
            batch = []
            for _, row in gdf.iterrows():
                geom_wkb = row.geometry.wkb_hex
                batch.append((row["geoid"], geom_wkb))
                
                if len(batch) >= BATCH_SIZE:
                    psycopg2.extras.execute_values(
                        cur,
                        "INSERT INTO public.geo_tract20_tx (geoid, geom) VALUES %s ON CONFLICT (geoid) DO NOTHING",
                        batch,
                        template="(%s, ST_Multi(ST_SetSRID(ST_GeomFromWKB(decode(%s, 'hex')), 4326)))"
                    )
                    inserted_state += len(batch)
                    batch = []
                    
            if batch:
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO public.geo_tract20_tx (geoid, geom) VALUES %s ON CONFLICT (geoid) DO NOTHING",
                    batch,
                    template="(%s, ST_Multi(ST_SetSRID(ST_GeomFromWKB(decode(%s, 'hex')), 4326)))"
                )
                inserted_state += len(batch)

        print(f"[{ts()}]   Uploaded {inserted_state:,} tracts for FIPS {fips}")
        total_inserted += inserted_state

    conn.close()
    print(f"\n[{ts()}] 🎉 DONE! Uploaded {total_inserted:,} total nationwide tracts (out of {total_read:,} read).")


if __name__ == "__main__":
    upload_tracts_nationwide()
