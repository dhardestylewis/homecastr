#!/usr/bin/env python3
"""
Geometry Uploader for Forecast Choropleth Map
==============================================
Populates the empty public.geo_* tables in Supabase with:
  1. geo_parcel_poly     — from local HCAD Parcels.zip shapefile
  2. geo_zcta20_us       — Census TIGER ZCTA 2020 (national)
  3. geo_tract20_tx      — Census TIGER Tracts 2020 (TX FIPS=48)
  4. geo_tabblock20_tx   — Census TIGER Tabblocks 2020 (TX FIPS=48)

Usage:
  pip install psycopg2-binary geopandas pyogrio fiona shapely requests
  cd v0-properlytic-8v
  python scripts/upload_geo.py [--parcels] [--zcta] [--tracts] [--tabblocks] [--all]
"""
import os, sys, zipfile, tempfile, time, argparse, io
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
try:
    import requests
except ImportError:
    os.system(f"{sys.executable} -m pip install requests")
    import requests

from shapely import wkb

# --- Config ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
HCAD_GIS_DIR = PROJECT_ROOT / "data" / "hcad" / "gis"
TIGER_CACHE = PROJECT_ROOT / "data" / "tiger_cache"
TIGER_CACHE.mkdir(parents=True, exist_ok=True)

BATCH_SIZE = 500  # rows per INSERT batch

# Census TIGER URLs (2020 vintage)
TIGER_ZCTA_URL = "https://www2.census.gov/geo/tiger/TIGER2020/ZCTA520/tl_2020_us_zcta520.zip"
TIGER_TRACT_TX_URL = "https://www2.census.gov/geo/tiger/TIGER2020/TRACT/tl_2020_48_tract.zip"
TIGER_TABBLOCK_TX_URL = "https://www2.census.gov/geo/tiger/TIGER2020/TABBLOCK20/tl_2020_48_tabblock20.zip"

def ts():
    return time.strftime("%H:%M:%S")

# --- DB Connection (same pattern as h3_mvt_pipeline.py) ---
def get_db_connection():
    # Load .env.local
    env_path = PROJECT_ROOT / ".env.local"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                v = v.strip().strip('"').strip("'")
                os.environ.setdefault(k.strip(), v)

    db_url = ""
    for key in ["SUPABASE_DB_URL", "POSTGRES_URL_NON_POOLING", "POSTGRES_URL"]:
        raw = os.environ.get(key, "").strip()
        if raw:
            parts = urlsplit(raw)
            q = dict(parse_qsl(parts.query, keep_blank_values=True))
            allowed = {"sslmode": q.get("sslmode")} if "sslmode" in q else {}
            db_url = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(allowed), parts.fragment)).strip()
            break

    if not db_url:
        raise RuntimeError("No database URL found in environment")

    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '60min';")
        try:
            cur.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE;")
        except:
            pass
    print(f"[{ts()}] Connected to database")
    return conn


def download_tiger(url: str, name: str) -> Path:
    """Download a TIGER shapefile zip if not cached."""
    local = TIGER_CACHE / f"{name}.zip"
    if local.exists() and local.stat().st_size > 1000:
        print(f"[{ts()}] Using cached: {local}")
        return local
    print(f"[{ts()}] Downloading {name} from Census TIGER...")
    resp = requests.get(url, stream=True, timeout=120)
    resp.raise_for_status()
    with open(local, "wb") as f:
        for chunk in resp.iter_content(1024 * 1024):
            f.write(chunk)
    print(f"[{ts()}] Downloaded {local.stat().st_size / 1e6:.1f} MB")
    return local


def ensure_postgis(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
    print(f"[{ts()}] PostGIS extension ensured")


# ==========================================================================
# PARCEL UPLOAD
# ==========================================================================
def upload_parcels(conn):
    """Upload HCAD parcel polygons to public.geo_parcel_poly."""
    # Find the best shapefile (prefer Parcels.zip = 2025 current)
    candidates = [
        HCAD_GIS_DIR / "Parcels.zip",
        HCAD_GIS_DIR / "Parcels_2025_Oct.zip",
        HCAD_GIS_DIR / "Parcels_2024_Oct.zip",
    ]
    shp_zip = None
    for c in candidates:
        if c.exists():
            shp_zip = c
            break

    if not shp_zip:
        print(f"[{ts()}] ERROR: No HCAD parcel zip found in {HCAD_GIS_DIR}")
        return

    print(f"[{ts()}] Reading parcels from {shp_zip.name}...")
    # Find the .shp file inside the zip (may be nested)
    import zipfile as _zf
    with _zf.ZipFile(shp_zip) as zf:
        shp_entries = [n for n in zf.namelist() if n.lower().endswith(".shp")]
    if not shp_entries:
        print(f"[{ts()}] ERROR: No .shp file found inside {shp_zip.name}")
        return
    inner_shp = shp_entries[0]
    print(f"[{ts()}] Found shapefile inside zip: {inner_shp}")
    gdf = gpd.read_file(f"zip://{shp_zip}!{inner_shp}")
    print(f"[{ts()}] Loaded {len(gdf):,} parcels, columns: {list(gdf.columns)}")

    # Find the account column
    acct_col = None
    for c in ["HCAD_NUM", "hcad_num", "ACCT", "acct", "Account", "ACCOUNT"]:
        if c in gdf.columns:
            acct_col = c
            break
    if not acct_col:
        print(f"[{ts()}] ERROR: Cannot find account column. Columns: {list(gdf.columns)}")
        print(f"[{ts()}] First row: {gdf.iloc[0].to_dict()}")
        return

    print(f"[{ts()}] Using account column: {acct_col}")

    # Ensure geometry is EPSG:4326
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        print(f"[{ts()}] Reprojecting from {gdf.crs} to EPSG:4326...")
        gdf = gdf.to_crs(epsg=4326)

    # Filter to valid polygons only
    gdf = gdf[gdf.geometry.notna() & gdf.geometry.is_valid].copy()
    gdf["acct"] = gdf[acct_col].astype(str).str.strip()
    gdf = gdf[gdf["acct"].str.len() > 0]
    print(f"[{ts()}] Valid parcels: {len(gdf):,}")

    # Drop and recreate table (clears any old type constraints)
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS public.geo_parcel_poly CASCADE;")
        cur.execute("""
            CREATE TABLE public.geo_parcel_poly (
                acct text PRIMARY KEY,
                geom geometry(Geometry, 4326)
            );
        """)
        cur.execute("CREATE INDEX idx_geo_parcel_poly_geom ON public.geo_parcel_poly USING GIST (geom);")

    # Batch insert
    inserted = 0
    with conn.cursor() as cur:
        batch = []
        for _, row in gdf.iterrows():
            geom_wkb = row.geometry.wkb_hex
            batch.append((row["acct"], geom_wkb))
            if len(batch) >= BATCH_SIZE:
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO public.geo_parcel_poly (acct, geom) VALUES %s ON CONFLICT (acct) DO NOTHING",
                    batch,
                    template="(%s, ST_Multi(ST_SetSRID(ST_GeomFromWKB(decode(%s, 'hex')), 4326)))"
                )
                inserted += len(batch)
                if inserted % 10000 == 0:
                    print(f"[{ts()}] Parcels: {inserted:,} / {len(gdf):,}")
                batch = []
        if batch:
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO public.geo_parcel_poly (acct, geom) VALUES %s ON CONFLICT (acct) DO NOTHING",
                batch,
                template="(%s, ST_Multi(ST_SetSRID(ST_GeomFromWKB(decode(%s, 'hex')), 4326)))"
            )
            inserted += len(batch)

    print(f"[{ts()}] ✅ Uploaded {inserted:,} parcels to geo_parcel_poly")


# ==========================================================================
# ZCTA UPLOAD
# ==========================================================================
def upload_zcta(conn):
    """Upload Census ZCTA 2020 polygons to public.geo_zcta20_us."""
    shp_zip = download_tiger(TIGER_ZCTA_URL, "tl_2020_us_zcta520")

    print(f"[{ts()}] Reading ZCTA shapefile...")
    gdf = gpd.read_file(f"zip://{shp_zip}")
    print(f"[{ts()}] Loaded {len(gdf):,} ZCTAs, columns: {list(gdf.columns)}")

    # Core column: ZCTA5CE20
    zcta_col = None
    for c in ["ZCTA5CE20", "ZCTA5CE10", "ZCTA5"]:
        if c in gdf.columns:
            zcta_col = c
            break
    if not zcta_col:
        print(f"[{ts()}] ERROR: Cannot find ZCTA column. Columns: {list(gdf.columns)}")
        return

    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    gdf = gdf[gdf.geometry.notna() & gdf.geometry.is_valid].copy()
    gdf["zcta5"] = gdf[zcta_col].astype(str).str.strip()

    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS public.geo_zcta20_us CASCADE;")
        cur.execute("""
            CREATE TABLE public.geo_zcta20_us (
                zcta5 text PRIMARY KEY,
                geom geometry(Geometry, 4326)
            );
        """)
        cur.execute("CREATE INDEX idx_geo_zcta20_us_geom ON public.geo_zcta20_us USING GIST (geom);")

    inserted = 0
    with conn.cursor() as cur:
        batch = []
        for _, row in gdf.iterrows():
            geom_wkb = row.geometry.wkb_hex
            batch.append((row["zcta5"], geom_wkb))
            if len(batch) >= BATCH_SIZE:
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO public.geo_zcta20_us (zcta5, geom) VALUES %s ON CONFLICT (zcta5) DO NOTHING",
                    batch,
                    template="(%s, ST_Multi(ST_SetSRID(ST_GeomFromWKB(decode(%s, 'hex')), 4326)))"
                )
                inserted += len(batch)
                if inserted % 5000 == 0:
                    print(f"[{ts()}] ZCTAs: {inserted:,} / {len(gdf):,}")
                batch = []
        if batch:
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO public.geo_zcta20_us (zcta5, geom) VALUES %s ON CONFLICT (zcta5) DO NOTHING",
                batch,
                template="(%s, ST_Multi(ST_SetSRID(ST_GeomFromWKB(decode(%s, 'hex')), 4326)))"
            )
            inserted += len(batch)

    print(f"[{ts()}] ✅ Uploaded {inserted:,} ZCTAs to geo_zcta20_us")


# ==========================================================================
# TRACT UPLOAD
# ==========================================================================
def upload_tracts(conn):
    """Upload Census Tract 2020 polygons (TX) to public.geo_tract20_tx."""
    shp_zip = download_tiger(TIGER_TRACT_TX_URL, "tl_2020_48_tract")

    print(f"[{ts()}] Reading TX tract shapefile...")
    gdf = gpd.read_file(f"zip://{shp_zip}")
    print(f"[{ts()}] Loaded {len(gdf):,} tracts, columns: {list(gdf.columns)}")

    geoid_col = "GEOID" if "GEOID" in gdf.columns else "GEOID20"
    if geoid_col not in gdf.columns:
        print(f"[{ts()}] ERROR: Cannot find GEOID column")
        return

    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    gdf = gdf[gdf.geometry.notna() & gdf.geometry.is_valid].copy()
    gdf["geoid"] = gdf[geoid_col].astype(str).str.strip()

    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS public.geo_tract20_tx CASCADE;")
        cur.execute("""
            CREATE TABLE public.geo_tract20_tx (
                geoid text PRIMARY KEY,
                geom geometry(Geometry, 4326)
            );
        """)
        cur.execute("CREATE INDEX idx_geo_tract20_tx_geom ON public.geo_tract20_tx USING GIST (geom);")

    inserted = 0
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
                inserted += len(batch)
                batch = []
        if batch:
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO public.geo_tract20_tx (geoid, geom) VALUES %s ON CONFLICT (geoid) DO NOTHING",
                batch,
                template="(%s, ST_Multi(ST_SetSRID(ST_GeomFromWKB(decode(%s, 'hex')), 4326)))"
            )
            inserted += len(batch)

    print(f"[{ts()}] ✅ Uploaded {inserted:,} TX tracts to geo_tract20_tx")


# ==========================================================================
# TABBLOCK UPLOAD
# ==========================================================================
def upload_tabblocks(conn):
    """Upload Census Tabblock 2020 polygons (TX) to public.geo_tabblock20_tx."""
    shp_zip = download_tiger(TIGER_TABBLOCK_TX_URL, "tl_2020_48_tabblock20")

    print(f"[{ts()}] Reading TX tabblock shapefile...")
    gdf = gpd.read_file(f"zip://{shp_zip}")
    print(f"[{ts()}] Loaded {len(gdf):,} tabblocks, columns: {list(gdf.columns)}")

    geoid_col = "GEOID20" if "GEOID20" in gdf.columns else "GEOID"
    if geoid_col not in gdf.columns:
        print(f"[{ts()}] ERROR: Cannot find GEOID20 column")
        return

    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    gdf = gdf[gdf.geometry.notna() & gdf.geometry.is_valid].copy()
    gdf["geoid20"] = gdf[geoid_col].astype(str).str.strip()

    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS public.geo_tabblock20_tx CASCADE;")
        cur.execute("""
            CREATE TABLE public.geo_tabblock20_tx (
                geoid20 text PRIMARY KEY,
                geom geometry(Geometry, 4326)
            );
        """)
        cur.execute("CREATE INDEX idx_geo_tabblock20_tx_geom ON public.geo_tabblock20_tx USING GIST (geom);")

    inserted = 0
    with conn.cursor() as cur:
        batch = []
        for _, row in gdf.iterrows():
            geom_wkb = row.geometry.wkb_hex
            batch.append((row["geoid20"], geom_wkb))
            if len(batch) >= BATCH_SIZE:
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO public.geo_tabblock20_tx (geoid20, geom) VALUES %s ON CONFLICT (geoid20) DO NOTHING",
                    batch,
                    template="(%s, ST_Multi(ST_SetSRID(ST_GeomFromWKB(decode(%s, 'hex')), 4326)))"
                )
                inserted += len(batch)
                if inserted % 50000 == 0:
                    print(f"[{ts()}] Tabblocks: {inserted:,} / {len(gdf):,}")
                batch = []
        if batch:
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO public.geo_tabblock20_tx (geoid20, geom) VALUES %s ON CONFLICT (geoid20) DO NOTHING",
                batch,
                template="(%s, ST_Multi(ST_SetSRID(ST_GeomFromWKB(decode(%s, 'hex')), 4326)))"
            )
            inserted += len(batch)

    print(f"[{ts()}] ✅ Uploaded {inserted:,} TX tabblocks to geo_tabblock20_tx")


# ==========================================================================
# GRANTS (so MVT functions can read these tables)
# ==========================================================================
def apply_grants(conn):
    tables = [
        "public.geo_parcel_poly",
        "public.geo_zcta20_us",
        "public.geo_tract20_tx",
        "public.geo_tabblock20_tx",
    ]
    with conn.cursor() as cur:
        for t in tables:
            cur.execute(f"GRANT SELECT ON {t} TO anon, authenticated, service_role;")
    print(f"[{ts()}] ✅ Grants applied to all geo tables")


# ==========================================================================
# MAIN
# ==========================================================================
def main():
    parser = argparse.ArgumentParser(description="Upload geometry data to Supabase")
    parser.add_argument("--parcels", action="store_true", help="Upload HCAD parcels")
    parser.add_argument("--zcta", action="store_true", help="Upload Census ZCTA 2020")
    parser.add_argument("--tracts", action="store_true", help="Upload Census Tracts 2020 (TX)")
    parser.add_argument("--tabblocks", action="store_true", help="Upload Census Tabblocks 2020 (TX)")
    parser.add_argument("--all", action="store_true", help="Upload everything")
    args = parser.parse_args()

    if not any([args.parcels, args.zcta, args.tracts, args.tabblocks, args.all]):
        # Default to --all
        args.all = True

    conn = get_db_connection()
    ensure_postgis(conn)

    try:
        if args.all or args.zcta:
            upload_zcta(conn)
        if args.all or args.tracts:
            upload_tracts(conn)
        if args.all or args.tabblocks:
            upload_tabblocks(conn)
        if args.all or args.parcels:
            upload_parcels(conn)
        apply_grants(conn)
    finally:
        conn.close()

    print(f"\n[{ts()}] 🎉 All done!")


if __name__ == "__main__":
    main()
