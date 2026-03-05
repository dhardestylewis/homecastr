"""
Upload Census TIGER tract + ZCTA geometry for Washington State to Supabase.
Also uploads tabblock geometry for King County.

Prerequisites:
  pip install geopandas psycopg2-binary sqlalchemy geoalchemy2

Usage:
  python scripts/inference/upload_seattle_geometry.py
"""

import os
import sys
import tempfile
import zipfile
import io
import requests
import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, text

# ── Supabase connection ──
DB_URL = os.environ.get("DATABASE_URL",
    "postgresql://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"
)

# ── Census Boundary URLs ──
# Cartographic Boundary files (cb_) are smaller and on a different CDN than TIGER
CB_BASE = "https://www2.census.gov/geo/tiger/GENZ2020/shp"

# Washington State FIPS = 53, King County FIPS = 53033
WA_FIPS = "53"
KING_COUNTY_FIPS = "53033"

# Cartographic boundary files (smaller, more reliable)
DOWNLOAD_URLS = {
    "tract": f"{CB_BASE}/cb_2020_{WA_FIPS}_tract_500k.zip",
    "tabblock": f"https://www2.census.gov/geo/tiger/TIGER2020/TABBLOCK20/tl_2020_{WA_FIPS}_tabblock20.zip",
    "zcta": f"{CB_BASE}/cb_2020_us_zcta520_500k.zip",
}


def download_shapefile(url: str) -> gpd.GeoDataFrame:
    """Download a Census TIGER shapefile ZIP and return as GeoDataFrame."""
    print(f"  Downloading {url} ...")
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    
    # Extract ZIP to temp dir
    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            zf.extractall(tmpdir)
        
        # Find the .shp file
        shp_files = [f for f in os.listdir(tmpdir) if f.endswith(".shp")]
        if not shp_files:
            raise FileNotFoundError(f"No .shp file found in {url}")
        
        shp_path = os.path.join(tmpdir, shp_files[0])
        gdf = gpd.read_file(shp_path)
        
    # Ensure EPSG:4326
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)
    
    print(f"  → {len(gdf)} features loaded")
    return gdf


def upload_tracts(engine):
    """Upload WA tract geometry to public.geo_tract20_tx (shared table)."""
    print("\n── Tracts ──")
    gdf = download_shapefile(DOWNLOAD_URLS["tract"])
    
    # Schema: geoid text PK, geom geometry
    gdf = gdf.rename(columns={"GEOID20": "geoid"} if "GEOID20" in gdf.columns else {"GEOID": "geoid"})
    gdf = gdf[["geoid", "geometry"]].rename(columns={"geometry": "geom"})
    gdf = gdf.set_geometry("geom")
    
    # Force MultiPolygon
    gdf["geom"] = gdf["geom"].apply(
        lambda g: g if g.geom_type == "MultiPolygon" else gpd.GeoSeries([g]).unary_union
        if g is not None else None
    )
    
    n = len(gdf)
    print(f"  Uploading {n} tracts to geo_tract20_tx ...")
    
    # Use upsert via raw SQL to avoid conflicts with existing TX data
    with engine.begin() as conn:
        for _, row in gdf.iterrows():
            conn.execute(text("""
                INSERT INTO public.geo_tract20_tx (geoid, geom)
                VALUES (:geoid, ST_GeomFromText(:wkt, 4326))
                ON CONFLICT (geoid) DO UPDATE SET geom = EXCLUDED.geom
            """), {"geoid": row["geoid"], "wkt": row["geom"].wkt})
    
    print(f"  ✅ {n} WA tracts uploaded")


def upload_tabblocks(engine):
    """Upload WA tabblock geometry to public.geo_tabblock20_tx."""
    print("\n── Tabblocks ──")
    gdf = download_shapefile(DOWNLOAD_URLS["tabblock"])
    
    # Filter to only King County for now (otherwise too many)
    gdf_col = "GEOID20" if "GEOID20" in gdf.columns else "GEOID"
    gdf = gdf[gdf[gdf_col].str.startswith(KING_COUNTY_FIPS)]
    print(f"  Filtered to King County: {len(gdf)} tabblocks")
    
    gdf = gdf.rename(columns={gdf_col: "geoid20"})
    gdf = gdf[["geoid20", "geometry"]].rename(columns={"geometry": "geom"})
    gdf = gdf.set_geometry("geom")
    
    # Force MultiPolygon
    gdf["geom"] = gdf["geom"].apply(
        lambda g: g if g.geom_type == "MultiPolygon" else gpd.GeoSeries([g]).unary_union
        if g is not None else None
    )
    
    n = len(gdf)
    print(f"  Uploading {n} tabblocks to geo_tabblock20_tx ...")
    
    with engine.begin() as conn:
        for i, (_, row) in enumerate(gdf.iterrows()):
            conn.execute(text("""
                INSERT INTO public.geo_tabblock20_tx (geoid20, geom)
                VALUES (:geoid20, ST_GeomFromText(:wkt, 4326))
                ON CONFLICT (geoid20) DO UPDATE SET geom = EXCLUDED.geom
            """), {"geoid20": row["geoid20"], "wkt": row["geom"].wkt})
            if (i + 1) % 500 == 0:
                print(f"    {i+1}/{n} ...")
    
    print(f"  ✅ {n} King County tabblocks uploaded")


def upload_zctas(engine):
    """Upload WA-area ZCTAs to public.geo_zcta20_us."""
    print("\n── ZCTAs ──")
    gdf = download_shapefile(DOWNLOAD_URLS["zcta"])
    
    # Filter to WA-area ZCTAs (98xxx and 99xxx zip codes cover WA)
    zcta_col = "ZCTA5CE20" if "ZCTA5CE20" in gdf.columns else "ZCTA5CE10"
    gdf = gdf[gdf[zcta_col].str.startswith(("98", "99"))]
    print(f"  Filtered to WA-area: {len(gdf)} ZCTAs")
    
    gdf = gdf.rename(columns={zcta_col: "zcta5"})
    gdf = gdf[["zcta5", "geometry"]].rename(columns={"geometry": "geom"})
    gdf = gdf.set_geometry("geom")
    
    # Force MultiPolygon
    gdf["geom"] = gdf["geom"].apply(
        lambda g: g if g.geom_type == "MultiPolygon" else gpd.GeoSeries([g]).unary_union
        if g is not None else None
    )
    
    n = len(gdf)
    print(f"  Uploading {n} ZCTAs to geo_zcta20_us ...")
    
    with engine.begin() as conn:
        for _, row in gdf.iterrows():
            conn.execute(text("""
                INSERT INTO public.geo_zcta20_us (zcta5, geom)
                VALUES (:zcta5, ST_GeomFromText(:wkt, 4326))
                ON CONFLICT (zcta5) DO UPDATE SET geom = EXCLUDED.geom
            """), {"zcta5": row["zcta5"], "wkt": row["geom"].wkt})
    
    print(f"  ✅ {n} WA ZCTAs uploaded")


def main():
    print("=" * 60)
    print("Upload Seattle / WA Geometry to Supabase")
    print("=" * 60)
    
    engine = create_engine(DB_URL, pool_pre_ping=True)
    
    # Test connection
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        print("✅ Connected to Supabase")
    
    upload_tracts(engine)
    upload_zctas(engine)
    upload_tabblocks(engine)
    
    print("\n" + "=" * 60)
    print("✅ ALL GEOMETRY UPLOADED")
    print("=" * 60)


if __name__ == "__main__":
    main()
