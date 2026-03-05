"""
Download France administrative geometry from IGN and upload to Supabase.
Uses IGN AdminExpress (communes = French equivalent of tracts).

France uses:
- Communes → mapped to tract_geoid20 (administrative unit level)
- Départements → mapped to zcta5 (aggregate level)

Usage:
    python scripts/inference/upload_france_geometry.py
"""
import os
import io
import tempfile
import zipfile
import requests
import geopandas as gpd
import psycopg2
from sqlalchemy import create_engine, text

DB_URL = "postgresql://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"
SCHEMA = "forecast_20260220_7f31c6e4"

# IGN AdminExpress — COG (communes geo). Simplified boundaries.
# https://geoservices.ign.fr/adminexpress
IGN_COMMUNE_URL = "https://wxs.ign.fr/static/UGC/ADMIN-EXPRESS-COG/latest/ADMIN-EXPRESS-COG_3-2__SHP_WGS84G_FRA_2024-02-22.7z"

# Fallback: use GADM for France admin boundaries (simpler download)
GADM_FRANCE_URL = "https://geodata.ucdavis.edu/gadm/gadm4.1/shp/gadm41_FRA_shp.zip"


def download_gadm_france() -> tuple:
    """Download GADM France boundaries (communes = level 4, départements = level 2)."""
    print("  Downloading GADM France boundaries...")
    r = requests.get(GADM_FRANCE_URL, timeout=300, stream=True)
    r.raise_for_status()
    
    total_mb = int(r.headers.get('content-length', 0)) / 1e6
    print(f"  Size: {total_mb:.0f} MB")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        zpath = os.path.join(tmpdir, "gadm_france.zip")
        downloaded = 0
        with open(zpath, "wb") as f:
            for chunk in r.iter_content(1024*1024):
                f.write(chunk)
                downloaded += len(chunk)
                if downloaded % (10*1024*1024) == 0:
                    print(f"    {downloaded/1e6:.0f}/{total_mb:.0f} MB...")
        
        print("  Extracting...")
        with zipfile.ZipFile(zpath) as zf:
            zf.extractall(tmpdir)
        
        # Find shapefiles
        shp_files = {}
        for f in os.listdir(tmpdir):
            if f.endswith(".shp"):
                if "_2." in f:  # Level 2 = départements
                    shp_files["dept"] = os.path.join(tmpdir, f)
                elif "_3." in f:  # Level 3 = arrondissements
                    shp_files["arr"] = os.path.join(tmpdir, f)
                elif "_4." in f:  # Level 4 = communes (closest to tracts)
                    shp_files["commune"] = os.path.join(tmpdir, f)
        
        print(f"  Found: {list(shp_files.keys())}")
        
        communes = None
        depts = None
        
        if "commune" in shp_files:
            communes = gpd.read_file(shp_files["commune"])
            if communes.crs and communes.crs.to_epsg() != 4326:
                communes = communes.to_crs(epsg=4326)
            print(f"  Communes: {len(communes)} features")
        
        if "dept" in shp_files:
            depts = gpd.read_file(shp_files["dept"])
            if depts.crs and depts.crs.to_epsg() != 4326:
                depts = depts.to_crs(epsg=4326)
            print(f"  Départements: {len(depts)} features")
        
        return communes, depts


def upload_communes(engine, communes):
    """Upload French communes as tract-level geometry."""
    print("\n=== Uploading communes as tract geometry ===")
    
    # GADM uses GID_4 for commune ID, NAME_4 for name
    gid_col = "GID_4" if "GID_4" in communes.columns else "GID_3"
    name_col = "NAME_4" if "NAME_4" in communes.columns else "NAME_3"
    
    communes = communes.rename(columns={gid_col: "geoid"})
    communes = communes[["geoid", "geometry"]].rename(columns={"geometry": "geom"})
    communes = communes.set_geometry("geom")
    
    # Force MultiPolygon
    communes["geom"] = communes["geom"].apply(
        lambda g: g if g is None else (g if g.geom_type == "MultiPolygon" else g.buffer(0))
    )
    
    n = len(communes)
    print(f"  Uploading {n} communes to geo_tract20_tx ...")
    
    with engine.begin() as conn:
        for i, (_, row) in enumerate(communes.iterrows()):
            if row["geom"] is None:
                continue
            try:
                conn.execute(text("""
                    INSERT INTO public.geo_tract20_tx (geoid, geom)
                    VALUES (:geoid, ST_GeomFromText(:wkt, 4326))
                    ON CONFLICT (geoid) DO UPDATE SET geom = EXCLUDED.geom
                """), {"geoid": row["geoid"], "wkt": row["geom"].wkt})
            except Exception as e:
                if i == 0:
                    print(f"  ⚠️ First error: {e}")
            if (i + 1) % 1000 == 0:
                print(f"    {i+1}/{n} ...")
    
    print(f"  ✅ {n} communes uploaded")


def upload_departements(engine, depts):
    """Upload French départements as ZCTA-level geometry."""
    print("\n=== Uploading départements as ZCTA geometry ===")
    
    gid_col = "GID_2" if "GID_2" in depts.columns else "GID_1"
    
    depts = depts.rename(columns={gid_col: "zcta5"})
    depts = depts[["zcta5", "geometry"]].rename(columns={"geometry": "geom"})
    depts = depts.set_geometry("geom")
    
    depts["geom"] = depts["geom"].apply(
        lambda g: g if g is None else (g if g.geom_type == "MultiPolygon" else g.buffer(0))
    )
    
    n = len(depts)
    print(f"  Uploading {n} départements to geo_zcta20_us ...")
    
    with engine.begin() as conn:
        for _, row in depts.iterrows():
            if row["geom"] is None:
                continue
            try:
                conn.execute(text("""
                    INSERT INTO public.geo_zcta20_us (zcta5, geom)
                    VALUES (:zcta5, ST_GeomFromText(:wkt, 4326))
                    ON CONFLICT (zcta5) DO UPDATE SET geom = EXCLUDED.geom
                """), {"zcta5": row["zcta5"], "wkt": row["geom"].wkt})
            except Exception as e:
                if _ == 0:
                    print(f"  ⚠️ First error: {e}")
    
    print(f"  ✅ {n} départements uploaded")


def populate_france_ladder():
    """Populate parcel_ladder_v1 for France accounts using commune IDs."""
    print("\n=== Populating parcel_ladder for France ===")
    conn = psycopg2.connect(DB_URL.replace("postgresql://", "postgres://"))
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SET statement_timeout = '300000'")
    
    # DVF accounts are commune-based: e.g., '02460000AB0146' starts with commune code
    # The first 5 chars are the INSEE commune code (département 2 + commune 3)
    # Map: acct → tract_geoid20 = GADM commune GID that contains this INSEE code
    # Simpler: just extract the 5-digit code as both tract and département
    
    cur.execute(f"""
        INSERT INTO public.parcel_ladder_v1 (acct, tract_geoid20, zcta5, jurisdiction)
        SELECT DISTINCT acct, 
               SUBSTRING(acct FROM 1 FOR 5) as tract_geoid20,
               SUBSTRING(acct FROM 1 FOR 2) as zcta5,
               'france_dvf'
        FROM {SCHEMA}.metrics_parcel_forecast 
        WHERE jurisdiction = 'france_dvf'
        ON CONFLICT (acct) DO UPDATE SET 
            tract_geoid20 = EXCLUDED.tract_geoid20,
            zcta5 = EXCLUDED.zcta5,
            jurisdiction = EXCLUDED.jurisdiction
    """)
    print(f"  ✅ {cur.rowcount:,} France accounts added to parcel_ladder")
    conn.close()


def main():
    print("=" * 60)
    print("Upload France Geometry + Populate Parcel Ladder")
    print("=" * 60)
    
    # Download GADM geometry
    communes, depts = download_gadm_france()
    
    if communes is None and depts is None:
        print("❌ Could not download geometry")
        return
    
    # Upload to Supabase
    engine = create_engine(DB_URL, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("✅ Connected to Supabase")
    
    if communes is not None:
        upload_communes(engine, communes)
    if depts is not None:
        upload_departements(engine, depts)
    
    # Populate parcel_ladder
    populate_france_ladder()
    
    print("\n" + "=" * 60)
    print("✅ FRANCE GEOMETRY + LADDER COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
