"""
Download King County parcel centroids and populate parcel_ladder_v1 for WA.
Uses King County GIS open data (parcel shapefile with centroids).
Then spatial-joins to Census tracts/ZCTAs.

Usage:
    python scripts/inference/download_and_populate_wa_ladder.py
"""
import os
import io
import tempfile
import zipfile
import requests
import geopandas as gpd
import pandas as pd
import psycopg2

DB_URL = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"

# King County Parcels Open Data - point centroids
# https://gis-kingcounty.opendata.arcgis.com/datasets/king-county-parcels/
KC_PARCELS_URL = "https://opendata.arcgis.com/api/v3/datasets/e6c555d6ae7542b2bdec92485892b6e6_113/downloads/data?format=geojson&spatialRefId=4326"

# Census geometry for spatial join
CB_BASE = "https://www2.census.gov/geo/tiger/GENZ2020/shp"
WA_FIPS = "53"
CENSUS_URLS = {
    "tract": f"{CB_BASE}/cb_2020_{WA_FIPS}_tract_500k.zip",
    "zcta": f"{CB_BASE}/cb_2020_us_zcta520_500k.zip",
}


def download_census_shapefile(url: str) -> gpd.GeoDataFrame:
    print(f"  Downloading {url.split('/')[-1]} ...")
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            zf.extractall(tmpdir)
        shp = [f for f in os.listdir(tmpdir) if f.endswith(".shp")][0]
        gdf = gpd.read_file(os.path.join(tmpdir, shp))
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)
    return gdf


def main():
    # ── 1. Get unique account IDs from the Seattle panel in GCS ──
    print("=== Loading Seattle panel accounts from GCS ===")
    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket("properlytic-raw-data")
    blob = bucket.blob("panel/jurisdiction=seattle_wa/part.parquet")
    blob.download_to_filename("/tmp/seattle_panel.parquet")
    
    panel_df = pd.read_parquet("/tmp/seattle_panel.parquet")
    acct_col = "parcel_id" if "parcel_id" in panel_df.columns else "acct"
    unique_accts = set(panel_df[acct_col].unique())
    print(f"  Panel has {len(unique_accts):,} unique accounts")
    print(f"  Sample IDs: {list(unique_accts)[:5]}")
    del panel_df
    
    # ── 2. Download King County parcel centroids ──
    print("\n=== Downloading King County parcel data ===")
    # Try GeoJSON endpoint first (smaller, faster)
    try:
        print("  Trying GeoJSON endpoint...")
        gdf = gpd.read_file(KC_PARCELS_URL)
        print(f"  → {len(gdf)} parcels from ArcGIS endpoint")
    except Exception as e:
        print(f"  GeoJSON failed: {e}")
        # Fallback: download shapefile
        SHP_URL = "https://aqua.kingcounty.gov/extranet/assessor/ParcelShapes/parcel_SHP.zip"
        print(f"  Trying shapefile from {SHP_URL}...")
        r = requests.get(SHP_URL, timeout=300, stream=True)
        r.raise_for_status()
        with tempfile.TemporaryDirectory() as tmpdir:
            zpath = os.path.join(tmpdir, "parcels.zip")
            with open(zpath, "wb") as f:
                for chunk in r.iter_content(1024*1024):
                    f.write(chunk)
            with zipfile.ZipFile(zpath) as zf:
                zf.extractall(tmpdir)
            shp = [f for f in os.listdir(tmpdir) if f.endswith(".shp")][0]
            gdf = gpd.read_file(os.path.join(tmpdir, shp))
        if gdf.crs and gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(epsg=4326)
        print(f"  → {len(gdf)} parcels from shapefile")
    
    # ── 3. Match parcel_ids to parcel data ──
    print("\n=== Matching parcel IDs ===")
    print(f"  Parcel columns: {gdf.columns.tolist()[:10]}")
    
    # Find the parcel ID column
    pid_candidates = ["PIN", "PARCEL_ID", "PIN_NUM", "MAJOR", "ACCT"]
    pid_col = None
    for c in pid_candidates:
        if c in gdf.columns:
            pid_col = c
            break
    if pid_col is None:
        # Try case-insensitive
        for c in gdf.columns:
            if c.upper() in [x.upper() for x in pid_candidates]:
                pid_col = c
                break
    
    if pid_col:
        gdf["acct"] = gdf[pid_col].astype(str).str.strip()
    else:
        print(f"  ⚠️ No parcel ID column found. Available: {gdf.columns.tolist()}")
        # Use centroid for all parcels and match later
        gdf["acct"] = gdf.index.astype(str)
    
    # Compute centroids
    gdf["centroid"] = gdf.geometry.centroid
    gdf = gdf.set_geometry("centroid")
    
    # Filter to panel accounts
    matched = gdf[gdf["acct"].isin(unique_accts)]
    print(f"  Matched {len(matched):,} / {len(unique_accts):,} panel accounts")
    
    if len(matched) == 0:
        # Try different ID formats
        print("  Trying string normalization...")
        gdf["acct_norm"] = gdf["acct"].str.lstrip("0")
        unique_accts_norm = {str(a).lstrip("0") for a in unique_accts}
        matched = gdf[gdf["acct_norm"].isin(unique_accts_norm)]
        if len(matched) > 0:
            matched = matched.rename(columns={"acct_norm": "acct"})
        print(f"  After normalization: {len(matched):,} matches")
    
    # ── 4. Spatial join with Census tracts ──
    print("\n=== Spatial join: Census Tracts ===")
    tracts = download_census_shapefile(CENSUS_URLS["tract"])
    geoid_col = "GEOID20" if "GEOID20" in tracts.columns else "GEOID"
    tracts = tracts.rename(columns={geoid_col: "tract_geoid20"})
    joined = gpd.sjoin(matched[["acct", "centroid"]].set_geometry("centroid"), 
                       tracts[["tract_geoid20", "geometry"]], 
                       how="left", predicate="within")
    print(f"  Matched: {joined['tract_geoid20'].notna().sum():,}")
    
    # ── 5. Spatial join with ZCTAs ──
    print("\n=== Spatial join: ZCTAs ===")
    zctas = download_census_shapefile(CENSUS_URLS["zcta"])
    zcta_col = "ZCTA5CE20" if "ZCTA5CE20" in zctas.columns else "ZCTA5CE10"
    zctas = zctas[zctas[zcta_col].str.startswith(("98", "99"))]
    zctas = zctas.rename(columns={zcta_col: "zcta5"})
    joined_z = gpd.sjoin(matched[["acct", "centroid"]].set_geometry("centroid"),
                         zctas[["zcta5", "geometry"]],
                         how="left", predicate="within")
    print(f"  Matched: {joined_z['zcta5'].notna().sum():,}")
    
    # ── 6. Build ladder ──
    ladder = pd.DataFrame({
        "acct": joined["acct"].values,
        "tract_geoid20": joined["tract_geoid20"].values,
        "zcta5": joined_z["zcta5"].values,
        "jurisdiction": "seattle_wa",
    })
    ladder = ladder.drop_duplicates(subset=["acct"])
    print(f"\n=== Ladder: {len(ladder):,} accounts ===")
    
    # ── 7. Upload to Supabase ──
    print("\n=== Uploading to parcel_ladder_v1 ===")
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    cur = conn.cursor()
    
    batch_size = 500
    total = 0
    for i in range(0, len(ladder), batch_size):
        batch = ladder.iloc[i:i+batch_size]
        values = []
        for _, row in batch.iterrows():
            values.append(cur.mogrify(
                "(%s, %s, %s, %s)",
                (row["acct"],
                 row.get("tract_geoid20") if pd.notna(row.get("tract_geoid20")) else None,
                 row.get("zcta5") if pd.notna(row.get("zcta5")) else None,
                 "seattle_wa")
            ).decode())
        
        sql = f"""INSERT INTO public.parcel_ladder_v1 (acct, tract_geoid20, zcta5, jurisdiction)
                  VALUES {','.join(values)}
                  ON CONFLICT (acct) DO UPDATE SET 
                    tract_geoid20 = EXCLUDED.tract_geoid20,
                    zcta5 = EXCLUDED.zcta5,
                    jurisdiction = EXCLUDED.jurisdiction"""
        cur.execute(sql)
        total += len(batch)
        if total % 2000 == 0:
            print(f"  {total:,}/{len(ladder):,} ...")
    
    print(f"  ✅ {total:,} WA accounts in parcel_ladder_v1")
    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
