"""
Populate parcel_ladder_v1 for WA (Seattle) accounts.
Spatial-joins panel coordinates to Census tracts, ZCTAs, and tabblocks.

Prerequisites:
    pip install geopandas pyarrow psycopg2-binary requests

Usage:
    python scripts/inference/populate_wa_parcel_ladder.py
"""
import os
import io
import tempfile
import zipfile
import requests
import geopandas as gpd
import pandas as pd
import psycopg2
from google.cloud import storage

DB_URL = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"
SCHEMA = "forecast_20260220_7f31c6e4"
WA_FIPS = "53"

# Census boundary URLs
CB_BASE = "https://www2.census.gov/geo/tiger/GENZ2020/shp"
URLS = {
    "tract": f"{CB_BASE}/cb_2020_{WA_FIPS}_tract_500k.zip",
    "tabblock": f"https://www2.census.gov/geo/tiger/TIGER2020/TABBLOCK20/tl_2020_{WA_FIPS}_tabblock20.zip",
    "zcta": f"{CB_BASE}/cb_2020_us_zcta520_500k.zip",
}


def download_shapefile(url: str) -> gpd.GeoDataFrame:
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
    print(f"  → {len(gdf)} features")
    return gdf


def main():
    # 1. Download Seattle panel from GCS
    print("=== Downloading Seattle panel from GCS ===")
    client = storage.Client()
    bucket = client.bucket("properlytic-raw-data")
    blob = bucket.blob("panel/jurisdiction=seattle_wa/part.parquet")
    local_panel = "/tmp/seattle_panel.parquet"
    blob.download_to_filename(local_panel)
    
    df = pd.read_parquet(local_panel)
    print(f"Panel: {len(df):,} rows, columns: {list(df.columns)[:15]}")
    
    # Identify acct + coordinate columns
    acct_col = "parcel_id" if "parcel_id" in df.columns else "acct"
    lat_col = "lat" if "lat" in df.columns else "gis_lat" if "gis_lat" in df.columns else None
    lon_col = "lon" if "lon" in df.columns else "gis_lon" if "gis_lon" in df.columns else None
    
    if not lat_col or not lon_col:
        print(f"ERROR: No lat/lon columns found. Available: {df.columns.tolist()}")
        return
    
    # Deduplicate to unique accounts with coords
    accounts = df[[acct_col, lat_col, lon_col]].drop_duplicates(subset=[acct_col])
    accounts = accounts.dropna(subset=[lat_col, lon_col])
    accounts = accounts.rename(columns={acct_col: "acct", lat_col: "lat", lon_col: "lon"})
    print(f"Unique accounts with coords: {len(accounts):,}")
    
    # Create GeoDataFrame from coords
    points = gpd.GeoDataFrame(
        accounts,
        geometry=gpd.points_from_xy(accounts["lon"], accounts["lat"]),
        crs="EPSG:4326"
    )
    
    # 2. Spatial join with Census tracts
    print("\n=== Spatial join: Tracts ===")
    tracts = download_shapefile(URLS["tract"])
    geoid_col = "GEOID20" if "GEOID20" in tracts.columns else "GEOID"
    tracts = tracts.rename(columns={geoid_col: "tract_geoid20"})
    joined = gpd.sjoin(points, tracts[["tract_geoid20", "geometry"]], how="left", predicate="within")
    print(f"  Matched: {joined['tract_geoid20'].notna().sum():,}/{len(joined):,}")
    
    # 3. Spatial join with ZCTAs
    print("\n=== Spatial join: ZCTAs ===")
    zctas = download_shapefile(URLS["zcta"])
    zcta_col = "ZCTA5CE20" if "ZCTA5CE20" in zctas.columns else "ZCTA5CE10"
    zctas = zctas[zctas[zcta_col].str.startswith(("98", "99"))]
    print(f"  Filtered to WA ZCTAs: {len(zctas)}")
    zctas = zctas.rename(columns={zcta_col: "zcta5"})
    
    # Need to reset geometry for second sjoin
    points2 = points.copy()
    joined_zcta = gpd.sjoin(points2, zctas[["zcta5", "geometry"]], how="left", predicate="within")
    print(f"  Matched: {joined_zcta['zcta5'].notna().sum():,}/{len(joined_zcta):,}")
    
    # 4. Spatial join with tabblocks (King County only)
    print("\n=== Spatial join: Tabblocks ===")
    tabblocks = download_shapefile(URLS["tabblock"])
    tb_col = "GEOID20" if "GEOID20" in tabblocks.columns else "GEOID"
    tabblocks = tabblocks[tabblocks[tb_col].str.startswith("53033")]  # King County
    print(f"  King County tabblocks: {len(tabblocks)}")
    tabblocks = tabblocks.rename(columns={tb_col: "tabblock_geoid20"})
    
    points3 = points.copy()
    joined_tb = gpd.sjoin(points3, tabblocks[["tabblock_geoid20", "geometry"]], how="left", predicate="within")
    print(f"  Matched: {joined_tb['tabblock_geoid20'].notna().sum():,}/{len(joined_tb):,}")
    
    # 5. Merge all joins
    ladder = pd.DataFrame({
        "acct": joined["acct"].values,
        "tract_geoid20": joined["tract_geoid20"].values,
        "zcta5": joined_zcta["zcta5"].values,
        "tabblock_geoid20": joined_tb["tabblock_geoid20"].values,
        "jurisdiction": "seattle_wa",
    })
    ladder = ladder.drop_duplicates(subset=["acct"])
    print(f"\n=== Ladder: {len(ladder):,} accounts ===")
    print(f"  With tract: {ladder['tract_geoid20'].notna().sum():,}")
    print(f"  With zcta: {ladder['zcta5'].notna().sum():,}")
    print(f"  With tabblock: {ladder['tabblock_geoid20'].notna().sum():,}")
    
    # 6. Upload to Supabase
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
                "(%s, %s, %s, %s, %s)",
                (row["acct"], 
                 row.get("tabblock_geoid20") if pd.notna(row.get("tabblock_geoid20")) else None,
                 row.get("tract_geoid20") if pd.notna(row.get("tract_geoid20")) else None,
                 row.get("zcta5") if pd.notna(row.get("zcta5")) else None,
                 "seattle_wa")
            ).decode())
        
        sql = f"""INSERT INTO public.parcel_ladder_v1 (acct, tabblock_geoid20, tract_geoid20, zcta5, jurisdiction)
                  VALUES {','.join(values)}
                  ON CONFLICT (acct) DO UPDATE SET 
                    tabblock_geoid20 = EXCLUDED.tabblock_geoid20,
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
