"""
Join RPAD (DOF Assessment) panel with historical MapPLUTO geometry.
Produces the final canonical NYC panel parquet file for the World Model pipeline.

Process:
1. Loads the 20M+ row RPAD panel from GCS (panel_fy09_..._fy26.parquet).
2. For each FY in RPAD, finds the corresponding closest vintage of MapPLUTO.
   (e.g., FY19 joins to MapPLUTO 19v1 or 19v2)
3. Performs a spatial join on BBL.
4. Computes Representative Point (centroid-ish) from the polygon.
5. Computes H3 index at resolution 12 from the point.
6. Maps to canonical schema expected by `schema_registry.yaml`.
7. Uploads to `gs://properlytic-raw-data/panel/jurisdiction=nyc/nyc_panel_h3.parquet`.

Usage:
  python join_nyc_spatial.py --inspect   # Dry-run on a single year
  python join_nyc_spatial.py --all       # Full pipeline execution
"""
import os, sys, argparse, io, zipfile, gc
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import h3
from google.cloud import storage
import warnings
warnings.filterwarnings('ignore')

GCS_BUCKET = "properlytic-raw-data"
RPAD_PATH = "nyc/dof_panel/panel_fy09_fy10_fy11_fy12_fy13_fy14_fy15_fy16_fy17_fy18_fy19_fy20_fy21_fy22_fy23_fy24_fy25_fy26.parquet"

# MapPLUTO crosswalk (closest vintage per FY)
# RPAD FY is generally July 1 (e.g., FY19 = July 2018 - June 2019)
# MapPLUTO vintages: e.g. 18v1 (early 2018), 18v2 (late 2018)
# We map each FY to the most appropriate MapPLUTO shapefile we have downloaded.
MAPPLUTO_VINTAGES = {
    "FY09": "nyc/mappluto/mappluto_09v2.zip",
    "FY10": "nyc/mappluto/mappluto_10v2.zip",
    "FY11": "nyc/mappluto/mappluto_11v2.zip",
    "FY12": "nyc/mappluto/mappluto_12v2.zip",
    "FY13": "nyc/mappluto/mappluto_13v2.zip",
    "FY14": "nyc/mappluto/mappluto_14v2.zip",
    "FY15": "nyc/mappluto/mappluto_15v1.zip",
    "FY16": "nyc/mappluto/mappluto_16v2.zip",
    "FY17": "nyc/mappluto/mappluto_17v1.1.zip",
    "FY18": "nyc/mappluto/mappluto_18v2.1.zip",
    "FY19": "nyc/mappluto/mappluto_19v2.zip",
    "FY20": "nyc/mappluto/mappluto_20v8.zip",
    "FY21": "nyc/mappluto/mappluto_21v4.zip",
    "FY22": "nyc/mappluto/mappluto_22v3.zip",
    "FY23": "nyc/mappluto/mappluto_23v3.zip",
    "FY24": "nyc/mappluto/mappluto_24v3.1.zip",
    "FY25": "nyc/mappluto/mappluto_25v3.zip",
    "FY26": "nyc/mappluto/mappluto_25v3.zip", # Use latest available for FY26
}


def load_mappluto_bbls(bucket, zip_path):
    """Load only [BBL, geometry] from a MapPLUTO shapefile directly from GCS ZIP."""
    blob = bucket.blob(zip_path)
    if not blob.exists():
        print(f"  [ERROR] MapPLUTO missing: {zip_path}")
        return None
        
    print(f"  Downloading {zip_path} ({(blob.size or 0)/1e6:.1f}MB)...")
    data = blob.download_as_bytes()
    z = zipfile.ZipFile(io.BytesIO(data))
    
    # Needs to be extracted to disk temporarily because fiona/geopandas 
    # struggles with nested SHP inside ZIP in memory easily on all versions.
    tmp_dir = f"/tmp/mappluto_{os.path.basename(zip_path).split('.')[0]}"
    os.makedirs(tmp_dir, exist_ok=True)
    z.extractall(tmp_dir)
    
    # Find the .shp file or .gdb directory
    shp_path = None
    gdb_path = None
    for root, dirs, files in os.walk(tmp_dir):
        for d in dirs:
            if d.endswith(".gdb"):
                gdb_path = os.path.join(root, d)
                break
        for f in files:
            if f.endswith(".shp"):
                shp_path = os.path.join(root, f)
                break
        if shp_path or gdb_path:
            break
    
    target_path = gdb_path or shp_path
    if not target_path:
        print(f"  [ERROR] No .shp or .gdb found in {zip_path}")
        return None
        
    print(f"  Reading geometry from {target_path}...")
    # Only read the BBL column plus geometry to save memory
    try:
        # For GDB we might need to specify a layer, but usually MapPLUTO is the default/only layer
        if gdb_path:
            import fiona
            layers = fiona.listlayers(gdb_path)
            # Typically named 'MapPLUTO'
            target_layer = next((l for l in layers if 'PLUTO' in l.upper()), layers[0])
            print(f"    Layer: {target_layer}")
            gdf = gpd.read_file(gdb_path, layer=target_layer)
        else:
            gdf = gpd.read_file(shp_path)
        # Find BBL column
        bbl_col = [c for c in gdf.columns if c.upper() == 'BBL']
        if not bbl_col:
            # Maybe it has Boro, Block, Lot separate
            boro_col = [c for c in gdf.columns if c.upper() == 'BORO']
            block_col = [c for c in gdf.columns if c.upper() == 'BLOCK']
            lot_col = [c for c in gdf.columns if c.upper() == 'LOT']
            if boro_col and block_col and lot_col:
                gdf['BBL'] = (
                    gdf[boro_col[0]].astype(str).str.zfill(1) +
                    gdf[block_col[0]].astype(str).str.zfill(5) +
                    gdf[lot_col[0]].astype(str).str.zfill(4)
                )
                bbl_col = ['BBL']
            else:
                print(f"  [ERROR] No BBL column found in {shp_path}. Columns: {gdf.columns}")
                return None
                
        # Keep only BBL and geometry
        gdf = gdf[[bbl_col[0], 'geometry']].copy()
        gdf = gdf.rename(columns={bbl_col[0]: 'BBL'})
        
        # Ensure BBL is string and 10 chars
        gdf['BBL'] = gdf['BBL'].astype(str).str.replace(r'\.0$', '', regex=True)
        gdf = gdf[gdf['BBL'].str.len() == 10].copy()
        
        # Reproject to WGS84 (EPSG:4326) if it's likely State Plane (EPSG:2263)
        if gdf.crs and gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(epsg=4326)
        elif not gdf.crs:
            gdf.set_crs(epsg=2263, inplace=True) # Assume NY State Plane LI
            gdf = gdf.to_crs(epsg=4326)
            
        print(f"  Loaded {len(gdf):,} geometries.")
        # Clean up temp
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return gdf
    except Exception as e:
        print(f"  [ERROR] reading SHP: {e}")
        return None


def calculate_h3(gdf, resolution=12):
    """Calculate representative point and H3 index."""
    # representative_point is guaranteed to be within the polygon
    pts = gdf.geometry.representative_point()
    gdf['lon'] = pts.x
    gdf['lat'] = pts.y
    print("  Calculating H3 indices...")
    # modern h3 API uses latlng_to_cell
    gdf['h3_12'] = gdf.apply(lambda row: h3.latlng_to_cell(row.lat, row.lon, resolution), axis=1)
    return gdf.drop(columns=['geometry'])


def process_year(bucket, fy, df_year):
    """Join one year of RPAD with its corresponding MapPLUTO."""
    print(f"\n{'='*40}")
    print(f"Processing {fy} ({len(df_year):,} RPAD rows)")
    
    zip_path = MAPPLUTO_VINTAGES.get(fy)
    if not zip_path:
        print(f"  [SKIP] No MapPLUTO vintage defined for {fy}")
        return None
        
    gdf_pluto = load_mappluto_bbls(bucket, zip_path)
    if gdf_pluto is None:
        return None
        
    # Drop duplicate BBLs in PLUTO just in case (should be unique but older years might have artifacts)
    gdf_pluto = gdf_pluto.drop_duplicates(subset=['BBL'])
    
    # Compute H3 on the geometry to keep pandas memory low
    df_spatial = calculate_h3(gdf_pluto)
    del gdf_pluto
    gc.collect()
    
    # Merge RPAD with spatial attributes
    # RPAD is the left dataset (we keep all RPAD records even if missing MapPLUTO geometry)
    # Condos will have many RPAD rows per 1 MapPLUTO BBL (the billing BBL vs condo BBLs)
    # WAIT: RPAD has actual condo unit BBLs (7501+ lot #s). 
    # MapPLUTO only has the Condo Billing BBL (usually lot 7501).
    # To join RPAD unit BBLs to MapPLUTO, we need to map the unit BBL back to the billing BBL.
    # In NYC, Condo units are Lots 1001+. The billing lot is usually 7501.
    
    df_year = df_year.copy()
    
    # Identify condo units
    df_year['LOT_NUM'] = df_year['BBL'].str[6:10].astype(int)
    is_condo = df_year['LOT_NUM'] >= 1001
    
    print(f"  Identified {is_condo.sum():,} condo units out of {len(df_year):,}")
    
    # Strategy: 
    # Try an exact BBL join first.
    # For unmatched BBLs (mostly condo units), create a 'Condo Billing BBL' by setting lot to 7501
    # and try joining again.
    
    # Join 1: Exact matches
    merged_exact = pd.merge(df_year, df_spatial, on='BBL', how='left')
    has_geom = merged_exact['h3_12'].notna()
    
    print(f"  Exact BBL matches: {has_geom.sum():,} ({(has_geom.sum()/len(df_year))*100:.1f}%)")
    
    # Join 2: For unmatched, try Condo Billing BBL
    missing_geom = merged_exact[~has_geom].copy()
    if len(missing_geom) > 0:
        missing_geom = missing_geom.drop(columns=['lon', 'lat', 'h3_12'])
        # Construct billing BBL (boro + block + 7501)
        missing_geom['BILLING_BBL'] = missing_geom['BBL'].str[0:6] + '7501'
        
        # Join against PLUTO using the Billing BBL
        df_spatial_billing = df_spatial.rename(columns={'BBL': 'BILLING_BBL'})
        merged_condo = pd.merge(missing_geom, df_spatial_billing, on='BILLING_BBL', how='left')
        
        has_geom_condo = merged_condo['h3_12'].notna()
        print(f"  Condo BBL matches: {has_geom_condo.sum():,} out of {len(missing_geom):,} unmatched")
        
        # Recombine
        merged_condo = merged_condo.drop(columns=['BILLING_BBL'])
        
        final_merged = pd.concat([
            merged_exact[has_geom],
            merged_condo
        ], ignore_index=True)
    else:
        final_merged = merged_exact
        
    # Stats
    total_has_geom = final_merged['h3_12'].notna().sum()
    print(f"  Final Join Rate: {total_has_geom:,} / {len(final_merged):,} ({(total_has_geom/len(final_merged))*100:.1f}%)")
    
    return final_merged


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--inspect", action="store_true", help="Run only on FY24 as a test")
    parser.add_argument("--all", action="store_true", help="Run full pipeline")
    args = parser.parse_args()
    
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    
    print(f"Loading RPAD panel (~600MB) from gs://{GCS_BUCKET}/{RPAD_PATH}...")
    blob = bucket.blob(RPAD_PATH)
    data = blob.download_as_bytes()
    df_rpad = pd.read_parquet(io.BytesIO(data))
    
    print(f"Loaded {len(df_rpad):,} RPAD rows.")
    
    fys_to_process = ["FY24"] if args.inspect else sorted(df_rpad["FY"].unique())
    
    all_years = []
    
    for fy in fys_to_process:
        df_year = df_rpad[df_rpad["FY"] == fy]
        res = process_year(bucket, fy, df_year)
        if res is not None:
            all_years.append(res)
            
    if not all_years:
        print("No data processed!")
        return
        
    final_panel = pd.concat(all_years, ignore_index=True)
    
    # Canonical Column Mapping for World Model schema registry
    print("\nMapping to canonical schema...")
    
    # parcel_id (string): Unique identifier for the property (BBL)
    final_panel['parcel_id'] = final_panel['BBL']
    
    # For year we convert FY24 -> 2024
    final_panel['year'] = final_panel['FY'].str.replace('FY', '').astype(int) + 2000
    
    # Target value (Market Total) -> total_appraised_value
    # RPAD has CURMKTTOT (Current Market Total) and FINACTTOT (Final Assessed)
    # The world model expects total market value.
    if 'CURMKTTOT' in final_panel.columns:
        final_panel['total_appraised_value'] = final_panel['CURMKTTOT']
    else:
        final_panel['total_appraised_value'] = np.nan
        
    # We will also keep the raw columns so we have FINACTTOT if needed.
    # Write to final Parquet
    out_name = f"nyc_panel_h3{'_inspect' if args.inspect else ''}.parquet"
    out_path = f"panel/jurisdiction=nyc/{out_name}"
    
    print(f"Writing {len(final_panel):,} rows to gs://{GCS_BUCKET}/{out_path}...")
    buf = io.BytesIO()
    final_panel.to_parquet(buf, index=False)
    size = buf.tell()
    buf.seek(0)
    
    out_blob = bucket.blob(out_path)
    out_blob.upload_from_file(buf, content_type="application/octet-stream")
    print(f"Upload complete ({size/1e6:.1f}MB).")
    
if __name__ == "__main__":
    main()
