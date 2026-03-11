"""
Join RPAD (DOF Assessment) panel with historical MapPLUTO geometry using Modal.
Produces the final canonical NYC panel parquet file for the World Model pipeline.

Process:
1. Loads the 20M+ row RPAD panel from GCS.
2. In parallel (via Modal), for each FY:
   a. Downloads the corresponding MapPLUTO vintage.
   b. Performs a spatial join on BBL (Exact match -> Condo Billing BBL fallback).
   c. Computes Representative Point and H3 index.
3. Collects all FYs, maps to canonical schema, and uploads to GCS.

Usage:
  modal run scripts_new/data_acquisition/panel/join_nyc_spatial_modal.py --inspect
  modal run scripts_new/data_acquisition/panel/join_nyc_spatial_modal.py --all
"""
import os, sys, io, zipfile, gc
import pandas as pd
import numpy as np
from google.cloud import storage
import modal

GCS_BUCKET = "properlytic-raw-data"
RPAD_PATH = "nyc/dof_panel/panel_fy09_fy10_fy11_fy12_fy13_fy14_fy15_fy16_fy17_fy18_fy19_fy20_fy21_fy22_fy23_fy24_fy25_fy26.parquet"

# RPAD FY -> MapPLUTO vintage
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
    "FY21": "nyc/mappluto/mappluto_21v3.zip",
    "FY22": "nyc/mappluto/mappluto_22v3.zip",
    "FY23": "nyc/mappluto/mappluto_23v3.1.zip",
    "FY24": "nyc/mappluto/mappluto_24v4.1.zip",
    "FY25": "nyc/mappluto/mappluto_25v3.1.zip",
    "FY26": "nyc/mappluto/mappluto_25v3.1.zip", 
}

# Modal Setup
image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "pandas==2.2.0",
        "geopandas==0.14.3",
        "pyogrio",
        "shapely",
        "h3==4.1.2",
        "pyarrow",
        "google-cloud-storage",
    )
)
app = modal.App(name="dhl-nyc-spatial-join")

@app.function(
    image=image,
    secrets=[modal.Secret.from_name("gcs-creds")],
    timeout=3600,
    memory=8192,
    cpu=4.0
)
def process_year(fy_label: str, df_year_bytes: bytes) -> bytes:
    """Process a single FY of RPAD data against its MapPLUTO vintage."""
    import geopandas as gpd
    import h3
    
    print(f"[{fy_label}] Loading RPAD subset...")
    df_year = pd.read_parquet(io.BytesIO(df_year_bytes))
    print(f"[{fy_label}] RPAD subset has {len(df_year):,} rows.")
    
    import json
    
    zip_path = MAPPLUTO_VINTAGES.get(fy_label)
    if not zip_path:
        print(f"[{fy_label}] SKIP: No MapPLUTO vintage defined.")
        return b""
        
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(zip_path)
    if not blob.exists():
        print(f"[{fy_label}] ERROR: MapPLUTO file missing: {zip_path}")
        return b""
        
    print(f"[{fy_label}] Downloading {zip_path} ({(blob.size or 0)/1e6:.1f}MB)...")
    
    import time
    max_retries = 3
    data = None
    for attempt in range(max_retries):
        try:
            buf = io.BytesIO()
            blob.download_to_file(buf)
            data = buf.getvalue()
            break
        except Exception as e:
            print(f"[{fy_label}] Download attempt {attempt+1} failed: {e}")
            if attempt == max_retries - 1:
                return b""
            time.sleep(2)
            
    z = zipfile.ZipFile(io.BytesIO(data))
    
    tmp_dir = f"/tmp/mappluto_{fy_label}"
    os.makedirs(tmp_dir, exist_ok=True)
    z.extractall(tmp_dir)
    
    # Extract any nested zips
    for root, dirs, files in os.walk(tmp_dir):
        for f in files:
            if f.lower().endswith(".zip"):
                nested_zip_path = os.path.join(root, f)
                try:
                    with zipfile.ZipFile(nested_zip_path) as nz:
                        nz.extractall(root)
                except Exception as e:
                    print(f"[{fy_label}] ERROR extracting nested zip {f}: {e}")
                    
    target_path = None
    shp_paths = []
    
    for root, dirs, files in os.walk(tmp_dir):
        for d in dirs:
            if d.endswith(".gdb"):
                target_path = os.path.join(root, d)
                break
        for f in files:
            if f.endswith(".shp") and "pluto" in f.lower():
                shp_paths.append(os.path.join(root, f))
        if target_path:
            break
            
    if not target_path and not shp_paths:
        print(f"[{fy_label}] ERROR: No *pluto*.shp or .gdb found.")
        return b""
        
    try:
        if target_path and target_path.endswith(".gdb"):
            print(f"[{fy_label}] Reading geometry from GDB: {target_path}...")
            import pyogrio
            layers = pyogrio.list_layers(target_path)
            target_layer = next((l[0] for l in layers if 'PLUTO' in l[0].upper()), layers[0][0])
            gdf_pluto = gpd.read_file(target_path, layer=target_layer, engine="pyogrio")
        else:
            print(f"[{fy_label}] Reading geometry from {len(shp_paths)} SHP files...")
            gdfs = []
            for sp in shp_paths:
                print(f"[{fy_label}]   Loading {os.path.basename(sp)}")
                gdfs.append(gpd.read_file(sp, engine="pyogrio"))
            gdf_pluto = pd.concat(gdfs, ignore_index=True)
            
        bbl_col = [c for c in gdf_pluto.columns if c.upper() == 'BBL']
        if not bbl_col:
            boro_col = [c for c in gdf_pluto.columns if c.upper() == 'BORO']
            block_col = [c for c in gdf_pluto.columns if c.upper() == 'BLOCK']
            lot_col = [c for c in gdf_pluto.columns if c.upper() == 'LOT']
            if boro_col and block_col and lot_col:
                gdf_pluto['BBL'] = (
                    gdf_pluto[boro_col[0]].astype(str).str.zfill(1) +
                    gdf_pluto[block_col[0]].astype(str).str.zfill(5) +
                    gdf_pluto[lot_col[0]].astype(str).str.zfill(4)
                )
                bbl_col = ['BBL']
            else:
                print(f"[{fy_label}] ERROR: No BBL column found.")
                return b""
                
        gdf_pluto = gdf_pluto[[bbl_col[0], 'geometry']].copy()
        gdf_pluto = gdf_pluto.rename(columns={bbl_col[0]: 'BBL'})
        gdf_pluto['BBL'] = gdf_pluto['BBL'].astype(str).str.replace(r'\.0$', '', regex=True)
        gdf_pluto = gdf_pluto[gdf_pluto['BBL'].str.len() == 10]
        gdf_pluto = gdf_pluto.drop_duplicates(subset=['BBL'])
        
        if gdf_pluto.crs and gdf_pluto.crs.to_epsg() != 4326:
            gdf_pluto = gdf_pluto.to_crs(epsg=4326)
        elif not gdf_pluto.crs:
            gdf_pluto.set_crs(epsg=2263, inplace=True)
            gdf_pluto = gdf_pluto.to_crs(epsg=4326)
            
        print(f"[{fy_label}] Loaded {len(gdf_pluto):,} geometries.")
    except Exception as e:
        print(f"[{fy_label}] ERROR reading SHP/GDB: {e}")
        return b""
        
    print(f"[{fy_label}] Calculating H3 indices...")
    pts = gdf_pluto.geometry.representative_point()
    gdf_pluto['lon'] = pts.x
    gdf_pluto['lat'] = pts.y
    gdf_pluto['h3_12'] = gdf_pluto.apply(lambda row: h3.latlng_to_cell(row.lat, row.lon, 12), axis=1)
    df_spatial = gdf_pluto.drop(columns=['geometry'])
    del gdf_pluto
    gc.collect()
    
    print(f"[{fy_label}] Performing spatial join...")
    # Join 1: Exact matches
    merged_exact = pd.merge(df_year, df_spatial, on='BBL', how='left')
    has_geom = merged_exact['h3_12'].notna()
    print(f"[{fy_label}] Exact BBL matches: {has_geom.sum():,} ({(has_geom.sum()/len(df_year))*100:.1f}%)")
    
    # Join 2: Condo Billing BBL
    missing_geom = merged_exact[~has_geom].copy()
    if len(missing_geom) > 0:
        missing_geom = missing_geom.drop(columns=['lon', 'lat', 'h3_12'])
        missing_geom['BILLING_BBL'] = missing_geom['BBL'].str[0:6] + '7501'
        df_spatial_billing = df_spatial.rename(columns={'BBL': 'BILLING_BBL'})
        merged_condo = pd.merge(missing_geom, df_spatial_billing, on='BILLING_BBL', how='left')
        has_geom_condo = merged_condo['h3_12'].notna()
        print(f"[{fy_label}] Condo BBL matches: {has_geom_condo.sum():,} out of {len(missing_geom):,} unmatched")
        merged_condo = merged_condo.drop(columns=['BILLING_BBL'])
        
        final_merged = pd.concat([merged_exact[has_geom], merged_condo], ignore_index=True)
    else:
        final_merged = merged_exact
        
    total_has_geom = final_merged['h3_12'].notna().sum()
    print(f"[{fy_label}] Final Join Rate: {total_has_geom:,} / {len(final_merged):,} ({(total_has_geom/len(final_merged))*100:.1f}%)")
    
    # Clean up
    import shutil
    shutil.rmtree(tmp_dir, ignore_errors=True)
    
    # Serialize to bytes
    buf = io.BytesIO()
    final_merged.to_parquet(buf, index=False)
    return buf.getvalue()


@app.local_entrypoint()
def main(inspect: bool = False, all: bool = False):
    import json
    if "GOOGLE_APPLICATION_CREDENTIALS_JSON" in os.environ:
        creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
        client = storage.Client.from_service_account_info(creds)
    else:
        client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    
    print(f"Loading RPAD panel from gs://{GCS_BUCKET}/{RPAD_PATH}...")
    blob = bucket.blob(RPAD_PATH)
    data = blob.download_as_bytes()
    df_rpad = pd.read_parquet(io.BytesIO(data))
    
    print(f"Loaded {len(df_rpad):,} RPAD rows.")
    
    fys_to_process = ["FY24", "FY25"] if inspect else sorted(df_rpad["FY"].unique())
    
    # Prep payloads to minimize data transfer per worker
    print("Preparing payloads for Modal...")
    payloads = []
    for fy in fys_to_process:
        df_year = df_rpad[df_rpad["FY"] == fy]
        buf = io.BytesIO()
        df_year.to_parquet(buf, index=False)
        payloads.append((fy, buf.getvalue()))
        
    print(f"Launching {len(payloads)} workers on Modal...")
    
    # Run in parallel
    results_bytes = list(process_year.starmap(payloads))
    
    print("Gathering results...")
    all_years = []
    for fy, b in zip(fys_to_process, results_bytes):
        if b:
            all_years.append(pd.read_parquet(io.BytesIO(b)))
            
    if not all_years:
        print("No data processed!")
        return
        
    final_panel = pd.concat(all_years, ignore_index=True)
    
    print("\nMapping to canonical schema...")
    final_panel['parcel_id'] = final_panel['BBL']
    final_panel['year'] = final_panel['FY'].str.replace('FY', '').astype(int) + 2000
    
    if 'CURMKTTOT' in final_panel.columns:
        final_panel['total_appraised_value'] = final_panel['CURMKTTOT']
    else:
        final_panel['total_appraised_value'] = np.nan
        
    out_name = f"nyc_panel_h3{'_inspect' if inspect else ''}.parquet"
    out_path = f"panel/jurisdiction=nyc/{out_name}"
    
    print(f"Writing {len(final_panel):,} rows to gs://{GCS_BUCKET}/{out_path}...")
    buf = io.BytesIO()
    final_panel.to_parquet(buf, index=False)
    size = buf.tell()
    buf.seek(0)
    
    out_blob = bucket.blob(out_path)
    out_blob.upload_from_file(buf, content_type="application/octet-stream")
    print(f"Upload complete ({size/1e6:.1f}MB).")
