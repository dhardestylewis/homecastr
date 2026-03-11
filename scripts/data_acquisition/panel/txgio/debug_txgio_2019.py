import modal
import os
import io
import zipfile
import tempfile
import glob
import pandas as pd
import geopandas as gpd

app = modal.App("txgio-debug-2019")

image = (
    modal.Image.debian_slim(python_version="3.10")
    .pip_install("google-cloud-storage", "geopandas", "pyarrow", "pyogrio")
)

gcs_secret = modal.Secret.from_name("gcs-creds")

@app.function(
    image=image,
    secrets=[gcs_secret]
)
def debug_txgio_2019():
    import json
    from google.cloud import storage
    
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")
    blob = bucket.blob("txgio/stratmap19-landparcels_48_lp.zip")
    
    print(f"Downloading 2019 zip...")
    tmpdir = tempfile.mkdtemp()
    zip_path = os.path.join(tmpdir, "stratmap19.zip")
    
    # Actually just download a chunk if possible? No, we have to download the whole zip.
    # To be faster, this will take ~40 sec on Modal.
    blob.download_to_filename(zip_path)
    
    print("Extracting...")
    with zipfile.ZipFile(zip_path, 'r') as zf:
        # Just extract ONE shapefile to test! Look for one inside the zip.
        file_list = zf.namelist()
        shp_files = [f for f in file_list if f.endswith(".shp")]
        print(f"Found {len(shp_files)} shapefiles. Testing {shp_files[0]}")
        
        # Need the .shp, .dbf, .shx, .prj
        base = shp_files[0].replace('.shp', '')
        for ext in ['.shp', '.dbf', '.shx', '.prj', '.cpg']:
            if base + ext in file_list:
                zf.extract(base + ext, tmpdir)
        
        target_shp = os.path.join(tmpdir, shp_files[0])
        gdf = gpd.read_file(target_shp, engine="pyogrio", use_arrow=True, ignore_geometry=True)
        print(f"\nRead {len(gdf)} rows from {shp_files[0]}")
        print(f"Columns: {list(gdf.columns)}")
        
        val_cols = [c for c in gdf.columns if 'val' in c.lower() or 'appr' in c.lower()]
        print(f"Value columns found: {val_cols}")
        
        for vc in val_cols:
            print(f"\n--- {vc} ---")
            print(f"Dtype: {gdf[vc].dtype}")
            print(f"Head (first 10):")
            print(gdf[vc].dropna().head(10))
            
            # test numeric coercion
            s = gdf[vc]
            if s.dtype == object or str(s.dtype).startswith('string'):
                s = s.astype(str).str.replace(r'[$,]', '', regex=True)
            num = pd.to_numeric(s, errors='coerce')
            print(f"Successfully coerced to numeric: {num.notna().sum()} / {len(num)}")
            print(f"Greater than 0: {(num > 0).sum()} / {len(num)}")

@app.local_entrypoint()
def main():
    debug_txgio_2019.remote()
