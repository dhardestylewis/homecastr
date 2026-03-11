import modal
import os

app = modal.App("txgio-headers-parallel")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("gdal-bin", "libgdal-dev")
    .pip_install("google-cloud-storage", "pyogrio", "geopandas", "fiona")
)

gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])

@app.function(image=image, secrets=[gcs_secret])
def process_blob(blob_name):
    from google.cloud import storage
    import tempfile, zipfile, glob, json
    import geopandas as gpd

    creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON", "")
    if creds_json:
        with open("/tmp/gcs_creds.json", "w") as f:
            f.write(creds_json)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/gcs_creds.json"
        
    client = storage.Client()
    bucket = client.bucket("properlytic-raw-data")
    blob = bucket.blob(blob_name)
    
    print(f"\n--- {blob.name} ---")
    tmpdir = tempfile.mkdtemp()
    zip_path = os.path.join(tmpdir, "test.zip")
    blob.download_to_filename(zip_path)
    
    with zipfile.ZipFile(zip_path, 'r') as zf:
        zf.extractall(tmpdir)
        
    shp_files = glob.glob(os.path.join(tmpdir, "**/*.shp"), recursive=True)
    gdb_dirs = [d for d in glob.glob(os.path.join(tmpdir, "**/*.gdb"), recursive=True) if os.path.isdir(d)]
    
    res = {"name": blob.name, "cols": [], "samples": {}}
    
    if shp_files:
        file = shp_files[0]
        try:
            gdf = gpd.read_file(file, engine="pyogrio", use_arrow=True, ignore_geometry=True, rows=5)
            res["cols"] = list(gdf.columns)
            for col in gdf.columns:
                if any(k in col.lower() for k in ['id', 'fips', 'cnty', 'state', 'code', 'acct', 'prop']):
                    res["samples"][col] = gdf[col].tolist()
        except Exception as e:
            res["error"] = str(e)
    elif gdb_dirs:
        file = gdb_dirs[0]
        import pyogrio
        try:
            layers = pyogrio.list_layers(file)
            if layers:
                gdf = gpd.read_file(file, layer=layers[0][0], engine="pyogrio", use_arrow=True, ignore_geometry=True, rows=5)
                res["cols"] = list(gdf.columns)
                for col in gdf.columns:
                    if any(k in col.lower() for k in ['id', 'fips', 'cnty', 'state', 'code', 'acct', 'prop']):
                        res["samples"][col] = gdf[col].tolist()
        except Exception as e:
            res["error"] = str(e)
    
    import shutil
    shutil.rmtree(tmpdir, ignore_errors=True)
    return res

@app.local_entrypoint()
def main():
    import os
    from google.cloud import storage
    # We must configure GCS to get the list
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        # Just hardcode the blobs for this 1-off run
        blob_names = [
            "txgio/stratmap19-landparcels_48_lp.zip",
            "txgio/stratmap21-landparcels_48_lp.zip",
            "txgio/stratmap22-landparcels_48_lp.zip",
            "txgio/stratmap23-landparcels_48_lp.zip",
            "txgio/stratmap24-landparcels_48_lp.zip",
            "txgio/stratmap25-landparcels_48_lp.zip",
        ]
    else:
        client = storage.Client()
        bucket = client.bucket("properlytic-raw-data")
        blobs = list(bucket.list_blobs(prefix="txgio/"))
        blob_names = [b.name for b in blobs if b.name.endswith(".zip")]
        
    print(f"Mapping over {len(blob_names)} zips...")
    for res in process_blob.map(blob_names):
        print(f"\n======== {res['name']} ========")
        if "error" in res:
            print("ERROR:", res["error"])
        else:
            print("COLS:", res["cols"])
            for k, v in res["samples"].items():
                print(f"  {k}: {v}")
