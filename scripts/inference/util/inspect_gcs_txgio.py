import modal, os

app = modal.App('txgio-inspector')
image = modal.Image.debian_slim(python_version='3.11').pip_install('google-cloud-storage', 'pandas', 'pyarrow', 'geopandas', 'fiona')
gcs_secret = modal.Secret.from_name('gcs-creds')

@app.function(image=image, secrets=[gcs_secret], timeout=1800)
def inspect(year: int):
    import json, zipfile, io, os, geopandas as gpd
    from google.cloud import storage
    creds = json.loads(os.environ['GOOGLE_APPLICATION_CREDENTIALS_JSON'])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket('properlytic-raw-data')
    blob_name = f'txgio/stratmap{str(year)[2:]}-landparcels_48_lp.zip'
    blob = bucket.blob(blob_name)
    print(f'Downloading {blob_name} ...')
    blob.download_to_filename('/tmp/txgio.zip')
    
    with zipfile.ZipFile('/tmp/txgio.zip', 'r') as zf:
        shps = [n for n in zf.namelist() if n.endswith('.shp') and '48003' in n]
        if not shps:
            shps = [n for n in zf.namelist() if n.endswith('.shp')]
        first = shps[0]
        base = first.replace('.shp', '')
        print(f'Extracting {base}.*')
        for ext in ['.shp', '.shx', '.dbf']:
            zf.extract(base + ext, '/tmp')
        
        gdf = gpd.read_file(f'/tmp/{first}', rows=5)
        return {
            "year": year,
            "columns": gdf.columns.tolist(),
            "sample_row": gdf.iloc[0].to_dict() if len(gdf) > 0 else None
        }

@app.local_entrypoint()
def main():
    for year in [2022, 2023]:
        res = inspect.remote(year)
        print(f"\n=== {year} ===")
        print(f"Columns: {res['columns']}")
        import pprint
        pprint.pprint(res['sample_row'])

