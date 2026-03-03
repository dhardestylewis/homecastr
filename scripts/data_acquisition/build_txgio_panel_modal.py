"""
Build TxGIO Cross-Year Panel for World Model Training
=====================================================
Reads TxGIO parcel shapefiles from GCS (one zip per year),
extracts parcel IDs + assessed values, builds cross-year panel.

Output: gs://properlytic-raw-data/panel/jurisdiction=txgio_texas/part.parquet
"""

import modal, os

app = modal.App("txgio-panel-builder")
image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("gdal-bin", "libgdal-dev")
    .pip_install("google-cloud-storage", "pandas", "pyarrow", "geopandas", "fiona")
)
gcs_secret = modal.Secret.from_name("gcs-creds")


@app.function(
    image=image,
    secrets=[gcs_secret],
    timeout=14400,  # 4h — large files
    memory=32768,
    cpu=4,
)
def build_txgio_panel():
    """Download TxGIO zips from GCS, extract parcel values, build panel."""
    import json, io, time, tempfile, zipfile, glob
    import pandas as pd
    import geopandas as gpd
    from google.cloud import storage

    ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")

    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")

    # Find all TxGIO zips on GCS
    print(f"[{ts()}] Finding TxGIO files on GCS...")
    txgio_blobs = [b for b in bucket.list_blobs(prefix="txgio/") if b.name.endswith(".zip")]
    print(f"  Found {len(txgio_blobs)} zip files:")
    for b in txgio_blobs:
        print(f"    {b.size/1e9:.1f} GB  {b.name}")

    all_years = []

    for blob in txgio_blobs:
        # Extract year from filename like stratmap24-landparcels_48_lp.zip
        name = blob.name.split("/")[-1]
        try:
            yr_str = name.split("-")[0].replace("stratmap", "")
            year = int(yr_str) + 2000 if int(yr_str) < 100 else int(yr_str)
        except (ValueError, IndexError):
            print(f"  ⚠️ Can't parse year from {name}, skipping")
            continue

        print(f"\n[{ts()}] Processing year {year} ({blob.name}, {blob.size/1e9:.1f} GB)...")

        # Download zip to temp
        tmpdir = tempfile.mkdtemp()
        zip_path = os.path.join(tmpdir, name)
        print(f"  Downloading...")
        blob.download_to_filename(zip_path)
        print(f"  Downloaded: {os.path.getsize(zip_path)/1e9:.1f} GB")

        # Extract and find shapefiles
        print(f"  Extracting...")
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(tmpdir)

        # Find all .shp files
        shp_files = glob.glob(os.path.join(tmpdir, "**/*.shp"), recursive=True)
        print(f"  Found {len(shp_files)} shapefiles")

        year_dfs = []
        for shp_path in shp_files:
            try:
                gdf = gpd.read_file(shp_path, ignore_geometry=True)
                year_dfs.append(gdf)
                if len(year_dfs) == 1:
                    print(f"  Columns: {list(gdf.columns[:20])}{'...' if len(gdf.columns) > 20 else ''}")
            except Exception as e:
                print(f"  ⚠️ Error reading {shp_path}: {e}")

        if not year_dfs:
            print(f"  ⚠️ No valid data for {year}")
            continue

        df = pd.concat(year_dfs, ignore_index=True)
        print(f"  {year}: {len(df):,} parcels")

        # Identify key columns (TxGIO uses various naming conventions)
        # Parcel ID columns: Explicit priority for TxGIO (PROP_ID often used in 2022/2023)
        id_candidates = [c for c in df.columns if c.lower() in ['prop_id', 'pid', 'acct', 'geo_id', 'parcel_id', 'account', 'situs']]
        if not id_candidates:
            # Fallback to looser match
            id_candidates = [c for c in df.columns if any(k in c.lower() for k in
                            ['prop_id', 'pid', 'geo_id', 'parcel_id', 'acct', 'account', 'situs'])]
        
        id_col = id_candidates[0] if id_candidates else df.columns[0]

        # Value columns
        val_mapping = {}
        for col in df.columns:
            cl = col.lower()
            if 'tot' in cl and ('val' in cl or 'appr' in cl or 'mkt' in cl):
                val_mapping['total_value'] = col
            elif 'land' in cl and ('val' in cl or 'appr' in cl):
                val_mapping['land_value'] = col
            elif 'impr' in cl and ('val' in cl or 'appr' in cl):
                val_mapping['improvement_value'] = col
            elif 'mkt' in cl and 'val' in cl and 'tot' not in cl:
                val_mapping['market_value'] = col

        print(f"  ID col: {id_col}")
        print(f"  Value cols: {val_mapping}")

        # Build year dataframe
        result = pd.DataFrame()
        result['acct'] = df[id_col].astype(str)
        result['year'] = year

        for friendly_name, raw_col in val_mapping.items():
            result[friendly_name] = pd.to_numeric(df[raw_col], errors='coerce')

        # Use total_value as primary, fall back to market_value
        if 'total_value' in result.columns:
            result['value'] = result['total_value']
        elif 'market_value' in result.columns:
            result['value'] = result['market_value']
        else:
            # Sum land + improvement
            land = result.get('land_value', 0)
            impr = result.get('improvement_value', 0)
            result['value'] = pd.to_numeric(land, errors='coerce').fillna(0) + pd.to_numeric(impr, errors='coerce').fillna(0)

        # Drop zero/null values
        result = result[result['value'] > 0].copy()
        print(f"  After filtering: {len(result):,} parcels with value > 0")
        print(f"  Value stats: mean=${result['value'].mean():,.0f} median=${result['value'].median():,.0f}")

        all_years.append(result)

        # Cleanup
        import shutil
        shutil.rmtree(tmpdir, ignore_errors=True)

    if not all_years:
        print("❌ No data!")
        return {"error": "no data"}

    # Combine all years
    panel = pd.concat(all_years, ignore_index=True)
    panel = panel.sort_values(["acct", "year"]).reset_index(drop=True)

    # Compute growth
    panel = panel.sort_values(["acct", "year"])
    panel["prior_value"] = panel.groupby("acct")["value"].shift(1)
    panel["growth_pct"] = (panel["value"] - panel["prior_value"]) / panel["prior_value"]

    print(f"\n[{ts()}] Panel built:")
    print(f"  Total rows: {panel.shape[0]:,}")
    print(f"  Unique parcels: {panel['acct'].nunique():,}")
    print(f"  Years: {sorted(panel['year'].unique())}")
    print(f"\n  Value summary:")
    print(panel[['value', 'growth_pct']].describe().to_string())

    # Save to GCS
    print(f"\n[{ts()}] Uploading to GCS...")
    buf = io.BytesIO()
    panel.to_parquet(buf, index=False)
    size_mb = buf.tell() / 1e6
    buf.seek(0)

    blob = bucket.blob("panel/jurisdiction=txgio_texas/part.parquet")
    blob.upload_from_file(buf, content_type="application/octet-stream")
    print(f"  ✅ gs://properlytic-raw-data/panel/jurisdiction=txgio_texas/part.parquet ({size_mb:.0f} MB)")

    summary = {
        "n_rows": len(panel),
        "n_parcels": int(panel["acct"].nunique()),
        "years": sorted(panel["year"].unique().tolist()),
        "value_mean": float(panel["value"].mean()),
        "value_median": float(panel["value"].median()),
    }
    blob = bucket.blob("panel/jurisdiction=txgio_texas/summary.json")
    blob.upload_from_string(json.dumps(summary, indent=2))
    print(f"  ✅ Summary saved")

    return summary


@app.local_entrypoint()
def main():
    result = build_txgio_panel.remote()
    print(f"\n✅ TxGIO panel: {result}")
