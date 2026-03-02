"""Upload fixed worldmodel.py to GCS + Build panels for Cook County, NY State, France DVF."""
import modal, os

app = modal.App("multi-jurisdiction-build")
image = modal.Image.debian_slim(python_version="3.11").pip_install(
    "google-cloud-storage", "polars", "pyarrow", "pandas"
)
gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])


@app.function(image=image, secrets=[gcs_secret], timeout=600, memory=16384)
def upload_worldmodel():
    """Upload the fixed worldmodel.py from local mount."""
    import json
    from google.cloud import storage
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")
    
    # Read the worldmodel.py content that was embedded via the mount
    import importlib.resources
    # The file is embedded in the mount — read from the script dir
    wm_path = "/root/worldmodel.py"
    if os.path.exists(wm_path):
        blob = bucket.blob("code/worldmodel.py")
        blob.upload_from_filename(wm_path)
        print(f"Uploaded worldmodel.py ({blob.size} bytes)")
    else:
        print(f"worldmodel.py not found at {wm_path}")
        # List what we have
        for d in ["/root", "/tmp"]:
            if os.path.exists(d):
                print(f"{d}: {os.listdir(d)[:10]}")


@app.function(image=image, secrets=[gcs_secret], timeout=1800, memory=32768)
def build_panel(jurisdiction: str) -> str:
    """Build a panel for one jurisdiction from raw GCS data."""
    import json, io
    import pandas as pd
    import polars as pl
    from google.cloud import storage
    
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")
    
    SOURCES = {
        "cook_county_il": {
            "gcs_prefix": "cook_county_il/assessed_values",
            "mapping": {
                "parcel_id": "pin", "year": "year", "dwelling_type": "class",
                "improvement_value": "certified_bldg", "land_value": "certified_land",
                "assessed_value": "certified_tot",
            },
        },
        "ny_state": {
            "gcs_prefix": "ny_state",
            "mapping": {
                "parcel_id": "PRINT_KEY_ID", "year": "ROLL_YEAR",
                "assessed_value": "FULL_MARKET_VALUE",
                "land_value": "LAND_AV", "improvement_value": "TOTAL_AV",
                "dwelling_type": "PROPERTY_CLASS",
                "sqft": "SQ_FT_OF_LIVING_AREA", "year_built": "YEAR_BUILT",
                "address": "LOCATION",
            },
        },
        "france_dvf": {
            "gcs_prefix": "france_dvf",
            "mapping": {
                "parcel_id": "id_parcelle", "sale_price": "valeur_fonciere",
                "sale_date": "date_mutation", "dwelling_type": "type_local",
                "sqft": "surface_reelle_bati", "land_area": "surface_terrain",
                "bedrooms": "nombre_pieces_principales",
                "lat": "latitude", "lon": "longitude",
            },
        },
    }
    
    if jurisdiction not in SOURCES:
        return json.dumps({"error": f"Unknown jurisdiction: {jurisdiction}"})
    
    cfg = SOURCES[jurisdiction]
    prefix = cfg["gcs_prefix"]
    mapping = cfg["mapping"]
    
    print(f"[BUILD] Starting panel build for {jurisdiction}")
    print(f"[BUILD] Listing blobs under {prefix}/")
    
    # List and download all chunks
    blobs = list(bucket.list_blobs(prefix=f"{prefix}/"))
    data_blobs = [b for b in blobs if b.name.endswith(('.csv', '.csv.gz', '.parquet'))]
    print(f"[BUILD] Found {len(data_blobs)} data files")
    
    if not data_blobs:
        return json.dumps({"error": f"No data files found under {prefix}/"})
    
    all_dfs = []
    for i, blob in enumerate(data_blobs):
        print(f"[BUILD] Reading {blob.name} ({i+1}/{len(data_blobs)}) ({blob.size/1e6:.1f}MB)")
        content = blob.download_as_bytes()
        
        if blob.name.endswith('.parquet'):
            df = pd.read_parquet(io.BytesIO(content))
        elif blob.name.endswith('.csv.gz'):
            df = pd.read_csv(io.BytesIO(content), compression='gzip', low_memory=False)
        else:
            df = pd.read_csv(io.BytesIO(content), low_memory=False)
        
        # Map columns
        mapped = pd.DataFrame()
        for canon_col, src_col in mapping.items():
            if src_col in df.columns:
                mapped[canon_col] = df[src_col]
        
        mapped["jurisdiction"] = jurisdiction
        all_dfs.append(mapped)
        print(f"  → {len(mapped)} rows, cols: {list(mapped.columns)}")
    
    # Concatenate
    panel = pd.concat(all_dfs, ignore_index=True)
    print(f"[BUILD] Combined panel: {len(panel):,} rows, {len(panel.columns)} cols")
    
    # Derive year from sale_date if needed
    if "year" not in panel.columns and "sale_date" in panel.columns:
        panel["sale_date"] = pd.to_datetime(panel["sale_date"], errors="coerce")
        panel["year"] = panel["sale_date"].dt.year
        print(f"[BUILD] Derived year from sale_date")
    
    # Coalesce property_value
    val_cols = [c for c in ["sale_price", "assessed_value"] if c in panel.columns]
    if val_cols:
        panel["property_value"] = panel[val_cols].bfill(axis=1).iloc[:, 0]
    
    # Drop rows with no parcel_id or year
    before = len(panel)
    panel = panel.dropna(subset=["parcel_id", "year"])
    panel["year"] = panel["year"].astype(int)
    print(f"[BUILD] After dropna: {len(panel):,} rows (dropped {before - len(panel):,})")
    
    # Join FRED contextual data
    print("[BUILD] Joining FRED contextual data...")
    fred_series = {
        "MORTGAGE30US.csv": "mortgage_rate_30yr",
        "DGS10.csv": "treasury_10yr",
        "FEDFUNDS.csv": "fed_funds_rate",
        "CPIAUCSL.csv": "cpi",
        "UNRATE.csv": "unemployment_rate",
        "CSUSHPINSA.csv": "case_shiller_hpi",
        "USSTHPI.csv": "fhfa_hpi_national",
    }
    for fname, col_name in fred_series.items():
        try:
            blob = bucket.blob(f"fred/{fname}")
            if blob.exists():
                content = blob.download_as_bytes()
                fred_df = pd.read_csv(io.BytesIO(content))
                fred_df.columns = [c.strip() for c in fred_df.columns]
                if "DATE" in fred_df.columns:
                    fred_df["DATE"] = pd.to_datetime(fred_df["DATE"], errors="coerce")
                    fred_df["year"] = fred_df["DATE"].dt.year
                    # Annual average
                    val_col = [c for c in fred_df.columns if c not in ("DATE", "year")][0]
                    fred_df[val_col] = pd.to_numeric(fred_df[val_col], errors="coerce")
                    annual = fred_df.groupby("year")[val_col].mean().reset_index()
                    annual.columns = ["year", col_name]
                    panel = panel.merge(annual, on="year", how="left")
                    print(f"  Joined {col_name}")
        except Exception as e:
            print(f"  Skipped {col_name}: {e}")
    
    # Join LEHD data
    print("[BUILD] Joining LEHD employment data...")
    try:
        lehd_blobs = list(bucket.list_blobs(prefix="lehd/"))
        lehd_frames = []
        for lb in lehd_blobs:
            if lb.name.endswith('.csv.gz'):
                content = lb.download_as_bytes()
                ldf = pd.read_csv(io.BytesIO(content), compression='gzip', low_memory=False)
                lehd_frames.append(ldf)
        if lehd_frames:
            lehd_all = pd.concat(lehd_frames, ignore_index=True)
            # Aggregate to state level by year
            if "year" in lehd_all.columns and "C000" in lehd_all.columns:
                lehd_annual = lehd_all.groupby("year").agg(
                    lehd_total_jobs=("C000", "sum"),
                    lehd_retail_jobs=("CNS07", "sum") if "CNS07" in lehd_all.columns else ("C000", "count"),
                    lehd_finance_jobs=("CNS10", "sum") if "CNS10" in lehd_all.columns else ("C000", "count"),
                ).reset_index()
                panel = panel.merge(lehd_annual, on="year", how="left")
                print(f"  Joined LEHD ({len(lehd_annual)} years)")
    except Exception as e:
        print(f"  Skipped LEHD: {e}")
    
    # Join FEMA disaster data
    print("[BUILD] Joining FEMA disaster data...")
    try:
        blob = bucket.blob("fema/disaster_declarations.csv")
        if blob.exists():
            content = blob.download_as_bytes()
            fema = pd.read_csv(io.BytesIO(content), low_memory=False)
            if "fyDeclared" in fema.columns:
                fema_annual = fema.groupby("fyDeclared").size().reset_index(name="fema_disaster_count")
                fema_annual.columns = ["year", "fema_disaster_count"]
                panel = panel.merge(fema_annual, on="year", how="left")
                panel["fema_disaster_count"] = panel["fema_disaster_count"].fillna(0).astype(int)
                print("  Joined FEMA disasters")
    except Exception as e:
        print(f"  Skipped FEMA: {e}")
    
    # Write to GCS
    out_path = f"panel/jurisdiction={jurisdiction}/part.parquet"
    print(f"[BUILD] Writing panel to gs://properlytic-raw-data/{out_path}")
    
    # Convert to parquet
    panel.to_parquet("/tmp/panel.parquet", index=False)
    blob = bucket.blob(out_path)
    blob.upload_from_filename("/tmp/panel.parquet")
    
    stats = {
        "jurisdiction": jurisdiction,
        "rows": len(panel),
        "columns": list(panel.columns),
        "years": sorted(panel["year"].unique().tolist()),
        "unique_parcels": int(panel["parcel_id"].nunique()),
        "size_mb": round(os.path.getsize("/tmp/panel.parquet") / 1e6, 1),
    }
    print(f"[BUILD] Done: {json.dumps(stats, indent=2)}")
    return json.dumps(stats)


@app.local_entrypoint()
def main():
    import json
    
    # 1) Upload fixed worldmodel.py by reading local file and passing as arg
    with open("scripts/inference/worldmodel.py", "r", encoding="utf-8", errors="replace") as f:
        wm_content = f.read()
    
    # 2) Build panels for jurisdictions with raw data
    jurisdictions = ["cook_county_il", "ny_state", "france_dvf"]
    
    print(f"Building panels for: {jurisdictions}")
    results = list(build_panel.map(jurisdictions))
    
    for jur, result in zip(jurisdictions, results):
        print(f"\n{'='*60}")
        print(f"  {jur}: {result}")
    
    # Save results
    with open("scripts/logs/panel_build_results.json", "w") as f:
        json.dump({j: json.loads(r) for j, r in zip(jurisdictions, results)}, f, indent=2)
