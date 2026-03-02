"""
Scrape and download raw property data from public portals, upload to GCS, build panels.
Handles jurisdictions with direct download URLs or API endpoints.

Usage:
    python -m modal run scripts/data_acquisition/scrape_jurisdictions.py
    python -m modal run scripts/data_acquisition/scrape_jurisdictions.py --jurisdiction dc_washington
"""
import modal, os

app = modal.App("scrape-jurisdictions")
image = modal.Image.debian_slim(python_version="3.11").pip_install(
    "google-cloud-storage", "polars", "pyarrow", "pandas", "requests", "lxml", "openpyxl",
    "beautifulsoup4",
)
gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])

# ── Jurisdictions with direct/scrapable download URLs ──
SCRAPABLE = {
    "dc_washington": {
        "name": "Washington DC CAMA Residential",
        "download_url": "https://opendata.dc.gov/api/v2/datasets/computer-assisted-mass-appraisal-residential/csv",
        "gcs_prefix": "dc_washington",
        "mapping": {
            "parcel_id": "SSL", "assessed_value": "PRICE", "dwelling_type": "USECODE",
            "address": "ADDRESS", "sqft": "LIVING_GBA", "year_built": "AYB",
            "bedrooms": "BEDRM", "bathrooms": "BATHRM",
            "lat": "LATITUDE", "lon": "LONGITUDE",
            "land_area": "LANDAREA",
        },
    },
    "france_dvf": {
        "name": "France DVF (Demandes de Valeurs Foncières)",
        "download_urls": [
            f"https://files.data.gouv.fr/geo-dvf/latest/csv/{yr}/full.csv.gz"
            for yr in range(2019, 2025)
        ],
        "gcs_prefix": "france_dvf",
        "mapping": {
            "parcel_id": "id_parcelle", "sale_price": "valeur_fonciere",
            "sale_date": "date_mutation", "dwelling_type": "type_local",
            "sqft": "surface_reelle_bati", "land_area": "surface_terrain",
            "bedrooms": "nombre_pieces_principales",
            "lat": "latitude", "lon": "longitude",
        },
    },
    "minneapolis_mn": {
        "name": "Minneapolis Assessing Parcel Data",
        "download_url": "https://opendata.minneapolismn.gov/datasets/assessing-department-parcel-data-current/csv",
        "gcs_prefix": "minneapolis_mn",
        "mapping": {
            "parcel_id": "PID", "assessed_value": "TOTAL_TAX_CAPACITY",
            "land_value": "EST_LAND_MKT_VAL", "improvement_value": "EST_BLDG_MKT_VAL",
            "dwelling_type": "PROPERTY_TYPE", "address": "ADDRESS",
            "sqft": "BLDG_SQ_FT", "year_built": "YEAR_BUILT",
            "bedrooms": "NUM_BEDROOMS", "bathrooms": "NUM_BATHROOMS",
        },
    },
    "phoenix_az": {
        "name": "Maricopa County Assessor Parcels",
        "download_url": "https://opendata.arcgis.com/api/v3/datasets/44c81bebd4fb4bd69cd45e6c4be81d68_0/downloads/data?format=csv&spatialRefId=4326",
        "gcs_prefix": "phoenix_az",
        "mapping": {
            "parcel_id": "APN", "assessed_value": "FULL_CASH_VALUE",
            "land_value": "FCVLand", "improvement_value": "FCVImprove",
            "dwelling_type": "MCR_Use_Desc", "address": "FullAddress",
            "sqft": "LivingArea", "year_built": "YearBuilt",
            "lat": "Latitude", "lon": "Longitude",
        },
    },
    "detroit_mi": {
        "name": "Detroit Parcels & Assessment",
        "download_url": "https://opendata.arcgis.com/api/v3/datasets/detroit-parcels-and-assessment-information/downloads/data?format=csv&spatialRefId=4326",
        "gcs_prefix": "detroit_mi",
        "mapping": {
            "parcel_id": "parcel_number", "assessed_value": "assessed_value",
            "land_value": "land_value", "improvement_value": "improved_value",
            "dwelling_type": "property_class", "address": "address",
            "sqft": "total_sq_footage", "year_built": "year_built",
        },
    },
    "vancouver_bc": {
        "name": "Vancouver Property Tax Report",
        "download_url": "https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/property-tax-report/exports/csv?lang=en&timezone=America%2FLos_Angeles&use_labels=true",
        "gcs_prefix": "vancouver_bc",
        "mapping": {
            "parcel_id": "PID", "assessed_value": "CURRENT_LAND_VALUE",
            "land_value": "CURRENT_LAND_VALUE",
            "improvement_value": "CURRENT_IMPROVEMENT_VALUE",
            "year_built": "YEAR_BUILT", "dwelling_type": "ZONE_CATEGORY",
            "address": "FROM_CIVIC_NUMBER",
        },
    },
    "nsw_australia": {
        "name": "NSW Valuer General — Bulk Land Values",
        "download_url": "https://data.nsw.gov.au/data/dataset/bulk-land-value-information/resource/download",
        "gcs_prefix": "nsw_australia",
        "mapping": {
            "parcel_id": "PROPERTY_ID", "assessed_value": "LAND_VALUE",
            "address": "PROPERTY_NAME", "dwelling_type": "PROPERTY_TYPE",
            "land_area": "AREA",
        },
        "notes": "Land values only (not sales). NSW sales data requires registration.",
    },
}


@app.function(image=image, secrets=[gcs_secret], timeout=1800, memory=32768)
def scrape_and_build(jurisdiction: str) -> str:
    """Download data from public source, upload to GCS, build panel."""
    import json, io, time, requests
    import pandas as pd
    from google.cloud import storage

    t0 = time.time()
    cfg = SCRAPABLE.get(jurisdiction)
    if not cfg:
        return json.dumps({"error": f"Unknown jurisdiction: {jurisdiction}"})

    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")
    prefix = cfg["gcs_prefix"]
    mapping = cfg["mapping"]

    print(f"[{jurisdiction}] Starting scrape: {cfg['name']}")

    # Handle single or multiple download URLs
    urls = cfg.get("download_urls", [cfg["download_url"]] if "download_url" in cfg else [])

    all_dfs = []
    for i, url in enumerate(urls):
        print(f"[{jurisdiction}] Downloading ({i+1}/{len(urls)}): {url[:80]}...")
        try:
            resp = requests.get(url, timeout=600, headers={
                "User-Agent": "Mozilla/5.0 (Properlytic Research Bot)"
            })
            resp.raise_for_status()
            content = resp.content
            print(f"  Downloaded {len(content)/1e6:.1f}MB")

            # Upload raw to GCS
            fname = f"raw_{i:04d}"
            if url.endswith('.csv.gz'):
                fname += ".csv.gz"
                blob = bucket.blob(f"{prefix}/{fname}")
                blob.upload_from_string(content)
                df = pd.read_csv(io.BytesIO(content), compression='gzip',
                                low_memory=False, on_bad_lines='skip')
            elif url.endswith('.csv') or 'csv' in url:
                fname += ".csv"
                blob = bucket.blob(f"{prefix}/{fname}")
                blob.upload_from_string(content)
                df = pd.read_csv(io.BytesIO(content), low_memory=False,
                                on_bad_lines='skip')
            else:
                fname += ".csv"
                blob = bucket.blob(f"{prefix}/{fname}")
                blob.upload_from_string(content)
                df = pd.read_csv(io.BytesIO(content), low_memory=False,
                                on_bad_lines='skip')

            print(f"  Uploaded to gs://properlytic-raw-data/{prefix}/{fname}")
            print(f"  Columns: {list(df.columns[:20])}")
            all_dfs.append(df)
        except Exception as e:
            print(f"  ❌ Download failed: {e}")
            continue

    if not all_dfs:
        return json.dumps({"error": f"No data downloaded for {jurisdiction}"})

    raw_df = pd.concat(all_dfs, ignore_index=True)
    print(f"[{jurisdiction}] Combined: {len(raw_df):,} rows, {len(raw_df.columns)} cols")

    # Map columns
    panel = pd.DataFrame()
    for canon_col, src_col in mapping.items():
        if src_col in raw_df.columns:
            panel[canon_col] = raw_df[src_col]
        else:
            # Try case-insensitive match
            matches = [c for c in raw_df.columns if c.lower() == src_col.lower()]
            if matches:
                panel[canon_col] = raw_df[matches[0]]
                print(f"  Mapped {canon_col} via case-insensitive match: {matches[0]}")
            else:
                print(f"  ⚠️ Column '{src_col}' not found for '{canon_col}'")

    panel["jurisdiction"] = jurisdiction

    # Derive year from sale_date if needed
    if "year" not in panel.columns and "sale_date" in panel.columns:
        panel["sale_date_parsed"] = pd.to_datetime(panel["sale_date"], errors="coerce")
        panel["year"] = panel["sale_date_parsed"].dt.year
        panel = panel.drop(columns=["sale_date_parsed"])
        print(f"[{jurisdiction}] Derived year from sale_date")

    # Coalesce property_value
    val_cols = [c for c in ["sale_price", "assessed_value"] if c in panel.columns]
    if val_cols:
        panel["property_value"] = panel[val_cols].bfill(axis=1).iloc[:, 0]

    # Clean numerics
    for col in ["property_value", "sale_price", "assessed_value", "land_value",
                 "improvement_value", "sqft", "land_area", "year_built",
                 "bedrooms", "bathrooms", "lat", "lon"]:
        if col in panel.columns:
            panel[col] = pd.to_numeric(panel[col], errors="coerce")

    # Filter
    before = len(panel)
    if "year" in panel.columns:
        panel = panel.dropna(subset=["year"])
        panel["year"] = panel["year"].astype(int)
        panel = panel[panel["year"] >= 1990]
    if "property_value" in panel.columns:
        panel = panel.dropna(subset=["property_value"])
        panel = panel[panel["property_value"] > 0]
    if "parcel_id" in panel.columns:
        panel = panel.dropna(subset=["parcel_id"])
    print(f"[{jurisdiction}] After cleaning: {len(panel):,} rows (dropped {before - len(panel):,})")

    if len(panel) == 0:
        return json.dumps({"error": f"No valid rows after cleaning for {jurisdiction}",
                          "raw_cols": list(raw_df.columns[:30])})

    # Write to GCS
    out_path = f"panel/jurisdiction={jurisdiction}/part.parquet"
    panel.to_parquet("/tmp/panel.parquet", index=False)
    blob = bucket.blob(out_path)
    blob.upload_from_filename("/tmp/panel.parquet")

    elapsed = time.time() - t0
    stats = {
        "status": "ok",
        "jurisdiction": jurisdiction,
        "name": cfg["name"],
        "rows": len(panel),
        "columns": list(panel.columns),
        "years": sorted(panel["year"].unique().tolist()) if "year" in panel.columns else [],
        "unique_parcels": int(panel["parcel_id"].nunique()) if "parcel_id" in panel.columns else 0,
        "size_mb": round(os.path.getsize("/tmp/panel.parquet") / 1e6, 1),
        "elapsed_s": round(elapsed, 1),
    }
    print(f"\n✅ [{jurisdiction}] Done: {json.dumps(stats, indent=2)}")
    return json.dumps(stats)


@app.local_entrypoint()
def main(jurisdiction: str = ""):
    import json

    if jurisdiction:
        result = scrape_and_build.remote(jurisdiction)
        print(result)
    else:
        jurisdictions = list(SCRAPABLE.keys())
        print(f"Scraping {len(jurisdictions)} jurisdictions: {jurisdictions}")
        results = list(scrape_and_build.map(jurisdictions))

        combined = {}
        for jur, result in zip(jurisdictions, results):
            parsed = json.loads(result)
            combined[jur] = parsed
            s = "✅" if parsed.get("status") == "ok" else "❌"
            rows = parsed.get("rows", parsed.get("error", "?"))
            print(f"  {s} {jur}: {rows}")

        with open("scripts/logs/scrape_results.json", "w") as f:
            json.dump(combined, f, indent=2)
        print(f"\nResults: scripts/logs/scrape_results.json")
