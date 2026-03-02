"""
Wave 3 scraper: TX statewide, MA statewide, Miami-Dade, SF sales.
Downloads from public open data portals and uploads to GCS.

Usage:
    python -m modal run scripts/data_acquisition/scrape_wave3.py
"""
import modal, os

app = modal.App("scrape-wave3")
image = modal.Image.debian_slim(python_version="3.11").pip_install(
    "google-cloud-storage", "pandas", "pyarrow", "requests", "openpyxl",
)
gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])

SOURCES = {
    # SF recorded deeds (sale prices)
    "sf_sales": {
        "url": "https://data.sfgov.org/api/views/wv5m-vpq2/rows.csv?accessType=DOWNLOAD",
        "desc": "SF Assessor-Recorder Roll (assessed values + structural features)",
        "mapping": {
            "parcel_id": "Parcel Number",
            "address": "Property Location",
            "year": "Closed Roll Year",
            "dwelling_type": "Property Class Code",
            "sqft": "Property Area",
            "land_area": "Lot Area",
            "bedrooms": "Number of Bedrooms",
            "bathrooms": "Number of Bathrooms",
            "rooms": "Number of Rooms",
            "stories": "Number of Stories",
            "units": "Number of Units",
            "year_built": "Year Property Built",
            "basement_area": "Basement Area",
            "construction_type": "Construction Type",
            "zoning": "Zoning Code",
            "lot_depth": "Lot Depth",
        },
        "value_col_fallback": "Closed Roll Year",  # No explicit value; year-over-year assessed panel
    },
    # Miami-Dade property appraiser
    "miami_dade": {
        "url": "https://www.miamidade.gov/Apps/PA/PApublicServiceSearch/Data/Csv/Assessment",
        "desc": "Miami-Dade Property Appraiser Assessment Data",
        "mapping": {
            "parcel_id": "FOLIO",
            "assessed_value": "ASSESSED_VAL",
            "sale_price": "SALE_PRC",
            "sale_date": "SALE_DT",
            "year_built": "YR_BLT",
            "sqft": "BLDG_SQFT",
            "land_area": "LOT_SIZE",
            "bedrooms": "BEDROOM_CNT",
            "bathrooms": "BATH_CNT",
            "address": "SITUS_ADDR",
            "dwelling_type": "DOR_CD",
        },
    },
    # NYC PLUTO — comprehensive lot-level data with sale prices
    "nyc_pluto": {
        "url": "https://data.cityofnewyork.us/api/views/64uk-42ks/rows.csv?accessType=DOWNLOAD",
        "desc": "NYC PLUTO - Primary Land Use Tax Lot Output",
        "mapping": {
            "parcel_id": "BBL",
            "assessed_value": "AssessTot",
            "year_built": "YearBuilt",
            "sqft": "BldgArea",
            "land_area": "LotArea",
            "stories": "NumFloors",
            "dwelling_type": "LandUse",
            "address": "Address",
            "lat": "Latitude",
            "lon": "Longitude",
        },
    },
    # Texas Comptroller - statewide property tax data
    "texas_statewide": {
        "url": "https://comptroller.texas.gov/data/property-tax/pvs/2024_SummaryByCAD.xlsx",
        "desc": "Texas Comptroller - Statewide CAD Summary (2024)",
        "mapping": {},
        "format": "xlsx",
    },
    # Massachusetts DOR - statewide property tax (Level 3 parcel data)
    "mass_statewide": {
        "url": "https://dlsgateway.dor.state.ma.us/reports/rdPage.aspx?rdReport=PropertyTaxInformation.valuationbyclass",
        "desc": "Mass DOR - Valuation by Class (statewide summary)",
        "mapping": {},
        "format": "html",
    },
}


@app.function(image=image, secrets=[gcs_secret], timeout=1800, memory=16384)
def scrape_and_build(name: str) -> str:
    import json, io, time, zipfile
    import pandas as pd
    import requests
    from google.cloud import storage

    t0 = time.time()
    src = SOURCES[name]
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")

    url = src["url"]
    mapping = src["mapping"]
    fmt = src.get("format", "csv")
    print(f"[{name}] Downloading from {url[:80]}...")

    headers = {"User-Agent": "Mozilla/5.0 (compatible; Properlytic/1.0)"}
    resp = requests.get(url, timeout=120, headers=headers, allow_redirects=True)

    if resp.status_code != 200 or len(resp.content) < 500:
        return json.dumps({
            "status": "download_failed",
            "error": f"HTTP {resp.status_code}, size={len(resp.content)}",
            "url": url,
        })

    # Upload raw data
    raw_ext = fmt if fmt != "html" else "html"
    raw_blob = bucket.blob(f"{name}/raw_data.{raw_ext}")
    raw_blob.upload_from_string(resp.content)
    print(f"[{name}] Uploaded raw: {len(resp.content):,} bytes")

    # Parse data
    try:
        if fmt == "xlsx":
            df = pd.read_excel(io.BytesIO(resp.content), engine="openpyxl")
        elif fmt == "html":
            return json.dumps({
                "status": "html_only",
                "note": "HTML report, needs manual download or API",
                "jurisdiction": name,
            })
        elif resp.content[:2] == b'PK':
            # ZIP file
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
                if csv_files:
                    with zf.open(csv_files[0]) as f:
                        df = pd.read_csv(f, low_memory=False, on_bad_lines='skip')
                else:
                    return json.dumps({"status": "error", "error": "No CSV in ZIP"})
        else:
            for enc in ['utf-8', 'latin-1', 'cp1252']:
                try:
                    df = pd.read_csv(io.BytesIO(resp.content), low_memory=False,
                                     encoding=enc, on_bad_lines='skip')
                    break
                except UnicodeDecodeError:
                    continue

        print(f"[{name}] Parsed: {len(df):,} rows, cols: {list(df.columns[:10])}")
    except Exception as e:
        return json.dumps({"status": "parse_error", "error": str(e)})

    if not mapping:
        return json.dumps({
            "status": "raw_uploaded",
            "jurisdiction": name,
            "rows": len(df),
            "columns": list(df.columns[:20]),
        })

    # Build standardized panel
    panel = pd.DataFrame()
    panel["jurisdiction"] = name
    for canonical, raw_col in mapping.items():
        if raw_col in df.columns:
            panel[canonical] = df[raw_col].values
        else:
            # Try case-insensitive match
            matches = [c for c in df.columns if c.lower() == raw_col.lower()]
            if matches:
                panel[canonical] = df[matches[0]].values
            else:
                print(f"  ⚠️ Column not found: {raw_col}")

    if "sale_price" in panel.columns:
        panel["sale_price"] = pd.to_numeric(panel["sale_price"], errors="coerce")
        panel["property_value"] = panel["sale_price"]
    elif "assessed_value" in panel.columns:
        panel["assessed_value"] = pd.to_numeric(panel["assessed_value"], errors="coerce")
        panel["property_value"] = panel["assessed_value"]

    # Extract year
    if "sale_date" in panel.columns:
        panel["sale_date_parsed"] = pd.to_datetime(panel["sale_date"], errors="coerce")
        panel["year"] = panel["sale_date_parsed"].dt.year
        panel = panel.drop(columns=["sale_date_parsed"])
    elif "year" not in panel.columns:
        panel["year"] = 2024

    panel = panel.dropna(subset=["property_value"])
    panel = panel[panel["property_value"] > 0]
    if "year" in panel.columns:
        panel = panel[panel["year"] >= 1990]

    # Upload panel
    panel_path = f"panel/jurisdiction={name}/part.parquet"
    panel.to_parquet("/tmp/panel.parquet", index=False)
    bucket.blob(panel_path).upload_from_filename("/tmp/panel.parquet")

    elapsed = time.time() - t0
    stats = {
        "status": "ok",
        "jurisdiction": name,
        "rows": len(panel),
        "parcels": panel["parcel_id"].nunique() if "parcel_id" in panel.columns else 0,
        "years": f"{int(panel['year'].min())}-{int(panel['year'].max())}" if "year" in panel.columns else "N/A",
        "n_years": int(panel["year"].nunique()) if "year" in panel.columns else 0,
        "columns": list(panel.columns),
        "elapsed_s": round(elapsed, 1),
    }
    print(f"✅ [{name}] Panel built: {stats['rows']:,} rows, {stats['parcels']:,} parcels, {stats['years']}")
    return json.dumps(stats)


@app.local_entrypoint()
def main():
    import json
    names = list(SOURCES.keys())
    print(f"=== Wave 3: {len(names)} jurisdictions ===")
    results = {}
    for name, result in zip(names, scrape_and_build.map(names)):
        parsed = json.loads(result)
        results[name] = parsed
        s = "✅" if parsed.get("status") == "ok" else "⚠️"
        print(f"  {s} {name}: {parsed.get('status')} — {parsed.get('rows', 0):,} rows")

    # Save results
    with open("/tmp/wave3_results.json", "w") as f:
        json.dump(results, f, indent=2)
