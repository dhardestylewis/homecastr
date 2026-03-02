"""
Wave 2 scraper: Ireland, South Korea, Massachusetts, Hennepin County, Dallas, Austin.
Downloads raw data, uploads to GCS, builds panels.

Usage:
    python -m modal run scripts/data_acquisition/scrape_wave2.py
"""
import modal, os

app = modal.App("scrape-wave2")
image = modal.Image.debian_slim(python_version="3.11").pip_install(
    "google-cloud-storage", "polars", "pyarrow", "pandas", "requests",
    "beautifulsoup4", "lxml", "openpyxl", "xlrd",
)
gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])

WAVE2 = {
    "ireland": {
        "name": "Ireland Residential Property Price Register",
        "download_url": "https://www.propertypriceregister.ie/website/npsra/pprweb.nsf/PPRDownloads?OpenForm&File=PPR-ALL.csv&County=ALL&Year=ALL&Month=ALL",
        "gcs_prefix": "ireland",
        "mapping": {
            "sale_date": "Date of Sale (dd/mm/yyyy)",
            "sale_price": "Price (€)",  # Note: Euro prices
            "address": "Address",
            "dwelling_type": "Description of Property",
            "parcel_id": "Address",  # No parcel ID, use address as proxy
        },
    },
    "south_korea": {
        "name": "South Korea MOLIT Apartment Transactions",
        # data.go.kr open API — apartment real transaction prices
        "download_url": "https://rt.molit.go.kr/pt/xls/xls.do?srhThingSeCD=1&srhBjDongCode=11000&srhFromDt=201901&srhToDt=202412",
        "gcs_prefix": "south_korea",
        "mapping": {
            "parcel_id": "지번",  # lot number
            "sale_price": "거래금액",  # transaction amount (만원 = 10,000 KRW)
            "sale_date": "계약일",  # contract date
            "address": "아파트",  # apartment name / address
            "sqft": "전용면적",  # exclusive area (㎡)
            "dwelling_type": "거래유형",  # transaction type
            "year_built": "건축년도",  # year built
        },
    },
    "hennepin_county_mn": {
        "name": "Hennepin County Tax Parcels",
        "download_url": "https://gis-hennepin.opendata.arcgis.com/api/v2/datasets/fee4073e36c5415f847d2cfdd7dd3b91_0/csv",
        "gcs_prefix": "hennepin_county_mn",
        "mapping": {
            "parcel_id": "PID", "assessed_value": "EMV_TOTAL",
            "land_value": "EMV_LAND", "improvement_value": "EMV_BLDG",
            "dwelling_type": "USE_DESC", "year_built": "YEAR_BUILT",
            "address": "BLDG_NUM",
        },
    },
    "massachusetts": {
        "name": "Massachusetts Property Tax Data (Level 3 parcels)",
        # MA DOR publishes Excel files per town — this is the state summary
        "download_url": "https://www.mass.gov/files/documents/2024/01/16/PropTaxData_ExportData_Municipal.xlsx",
        "gcs_prefix": "massachusetts",
        "mapping": {
            "parcel_id": "Municipal", "assessed_value": "Total Assessed Value",
            "dwelling_type": "Class",
        },
        "notes": "State-level summary. Granular parcel-level data requires per-town downloads.",
    },
    "dallas_tx": {
        "name": "Dallas CAD Export (DCAD)",
        # DCAD certified export — this URL may require a browser session
        "download_url": "https://www.dallascad.org/ExportedFiles/Certified/2024CertifiedCSV.zip",
        "gcs_prefix": "dallas_tx",
        "mapping": {
            "parcel_id": "PROP_ID", "assessed_value": "CERTIFIED_VAL",
            "land_value": "LAND_VAL", "improvement_value": "IMPR_VAL",
            "year": "TAX_YEAR", "dwelling_type": "PROP_TYPE_CD",
            "address": "SITUS_ADDR",
        },
    },
    "austin_tx": {
        "name": "Travis CAD Export (TCAD)",
        # TCAD certified export
        "download_url": "https://www.traviscad.org/wp-content/exports/2024_Certified_Export.zip",
        "gcs_prefix": "austin_tx",
        "mapping": {
            "parcel_id": "PROP_ID", "assessed_value": "APPRAISED_VAL",
            "land_value": "LAND_VAL", "improvement_value": "IMPR_VAL",
            "year": "TAX_YEAR", "dwelling_type": "PROP_TYPE_CD",
            "address": "SITUS_ADDR", "sqft": "LIVING_AREA",
            "year_built": "YR_BUILT",
        },
    },
}


@app.function(image=image, secrets=[gcs_secret], timeout=1800, memory=32768)
def scrape_and_build(jurisdiction: str) -> str:
    """Download data from public source, upload to GCS, build panel."""
    import json, io, time, requests, zipfile
    import pandas as pd
    from google.cloud import storage

    t0 = time.time()
    cfg = WAVE2.get(jurisdiction)
    if not cfg:
        return json.dumps({"error": f"Unknown: {jurisdiction}"})

    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")
    prefix = cfg["gcs_prefix"]
    mapping = cfg["mapping"]

    print(f"[{jurisdiction}] Starting: {cfg['name']}")
    url = cfg["download_url"]
    print(f"[{jurisdiction}] Downloading: {url[:100]}...")

    try:
        resp = requests.get(url, timeout=600, headers={
            "User-Agent": "Mozilla/5.0 (Properlytic Research Bot)",
        }, allow_redirects=True)
        resp.raise_for_status()
        content = resp.content
        print(f"[{jurisdiction}] Downloaded {len(content)/1e6:.1f}MB (status {resp.status_code})")
    except Exception as e:
        return json.dumps({"status": "download_failed", "error": str(e),
                          "jurisdiction": jurisdiction, "url": url})

    # Upload raw to GCS
    ext = ".csv"
    if url.endswith('.zip') or resp.headers.get('content-type', '').startswith('application/zip'):
        ext = ".zip"
    elif url.endswith('.xlsx') or 'excel' in resp.headers.get('content-type', ''):
        ext = ".xlsx"
    elif url.endswith('.csv.gz'):
        ext = ".csv.gz"
    elif url.endswith('.xls'):
        ext = ".xls"

    raw_blob = bucket.blob(f"{prefix}/raw_download{ext}")
    raw_blob.upload_from_string(content)
    print(f"[{jurisdiction}] Uploaded raw to GCS ({ext})")

    # Parse based on format
    try:
        if ext == ".zip":
            zf = zipfile.ZipFile(io.BytesIO(content))
            csv_files = [f for f in zf.namelist() if f.endswith('.csv') or f.endswith('.txt')]
            print(f"[{jurisdiction}] ZIP contains: {zf.namelist()[:10]}")
            if csv_files:
                dfs = []
                for cf in csv_files[:5]:  # max 5 files from zip
                    with zf.open(cf) as f:
                        df = pd.read_csv(f, low_memory=False, on_bad_lines='skip',
                                        encoding='utf-8', encoding_errors='replace')
                        dfs.append(df)
                        print(f"  {cf}: {len(df):,} rows")
                raw_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
            else:
                return json.dumps({"error": f"No CSV/TXT in ZIP: {zf.namelist()[:10]}"})
        elif ext == ".xlsx":
            raw_df = pd.read_excel(io.BytesIO(content), engine='openpyxl')
        elif ext == ".xls":
            raw_df = pd.read_excel(io.BytesIO(content), engine='xlrd')
        elif ext == ".csv.gz":
            raw_df = pd.read_csv(io.BytesIO(content), compression='gzip',
                                low_memory=False, on_bad_lines='skip')
        else:
            # Try multiple encodings
            for enc in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
                try:
                    raw_df = pd.read_csv(io.BytesIO(content), low_memory=False,
                                        on_bad_lines='skip', encoding=enc)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                raw_df = pd.read_csv(io.BytesIO(content), low_memory=False,
                                    on_bad_lines='skip', encoding='utf-8',
                                    encoding_errors='replace')
    except Exception as e:
        return json.dumps({"error": f"Parse failed: {e}", "jurisdiction": jurisdiction})

    print(f"[{jurisdiction}] Parsed: {len(raw_df):,} rows, cols: {list(raw_df.columns[:15])}")

    # Map columns
    panel = pd.DataFrame()
    for canon_col, src_col in mapping.items():
        if src_col in raw_df.columns:
            panel[canon_col] = raw_df[src_col]
        else:
            matches = [c for c in raw_df.columns if c.lower().strip() == src_col.lower().strip()]
            if matches:
                panel[canon_col] = raw_df[matches[0]]
            else:
                print(f"  ⚠️ '{src_col}' not found for '{canon_col}'")

    panel["jurisdiction"] = jurisdiction

    # Special handling for Ireland price (remove € and commas)
    if jurisdiction == "ireland" and "sale_price" in panel.columns:
        panel["sale_price"] = (panel["sale_price"].astype(str)
                              .str.replace('€', '', regex=False)
                              .str.replace(',', '', regex=False))

    # Derive year from sale_date if needed
    if "year" not in panel.columns and "sale_date" in panel.columns:
        panel["sale_date_parsed"] = pd.to_datetime(panel["sale_date"], errors="coerce",
                                                    dayfirst=True)
        panel["year"] = panel["sale_date_parsed"].dt.year
        panel = panel.drop(columns=["sale_date_parsed"])

    # Coalesce property_value
    val_cols = [c for c in ["sale_price", "assessed_value"] if c in panel.columns]
    if val_cols:
        panel["property_value"] = panel[val_cols].bfill(axis=1).iloc[:, 0]

    # Clean numerics
    for col in ["property_value", "sale_price", "assessed_value", "land_value",
                 "improvement_value", "sqft", "land_area", "year_built",
                 "bedrooms", "bathrooms", "lat", "lon", "year"]:
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
        panel["parcel_id"] = panel["parcel_id"].astype(str)
        panel = panel.dropna(subset=["parcel_id"])
    print(f"[{jurisdiction}] After cleaning: {len(panel):,} rows (dropped {before - len(panel):,})")

    if len(panel) == 0:
        return json.dumps({"error": f"No valid rows", "raw_cols": list(raw_df.columns[:30]),
                          "raw_rows": len(raw_df), "jurisdiction": jurisdiction})

    # Write panel to GCS
    out_path = f"panel/jurisdiction={jurisdiction}/part.parquet"
    panel.to_parquet("/tmp/panel.parquet", index=False)
    blob = bucket.blob(out_path)
    blob.upload_from_filename("/tmp/panel.parquet")

    elapsed = time.time() - t0
    stats = {
        "status": "ok", "jurisdiction": jurisdiction, "name": cfg["name"],
        "rows": len(panel), "columns": list(panel.columns),
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
        jurisdictions = list(WAVE2.keys())
        print(f"Scraping {len(jurisdictions)} wave-2 jurisdictions: {jurisdictions}")
        results = list(scrape_and_build.map(jurisdictions))

        combined = {}
        for jur, result in zip(jurisdictions, results):
            parsed = json.loads(result)
            combined[jur] = parsed
            s = "✅" if parsed.get("status") == "ok" else "❌"
            rows = parsed.get("rows", parsed.get("error", "?"))
            print(f"  {s} {jur}: {rows}")

        with open("scripts/logs/scrape_wave2_results.json", "w") as f:
            json.dump(combined, f, indent=2)
        print(f"\nResults saved: scripts/logs/scrape_wave2_results.json")
