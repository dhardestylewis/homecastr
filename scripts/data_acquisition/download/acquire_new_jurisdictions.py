"""
Acquire raw data for new jurisdictions and upload to GCS.
Then build panels from the uploaded data.

Usage:
    python -m modal run scripts/data_acquisition/acquire_new_jurisdictions.py
    python -m modal run scripts/data_acquisition/acquire_new_jurisdictions.py --jurisdiction dallas_tx
"""
import modal, os

app = modal.App("acquire-new-jurisdictions")
image = modal.Image.debian_slim(python_version="3.11").pip_install(
    "google-cloud-storage", "polars", "pyarrow", "pandas", "requests", "lxml", "openpyxl"
)
gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])

# ── Data Source Registry ─────────────────────────────────────────────
SOURCES = {
    # ── US: Texas CADs (same format as HCAD) ──
    "dallas_tx": {
        "name": "Dallas Central Appraisal District (DCAD)",
        "urls": [
            "https://www.dallascad.org/ViewPDFs.aspx?type=1&id=2024CertifiedDataExport",
        ],
        "gcs_prefix": "dallas_tx",
        "type": "assessment",
        "mapping": {
            "parcel_id": "PROP_ID",
            "assessed_value": "CERTIFIED_VAL",
            "land_value": "LAND_VAL",
            "improvement_value": "IMPR_VAL",
            "year": "TAX_YEAR",
            "dwelling_type": "PROP_TYPE_CD",
            "address": "SITUS_ADDR",
        },
        "notes": "Dallas CAD — market values, same format as HCAD. Download manually from dallascad.org/DataProducts.aspx",
    },
    "austin_tx": {
        "name": "Travis Central Appraisal District (TCAD)",
        "urls": [
            "https://www.traviscad.org/publicinformation",
        ],
        "gcs_prefix": "austin_tx",
        "type": "assessment",
        "mapping": {
            "parcel_id": "PROP_ID",
            "assessed_value": "APPRAISED_VAL",
            "land_value": "LAND_VAL",
            "improvement_value": "IMPR_VAL",
            "year": "TAX_YEAR",
            "dwelling_type": "PROP_TYPE_CD",
            "address": "SITUS_ADDR",
            "sqft": "LIVING_AREA",
            "year_built": "YR_BUILT",
        },
        "notes": "Austin/Travis CAD — market values. Download Certified Export from traviscad.org/publicinformation",
    },

    # ── US: Other metros ──
    "minneapolis_mn": {
        "name": "Minneapolis Assessing Data",
        "urls": [
            "https://opendata.minneapolismn.gov/datasets/assessing-department-parcel-data-current/explore",
        ],
        "gcs_prefix": "minneapolis_mn",
        "type": "assessment",
        "mapping": {
            "parcel_id": "PID",
            "assessed_value": "TOTAL_TAX_CAPACITY",
            "land_value": "EST_LAND_MKT_VAL",
            "improvement_value": "EST_BLDG_MKT_VAL",
            "dwelling_type": "PROPERTY_TYPE",
            "address": "ADDRESS",
            "sqft": "BLDG_SQ_FT",
            "year_built": "YEAR_BUILT",
            "bedrooms": "NUM_BEDROOMS",
            "bathrooms": "NUM_BATHROOMS",
        },
        "notes": "Minneapolis open data parcel assessment. Download CSV from opendata.minneapolismn.gov",
    },
    "hennepin_county_mn": {
        "name": "Hennepin County Parcels",
        "urls": [
            "https://gis-hennepin.opendata.arcgis.com/datasets/hennepin-county-tax-parcels",
        ],
        "gcs_prefix": "hennepin_county_mn",
        "type": "assessment",
        "mapping": {
            "parcel_id": "PID",
            "assessed_value": "EMV_TOTAL",
            "land_value": "EMV_LAND",
            "improvement_value": "EMV_BLDG",
            "dwelling_type": "USE_DESC",
            "address": "BLDG_NUM",
            "year_built": "YEAR_BUILT",
        },
        "notes": "Hennepin County (includes Minneapolis) tax parcels with assessed values.",
    },
    "massachusetts": {
        "name": "Massachusetts DOR Property Tax Data",
        "urls": [
            "https://www.mass.gov/lists/property-tax-data",
        ],
        "gcs_prefix": "massachusetts",
        "type": "assessment",
        "mapping": {
            "parcel_id": "LOC_ID",
            "assessed_value": "TOTAL_VAL",
            "land_value": "LAND_VAL",
            "improvement_value": "BLDG_VAL",
            "dwelling_type": "USE_CODE",
            "address": "LOCATION",
            "year_built": "YR_BUILT",
            "sqft": "LIVING_AREA",
            "bedrooms": "NUM_BEDRMS",
            "bathrooms": "FULL_BTH",
        },
        "notes": "MA DOR statewide assessment data. Annual revaluation — close to market value.",
    },

    # ── Australia ──
    "nsw_australia": {
        "name": "NSW Valuer General Property Sales",
        "urls": [
            "https://valuation.property.nsw.gov.au/embed/propertySalesInformation",
        ],
        "gcs_prefix": "nsw_australia",
        "type": "sales",
        "mapping": {
            "parcel_id": "PROPERTY_ID",
            "sale_price": "PURCHASE_PRICE",
            "sale_date": "CONTRACT_DATE",
            "address": "PROPERTY_NAME",
            "dwelling_type": "PROPERTY_TYPE",
            "land_area": "AREA",
        },
        "notes": "NSW Valuer General bulk sales data from 1990+. Free download at nsw.gov.au",
    },
    "victoria_australia": {
        "name": "Victoria (VGSO) Property Sales",
        "urls": [
            "https://www.land.vic.gov.au/valuations/resources-and-reports/property-sales-statistics",
        ],
        "gcs_prefix": "victoria_australia",
        "type": "sales",
        "mapping": {
            "parcel_id": "PROPERTY_ID",
            "sale_price": "PRICE",
            "sale_date": "SALE_DATE",
            "address": "ADDRESS",
            "dwelling_type": "TYPE",
        },
        "notes": "Victoria property sales statistics. Available from land.vic.gov.au",
    },

    # ── Canada ──
    "vancouver_bc": {
        "name": "Vancouver BC Assessment Open Data",
        "urls": [
            "https://opendata.vancouver.ca/explore/dataset/property-tax-report/information/",
        ],
        "gcs_prefix": "vancouver_bc",
        "type": "assessment",
        "mapping": {
            "parcel_id": "PID",
            "assessed_value": "CURRENT_LAND_VALUE",
            "land_value": "CURRENT_LAND_VALUE",
            "improvement_value": "CURRENT_IMPROVEMENT_VALUE",
            "year_built": "YEAR_BUILT",
            "dwelling_type": "ZONE_CATEGORY",
            "address": "FROM_CIVIC_NUMBER",
        },
        "notes": "Vancouver open data — BC Assessment values. Free CSV download.",
    },

    # ── France ──
    "france_dvf": {
        "name": "France DVF (Demandes de Valeurs Foncières)",
        "urls": [
            "https://files.data.gouv.fr/geo-dvf/latest/csv/",
        ],
        "gcs_prefix": "france_dvf",
        "type": "sales",
        "mapping": {
            "parcel_id": "id_parcelle",
            "sale_price": "valeur_fonciere",
            "sale_date": "date_mutation",
            "dwelling_type": "type_local",
            "sqft": "surface_reelle_bati",
            "land_area": "surface_terrain",
            "bedrooms": "nombre_pieces_principales",
            "lat": "latitude",
            "lon": "longitude",
        },
        "notes": "French property transaction data — free from data.gouv.fr. Already in build_panels_modal.py.",
    },

    # ── Japan ──
    "japan_mlit": {
        "name": "Japan MLIT Land Price Survey (公示価格)",
        "urls": [
            "https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-L01-v3_0.html",
        ],
        "gcs_prefix": "japan_mlit",
        "type": "land_price",
        "mapping": {
            "parcel_id": "L01_001",  # 基準地番号
            "assessed_value": "L01_006",  # 公示価格
            "address": "L01_024",  # 所在
            "dwelling_type": "L01_027",  # 利用状況
            "land_area": "L01_028",  # 地積
            "lat": "latitude",
            "lon": "longitude",
        },
        "notes": "Japan land price survey — GML format from MLIT. Official benchmark prices, not transactions.",
    },

    # ── US: More metros ──
    "phoenix_az": {
        "name": "Maricopa County Assessor",
        "urls": [
            "https://mcassessor.maricopa.gov/open-data/",
        ],
        "gcs_prefix": "phoenix_az",
        "type": "assessment",
        "mapping": {
            "parcel_id": "APN",
            "assessed_value": "FULL_CASH_VALUE",
            "land_value": "LAND_FCV",
            "improvement_value": "IMPROVEMENT_FCV",
            "dwelling_type": "USE_CODE",
            "address": "SITUS_ADDRESS",
            "sqft": "LIVING_AREA",
            "year_built": "YEAR_BUILT",
            "lat": "LATITUDE",
            "lon": "LONGITUDE",
        },
        "notes": "Maricopa County (Phoenix) — full cash value = market value. Open data portal available.",
    },
    "denver_co": {
        "name": "Denver Assessor Property Data",
        "urls": [
            "https://www.denvergov.org/opendata/dataset/city-and-county-of-denver-real-property",
        ],
        "gcs_prefix": "denver_co",
        "type": "assessment",
        "mapping": {
            "parcel_id": "SCHEDNUM",
            "assessed_value": "TOTAL_VALUE",
            "land_value": "LAND_VALUE",
            "improvement_value": "IMPROVEMENT_VALUE",
            "dwelling_type": "CLASS_DESC",
            "address": "SITUS_ADDRESS",
            "sqft": "TOTAL_SQ_FT",
            "year_built": "YEAR_BUILT",
            "bedrooms": "BEDROOMS",
            "bathrooms": "FULL_BATHS",
        },
        "notes": "Denver open data — real property records with assessment values.",
    },
    "detroit_mi": {
        "name": "Detroit/Wayne County Property Data",
        "urls": [
            "https://data.detroitmi.gov/datasets/parcels-and-assessment-information",
        ],
        "gcs_prefix": "detroit_mi",
        "type": "assessment",
        "mapping": {
            "parcel_id": "PARCELNO",
            "assessed_value": "ASSESSED_VALUE",
            "land_value": "LAND_VALUE",
            "improvement_value": "IMPROVED_VALUE",
            "dwelling_type": "PROPCLASS",
            "address": "PROPSTREETCOMBINED",
            "sqft": "TOTAL_SQ_FOOTAGE",
            "year_built": "RESYRBLT",
        },
        "notes": "Detroit open data — parcel assessment data. Also available via Wayne County.",
    },
    "dc_washington": {
        "name": "Washington DC CAMA Data",
        "urls": [
            "https://opendata.dc.gov/datasets/computer-assisted-mass-appraisal-residential",
            "https://opendata.dc.gov/datasets/integrated-tax-system-public-extract",
        ],
        "gcs_prefix": "dc_washington",
        "type": "assessment",
        "mapping": {
            "parcel_id": "SSL",
            "assessed_value": "PRICE",
            "land_value": "LANDAREA",
            "dwelling_type": "USECODE",
            "address": "ADDRESS",
            "sqft": "LIVING_GBA",
            "year_built": "AYB",
            "bedrooms": "BEDRM",
            "bathrooms": "BATHRM",
            "lat": "LATITUDE",
            "lon": "LONGITUDE",
        },
        "notes": "DC CAMA — detailed residential property data with lat/lon. Free open data.",
    },
    # ── Cook County (Chicago) ──
    "cook_county_il": {
        "name": "Cook County IL Assessed Values",
        "urls": [
            "https://datacatalog.cookcountyil.gov/Property-Taxation/Assessor-Assessed-Values/uzyt-m557",
        ],
        "gcs_prefix": "cook_county_il/assessed_values",
        "type": "assessment",
        "mapping": {
            "parcel_id": "pin",
            "year": "year",
            "dwelling_type": "class",
            "assessed_value": "certified_tot",
            "land_value": "certified_land",
            "improvement_value": "certified_bldg",
        },
        "notes": "Cook County (Chicago): assessed at 10% of market value. Multiple CSV chunks.",
    },
    # ── Vancouver BC ──
    "vancouver_bc": {
        "name": "Vancouver BC Property Tax Report",
        "urls": [
            "https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/property-tax-report/exports/csv",
        ],
        "gcs_prefix": "vancouver_bc",
        "type": "assessment",
        "csv_sep": ";",  # Vancouver open data uses semicolons
        "mapping": {
            "parcel_id": "PID",
            "assessed_value": "CURRENT_LAND_VALUE",
            "land_value": "CURRENT_LAND_VALUE",
            "improvement_value": "CURRENT_IMPROVEMENT_VALUE",
            "year_built": "YEAR_BUILT",
            "year": "REPORT_YEAR",
            "dwelling_type": "ZONE_CATEGORY",
            "address": "FROM_CIVIC_NUMBER",
        },
        "notes": "Vancouver open data — BC Assessment values. Semicolon-delimited CSV.",
    },
}


@app.function(image=image, secrets=[gcs_secret], timeout=1800, memory=32768)
def acquire_and_build(jurisdiction: str) -> str:
    """Download raw data from public source and build panel, upload to GCS."""
    import json, io, time, requests
    import pandas as pd
    from google.cloud import storage

    t0 = time.time()
    cfg = SOURCES.get(jurisdiction)
    if not cfg:
        return json.dumps({"error": f"Unknown jurisdiction: {jurisdiction}"})

    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")

    prefix = cfg["gcs_prefix"]
    mapping = cfg["mapping"]
    print(f"[{jurisdiction}] Starting acquisition: {cfg['name']}")

    # Check if raw data already exists on GCS
    existing = list(bucket.list_blobs(prefix=f"{prefix}/", max_results=5))
    if existing:
        print(f"[{jurisdiction}] Found {len(existing)} existing files on GCS, using those.")
        # Read existing data
        all_dfs = []
        all_blobs = list(bucket.list_blobs(prefix=f"{prefix}/"))
        data_blobs = [b for b in all_blobs if b.name.endswith(('.csv', '.csv.gz', '.parquet', '.zip'))]
        for blob in data_blobs:
            print(f"  Reading {blob.name} ({blob.size / 1e6:.1f}MB)")
            content = blob.download_as_bytes()
            if blob.name.endswith('.parquet'):
                df = pd.read_parquet(io.BytesIO(content))
            elif blob.name.endswith('.csv.gz'):
                df = pd.read_csv(io.BytesIO(content), compression='gzip', low_memory=False)
            elif blob.name.endswith('.csv'):
                sep = cfg.get("csv_sep", ",")
                df = pd.read_csv(io.BytesIO(content), sep=sep, low_memory=False, on_bad_lines='skip')
            else:
                continue
            all_dfs.append(df)

        if all_dfs:
            raw_df = pd.concat(all_dfs, ignore_index=True)
        else:
            return json.dumps({"error": f"No readable data files found under {prefix}/"})
    else:
        # Try to download from URL
        url = cfg["urls"][0] if cfg.get("urls") else None
        if not url or not url.endswith(('.csv', '.csv.gz', '.zip', '.parquet')):
            return json.dumps({
                "status": "manual_download_required",
                "jurisdiction": jurisdiction,
                "name": cfg["name"],
                "urls": cfg["urls"],
                "notes": cfg["notes"],
                "instructions": f"Download raw data from the URL(s) above and upload to gs://properlytic-raw-data/{prefix}/",
            })

        print(f"[{jurisdiction}] Downloading from {url}")
        resp = requests.get(url, timeout=300)
        resp.raise_for_status()
        raw_df = pd.read_csv(io.BytesIO(resp.content), low_memory=False)

        # Upload raw to GCS
        raw_blob = bucket.blob(f"{prefix}/raw_data.csv")
        raw_blob.upload_from_string(resp.content)
        print(f"[{jurisdiction}] Uploaded raw data to GCS ({len(resp.content)/1e6:.1f}MB)")

    # Map columns
    print(f"[{jurisdiction}] Mapping columns ({len(raw_df):,} rows, {len(raw_df.columns)} cols)")
    panel = pd.DataFrame()
    for canon_col, src_col in mapping.items():
        if src_col in raw_df.columns:
            panel[canon_col] = raw_df[src_col]
        else:
            print(f"  WARNING: Source column '{src_col}' not found for '{canon_col}'")

    panel["jurisdiction"] = jurisdiction

    # Derive year from sale_date if needed
    if "year" not in panel.columns and "sale_date" in panel.columns:
        panel["sale_date"] = pd.to_datetime(panel["sale_date"], errors="coerce")
        panel["year"] = panel["sale_date"].dt.year
        print(f"[{jurisdiction}] Derived year from sale_date")

    # Coalesce property_value
    val_cols = [c for c in ["sale_price", "assessed_value"] if c in panel.columns]
    if val_cols:
        panel["property_value"] = panel[val_cols].bfill(axis=1).iloc[:, 0]

    # Clean
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
        return json.dumps({"error": f"No valid rows after cleaning for {jurisdiction}"})

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
    print(f"\n✅ [{jurisdiction}] Panel built: {json.dumps(stats, indent=2)}")
    return json.dumps(stats)


@app.local_entrypoint()
def main(jurisdiction: str = ""):
    import json

    if jurisdiction:
        # Build single jurisdiction
        result = acquire_and_build.remote(jurisdiction)
        print(result)
    else:
        # Build all jurisdictions
        jurisdictions = list(SOURCES.keys())
        print(f"Acquiring and building panels for {len(jurisdictions)} jurisdictions: {jurisdictions}")

        results = list(acquire_and_build.map(jurisdictions))

        combined = {}
        for jur, result in zip(jurisdictions, results):
            parsed = json.loads(result)
            combined[jur] = parsed
            status_icon = "✅" if parsed.get("status") == "ok" else "⚠️" if parsed.get("status") == "manual_download_required" else "❌"
            rows = parsed.get("rows", "?")
            print(f"  {status_icon} {jur}: {rows} rows" if isinstance(rows, int)
                  else f"  {status_icon} {jur}: {parsed.get('status', parsed.get('error', '?'))}")

        with open("scripts/logs/new_jurisdictions_build.json", "w") as f:
            json.dump(combined, f, indent=2)
        print(f"\nDone! Results: scripts/logs/new_jurisdictions_build.json")
