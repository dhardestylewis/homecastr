"""
Build panels for ALL jurisdictions with raw data on GCS.
Transforms raw jurisdiction data into canonical panel format,
joins contextual data (FRED, LEHD, FEMA), and uploads to GCS.

Usage: modal run scripts/build_all_panels.py
"""
import modal, os, sys

app = modal.App("build-all-panels")
image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "google-cloud-storage", "pandas", "pyarrow", "numpy", "polars",
        "requests",
    )
)
gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])


# ── Schema mappings per jurisdiction ──────────────────────────────────
JURISDICTIONS = {
    "uk_ppd": {
        "gcs_raw": "uk_ppd/pp-complete.csv",
        "mapping": {
            "parcel_id": 0,       # transaction unique ID (col index 0)
            "sale_price": 1,      # price paid
            "sale_date": 2,       # date of transfer
            "address": 7,         # PAON (primary addressable object)
            "dwelling_type": 4,   # D=Detached, S=Semi, T=Terraced, F=Flat, O=Other
        },
        "year_col_idx": 2,        # extract year from sale_date
        "has_header": False,
        "notes": "UK Land Registry: no assessed values, only sale prices. Each row is a transaction.",
    },
    "boston_ma": {
        "gcs_raw": "boston_ma/assessments_2024.csv",
        "mapping": {
            "parcel_id": "PID",
            "assessed_value": "AV_TOTAL",
            "land_value": "AV_LAND",
            "improvement_value": "AV_BLDG",
            "sqft": "LIVING_AREA",
            "land_area": "LOT_SIZE",
            "year_built": "YR_BUILT",
            "bedrooms": "R_BDRMS",
            "bathrooms": "R_FULL_BTH",
            "dwelling_type": "LU",           # land use code
            "address": "ST_NUM",
            "lat": "LATITUDE",
            "lon": "LONGITUDE",
        },
        "year_col": "FY",  # fiscal year
        "notes": "Boston Assessor: market value assessments, annual revaluation.",
    },
    "la_county": {
        "gcs_raw": "la_county/assessor_parcels.csv",
        "mapping": {
            "parcel_id": "AIN",              # Assessor's Identification Number
            "assessed_value": "TotalValue",
            "land_value": "LandValue",
            "improvement_value": "ImprovementValue",
            "sqft": "SQFTmain",
            "year_built": "YearBuilt",
            "bedrooms": "Bedrooms",
            "bathrooms": "Bathrooms",
            "dwelling_type": "UseType",
            "address": "SitusAddress",
            "lat": "CENTER_LAT",
            "lon": "CENTER_LON",
        },
        "year_col": "RollYear",
        "notes": "LA County Assessor: assessed values (Prop 13 capped like SF).",
    },
    "seattle_wa": {
        "gcs_raw": "seattle_wa/real_property_sales.zip",
        "mapping": {
            "parcel_id": "Major",   # Major + Minor = full PIN
            "sale_price": "SalePrice",
            "sale_date": "DocumentDate",
            "dwelling_type": "PropertyType",
        },
        "notes": "King County sales data. Need to join with assessor data for property details.",
    },
    "cook_county_il": {
        "gcs_raw": "cook_county_il/",  # multiple files
        "mapping": {
            "parcel_id": "pin",
            "assessed_value": "certified_tot",
            "sale_price": "sale_price",
            "sale_date": "sale_date",
        },
        "notes": "Cook County (Chicago): assessed at 10% of market value, need to adjust.",
    },
}


@app.function(image=image, secrets=[gcs_secret], timeout=3600, memory=32768)
def build_panel(jurisdiction: str) -> str:
    """Build a panel for one jurisdiction from raw data on GCS."""
    import json, io, time
    import pandas as pd
    import numpy as np
    from google.cloud import storage

    t0 = time.time()
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")

    cfg = JURISDICTIONS.get(jurisdiction)
    if not cfg:
        return json.dumps({"status": "error", "error": f"Unknown jurisdiction: {jurisdiction}"})

    print(f"[{jurisdiction}] Building panel from {cfg['gcs_raw']}")
    print(f"  Notes: {cfg.get('notes', '')}")

    try:
        # Download raw data
        blob = bucket.blob(cfg["gcs_raw"])
        if not blob.exists():
            return json.dumps({"status": "error", "error": f"Raw data not found: {cfg['gcs_raw']}"})

        content = blob.download_as_bytes()
        size_mb = len(content) / 1e6
        print(f"  Downloaded: {size_mb:.1f} MB")

        # Parse based on format
        if cfg["gcs_raw"].endswith(".zip"):
            import zipfile
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                csv_name = [f for f in zf.namelist() if f.endswith(".csv")][0]
                try:
                    df = pd.read_csv(zf.open(csv_name), low_memory=False)
                except UnicodeDecodeError:
                    df = pd.read_csv(zf.open(csv_name), low_memory=False, encoding="latin-1")
        elif cfg.get("has_header") is False:
            # UK PPD has no header
            df = pd.read_csv(io.BytesIO(content), header=None, low_memory=False)
        else:
            df = pd.read_csv(io.BytesIO(content), low_memory=False)

        print(f"  Raw rows: {len(df):,}, columns: {len(df.columns)}")

        # Map to canonical columns
        mapping = cfg["mapping"]
        panel = pd.DataFrame()
        panel["jurisdiction"] = jurisdiction

        for canon_col, raw_col in mapping.items():
            if isinstance(raw_col, int):
                if raw_col < len(df.columns):
                    panel[canon_col] = df.iloc[:, raw_col].values
            elif raw_col in df.columns:
                panel[canon_col] = df[raw_col].values
            else:
                print(f"  WARNING: column '{raw_col}' not found for {canon_col}")
                panel[canon_col] = np.nan

        # Extract year
        if "year_col" in cfg and cfg["year_col"] in df.columns:
            panel["year"] = pd.to_numeric(df[cfg["year_col"]], errors="coerce").astype("Int64")
        elif "year_col_idx" in cfg:
            # Extract year from date column
            date_col = panel.get("sale_date")
            if date_col is not None:
                panel["year"] = pd.to_datetime(date_col, errors="coerce").dt.year
        elif "sale_date" in panel.columns:
            panel["year"] = pd.to_datetime(panel["sale_date"], errors="coerce").dt.year

        # Compute property_value (coalesce sale_price, assessed_value)
        if "sale_price" in panel.columns and "assessed_value" in panel.columns:
            panel["property_value"] = panel["sale_price"].fillna(panel["assessed_value"])
        elif "sale_price" in panel.columns:
            panel["property_value"] = panel["sale_price"]
        elif "assessed_value" in panel.columns:
            panel["property_value"] = panel["assessed_value"]

        # Clean numeric columns
        for col in ["property_value", "sale_price", "assessed_value", "land_value",
                     "improvement_value", "sqft", "land_area", "year_built",
                     "bedrooms", "bathrooms", "stories", "lat", "lon"]:
            if col in panel.columns:
                panel[col] = pd.to_numeric(panel[col], errors="coerce")

        # Drop rows with no year or no value
        before = len(panel)
        panel = panel.dropna(subset=["year"])
        panel = panel[panel["year"] >= 1990]
        if "property_value" in panel.columns:
            panel = panel.dropna(subset=["property_value"])
            panel = panel[panel["property_value"] > 0]
        print(f"  After cleaning: {len(panel):,} rows (dropped {before - len(panel):,})")

        # v11.1: Enforce minimum feature set across ALL jurisdictions.
        # The model handles nulls via feature dropout — this just ensures the
        # column exists so grand panel training works without schema mismatches.
        MINIMUM_FEATURES = ["lat", "lon", "sqft", "year_built", "bedrooms"]
        for feat in MINIMUM_FEATURES:
            if feat not in panel.columns:
                panel[feat] = np.nan
                print(f"  Added missing feature column: {feat} (null)")

        # Stats
        years = sorted(panel["year"].dropna().unique())
        n_parcels = panel["parcel_id"].nunique() if "parcel_id" in panel.columns else "?"
        print(f"  Years: {min(years)}-{max(years)} ({len(years)} years)")
        print(f"  Parcels: {n_parcels:,}" if isinstance(n_parcels, int) else f"  Parcels: {n_parcels}")

        # ── Join FRED contextual data ─────────────────────────────────
        fred_series = {
            "MORTGAGE30US.csv": "mortgage_rate_30yr",
            "FEDFUNDS.csv": "fed_funds_rate",
            "CPIAUCSL.csv": "cpi",
            "UNRATE.csv": "unemployment_rate",
        }
        for gcs_file, col_name in fred_series.items():
            fred_blob = bucket.blob(f"contextual/fred/{gcs_file}")
            if fred_blob.exists():
                try:
                    fred_df = pd.read_csv(io.BytesIO(fred_blob.download_as_bytes()))
                    fred_df["DATE"] = pd.to_datetime(fred_df["DATE"], errors="coerce")
                    fred_df["year"] = fred_df["DATE"].dt.year
                    annual = fred_df.groupby("year").agg({fred_df.columns[1]: "mean"}).reset_index()
                    annual = annual.rename(columns={annual.columns[1]: col_name})
                    panel = panel.merge(annual[["year", col_name]], on="year", how="left")
                    print(f"  Joined FRED {col_name}: {panel[col_name].notna().sum():,} non-null")
                except Exception as e:
                    print(f"  FRED {gcs_file} join failed: {e}")

        # ── Join FEMA disaster data ───────────────────────────────────
        fema_blob = bucket.blob("contextual/fema/disasters_by_county_year.csv")
        if fema_blob.exists():
            try:
                fema_df = pd.read_csv(io.BytesIO(fema_blob.download_as_bytes()))
                # Simple: count disasters per year (national level for now)
                fema_annual = fema_df.groupby("year").size().reset_index(name="fema_disaster_count")
                panel = panel.merge(fema_annual, on="year", how="left")
                panel["fema_disaster_count"] = panel["fema_disaster_count"].fillna(0)
                print(f"  Joined FEMA disaster count")
            except Exception as e:
                print(f"  FEMA join failed: {e}")

        # ── Save panel to GCS ─────────────────────────────────────────
        output_path = f"panel/jurisdiction={jurisdiction}/part.parquet"
        parquet_buf = io.BytesIO()
        panel.to_parquet(parquet_buf, index=False)
        parquet_bytes = parquet_buf.getvalue()
        bucket.blob(output_path).upload_from_string(parquet_bytes, content_type="application/octet-stream")

        elapsed = time.time() - t0
        result = {
            "status": "ok",
            "jurisdiction": jurisdiction,
            "rows": len(panel),
            "parcels": int(n_parcels) if isinstance(n_parcels, int) else 0,
            "years": f"{min(years)}-{max(years)}",
            "n_years": len(years),
            "columns": list(panel.columns),
            "size_mb": round(len(parquet_bytes) / 1e6, 1),
            "gcs_path": output_path,
            "elapsed_s": round(elapsed, 1),
        }
        print(f"\n  ✅ Panel saved: {output_path} ({result['size_mb']}MB, {elapsed:.0f}s)")
        return json.dumps(result, default=str)

    except Exception as e:
        import traceback
        return json.dumps({"status": "error", "jurisdiction": jurisdiction, "error": str(e),
                          "traceback": traceback.format_exc()})


@app.local_entrypoint()
def main():
    import json

    # Build panels for all jurisdictions with raw data
    jurisdictions = list(JURISDICTIONS.keys())
    print(f"Building panels for {len(jurisdictions)} jurisdictions: {jurisdictions}")

    results = list(build_panel.map(jurisdictions))

    combined = {}
    for jur, result in zip(jurisdictions, results):
        combined[jur] = json.loads(result)
        status = "✅" if combined[jur].get("status") == "ok" else "❌"
        rows = combined[jur].get("rows", "?")
        years = combined[jur].get("years", "?")
        print(f"  {status} {jur}: {rows:,} rows, years {years}" if isinstance(rows, int)
              else f"  {status} {jur}: {combined[jur].get('error', '?')}")

    with open("scripts/logs/panel_build_all.json", "w") as f:
        json.dump(combined, f, indent=2)

    print(f"\nDone! Results: scripts/logs/panel_build_all.json")
