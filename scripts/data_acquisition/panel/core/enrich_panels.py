"""
Enrich panels with macro economic covariates (country-specific + global).
Downloads from public APIs (FRED, BOE, ECB, OECD) and joins to panels on GCS.

Usage:
    python -m modal run scripts/data_acquisition/enrich_panels.py
    python -m modal run scripts/data_acquisition/enrich_panels.py --jurisdiction uk_ppd
"""
import modal, os

app = modal.App("enrich-panels")
image = modal.Image.debian_slim(python_version="3.11").pip_install(
    "google-cloud-storage", "pandas", "pyarrow", "requests",
)
gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])

# FRED series (US + global)
FRED_SERIES = {
    "mortgage_rate_30yr": "MORTGAGE30US",
    "treasury_10yr": "DGS10",
    "fed_funds_rate": "FEDFUNDS",
    "cpi_us": "CPIAUCSL",
    "unemployment_us": "UNRATE",
    "case_shiller_hpi": "CSUSHPINSA",
    "sp500": "SP500",
    "vix": "VIXCLS",
    "oil_price_wti": "DCOILWTICO",
    "global_econ_policy_uncertainty": "GEPUCURRENT",
}

# Bank of England series
BOE_SERIES = {
    "boe_base_rate": "IUDBEDR",          # Official Bank Rate
    "uk_mortgage_rate": "CFMBS32",        # quoted mortgage rate
    "uk_cpi": "D7BT",                    # CPI annual rate
    "uk_rpi": "CZBH",                    # RPI annual rate
    "uk_unemployment": "MGSX",            # unemployment rate %
    "uk_hpi": "LPMVWYR",                 # house price index
}

# ECB/Eurostat series for France
ECB_SERIES = {
    "ecb_main_rate": "FM.B.U2.EUR.4F.KR.MRR_FR.LEV",
    "eu_hicp": "ICP.M.FR.N.000000.4.ANR",
}


@app.function(image=image, secrets=[gcs_secret], timeout=1200, memory=16384)
def enrich_panel(jurisdiction: str) -> str:
    """Download macro data and join to existing panel on GCS."""
    import json, io, time
    import pandas as pd
    import requests
    from google.cloud import storage

    t0 = time.time()
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")

    # Load existing panel
    panel_path = f"panel/jurisdiction={jurisdiction}/part.parquet"
    blob = bucket.blob(panel_path)
    if not blob.exists():
        return json.dumps({"error": f"Panel not found: {panel_path}"})

    content = blob.download_as_bytes()
    panel = pd.read_parquet(io.BytesIO(content))
    print(f"[{jurisdiction}] Loaded panel: {len(panel):,} rows, cols: {list(panel.columns)}")

    if "year" not in panel.columns:
        return json.dumps({"error": "Panel missing 'year' column"})

    macro_joined = 0

    # ── FRED data (global/US) — direct CSV download, no API key ──
    print(f"[{jurisdiction}] Fetching FRED data (CSV direct)...")
    for col_name, series_id in FRED_SERIES.items():
        try:
            # Try reading from GCS cache first
            fred_blob = bucket.blob(f"macro/fred/{series_id}.csv")
            if fred_blob.exists():
                fc = fred_blob.download_as_bytes()
                fd = pd.read_csv(io.BytesIO(fc))
            else:
                # Direct CSV download — no API key needed
                url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}&cosd=1990-01-01"
                resp = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
                if resp.status_code == 200 and len(resp.content) > 50:
                    fd = pd.read_csv(io.BytesIO(resp.content))
                    # Normalize column names: DATE -> date, series_id -> value
                    fd.columns = ["date"] + ["value"] + list(fd.columns[2:]) if len(fd.columns) > 1 else fd.columns
                    if len(fd.columns) == 2:
                        fd.columns = ["date", "value"]
                    # Cache to GCS
                    fred_blob.upload_from_string(resp.content)
                else:
                    print(f"  FRED {series_id}: HTTP {resp.status_code}")
                    continue

            if "date" in fd.columns and "value" in fd.columns:
                fd["date"] = pd.to_datetime(fd["date"], errors="coerce")
                fd["year"] = fd["date"].dt.year
                fd["value"] = pd.to_numeric(fd["value"], errors="coerce")
                annual = fd.groupby("year")["value"].mean().reset_index()
                annual.columns = ["year", col_name]
                panel = panel.merge(annual, on="year", how="left")
                macro_joined += 1
                print(f"  ✅ Joined {col_name} ({len(annual)} years)")
        except Exception as e:
            print(f"  ⚠️ FRED {col_name}: {e}")

    # ── UK-specific: Bank of England ──
    if jurisdiction in ("uk_ppd",):
        print(f"[{jurisdiction}] Fetching Bank of England data...")
        for col_name, series_code in BOE_SERIES.items():
            try:
                boe_blob = bucket.blob(f"macro/boe/{series_code}.csv")
                if boe_blob.exists():
                    bc = boe_blob.download_as_bytes()
                    bd = pd.read_csv(io.BytesIO(bc), on_bad_lines='skip')
                else:
                    url = f"https://www.bankofengland.co.uk/boeapps/database/_iadb-fromshowcolumns.asp?csv.x=yes&SeriesCodes={series_code}&CSVF=CN&Datefrom=01/Jan/1990&Dateto=31/Dec/2025"
                    resp = requests.get(url, timeout=30,
                                       headers={"User-Agent": "Mozilla/5.0"})
                    if resp.status_code == 200 and len(resp.content) > 100:
                        # BOE CSV has header rows — try multiple skiprows values
                        for skip in [0, 1, 3, 5]:
                            try:
                                bd = pd.read_csv(io.BytesIO(resp.content), skiprows=skip, on_bad_lines='skip')
                                if len(bd.columns) >= 2 and len(bd) > 5:
                                    break
                            except Exception:
                                continue
                        boe_blob.upload_from_string(resp.content)
                    else:
                        print(f"  BOE {series_code}: HTTP {resp.status_code}")
                        continue

                # Parse BOE CSV (DATE, VALUE format)
                if len(bd.columns) >= 2:
                    bd.columns = ["date", "value"] + list(bd.columns[2:])
                    bd["date"] = pd.to_datetime(bd["date"], errors="coerce", dayfirst=True)
                    bd["year"] = bd["date"].dt.year
                    bd["value"] = pd.to_numeric(bd["value"], errors="coerce")
                    annual = bd.groupby("year")["value"].mean().reset_index()
                    annual.columns = ["year", col_name]
                    panel = panel.merge(annual, on="year", how="left")
                    macro_joined += 1
                    print(f"  ✅ Joined {col_name} ({len(annual)} years)")
            except Exception as e:
                print(f"  ⚠️ BOE {col_name}: {e}")

    # ── France-specific: use FRED for Euro area ──
    if jurisdiction in ("france_dvf",):
        print(f"[{jurisdiction}] Fetching Euro area data...")
        eu_fred = {
            "ecb_interest_rate": "IR3TIB01EZM156N",  # 3-month interbank (Euro)
            "eu_cpi": "CP0000EZ19M086NEST",           # HICP Euro area
            "eu_unemployment": "LRHUTTTTEZM156S",     # Unemployment Euro area
            "france_hpi": "QFRN628BIS",               # France house prices (BIS)
        }
        for col_name, series_id in eu_fred.items():
            try:
                fred_blob = bucket.blob(f"macro/fred/{series_id}.csv")
                if fred_blob.exists():
                    fc = fred_blob.download_as_bytes()
                    fd = pd.read_csv(io.BytesIO(fc))
                else:
                    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}&cosd=1990-01-01"
                    resp = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
                    if resp.status_code == 200 and len(resp.content) > 50:
                        fd = pd.read_csv(io.BytesIO(resp.content))
                        if len(fd.columns) == 2:
                            fd.columns = ["date", "value"]
                        fred_blob.upload_from_string(resp.content)
                    else:
                        continue
                if "date" in fd.columns and "value" in fd.columns:
                    fd["date"] = pd.to_datetime(fd["date"], errors="coerce")
                    fd["year"] = fd["date"].dt.year
                    fd["value"] = pd.to_numeric(fd["value"], errors="coerce")
                    annual = fd.groupby("year")["value"].mean().reset_index()
                    annual.columns = ["year", col_name]
                    panel = panel.merge(annual, on="year", how="left")
                    macro_joined += 1
                    print(f"  ✅ Joined {col_name}")
            except Exception as e:
                print(f"  ⚠️ {col_name}: {e}")

    # Write enriched panel back to GCS
    panel.to_parquet("/tmp/enriched_panel.parquet", index=False)
    blob.upload_from_filename("/tmp/enriched_panel.parquet")

    elapsed = time.time() - t0
    numeric_cols = [c for c in panel.columns if panel[c].dtype in ('float64', 'float32', 'int64', 'int32')
                    and c not in ('year', 'year_built')]
    stats = {
        "status": "ok",
        "jurisdiction": jurisdiction,
        "rows": len(panel),
        "columns": list(panel.columns),
        "numeric_features": numeric_cols,
        "n_numeric": len(numeric_cols),
        "macro_joined": macro_joined,
        "elapsed_s": round(elapsed, 1),
    }
    print(f"\n✅ [{jurisdiction}] Enriched: {macro_joined} macro series joined, {len(numeric_cols)} numeric features")
    return json.dumps(stats)


@app.local_entrypoint()
def main(jurisdiction: str = ""):
    import json

    if jurisdiction:
        result = enrich_panel.remote(jurisdiction)
        print(result)
    else:
        # Enrich all panels that exist
        jurisdictions = ["uk_ppd", "france_dvf", "seattle_wa", "hcad_houston"]
        print(f"Enriching {len(jurisdictions)} panels with macro covariates")
        results = list(enrich_panel.map(jurisdictions))
        for jur, result in zip(jurisdictions, results):
            parsed = json.loads(result)
            s = "✅" if parsed.get("status") == "ok" else "❌"
            n = parsed.get("macro_joined", 0)
            print(f"  {s} {jur}: {n} macro series joined, {parsed.get('n_numeric', 0)} numeric features")
