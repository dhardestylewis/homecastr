"""
enrich_eurostat_globals.py
==========================
Enriches the Eurostat adapted panel with:
  1. FRED macro indicators (fetched from GCS)
  2. World Bank WDI indicators (fetched via REST API)
Outputs: _scratch/data/eurostat_enriched.parquet
"""
import os, sys, io, json, time
import pandas as pd
import polars as pl

sys.stdout.reconfigure(encoding='utf-8')
ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
DATA_DIR = os.path.join(BASE_DIR, "_scratch", "data")

ADAPTED_PATH = os.path.join(DATA_DIR, "eurostat_adapted.parquet")
OUTPUT_PATH = os.path.join(DATA_DIR, "eurostat_enriched.parquet")

# ─── FRED series (global, keyed by yr) ───
FRED_SERIES = {
    "MORTGAGE30US":            "macro_mortgage30",
    "FEDFUNDS":                "macro_fedfunds",
    "DGS10":                   "macro_10yr_treasury",
    "CPIAUCSL":                "macro_cpi_us",
    "CP0000EZ19M086NEST":      "macro_cpi_eurozone",
    "DCOILWTICO":              "macro_oil_price",
    "UNRATE":                  "macro_unemployment_us",
    "LRHUTTTTEZM156S":         "macro_unemployment_eurozone",
    "VIXCLS":                  "macro_vix",
    "GEPUCURRENT":             "macro_global_epu",
    "IR3TIB01EZM156N":         "macro_euribor_3m",
}

# ─── World Bank WDI indicators (per-country, keyed by iso2 x yr) ───
WDI_INDICATORS = {
    "NY.GDP.PCAP.CD":    "wdi_gdp_per_capita",
    "FR.INR.RINR":       "wdi_real_interest_rate",
    "SP.URB.TOTL.IN.ZS": "wdi_urban_pct",
    "EN.POP.DNST":       "wdi_pop_density",
    "AG.LND.AGRI.ZS":    "wdi_ag_land_pct",
    "FP.CPI.TOTL.ZG":    "wdi_cpi_inflation",
}

# NUTS code prefix -> ISO2 country code (for the EU members in Eurostat)
NUTS_TO_ISO2 = {
    "AT": "AT", "BE": "BE", "BG": "BG", "CY": "CY", "CZ": "CZ",
    "DE": "DE", "DK": "DK", "EE": "EE", "EL": "GR", "ES": "ES",
    "FI": "FI", "FR": "FR", "HR": "HR", "HU": "HU", "IE": "IE",
    "IT": "IT", "LT": "LT", "LU": "LU", "LV": "LV", "MT": "MT",
    "NL": "NL", "PL": "PL", "PT": "PT", "RO": "RO", "SE": "SE",
    "SI": "SI", "SK": "SK", "UK": "GB", "NO": "NO", "IS": "IS",
    "CH": "CH", "RS": "RS", "ME": "ME", "MK": "MK", "AL": "AL",
    "BA": "BA", "TR": "TR", "XK": "XK",
}


def fetch_fred_from_gcs():
    """Download FRED CSVs from GCS and build annual lookup."""
    from google.cloud import storage

    creds_path = os.path.join(BASE_DIR, "gcs-creds.json")
    if not os.path.exists(creds_path):
        print(f"[{ts()}] WARNING: gcs-creds.json not found, skipping FRED")
        return None

    client = storage.Client.from_service_account_json(creds_path)
    bucket = client.bucket("properlytic-raw-data")

    macro_frames = {}
    for series_id, col_name in FRED_SERIES.items():
        gcs_path = f"macro/fred/{series_id}.csv"
        blob = bucket.blob(gcs_path)
        if not blob.exists():
            print(f"  WARNING: Missing {gcs_path}")
            continue
        try:
            raw = pd.read_csv(io.BytesIO(blob.download_as_bytes()), on_bad_lines="skip")
            date_col, val_col = raw.columns[0], raw.columns[1]
            raw[date_col] = pd.to_datetime(raw[date_col], errors="coerce")
            raw[val_col] = pd.to_numeric(raw[val_col], errors="coerce")
            raw = raw.dropna(subset=[date_col, val_col])
            raw["_year"] = raw[date_col].dt.year
            annual = raw.groupby("_year")[val_col].mean().reset_index()
            annual.columns = ["yr", col_name]
            macro_frames[col_name] = annual
            print(f"  FRED {col_name}: {len(annual)} years")
        except Exception as e:
            print(f"  ERROR {gcs_path}: {e}")

    if not macro_frames:
        return None

    merged = None
    for col_name, frame in macro_frames.items():
        merged = frame if merged is None else merged.merge(frame, on="yr", how="outer")
    merged = merged.sort_values("yr").reset_index(drop=True)
    return pl.from_pandas(merged).cast({"yr": pl.Int64})


def fetch_wdi_indicators(countries, min_year=2005, max_year=2024):
    """Download WDI indicators from World Bank REST API."""
    import urllib.request

    all_frames = {}
    country_str = ";".join(sorted(set(countries)))

    for indicator_code, col_name in WDI_INDICATORS.items():
        url = (
            f"https://api.worldbank.org/v2/country/{country_str}/indicator/{indicator_code}"
            f"?date={min_year}:{max_year}&format=json&per_page=5000"
        )
        print(f"  WDI {col_name}: fetching {indicator_code}...")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))

            if not isinstance(data, list) or len(data) < 2:
                print(f"    WARNING: empty response for {indicator_code}")
                continue

            records = []
            for entry in data[1]:
                val = entry.get("value")
                yr = entry.get("date")
                iso2 = entry.get("countryiso3code", entry.get("country", {}).get("id", ""))
                # WB API returns ISO3 in countryiso3code, but country.id is ISO2
                country_id = entry.get("country", {}).get("id", "")
                if val is not None and yr is not None:
                    records.append({"iso2": country_id, "yr": int(yr), col_name: float(val)})

            if records:
                df = pl.DataFrame(records)
                all_frames[col_name] = df
                print(f"    OK: {len(records)} rows, {df['iso2'].n_unique()} countries")
            else:
                print(f"    WARNING: no records")
        except Exception as e:
            print(f"    ERROR: {e}")

    if not all_frames:
        return None

    # Merge all WDI indicators on (iso2, yr)
    merged = None
    for col_name, frame in all_frames.items():
        if merged is None:
            merged = frame
        else:
            merged = merged.join(frame, on=["iso2", "yr"], how="outer", coalesce=True)

    return merged


def main():
    print(f"[{ts()}] Loading adapted Eurostat data...")
    df = pl.read_parquet(ADAPTED_PATH)
    print(f"  {len(df)} rows, columns: {df.columns}")

    # Extract geo prefix for country mapping
    if "geo" in df.columns:
        df = df.with_columns(
            pl.col("geo").str.slice(0, 2).alias("nuts_prefix")
        )
    elif "acct" in df.columns:
        # acct is "geo_agriprod", extract the geo part
        df = df.with_columns(
            pl.col("acct").str.split("_").list.first().str.slice(0, 2).alias("nuts_prefix")
        )

    # Map NUTS prefix -> ISO2
    nuts_iso2_map = {k: v for k, v in NUTS_TO_ISO2.items()}
    df = df.with_columns(
        pl.col("nuts_prefix").replace(nuts_iso2_map, default=pl.col("nuts_prefix")).alias("iso2")
    )

    unique_countries = df["iso2"].unique().to_list()
    print(f"[{ts()}] {len(unique_countries)} unique countries: {sorted(unique_countries)[:15]}...")

    # ─── Phase 1: FRED macro (global, keyed on yr only) ───
    print(f"\n[{ts()}] === FRED Macro Indicators (from GCS) ===")
    fred_df = fetch_fred_from_gcs()
    if fred_df is not None:
        print(f"  FRED lookup: {len(fred_df)} years, {len(fred_df.columns)} cols")
        fred_df = fred_df.cast({"yr": pl.Int64})
        df = df.join(fred_df, on="yr", how="left")
        print(f"  After FRED join: {len(df)} rows, {len(df.columns)} cols")
    else:
        print(f"  FRED skipped (no GCS creds or data)")

    # ─── Phase 2: World Bank WDI (per-country, keyed on iso2 x yr) ───
    print(f"\n[{ts()}] === World Bank WDI Indicators ===")
    wdi_df = fetch_wdi_indicators(unique_countries)
    if wdi_df is not None:
        print(f"  WDI lookup: {len(wdi_df)} rows, {len(wdi_df.columns)} cols")
        wdi_df = wdi_df.cast({"yr": pl.Int64})
        df = df.join(wdi_df, on=["iso2", "yr"], how="left")
        print(f"  After WDI join: {len(df)} rows, {len(df.columns)} cols")
    else:
        print(f"  WDI skipped")

    # Drop temp columns
    df = df.drop(["nuts_prefix", "iso2"], strict=False)

    # Summary
    print(f"\n[{ts()}] === Final Enriched Panel ===")
    print(f"  Rows: {len(df)}")
    print(f"  Columns ({len(df.columns)}): {df.columns}")

    # Count non-null macro columns
    macro_cols = [c for c in df.columns if c.startswith("macro_") or c.startswith("wdi_")]
    for mc in macro_cols:
        nn = len(df) - df[mc].null_count()
        print(f"    {mc}: {nn}/{len(df)} non-null ({nn/len(df)*100:.1f}%)")

    df.write_parquet(OUTPUT_PATH)
    print(f"\n[{ts()}] Saved enriched panel to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
