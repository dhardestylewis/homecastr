"""
ingest_fao_land_prices.py
=========================
Downloads FAOSTAT Producer Prices and Land Use data,
extracts agricultural land price indicators, and harmonizes
to the world model panel schema.

Output: _scratch/data/fao_land_prices.parquet
Schema: source, iso2, region_name, yr, price_usd_per_ha, crop_type
"""

import os, sys, json, time, io, zipfile
import urllib.request
import pandas as pd
import polars as pl

sys.stdout.reconfigure(encoding='utf-8')
ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
DATA_DIR = os.path.join(BASE_DIR, "_scratch", "data")
os.makedirs(DATA_DIR, exist_ok=True)

# FAOSTAT bulk download URLs
# PP = Prices Received by Farmers (Producer Prices - Annual)
# LR = Land Use (for context on agricultural area)
FAOSTAT_BULK_URLS = {
    "PP": "https://fenixservices.fao.org/faostat/static/bulkdownloads/Prices_E_All_Data_(Normalized).zip",
    "PI": "https://fenixservices.fao.org/faostat/static/bulkdownloads/Deflators_E_All_Data_(Normalized).zip",
}

# ISO3 to ISO2 mapping for common countries
# We'll build this dynamically from the data
def iso3_to_iso2(iso3):
    """Convert ISO3 country code to ISO2 using pycountry-like mapping."""
    # Hardcoded map for the most important agricultural countries
    MAP = {
        "USA": "US", "BRA": "BR", "IND": "IN", "CHN": "CN", "ARG": "AR",
        "AUS": "AU", "CAN": "CA", "MEX": "MX", "ZAF": "ZA", "IDN": "ID",
        "THA": "TH", "VNM": "VN", "RUS": "RU", "UKR": "UA", "KAZ": "KZ",
        "NGA": "NG", "KEN": "KE", "TZA": "TZ", "ETH": "ET", "GHA": "GH",
        "COL": "CO", "PER": "PE", "CHL": "CL", "URY": "UY", "PRY": "PY",
        "BOL": "BO", "ECU": "EC", "VEN": "VE", "MYS": "MY", "PHL": "PH",
        "MMR": "MM", "BGD": "BD", "PAK": "PK", "NPL": "NP", "LKA": "LK",
        "JPN": "JP", "KOR": "KR", "TWN": "TW", "NZL": "NZ", "EGY": "EG",
        "MAR": "MA", "TUN": "TN", "DZA": "DZ", "IRN": "IR", "IRQ": "IQ",
        "SAU": "SA", "TUR": "TR", "ISR": "IL", "GBR": "GB", "FRA": "FR",
        "DEU": "DE", "ITA": "IT", "ESP": "ES", "PRT": "PT", "NLD": "NL",
        "BEL": "BE", "POL": "PL", "ROU": "RO", "BGR": "BG", "HUN": "HU",
        "CZE": "CZ", "SVK": "SK", "HRV": "HR", "SRB": "RS", "AUT": "AT",
        "CHE": "CH", "SWE": "SE", "NOR": "NO", "FIN": "FI", "DNK": "DK",
        "IRL": "IE", "GRC": "GR", "CYP": "CY", "MLT": "MT", "EST": "EE",
        "LVA": "LV", "LTU": "LT", "SVN": "SI", "LUX": "LU", "ISL": "IS",
        "MOZ": "MZ", "ZMB": "ZM", "ZWE": "ZW", "MWI": "MW", "UGA": "UG",
        "RWA": "RW", "CMR": "CM", "CIV": "CI", "SEN": "SN", "MLI": "ML",
        "BFA": "BF", "NER": "NE", "TCD": "TD", "SDN": "SD", "SOM": "SO",
        "AGO": "AO", "COD": "CD", "COG": "CG", "GAB": "GA", "MDG": "MG",
    }
    return MAP.get(iso3, iso3[:2] if len(iso3) >= 2 else iso3)


def download_faostat_bulk(domain_key, url):
    """Download and extract FAOSTAT bulk CSV."""
    cache_path = os.path.join(DATA_DIR, f"faostat_{domain_key}_raw.csv")
    
    if os.path.exists(cache_path):
        print(f"[{ts()}] Using cached {cache_path}")
        return pd.read_csv(cache_path, encoding='latin-1', low_memory=False)
    
    print(f"[{ts()}] Downloading FAOSTAT {domain_key} bulk data...")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            zip_data = resp.read()
    except Exception as e:
        print(f"[{ts()}] ERROR downloading {domain_key}: {e}")
        return None
    
    print(f"[{ts()}] Downloaded {len(zip_data)/1e6:.1f} MB, extracting...")
    
    with zipfile.ZipFile(io.BytesIO(zip_data)) as z:
        csv_files = [f for f in z.namelist() if f.endswith('.csv')]
        if not csv_files:
            print(f"[{ts()}] ERROR: No CSV files found in archive")
            return None
            
        # Use the largest CSV (the main data file)
        csv_name = max(csv_files, key=lambda f: z.getinfo(f).file_size)
        print(f"[{ts()}] Extracting {csv_name}...")
        
        with z.open(csv_name) as csv_file:
            df = pd.read_csv(csv_file, encoding='latin-1', low_memory=False)
            
    # Cache locally
    df.to_csv(cache_path, index=False)
    print(f"[{ts()}] Cached {len(df):,} rows to {cache_path}")
    return df


def extract_land_prices_from_producer_prices(df_pp):
    """
    FAOSTAT Producer Prices contain price per tonne for crops.
    We can derive a proxy for land value by looking at high-value crop prices
    and their trends, as these correlate strongly with farmland values.
    
    More directly useful: the "Prices Paid" domain has input prices including land rent.
    We extract crop producer prices as a proxy indicator of agricultural profitability.
    """
    if df_pp is None:
        return None
    
    print(f"[{ts()}] Processing Producer Prices: {len(df_pp):,} rows")
    print(f"  Columns: {list(df_pp.columns)}")
    
    # Filter to relevant elements (Producer Price USD/tonne)
    # Common element codes: 5532 = Producer Price (USD/tonne)
    price_mask = df_pp['Element'].str.contains('Producer Price', case=False, na=False)
    usd_mask = df_pp['Unit'].str.contains('USD', case=False, na=False) if 'Unit' in df_pp.columns else True
    
    df_prices = df_pp[price_mask].copy()
    if 'Unit' in df_pp.columns:
        df_prices = df_prices[df_prices['Unit'].str.contains('USD', case=False, na=False)]
    
    print(f"  Filtered to {len(df_prices):,} USD producer price rows")
    
    if len(df_prices) == 0:
        # Try alternative column names
        print(f"  Available Elements: {df_pp['Element'].unique()[:10]}")
        return None
    
    # Aggregate: country x year -> median crop price (proxy for ag profitability)
    # This is NOT a land price, but it's a covariate that correlates with land value
    area_col = 'Area Code (M49)' if 'Area Code (M49)' in df_prices.columns else 'Area Code'
    
    records = []
    for _, row in df_prices.iterrows():
        try:
            iso3 = str(row.get('Area Code (ISO3)', row.get('Area', '')))
            yr = int(row.get('Year', 0))
            val = float(row.get('Value', 0))
            item = str(row.get('Item', ''))
            
            if yr >= 1990 and val > 0:
                records.append({
                    'source': 'faostat_pp',
                    'iso3': iso3,
                    'iso2': iso3_to_iso2(iso3),
                    'yr': yr,
                    'crop': item,
                    'price_usd_per_tonne': val,
                })
        except (ValueError, TypeError):
            continue
    
    if not records:
        return None
        
    df_out = pd.DataFrame(records)
    print(f"[{ts()}] Extracted {len(df_out):,} crop price records across {df_out['iso2'].nunique()} countries")
    
    # Aggregate to country-year median producer price
    agg = df_out.groupby(['iso2', 'yr']).agg(
        median_crop_price_usd_tonne=('price_usd_per_tonne', 'median'),
        n_crops=('crop', 'nunique'),
        source=('source', 'first'),
    ).reset_index()
    
    return agg


def fetch_usda_nass_land_values():
    """
    Fetch USDA NASS agricultural land values per acre by state.
    Uses the QuickStats API.
    """
    print(f"[{ts()}] Fetching USDA NASS farmland values...")
    
    # USDA NASS API key is optional for basic queries
    # commodity_desc=AG LAND&statisticcat_desc=VALUE&unit_desc=$ / ACRE
    base_url = "https://quickstats.nass.usda.gov/api/api_GET/"
    
    # Try without API key first (may be rate limited)
    params = {
        "commodity_desc": "AG LAND",
        "statisticcat_desc": "VALUE",  
        "unit_desc": "$ / ACRE",
        "agg_level_desc": "STATE",
        "freq_desc": "ANNUAL",
        "format": "JSON",
    }
    
    query_str = "&".join(f"{k}={urllib.request.quote(v)}" for k, v in params.items())
    url = f"{base_url}?{query_str}"
    
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"[{ts()}] USDA NASS API error: {e}")
        print(f"[{ts()}] Note: USDA NASS may require an API key. Skipping.")
        return None
    
    if "data" not in data:
        print(f"[{ts()}] USDA NASS returned no data. Keys: {list(data.keys())}")
        if "error" in data:
            print(f"  Error: {data['error']}")
        return None
    
    records = []
    for entry in data["data"]:
        try:
            state = entry.get("state_name", "")
            yr = int(entry.get("year", 0))
            val_str = entry.get("Value", "").replace(",", "")
            val = float(val_str)
            desc = entry.get("short_desc", "")
            
            if yr >= 1990 and val > 0:
                # Convert $/acre to $/hectare (1 hectare = 2.47105 acres)
                val_per_ha = val * 2.47105
                
                records.append({
                    'source': 'usda_nass',
                    'iso2': 'US',
                    'region_name': state,
                    'yr': yr,
                    'price_usd_per_ha': val_per_ha,
                    'description': desc,
                })
        except (ValueError, TypeError):
            continue
    
    if not records:
        return None
        
    df = pd.DataFrame(records)
    print(f"[{ts()}] USDA NASS: {len(df):,} records, {df['region_name'].nunique()} states, years {df['yr'].min()}-{df['yr'].max()}")
    return df


def main():
    all_panels = []
    
    # 1. FAOSTAT Producer Prices (proxy for agricultural profitability)
    print(f"[{ts()}] === FAOSTAT Producer Prices ===")
    df_pp = download_faostat_bulk("PP", FAOSTAT_BULK_URLS["PP"])
    fao_prices = extract_land_prices_from_producer_prices(df_pp)
    if fao_prices is not None:
        print(f"[{ts()}] FAO Producer Prices: {len(fao_prices):,} country-year records")
        all_panels.append(("fao_producer_prices", pl.from_pandas(fao_prices)))
    
    # 2. USDA NASS Land Values (direct land price)
    print(f"\n[{ts()}] === USDA NASS Land Values ===")
    usda_prices = fetch_usda_nass_land_values()
    if usda_prices is not None:
        print(f"[{ts()}] USDA NASS: {len(usda_prices):,} state-year records")
        all_panels.append(("usda_nass", pl.from_pandas(usda_prices)))
    
    # 3. Summary
    print(f"\n[{ts()}] === Panel Summary ===")
    for name, df in all_panels:
        print(f"  {name}: {len(df):,} rows, {df.columns}")
    
    # Save each panel separately for now
    for name, df in all_panels:
        out_path = os.path.join(DATA_DIR, f"{name}.parquet")
        df.write_parquet(out_path)
        print(f"[{ts()}] Saved {out_path}")
    
    print(f"\n[{ts()}] Done. {len(all_panels)} panels saved.")


if __name__ == "__main__":
    main()
