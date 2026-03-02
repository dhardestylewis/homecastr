"""
Build ACS Tract-Level Nationwide Panel for World Model Training
================================================================
Downloads multi-year ACS 5-year estimates at tract level, builds a cross-year
panel with median home value (B25077) as target and demographic features.

Panel columns:
  - geoid (tract FIPS)
  - year
  - median_home_value (B25077 - target, self-reported)
  - median_hh_income (B19013)
  - total_population (B01003)
  - total_housing_units (B25002_001)
  - occupied_units (B25002_002)
  - vacant_units (B25002_003)
  - owner_occupied (B25003_002)
  - renter_occupied (B25003_003)
  - median_year_built (B25035)
  - median_gross_rent (B25064)

Output: gs://properlytic-raw-data/panel/jurisdiction=acs_nationwide/part.parquet
"""

import modal, os

app = modal.App("acs-panel-builder")
image = modal.Image.debian_slim(python_version="3.11").pip_install(
    "google-cloud-storage", "pandas", "pyarrow", "requests"
)
gcs_secret = modal.Secret.from_name("gcs-creds")


# ACS tables and variables to download
ACS_VARIABLES = {
    "B25077_001E": "median_home_value",    # Median value (dollars) — TARGET
    "B19013_001E": "median_hh_income",     # Median household income
    "B01003_001E": "total_population",     # Total population
    "B25002_001E": "total_housing_units",  # Total housing units
    "B25002_002E": "occupied_units",       # Occupied housing units
    "B25002_003E": "vacant_units",         # Vacant housing units
    "B25003_002E": "owner_occupied",       # Owner-occupied units
    "B25003_003E": "renter_occupied",      # Renter-occupied units
    "B25035_001E": "median_year_built",    # Median year structure built
    "B25064_001E": "median_gross_rent",    # Median gross rent
}

# Years with 5-year ACS tract data available
ACS_YEARS = [2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023]


@app.function(
    image=image,
    secrets=[gcs_secret],
    timeout=7200,  # 2h — many API calls
    memory=8192,
)
def build_acs_panel():
    """Download ACS tract data for all years and build cross-year panel."""
    import json, time, io
    import pandas as pd
    import requests
    from google.cloud import storage

    ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")
    BASE_URL = "https://api.census.gov/data/{year}/acs/acs5"

    variables = ",".join(ACS_VARIABLES.keys())
    all_dfs = []

    for year in ACS_YEARS:
        print(f"[{ts()}] Downloading ACS {year} tract data...")
        url = f"{BASE_URL.format(year=year)}?get=NAME,{variables}&for=tract:*&in=state:*"

        try:
            resp = requests.get(url, timeout=120)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  ⚠️ {year} failed: {e}")
            continue

        # First row is header
        header = data[0]
        rows = data[1:]
        df = pd.DataFrame(rows, columns=header)

        # Build geoid from state + county + tract
        df["geoid"] = df["state"] + df["county"] + df["tract"]
        df["year"] = year

        # Rename variables
        for census_var, friendly_name in ACS_VARIABLES.items():
            if census_var in df.columns:
                df[friendly_name] = pd.to_numeric(df[census_var], errors="coerce")

        # Select only needed columns
        keep_cols = ["geoid", "year"] + list(ACS_VARIABLES.values())
        df = df[[c for c in keep_cols if c in df.columns]]

        # Drop rows where target is missing
        df = df.dropna(subset=["median_home_value"])

        # Filter out negative/suppressed values (Census uses -666666666 for suppressed)
        df = df[df["median_home_value"] > 0]

        all_dfs.append(df)
        print(f"  ✅ {year}: {len(df):,} tracts with valid home values")

    if not all_dfs:
        print("❌ No data downloaded!")
        return {"error": "no data"}

    # Combine all years
    panel = pd.concat(all_dfs, ignore_index=True)
    panel = panel.sort_values(["geoid", "year"]).reset_index(drop=True)

    print(f"\n[{ts()}] Panel built:")
    print(f"  Total rows: {panel.shape[0]:,}")
    print(f"  Unique tracts: {panel['geoid'].nunique():,}")
    print(f"  Years: {sorted(panel['year'].unique())}")
    print(f"  Columns: {list(panel.columns)}")
    print(f"\n  Numeric summary:")
    print(panel.describe().to_string())

    # Compute growth rates (key feature for the model)
    panel = panel.sort_values(["geoid", "year"])
    panel["prior_home_value"] = panel.groupby("geoid")["median_home_value"].shift(1)
    panel["home_value_growth"] = (
        (panel["median_home_value"] - panel["prior_home_value"]) / panel["prior_home_value"]
    )

    # Add acct column (alias for geoid, needed by world model)
    panel["acct"] = panel["geoid"]

    # Save to GCS
    print(f"\n[{ts()}] Uploading to GCS...")
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")

    buf = io.BytesIO()
    panel.to_parquet(buf, index=False)
    buf.seek(0)

    blob = bucket.blob("panel/jurisdiction=acs_nationwide/part.parquet")
    blob.upload_from_file(buf, content_type="application/octet-stream")
    print(f"  ✅ Uploaded: gs://properlytic-raw-data/panel/jurisdiction=acs_nationwide/part.parquet")
    print(f"     Size: {buf.tell() / 1e6:.1f} MB")

    # Also save a summary
    summary = {
        "n_rows": len(panel),
        "n_tracts": int(panel["geoid"].nunique()),
        "years": sorted(panel["year"].unique().tolist()),
        "columns": list(panel.columns),
        "median_home_value_mean": float(panel["median_home_value"].mean()),
        "median_home_value_median": float(panel["median_home_value"].median()),
    }
    blob = bucket.blob("panel/jurisdiction=acs_nationwide/summary.json")
    blob.upload_from_string(json.dumps(summary, indent=2))
    print(f"  ✅ Summary: gs://properlytic-raw-data/panel/jurisdiction=acs_nationwide/summary.json")

    return summary


@app.local_entrypoint()
def main():
    result = build_acs_panel.remote()
    print(f"\n✅ Panel built: {result}")
