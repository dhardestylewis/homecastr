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
    # TARGET
    "B25077_001E": "median_home_value",    # Median value (dollars) — TARGET
    # Income / affordability
    "B19013_001E": "median_hh_income",     # Median household income
    "B25071_001E": "rent_burden_pct",      # Median gross rent as % of income
    "B25064_001E": "median_gross_rent",    # Median gross rent
    # Demographics
    "B01003_001E": "total_population",     # Total population
    "B23025_003E": "employed_pop",         # Employed civilian pop 16+
    "B23025_005E": "unemployed_pop",       # Unemployed civilian pop 16+
    "B15003_022E": "bachelors_degree",     # Pop with bachelor's degree
    "B15003_023E": "masters_degree",       # Pop with master's degree
    "B15003_025E": "doctorate_degree",     # Pop with doctorate
    # Housing supply
    "B25001_001E": "total_housing_units",  # Total housing units (supply)
    "B25002_002E": "occupied_units",       # Occupied housing units
    "B25002_003E": "vacant_units",         # Vacant housing units
    "B25003_002E": "owner_occupied",       # Owner-occupied units
    "B25003_003E": "renter_occupied",      # Renter-occupied units
    # Structure
    "B25035_001E": "median_year_built",    # Median year structure built
    # Mobility
    "B07001_001E": "total_movers",         # Geographic mobility (total)
    "B07001_017E": "moved_from_abroad",    # Moved from abroad
}

# Years: ACS 5-year estimates available from 2009 (first 5-year release) through 2023
ACS_YEARS = list(range(2009, 2024))  # 2009-2023 = 15 years


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
    BASE = "https://api.census.gov/data/{year}/acs/acs5"
    import time as _time

    # All state FIPS codes (50 states + DC + PR)
    STATE_FIPS = [f"{i:02d}" for i in list(range(1, 57)) + [72] if i not in (3, 7, 14, 43, 52)]

    all_vars = list(ACS_VARIABLES.keys())
    all_dfs = []

    for year in ACS_YEARS:
        print(f"\n[{ts()}] ACS {year}...")
        url_base = BASE.format(year=year)

        # Probe which variables work for this year (test against Texas)
        var_str = ",".join(all_vars)
        test_url = f"{url_base}?get=NAME,{var_str}&for=tract:*&in=state:48"
        try:
            r = requests.get(test_url, timeout=60)
            if r.status_code == 200:
                working_vars = all_vars  # All work
                print(f"  All {len(all_vars)} vars available")
            else:
                # Probe individually
                print(f"  Some vars unavailable, probing...")
                working_vars = []
                for v in all_vars:
                    try:
                        r2 = requests.get(f"{url_base}?get=NAME,{v}&for=tract:*&in=state:48", timeout=20)
                        if r2.status_code == 200:
                            working_vars.append(v)
                    except:
                        pass
                    _time.sleep(0.2)
                print(f"  {len(working_vars)}/{len(all_vars)} vars available")
        except Exception as e:
            print(f"  ⚠️ Probe failed: {e}")
            continue

        if "B25077_001E" not in working_vars:
            print(f"  ⚠️ Target variable missing, skipping year")
            continue

        # Download per state
        var_str = ",".join(working_vars)
        state_dfs = []
        for fips in STATE_FIPS:
            url = f"{url_base}?get=NAME,{var_str}&for=tract:*&in=state:{fips}"
            try:
                resp = requests.get(url, timeout=120)
                if resp.status_code == 200:
                    data = resp.json()
                    if len(data) > 1:
                        sdf = pd.DataFrame(data[1:], columns=data[0])
                        state_dfs.append(sdf)
            except:
                pass
            _time.sleep(0.1)  # 100ms between states

        if not state_dfs:
            print(f"  ⚠️ No state data")
            continue

        df = pd.concat(state_dfs, ignore_index=True)
        df["geoid"] = df["state"] + df["county"] + df["tract"]
        df["year"] = year
        for cv, fn in ACS_VARIABLES.items():
            if cv in df.columns:
                df[fn] = pd.to_numeric(df[cv], errors="coerce")
        keep = ["geoid", "year"] + [ACS_VARIABLES[v] for v in working_vars if ACS_VARIABLES[v] in df.columns]
        df = df[[c for c in keep if c in df.columns]]
        if "median_home_value" in df.columns:
            df = df.dropna(subset=["median_home_value"])
            df = df[df["median_home_value"] > 0]
        all_dfs.append(df)
        print(f"  ✅ {len(df):,} tracts from {len(state_dfs)} states, {len(df.columns)} cols")

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

    # Derived rates
    panel["vacancy_rate"] = panel["vacant_units"] / panel["total_housing_units"].replace(0, float('nan'))
    panel["owner_occ_rate"] = panel["owner_occupied"] / panel["occupied_units"].replace(0, float('nan'))
    total_labor = (panel.get("employed_pop", 0) + panel.get("unemployed_pop", 0))
    if "employed_pop" in panel.columns and "unemployed_pop" in panel.columns:
        labor_force = panel["employed_pop"] + panel["unemployed_pop"]
        panel["unemployment_rate"] = panel["unemployed_pop"] / labor_force.replace(0, float('nan'))
    if all(c in panel.columns for c in ["bachelors_degree", "masters_degree", "doctorate_degree"]):
        panel["college_rate"] = (
            panel["bachelors_degree"] + panel["masters_degree"] + panel["doctorate_degree"]
        ) / panel["total_population"].replace(0, float('nan'))

    # Try to join FHFA HPI (3-digit zip level)
    print(f"\n[{ts()}] Joining FHFA HPI...")
    try:
        hpi_blob = bucket.blob("fhfa/HPI_AT_3zip.csv")
        if hpi_blob.exists():
            hpi_buf = io.BytesIO()
            hpi_blob.download_to_file(hpi_buf)
            hpi_buf.seek(0)
            hpi = pd.read_csv(hpi_buf)
            print(f"  HPI loaded: {hpi.shape}, cols={list(hpi.columns)}")
            # FHFA HPI has: ThreeDigitZIPCode, Year, Quarter, IndexSA, IndexNSA
            yr_col = next((c for c in hpi.columns if c.lower() in ('yr', 'year')), None)
            zip_col = next((c for c in hpi.columns if '3' in c.lower() or 'zip' in c.lower()), None)
            idx_col = next((c for c in hpi.columns if 'index' in c.lower() and 'sa' in c.lower()), None)
            if yr_col and zip_col and idx_col:
                # Annual average
                hpi_annual = hpi.groupby([zip_col, yr_col])[idx_col].mean().reset_index()
                hpi_annual.columns = ["zip3", "year", "hpi_index"]
                hpi_annual["zip3"] = hpi_annual["zip3"].astype(str).str.zfill(3)
                # Tract → zip3: use first 3 digits of state+county as proxy (imperfect but useful)
                # Better: use HUD USPS crosswalk, but for now state-level is fine
                panel["state_fips"] = panel["geoid"].str[:2]
                # Join at state level instead (HPI_AT_state)
                st_blob = bucket.blob("fhfa/HPI_AT_state.csv")
                if st_blob.exists():
                    st_buf = io.BytesIO()
                    st_blob.download_to_file(st_buf)
                    st_buf.seek(0)
                    st_hpi = pd.read_csv(st_buf)
                    yr_c2 = next((c for c in st_hpi.columns if c.lower() in ('yr', 'year')), None)
                    st_c2 = next((c for c in st_hpi.columns if 'state' in c.lower() or 'fips' in c.lower()), None)
                    idx_c2 = next((c for c in st_hpi.columns if 'index' in c.lower()), None)
                    if yr_c2 and idx_c2:
                        st_annual = st_hpi.groupby([yr_c2])[idx_c2].mean().reset_index()
                        st_annual.columns = ["year", "hpi_national"]
                        panel = panel.merge(st_annual, on="year", how="left")
                        print(f"  Joined national HPI: {panel['hpi_national'].notna().sum():,} matches")
        else:
            print("  No FHFA HPI files on GCS yet")
    except Exception as e:
        print(f"  HPI join failed: {e}")

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
