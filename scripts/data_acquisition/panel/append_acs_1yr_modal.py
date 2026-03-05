"""
ACS 1-Year Estimates Ingestion
==============================
Downloads ACS 1-year estimates at tract level and appends to existing
ACS panel. 1-year estimates cover areas with 65,000+ population only.

This provides MORE RECENT data (2024 released Sep 2025) at the cost of
sparser coverage (~800 tracts vs ~84K in 5-year).

Strategy: append 1-year rows with an `acs_product` column = '1yr' to
distinguish from 5-year rows ('5yr'). The model sees them as additional
observations for the same geoid.

Usage:
    modal run scripts/data_acquisition/append_acs_1yr_modal.py
    modal run scripts/data_acquisition/append_acs_1yr_modal.py --year 2023
"""

import modal, os

app = modal.App("acs-append-1yr")
image = modal.Image.debian_slim(python_version="3.11").pip_install(
    "google-cloud-storage", "pandas", "pyarrow", "requests"
)
gcs_secret = modal.Secret.from_name("gcs-creds")

# Same variables as the 5-year panel
ACS_VARIABLES = {
    "B25077_001E": "median_home_value",
    "B19013_001E": "median_hh_income",
    "B25071_001E": "rent_burden_pct",
    "B25064_001E": "median_gross_rent",
    "B01003_001E": "total_population",
    "B23025_003E": "employed_pop",
    "B23025_005E": "unemployed_pop",
    "B15003_022E": "bachelors_degree",
    "B15003_023E": "masters_degree",
    "B15003_025E": "doctorate_degree",
    "B25001_001E": "total_housing_units",
    "B25002_002E": "occupied_units",
    "B25002_003E": "vacant_units",
    "B25003_002E": "owner_occupied",
    "B25003_003E": "renter_occupied",
    "B25035_001E": "median_year_built",
    "B07001_001E": "total_movers",
    "B07001_017E": "moved_from_abroad",
}


@app.function(
    image=image,
    secrets=[gcs_secret],
    timeout=3600,
    memory=8192,
)
def append_1yr(year: int = 2024):
    """Fetch ACS 1-year estimates and append to existing panel."""
    import json, time, io
    import pandas as pd
    import requests
    from google.cloud import storage

    ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")

    # ─── 1. Download existing panel from GCS ───
    print(f"[{ts()}] Downloading existing panel from GCS...")
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")

    blob = bucket.blob("panel/jurisdiction=acs_nationwide/part.parquet")
    if not blob.exists():
        raise FileNotFoundError("Existing panel not found on GCS!")

    blob.download_to_filename("/tmp/existing_panel.parquet")
    existing = pd.read_parquet("/tmp/existing_panel.parquet")
    print(f"[{ts()}] Existing panel: {len(existing):,} rows, years: {sorted(existing['year'].unique())}")

    # Add acs_product column if not present (existing data is all 5yr)
    if "acs_product" not in existing.columns:
        existing["acs_product"] = "5yr"

    # ─── 2. Fetch 1-year data from Census API ───
    # ACS 1-year is available at tract level ONLY for tracts in areas >= 65K pop
    print(f"\n[{ts()}] Fetching ACS 1-year {year}...")
    BASE = f"https://api.census.gov/data/{year}/acs/acs1"
    STATE_FIPS = [f"{i:02d}" for i in list(range(1, 57)) + [72] if i not in (3, 7, 14, 43, 52)]

    all_vars = list(ACS_VARIABLES.keys())

    # Probe availability
    var_str = ",".join(all_vars)
    test_url = f"{BASE}?get=NAME,{var_str}&for=tract:*&in=state:48"
    try:
        r = requests.get(test_url, timeout=60)
        if r.status_code == 200:
            working_vars = all_vars
            test_data = r.json()
            print(f"  All {len(all_vars)} vars available, TX has {len(test_data)-1} tracts in 1-year")
        else:
            # ACS 1-year at tract level may not be available — try county level
            print(f"  Tract-level 1-year not available (status={r.status_code})")
            print(f"  Trying county level...")
            test_url_county = f"{BASE}?get=NAME,{var_str}&for=county:*&in=state:48"
            r2 = requests.get(test_url_county, timeout=60)
            if r2.status_code != 200:
                raise RuntimeError(f"Neither tract nor county 1-year available for {year}")
            print(f"  County-level available. Will download at county level.")
            # For county-level, we'll need to map to tracts differently
            # For now, just get what's available at tract
            working_vars = []
            for v in all_vars:
                try:
                    r3 = requests.get(f"{BASE}?get=NAME,{v}&for=tract:*&in=state:48", timeout=20)
                    if r3.status_code == 200:
                        working_vars.append(v)
                except:
                    pass
                time.sleep(0.2)
            print(f"  {len(working_vars)}/{len(all_vars)} vars available at tract level")
            if not working_vars:
                raise RuntimeError(f"No tract-level 1-year variables available for {year}")
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"API probe failed: {e}")

    if "B25077_001E" not in working_vars:
        raise RuntimeError(f"Target variable (median_home_value) not available for 1-year {year}!")

    # Download per state
    var_str = ",".join(working_vars)
    state_dfs = []
    failed_states = []
    for fips in STATE_FIPS:
        url = f"{BASE}?get=NAME,{var_str}&for=tract:*&in=state:{fips}"
        try:
            resp = requests.get(url, timeout=120)
            if resp.status_code == 200:
                data = resp.json()
                if len(data) > 1:
                    sdf = pd.DataFrame(data[1:], columns=data[0])
                    state_dfs.append(sdf)
            # 204 or empty = no tracts meet 65K threshold in this state
        except Exception as e:
            failed_states.append((fips, str(e)))
        time.sleep(0.1)

    if not state_dfs:
        raise RuntimeError(f"No tract data downloaded for 1-year {year}!")

    df_1yr = pd.concat(state_dfs, ignore_index=True)
    df_1yr["geoid"] = df_1yr["state"] + df_1yr["county"] + df_1yr["tract"]
    df_1yr["year"] = year
    df_1yr["acs_product"] = "1yr"

    for cv, fn in ACS_VARIABLES.items():
        if cv in df_1yr.columns:
            df_1yr[fn] = pd.to_numeric(df_1yr[cv], errors="coerce")

    keep = ["geoid", "year", "acs_product"] + [ACS_VARIABLES[v] for v in working_vars if ACS_VARIABLES[v] in df_1yr.columns]
    df_1yr = df_1yr[[c for c in keep if c in df_1yr.columns]]

    if "median_home_value" in df_1yr.columns:
        df_1yr = df_1yr.dropna(subset=["median_home_value"])
        df_1yr = df_1yr[df_1yr["median_home_value"] > 0]

    n_tracts = df_1yr["geoid"].nunique()
    n_states = len(state_dfs)
    print(f"[{ts()}] ✅ 1-year {year}: {len(df_1yr):,} tracts from {n_states} states")
    if failed_states:
        print(f"  ⚠️ {len(failed_states)} states failed: {[f[0] for f in failed_states[:5]]}")

    # ─── 3. Check overlap with 5-year data ───
    existing_geoids = set(existing["geoid"].unique())
    new_geoids = set(df_1yr["geoid"].unique())
    overlap = existing_geoids & new_geoids
    new_only = new_geoids - existing_geoids
    print(f"  Overlap with 5-year panel: {len(overlap):,} tracts")
    print(f"  New tracts (not in 5-year): {len(new_only):,}")

    # ─── 4. Append to panel ───
    # Align columns
    for col in existing.columns:
        if col not in df_1yr.columns:
            df_1yr[col] = float("nan") if existing[col].dtype in ("float64", "float32", "int64") else None
    df_1yr = df_1yr[[c for c in existing.columns if c in df_1yr.columns]]

    panel = pd.concat([existing, df_1yr], ignore_index=True)
    panel = panel.sort_values(["geoid", "year", "acs_product"]).reset_index(drop=True)

    # ─── 5. Recompute derived features ───
    panel["prior_home_value"] = panel.groupby("geoid")["median_home_value"].shift(1)
    panel["home_value_growth"] = (
        (panel["median_home_value"] - panel["prior_home_value"]) / panel["prior_home_value"]
    )
    panel["vacancy_rate"] = panel["vacant_units"] / panel["total_housing_units"].replace(0, float("nan"))
    panel["owner_occ_rate"] = panel["owner_occupied"] / panel["occupied_units"].replace(0, float("nan"))
    if "employed_pop" in panel.columns and "unemployed_pop" in panel.columns:
        labor_force = panel["employed_pop"] + panel["unemployed_pop"]
        panel["unemployment_rate"] = panel["unemployed_pop"] / labor_force.replace(0, float("nan"))
    if all(c in panel.columns for c in ["bachelors_degree", "masters_degree", "doctorate_degree"]):
        panel["college_rate"] = (
            panel["bachelors_degree"] + panel["masters_degree"] + panel["doctorate_degree"]
        ) / panel["total_population"].replace(0, float("nan"))
    panel["acct"] = panel["geoid"]

    print(f"\n[{ts()}] Combined panel:")
    print(f"  Total rows: {len(panel):,}")
    print(f"  Unique tracts: {panel['geoid'].nunique():,}")
    print(f"  Years: {sorted(panel['year'].unique())}")
    print(f"  Products: {dict(panel['acs_product'].value_counts())}")

    # ─── 6. Upload to GCS ───
    print(f"\n[{ts()}] Uploading to GCS...")
    buf = io.BytesIO()
    panel.to_parquet(buf, index=False)
    buf.seek(0)

    blob = bucket.blob("panel/jurisdiction=acs_nationwide/part.parquet")
    blob.upload_from_file(buf, content_type="application/octet-stream")
    size_mb = buf.tell() / 1e6
    print(f"  ✅ Uploaded: {size_mb:.1f} MB")

    summary = {
        "n_rows": len(panel),
        "n_tracts": int(panel["geoid"].nunique()),
        "years": sorted([int(y) for y in panel["year"].unique()]),
        "products": dict(panel["acs_product"].value_counts()),
        "new_1yr_rows": len(df_1yr),
        "new_1yr_tracts": n_tracts,
    }
    blob = bucket.blob("panel/jurisdiction=acs_nationwide/summary.json")
    blob.upload_from_string(json.dumps(summary, indent=2))
    print(f"  ✅ Summary uploaded")

    return summary


@app.local_entrypoint()
def main(year: int = 2024):
    """Use: modal run scripts/data_acquisition/append_acs_1yr_modal.py --year 2024"""
    print(f"🚀 Appending ACS 1-year {year}...")
    result = append_1yr.remote(year=year)
    print(f"\n✅ Panel updated: {result}")
