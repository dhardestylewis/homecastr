"""
Incremental ACS 2024 Panel Append
==================================
Downloads ONLY the 2024 ACS 5-year estimates, appends to the existing
panel on GCS, recomputes derived features, and re-uploads.

Much faster than rebuilding the full 2009-2024 panel from scratch.

Usage:
    modal run scripts/data_acquisition/append_acs_2024_modal.py
"""

import modal, os

app = modal.App("acs-append-2024")
image = modal.Image.debian_slim(python_version="3.11").pip_install(
    "google-cloud-storage", "pandas", "pyarrow", "requests"
)
gcs_secret = modal.Secret.from_name("gcs-creds")

# Same variables as the full panel builder
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
def append_2024():
    """Fetch 2024 ACS data and append to existing panel."""
    import json, time, io
    import pandas as pd
    import requests
    from google.cloud import storage

    ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")
    year = 2024

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

    # Check if 2024 already exists
    if year in existing["year"].unique():
        print(f"[{ts()}] ⚠️ Year {year} already in panel ({len(existing[existing['year'] == year]):,} rows). Will replace.")
        existing = existing[existing["year"] != year]

    # ─── 2. Fetch 2024 ACS data ───
    print(f"\n[{ts()}] Fetching ACS {year}...")
    BASE = f"https://api.census.gov/data/{year}/acs/acs5"
    STATE_FIPS = [f"{i:02d}" for i in list(range(1, 57)) + [72] if i not in (3, 7, 14, 43, 52)]

    all_vars = list(ACS_VARIABLES.keys())

    # Probe which variables work
    var_str = ",".join(all_vars)
    test_url = f"{BASE}?get=NAME,{var_str}&for=tract:*&in=state:48"
    try:
        r = requests.get(test_url, timeout=60)
        if r.status_code == 200:
            working_vars = all_vars
            print(f"  All {len(all_vars)} vars available")
        else:
            print(f"  Some vars unavailable (status={r.status_code}), probing individually...")
            working_vars = []
            for v in all_vars:
                try:
                    r2 = requests.get(f"{BASE}?get=NAME,{v}&for=tract:*&in=state:48", timeout=20)
                    if r2.status_code == 200:
                        working_vars.append(v)
                except:
                    pass
                time.sleep(0.2)
            print(f"  {len(working_vars)}/{len(all_vars)} vars available")
    except Exception as e:
        raise RuntimeError(f"API probe failed: {e}")

    if "B25077_001E" not in working_vars:
        raise RuntimeError("Target variable (median_home_value) not available for 2024!")

    # Download per state
    var_str = ",".join(working_vars)
    state_dfs = []
    for fips in STATE_FIPS:
        url = f"{BASE}?get=NAME,{var_str}&for=tract:*&in=state:{fips}"
        try:
            resp = requests.get(url, timeout=120)
            if resp.status_code == 200:
                data = resp.json()
                if len(data) > 1:
                    sdf = pd.DataFrame(data[1:], columns=data[0])
                    state_dfs.append(sdf)
        except:
            pass
        time.sleep(0.1)

    if not state_dfs:
        raise RuntimeError("No state data downloaded for 2024!")

    df_2024 = pd.concat(state_dfs, ignore_index=True)
    df_2024["geoid"] = df_2024["state"] + df_2024["county"] + df_2024["tract"]
    df_2024["year"] = year
    for cv, fn in ACS_VARIABLES.items():
        if cv in df_2024.columns:
            df_2024[fn] = pd.to_numeric(df_2024[cv], errors="coerce")

    keep = ["geoid", "year"] + [ACS_VARIABLES[v] for v in working_vars if ACS_VARIABLES[v] in df_2024.columns]
    df_2024 = df_2024[[c for c in keep if c in df_2024.columns]]
    if "median_home_value" in df_2024.columns:
        df_2024 = df_2024.dropna(subset=["median_home_value"])
        df_2024 = df_2024[df_2024["median_home_value"] > 0]

    print(f"[{ts()}] ✅ 2024: {len(df_2024):,} tracts from {len(state_dfs)} states, {len(df_2024.columns)} cols")

    # ─── 3. Append to existing panel ───
    # Align columns
    for col in existing.columns:
        if col not in df_2024.columns:
            df_2024[col] = float("nan") if existing[col].dtype in ("float64", "float32", "int64") else None

    # Keep only columns that exist in the existing panel
    df_2024 = df_2024[[c for c in existing.columns if c in df_2024.columns]]

    panel = pd.concat([existing, df_2024], ignore_index=True)
    panel = panel.sort_values(["geoid", "year"]).reset_index(drop=True)

    # ─── 4. Recompute derived features (growth rates etc.) ───
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

    # Ensure acct column
    panel["acct"] = panel["geoid"]

    print(f"\n[{ts()}] Combined panel:")
    print(f"  Total rows: {len(panel):,}")
    print(f"  Unique tracts: {panel['geoid'].nunique():,}")
    print(f"  Years: {sorted(panel['year'].unique())}")
    print(f"  Columns: {list(panel.columns)}")

    # ─── 5. Upload to GCS ───
    print(f"\n[{ts()}] Uploading to GCS...")
    buf = io.BytesIO()
    panel.to_parquet(buf, index=False)
    buf.seek(0)

    blob = bucket.blob("panel/jurisdiction=acs_nationwide/part.parquet")
    blob.upload_from_file(buf, content_type="application/octet-stream")
    size_mb = buf.tell() / 1e6
    print(f"  ✅ Uploaded: gs://properlytic-raw-data/panel/jurisdiction=acs_nationwide/part.parquet ({size_mb:.1f} MB)")

    summary = {
        "n_rows": len(panel),
        "n_tracts": int(panel["geoid"].nunique()),
        "years": sorted([int(y) for y in panel["year"].unique()]),
        "columns": list(panel.columns),
        "new_2024_rows": len(df_2024),
    }
    blob = bucket.blob("panel/jurisdiction=acs_nationwide/summary.json")
    blob.upload_from_string(json.dumps(summary, indent=2))
    print(f"  ✅ Summary uploaded")

    return summary


@app.local_entrypoint()
def main():
    result = append_2024.remote()
    print(f"\n✅ Panel updated: {result}")
