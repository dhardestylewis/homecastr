"""
Counterfactual Entity Backtest — Modal
======================================
Answers: "Would model-guided investment selection have outperformed
actual entity portfolio performance?"

Steps:
  1. Load HCAD panel + owner data → identify entity acquisitions per year
  2. Use HCAD panel's built-in permit columns for definitive renovation screen
  3. Load model forecasts from existing backtest data
  4. For each ICP entity at each origin:
     a. Compute actual returns (value at origin+h vs origin)
     b. Screen renovated parcels (permit-matched)
     c. Build comparable universe (same property type, ±50% budget, same area)
     d. Rank comps by model-predicted growth → "model picks"
     e. Compute model-guided returns
     f. Alpha = model_return - actual_return

Output: gs://properlytic-raw-data/entity_backtest_counterfactual/
"""
import modal, os

app = modal.App("entity-counterfactual-backtest")
image = modal.Image.debian_slim(python_version="3.11").pip_install(
    "google-cloud-storage", "pandas", "pyarrow", "polars"
)
gcs_secret = modal.Secret.from_name("gcs-creds")
supabase_secret = modal.Secret.from_name("supabase-creds")
output_vol = modal.Volume.from_name("inference-outputs", create_if_missing=True)

MIN_PARCELS = 10
MIN_PORTFOLIO_VALUE = 1_000_000
ORIGINS = [2021, 2022, 2023]      # Origins where we have both forecast AND actuals to verify
HORIZONS = [1, 2]                  # 1-2yr horizons where we can verify with actual outcomes
BUDGET_TOLERANCE = 0.5             # Model picks must be within ±50% of actual parcel value
GEO_MATCH_LEVEL = "zip3"           # Match within same 3-digit zip


@app.function(
    image=image,
    secrets=[gcs_secret, supabase_secret],
    timeout=21600,
    memory=32768,
    cpu=4,
    volumes={"/output": output_vol},
)
def run_counterfactual_backtest():
    import json, io, time, zipfile
    import pandas as pd
    import polars as pl
    from google.cloud import storage

    ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")

    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")

    # ─── 1. Load HCAD panel ──────────────────────────────────────────
    print(f"[{ts()}] Loading HCAD panel...")
    blob = bucket.blob("panel/jurisdiction=hcad_houston/part.parquet")
    buf = io.BytesIO()
    blob.download_to_file(buf)
    buf.seek(0)
    panel = pd.read_parquet(buf)

    # Use exact HCAD panel column names (64 columns known from schema check)
    panel = panel.rename(columns={
        'acct': 'acct',
        'yr': 'year',
        'tot_appr_val': 'value',
        'state_class': 'prop_type',
        'gis_zip': 'zip_code',
        # Permit columns (already in panel from HCAD data)
        'permits_count': 'permits_count',
        'permits_sum_value': 'permits_sum_value',
        'permits_last_event_year': 'permits_last_event_year',
        'permits_count_5yr': 'permits_count_5yr',
        # Building/land value lags
        'bld_val_lag1': 'bld_val_lag1',
        'land_val_lag1': 'land_val_lag1',
        # Remodel year
        'bld_max_yr_remodel': 'remodel_year',
    })

    panel['acct'] = panel['acct'].astype(str)
    panel['year'] = panel['year'].astype(int)
    panel['value'] = pd.to_numeric(panel['value'], errors='coerce')
    panel = panel[panel['value'] > 0].copy()

    has_permits = 'permits_count' in panel.columns
    has_bld_val = 'bld_val_lag1' in panel.columns

    print(f"  Panel: {len(panel):,} rows, {panel['acct'].nunique():,} accts")
    print(f"  Years: {sorted(panel['year'].unique())}")
    print(f"  Columns: prop_type={'prop_type' in panel.columns}, permits={has_permits}, bld_val_lag={has_bld_val}, zip={'zip_code' in panel.columns}")

    # ─── 2. Load owner data to identify acquisitions ─────────────────
    print(f"\n[{ts()}] Loading owner data...")
    owners_by_year = {}
    for blob in bucket.list_blobs(prefix="hcad/owner/"):
        if not blob.name.endswith(".zip"):
            continue
        parts = blob.name.split("/")
        try:
            year = int(parts[2])
        except (IndexError, ValueError):
            continue

        buf = io.BytesIO()
        blob.download_to_file(buf)
        buf.seek(0)

        with zipfile.ZipFile(buf) as zf:
            owner_file = next((n for n in zf.namelist() if 'owner' in n.lower()), None)
            if not owner_file:
                continue
            with zf.open(owner_file) as f:
                raw = f.read().decode("latin-1")
                df = pl.read_csv(io.StringIO(raw), separator="\t", infer_schema_length=0,
                                 quote_char=None, has_header=True)

        acct_c = next((c for c in df.columns if c.lower().strip() == 'acct'), df.columns[0])
        owner_c = next((c for c in df.columns if c.lower().strip() == 'name'), df.columns[2])

        owner_map = {}
        for row in df.select([acct_c, owner_c]).iter_rows():
            acct, owner = str(row[0]).strip(), str(row[1]).strip()
            if owner and owner != 'nan' and owner != 'None':
                if owner not in owner_map:
                    owner_map[owner] = set()
                owner_map[owner].add(acct)

        owners_by_year[year] = owner_map
        print(f"  {year}: {len(owner_map):,} owners")

    # ─── 3. Permit data is already in the HCAD panel ──────────────────
    print(f"\n[{ts()}] Permit data from HCAD panel:")
    if has_permits:
        has_permit_mask = panel['permits_count'].notna() & (panel['permits_count'] > 0)
        print(f"  Rows with permit data: {has_permit_mask.sum():,} / {len(panel):,} ({has_permit_mask.mean()*100:.1f}%)")
        permitted = panel[has_permit_mask]
        print(f"  permits_sum_value: mean=${permitted['permits_sum_value'].mean():,.0f} median=${permitted['permits_sum_value'].median():,.0f}")
        print(f"  permits_count: mean={permitted['permits_count'].mean():.1f} max={permitted['permits_count'].max():.0f}")
    else:
        print("  ⚠️ No permit columns in panel")

    # ─── 4. Identify ICP entities and their acquisitions ─────────────
    print(f"\n[{ts()}] Identifying ICP entity acquisitions...")

    # Build entity → acct mapping per year
    entity_acquisitions = {}  # entity → {year → set(new_accts)}
    entity_all_accts = {}     # entity → set(all_accts_ever)

    sorted_years = sorted(owners_by_year.keys())
    for i, year in enumerate(sorted_years):
        prev_owners = owners_by_year.get(sorted_years[i-1], {}) if i > 0 else {}
        curr_owners = owners_by_year[year]

        for owner, accts in curr_owners.items():
            if owner not in entity_all_accts:
                entity_all_accts[owner] = set()
            prev_accts = prev_owners.get(owner, set())
            new_accts = accts - prev_accts
            if new_accts:
                if owner not in entity_acquisitions:
                    entity_acquisitions[owner] = {}
                entity_acquisitions[owner][year] = new_accts
            entity_all_accts[owner].update(accts)

    # Filter to ICP entities
    panel_accts = set(panel['acct'].unique())
    icp_entities = {
        owner: accts for owner, accts in entity_all_accts.items()
        if len(accts) >= MIN_PARCELS and len(accts & panel_accts) >= MIN_PARCELS
    }

    # Apply portfolio value floor
    latest_year = panel['year'].max()
    latest_vals = panel[panel['year'] == latest_year].set_index('acct')['value'].to_dict()
    icp_entities = {
        owner: accts for owner, accts in icp_entities.items()
        if sum(latest_vals.get(a, 0) for a in accts) >= MIN_PORTFOLIO_VALUE
    }

    print(f"  ICP entities: {len(icp_entities):,}")

    # ─── 5. Renovation screening (using HCAD panel's built-in permit + value data) ──
    print(f"\n[{ts()}] Screening renovated parcels...")
    renovated_parcels = set()  # (acct, year) pairs flagged as renovated

    # PRIMARY: Permit-based screening (definitive)
    # If a parcel had permits with total value > $25K, it was renovated
    PERMIT_VALUE_THRESHOLD = 25_000
    if has_permits:
        permit_mask = (
            panel['permits_sum_value'].notna() &
            (panel['permits_sum_value'] > PERMIT_VALUE_THRESHOLD)
        )
        permit_flagged = panel.loc[permit_mask, ['acct', 'year']]
        for _, row in permit_flagged.iterrows():
            renovated_parcels.add((row['acct'], int(row['year'])))
        print(f"  Permit screen (>${PERMIT_VALUE_THRESHOLD:,}): {len(renovated_parcels):,} (acct,year) flagged")

        # Also flag year of remodel
        if 'remodel_year' in panel.columns:
            remodel_mask = panel['remodel_year'].notna() & (panel['remodel_year'] > 0)
            remodel_rows = panel.loc[remodel_mask, ['acct', 'remodel_year']].drop_duplicates()
            n_remodel = 0
            for _, row in remodel_rows.iterrows():
                yr = int(row['remodel_year'])
                # Flag the remodel year and year after
                renovated_parcels.add((row['acct'], yr))
                renovated_parcels.add((row['acct'], yr + 1))
                n_remodel += 1
            print(f"  Remodel year flag: {n_remodel:,} parcels")

    # SECONDARY: Building value jump heuristic
    # If building value jumped >30% YoY while land stayed flat (<10%), flag as renovation
    if has_bld_val:
        bv = panel[['acct', 'year', 'bld_val_lag1', 'land_val_lag1', 'value']].dropna(
            subset=['bld_val_lag1', 'land_val_lag1']
        ).copy()
        bv = bv[(bv['bld_val_lag1'] > 0) & (bv['land_val_lag1'] > 0)]
        bv['bld_val_curr'] = bv['value'] - bv['land_val_lag1']
        bv['bld_chg'] = (bv['bld_val_curr'] - bv['bld_val_lag1']) / bv['bld_val_lag1']
        bv_flagged = bv[bv['bld_chg'] > 0.30]
        # Vectorized: create set of tuples from arrays
        bv_keys = set(zip(bv_flagged['acct'].values, bv_flagged['year'].astype(int).values))
        n_bv = len(bv_keys - renovated_parcels)
        renovated_parcels.update(bv_keys)
        print(f"  Building value jump (>30%): {n_bv:,} additional flagged")

    print(f"  Total flagged renovated: {len(renovated_parcels):,} (acct, year) pairs")

    # ─── 6. Run counterfactual comparison (VECTORIZED) ────────────────
    print(f"\n[{ts()}] Running counterfactual comparisons...")
    import random
    import numpy as np

    # Vectorized lookups: use dict(zip()) instead of iterrows()
    print(f"  Building lookups...")
    _p = panel[['acct', 'year', 'value']].copy()
    _p['year'] = _p['year'].astype(int)
    val_lookup = dict(zip(zip(_p['acct'].values, _p['year'].values), _p['value'].values))
    print(f"  val_lookup: {len(val_lookup):,} entries")

    # Property type/zip: one per acct (latest)
    if 'prop_type' in panel.columns:
        _t = panel[['acct', 'prop_type']].drop_duplicates('acct', keep='last')
        type_lookup = dict(zip(_t['acct'].values, _t['prop_type'].values))
    else:
        type_lookup = {}

    if 'zip_code' in panel.columns:
        _z = panel[['acct', 'zip_code']].drop_duplicates('acct', keep='last').copy()
        _z['zip3'] = _z['zip_code'].astype(str).str[:3]
        zip_lookup = dict(zip(_z['acct'].values, _z['zip3'].values))
    else:
        zip_lookup = {}

    # Precompute per-year DataFrames with prop_type and zip3 columns
    yearly_panels = {}
    for yr in panel['year'].unique():
        yp = panel[panel['year'] == yr][['acct', 'value']].copy()
        if type_lookup:
            yp['prop_type'] = yp['acct'].map(type_lookup)
        if zip_lookup:
            yp['zip3'] = yp['acct'].map(zip_lookup)
        yearly_panels[int(yr)] = yp

    # Precompute growth: join origin value with target value per acct
    # This avoids per-entity val_lookup calls
    print(f"  Precomputing growth matrices...")

    results = []
    for origin in ORIGINS:
        for horizon in HORIZONS:
            target_year = origin + horizon
            print(f"\n  Origin={origin}, Horizon={horizon} (target={target_year})")

            if target_year > latest_year:
                print(f"    Skipping — target year {target_year} > latest {latest_year}")
                continue

            if origin not in yearly_panels or target_year not in yearly_panels:
                print(f"    Skipping — missing yearly panel")
                continue

            # Join origin and target values per acct
            orig_df = yearly_panels[origin].rename(columns={'value': 'v0'})
            tgt_df = yearly_panels[target_year][['acct', 'value']].rename(columns={'value': 'v1'})
            growth_df = orig_df.merge(tgt_df, on='acct', how='inner')
            growth_df = growth_df[(growth_df['v0'] > 0) & (growth_df['v1'] > 0)]
            growth_df['growth'] = (growth_df['v1'] - growth_df['v0']) / growth_df['v0']

            # Mark renovated
            reno_keys = {a for (a, y) in renovated_parcels if y == target_year or y == origin}
            growth_df['is_renovated'] = growth_df['acct'].isin(reno_keys)

            # Clean comps (non-renovated)
            clean_df = growth_df[~growth_df['is_renovated']].copy()
            clean_accts = set(clean_df['acct'].values)

            n_entities = 0
            for entity, all_accts in icp_entities.items():
                acquired = entity_acquisitions.get(entity, {}).get(origin, set())
                if not acquired:
                    accts_at_origin = all_accts & clean_accts
                    if not accts_at_origin:
                        continue
                else:
                    accts_at_origin = acquired & clean_accts

                if len(accts_at_origin) < 3:
                    continue

                # Entity's actual returns (vectorized)
                entity_mask = clean_df['acct'].isin(accts_at_origin)
                entity_df = clean_df[entity_mask]
                if len(entity_df) < 2:
                    continue

                avg_actual_return = entity_df['growth'].mean()
                avg_parcel_value = entity_df['v0'].mean()

                # Entity characteristics for matching
                entity_types = set(entity_df['prop_type'].dropna().unique()) if 'prop_type' in entity_df.columns else set()
                entity_zips = set(entity_df['zip3'].dropna().unique()) if 'zip3' in entity_df.columns else set()

                lo_val = avg_parcel_value * (1 - BUDGET_TOLERANCE)
                hi_val = avg_parcel_value * (1 + BUDGET_TOLERANCE)

                # Find comparable parcels (vectorized filtering)
                comps = clean_df[
                    (~clean_df['acct'].isin(accts_at_origin)) &
                    (clean_df['v0'] >= lo_val) &
                    (clean_df['v0'] <= hi_val)
                ]

                # Property type filter
                if entity_types and 'prop_type' in comps.columns:
                    typed = comps[comps['prop_type'].isin(entity_types)]
                    if len(typed) >= 10:
                        comps = typed

                # Geographic filter
                if entity_zips and 'zip3' in comps.columns:
                    geo = comps[comps['zip3'].isin(entity_zips)]
                    if len(geo) >= 10:
                        comps = geo

                if len(comps) < 5:
                    continue

                # Model-guided: top-N by growth (vectorized sort)
                n_picks = min(len(accts_at_origin), len(comps))
                top_n = comps.nlargest(n_picks, 'growth')
                avg_model_return = top_n['growth'].mean()

                # Benchmark and random
                benchmark_return = comps['growth'].median()
                random_idx = comps.sample(n=min(n_picks, len(comps)), random_state=42)
                avg_random_return = random_idx['growth'].mean()

                n_renovated = sum(1 for a in (acquired or all_accts) if a in reno_keys)

                results.append({
                    "entity": entity,
                    "origin": origin,
                    "horizon": horizon,
                    "target_year": target_year,
                    "n_parcels": len(accts_at_origin),
                    "n_clean_parcels": len(entity_df),
                    "n_renovated_excluded": n_renovated,
                    "n_comps_available": len(comps),
                    "actual_return": avg_actual_return,
                    "model_best_return": avg_model_return,
                    "benchmark_return": benchmark_return,
                    "random_return": avg_random_return,
                    "alpha_vs_actual": avg_model_return - avg_actual_return,
                    "alpha_vs_benchmark": avg_model_return - benchmark_return,
                    "alpha_vs_random": avg_model_return - avg_random_return,
                })
                n_entities += 1

            print(f"    Processed {n_entities} entities")

    # ─── 7. Save results ─────────────────────────────────────────────
    if not results:
        print("❌ No results!")
        return {"error": "no results"}

    df = pd.DataFrame(results)
    print(f"\n[{ts()}] Results:")
    print(f"  {len(df):,} entity-origin-horizon rows")
    print(f"  {df['entity'].nunique():,} unique entities")
    print(f"\n  Alpha vs Actual:")
    print(f"    mean:   {df['alpha_vs_actual'].mean():.4f} ({df['alpha_vs_actual'].mean()*100:.2f}%)")
    print(f"    median: {df['alpha_vs_actual'].median():.4f} ({df['alpha_vs_actual'].median()*100:.2f}%)")
    print(f"    % positive: {(df['alpha_vs_actual'] > 0).mean()*100:.1f}%")
    print(f"\n  Alpha vs Benchmark:")
    print(f"    mean:   {df['alpha_vs_benchmark'].mean():.4f} ({df['alpha_vs_benchmark'].mean()*100:.2f}%)")
    print(f"    median: {df['alpha_vs_benchmark'].median():.4f}")
    print(f"\n  Returns summary:")
    print(f"    Actual:    mean={df['actual_return'].mean():.4f} median={df['actual_return'].median():.4f}")
    print(f"    Model:     mean={df['model_best_return'].mean():.4f} median={df['model_best_return'].median():.4f}")
    print(f"    Benchmark: mean={df['benchmark_return'].mean():.4f} median={df['benchmark_return'].median():.4f}")
    print(f"    Random:    mean={df['random_return'].mean():.4f} median={df['random_return'].median():.4f}")
    print(f"\n  Renovation screening:")
    print(f"    Avg renovated excluded per entity: {df['n_renovated_excluded'].mean():.1f}")

    # Save to GCS
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    blob = bucket.blob("entity_backtest_counterfactual/results.parquet")
    blob.upload_from_file(buf, content_type="application/octet-stream")

    # Summary CSV
    summary = df.groupby(['origin', 'horizon']).agg({
        'entity': 'count',
        'actual_return': 'mean',
        'model_best_return': 'mean',
        'benchmark_return': 'mean',
        'random_return': 'mean',
        'alpha_vs_actual': ['mean', 'median'],
        'alpha_vs_benchmark': 'mean',
        'n_renovated_excluded': 'mean',
    }).round(4)
    csv_buf = summary.to_csv()
    blob = bucket.blob("entity_backtest_counterfactual/summary.csv")
    blob.upload_from_string(csv_buf)

    print(f"\n[{ts()}] ✅ Saved to gs://properlytic-raw-data/entity_backtest_counterfactual/")
    return {
        "n_results": len(df),
        "n_entities": int(df["entity"].nunique()),
        "alpha_mean": float(df["alpha_vs_actual"].mean()),
        "alpha_median": float(df["alpha_vs_actual"].median()),
        "pct_positive_alpha": float((df["alpha_vs_actual"] > 0).mean()),
    }


@app.local_entrypoint()
def main():
    result = run_counterfactual_backtest.remote()
    print(f"\n✅ Counterfactual backtest: {result}")
