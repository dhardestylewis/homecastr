"""
Counterfactual Entity Backtest v2 — Modal
==========================================
Uses MODEL PREDICTIONS (not oracle/actual growth) to select investments.
Compares model-guided selection against actual entity performance.

Changes from v1:
  - Loads forecast p50 from Supabase per-checkpoint (no future leakage)
  - Log returns (not raw growth) to handle fat tails
  - Budget tolerance ±25%, min base value $50K
  - Individual return cap at 200% (log ~1.1)
  - Tighter renovation screening (20% bld jump, development detection)
  - Vectorized by (zip3, prop_type) for O(seconds) not O(30min)
  - Smaller sample mode for iteration
"""

import os
import modal

app = modal.App("entity-counterfactual-backtest-v2")
image = modal.Image.debian_slim(python_version="3.11").pip_install(
    "pandas", "polars", "numpy", "google-cloud-storage", "psycopg2-binary"
)
gcs_secret = modal.Secret.from_name("gcs-key")
supabase_secret = modal.Secret.from_name("supabase-db-url")
output_vol = modal.Volume.from_name("inference-outputs", create_if_missing=True)

MIN_PARCELS = 10
MIN_PORTFOLIO_VALUE = 1_000_000
MIN_BASE_VALUE = 50_000          # Floor to exclude land/vacant
BUDGET_TOLERANCE = 0.25          # ±25% (was ±50%)
MAX_LOG_RETURN = 1.1             # Cap at ~200% growth
ORIGINS = [2021, 2022, 2023]
HORIZONS = [1, 2]
SUPABASE_SCHEMA = "forecast_20260220_7f31c6e4"


@app.function(
    image=image,
    secrets=[gcs_secret, supabase_secret],
    timeout=21600,
    memory=32768,
    cpu=4,
    volumes={"/output": output_vol},
)
def run_counterfactual_backtest(sample_entities: int = 0, sample_parcels: int = 0):
    import json, io, time, zipfile
    import pandas as pd
    import numpy as np
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

    panel = panel.rename(columns={
        'acct': 'acct', 'yr': 'year', 'tot_appr_val': 'value',
        'state_class': 'prop_type', 'gis_zip': 'zip_code',
        'permits_count': 'permits_count', 'permits_sum_value': 'permits_sum_value',
        'bld_val_lag1': 'bld_val_lag1', 'land_val_lag1': 'land_val_lag1',
        'bld_max_yr_remodel': 'remodel_year',
        'new_construction_val_lag1': 'new_construction_val_lag1',
    })

    panel['acct'] = panel['acct'].astype(str)
    panel['year'] = panel['year'].astype(int)
    panel['value'] = pd.to_numeric(panel['value'], errors='coerce')
    panel = panel[panel['value'] > 0].copy()

    print(f"  Panel: {len(panel):,} rows, {panel['acct'].nunique():,} accts")
    print(f"  Years: {sorted(panel['year'].unique())}")

    # ─── 2. Load model predictions from Supabase ─────────────────────
    print(f"\n[{ts()}] Loading model predictions from Supabase...")
    import psycopg2

    db_url = os.environ["SUPABASE_DB_URL"]
    conn = psycopg2.connect(db_url)

    # Load forecast p50 per (acct, origin_year, forecast_year)
    query = f"""
        SELECT acct, origin_year, forecast_year, p50
        FROM "{SUPABASE_SCHEMA}"."metrics_parcel_forecast"
        WHERE jurisdiction = 'hcad_houston'
          AND series_kind = 'forecast'
          AND origin_year IN ({','.join(str(o) for o in ORIGINS)})
    """
    forecasts = pd.read_sql(query, conn)
    conn.close()

    print(f"  Loaded {len(forecasts):,} forecast rows")
    print(f"  Origins: {sorted(forecasts['origin_year'].unique())}")
    print(f"  Forecast years: {sorted(forecasts['forecast_year'].unique())}")

    # Build lookup: (acct, origin, target_year) → predicted_value
    forecasts['acct'] = forecasts['acct'].astype(str)
    forecast_lookup = {}
    for _, row in forecasts.iterrows():
        key = (row['acct'], int(row['origin_year']), int(row['forecast_year']))
        forecast_lookup[key] = float(row['p50'])

    print(f"  Forecast lookup: {len(forecast_lookup):,} entries")

    # ─── 3. Load owner data ──────────────────────────────────────────
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

        pairs = df.select([acct_c, owner_c]).to_pandas()
        pairs.columns = ['acct', 'owner']
        pairs['acct'] = pairs['acct'].astype(str).str.strip()
        pairs['owner'] = pairs['owner'].astype(str).str.strip()
        pairs = pairs[(pairs['owner'] != '') & (pairs['owner'] != 'nan')]
        owner_map = pairs.groupby('owner')['acct'].apply(set).to_dict()

        owners_by_year[year] = owner_map
        print(f"  {year}: {len(owner_map):,} owners")

    # ─── 4. ICP entities and acquisitions ────────────────────────────
    print(f"\n[{ts()}] Identifying ICP entities...")
    entity_acquisitions = {}
    entity_all_accts = {}

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

    panel_accts = set(panel['acct'].unique())
    icp_entities = {
        owner: accts for owner, accts in entity_all_accts.items()
        if len(accts) >= MIN_PARCELS and len(accts & panel_accts) >= MIN_PARCELS
    }

    latest_year = panel['year'].max()
    latest_vals = panel[panel['year'] == latest_year].set_index('acct')['value'].to_dict()
    icp_entities = {
        owner: accts for owner, accts in icp_entities.items()
        if sum(latest_vals.get(a, 0) for a in accts) >= MIN_PORTFOLIO_VALUE
    }

    print(f"  ICP entities: {len(icp_entities):,}")

    # Sample if requested
    if sample_entities > 0 and len(icp_entities) > sample_entities:
        import random
        sampled_keys = random.sample(list(icp_entities.keys()), sample_entities)
        icp_entities = {k: icp_entities[k] for k in sampled_keys}
        print(f"  Sampled to {sample_entities} entities")

    # ─── 5. Renovation screening (tighter thresholds) ────────────────
    print(f"\n[{ts()}] Screening renovated parcels...")
    renovated_parcels = set()

    # Remodel year flag
    if 'remodel_year' in panel.columns:
        rm = panel.loc[panel['remodel_year'].notna() & (panel['remodel_year'] > 0),
                      ['acct', 'remodel_year']].drop_duplicates()
        yrs = rm['remodel_year'].astype(int).values
        accts = rm['acct'].values
        remo_keys = set(zip(accts, yrs)) | set(zip(accts, yrs + 1))
        renovated_parcels.update(remo_keys)
        print(f"  Remodel year flag: {len(remo_keys):,} (acct,year) pairs")

    # Building value jump >20% (tighter than 30%)
    if 'bld_val_lag1' in panel.columns:
        bv = panel[['acct', 'year', 'bld_val_lag1', 'value']].dropna(
            subset=['bld_val_lag1']
        ).copy()
        bv = bv[bv['bld_val_lag1'] > 10000]  # Need meaningful prior building value
        bv['bld_chg'] = (bv['value'] - bv['bld_val_lag1']) / bv['bld_val_lag1']
        bv_flagged = bv[bv['bld_chg'] > 0.20]  # Tighter: 20% (was 30%)
        bv_keys = set(zip(bv_flagged['acct'].values, bv_flagged['year'].astype(int).values))
        n_new = len(bv_keys - renovated_parcels)
        renovated_parcels.update(bv_keys)
        print(f"  Building value jump (>20%): {n_new:,} additional")

    # New construction detection
    if 'new_construction_val_lag1' in panel.columns:
        nc = panel[panel['new_construction_val_lag1'].notna() & (panel['new_construction_val_lag1'] > 0)]
        nc_keys = set(zip(nc['acct'].values, nc['year'].astype(int).values))
        n_new = len(nc_keys - renovated_parcels)
        renovated_parcels.update(nc_keys)
        print(f"  New construction flag: {n_new:,} additional")

    # Development detection: value <$30K at origin AND >$100K at target
    print(f"  Scanning for development parcels (value <$30K → >$100K)...")
    for origin in ORIGINS:
        for horizon in HORIZONS:
            target_year = origin + horizon
            if target_year > latest_year:
                continue
            orig_vals_check = panel[panel['year'] == origin].set_index('acct')['value']
            tgt_vals_check = panel[panel['year'] == target_year].set_index('acct')['value']
            common = orig_vals_check.index.intersection(tgt_vals_check.index)
            for a in common:
                if orig_vals_check[a] < 30_000 and tgt_vals_check[a] > 100_000:
                    renovated_parcels.add((a, target_year))

    print(f"  Total flagged: {len(renovated_parcels):,} (acct, year) pairs")

    # ─── 6. Vectorized counterfactual comparison ─────────────────────
    print(f"\n[{ts()}] Running counterfactual comparisons (vectorized)...")

    # Pre-build acct→entity index
    acct_to_entities = {}
    for entity, accts in icp_entities.items():
        for a in accts:
            if a not in acct_to_entities:
                acct_to_entities[a] = []
            acct_to_entities[a].append(entity)

    # Pre-index renovated by year
    reno_by_year = {}
    for (a, y) in renovated_parcels:
        reno_by_year.setdefault(y, set()).add(a)

    # Pre-compute prop_type and zip3 per acct
    if 'prop_type' in panel.columns:
        _pt = panel[['acct', 'prop_type']].drop_duplicates('acct', keep='last')
        pt_dict = dict(zip(_pt['acct'].values, _pt['prop_type'].values))
    else:
        pt_dict = {}

    if 'zip_code' in panel.columns:
        _z = panel[['acct', 'zip_code']].drop_duplicates('acct', keep='last').copy()
        _z['zip3'] = _z['zip_code'].astype(str).str[:3]
        z3_dict = dict(zip(_z['acct'].values, _z['zip3'].values))
    else:
        z3_dict = {}

    results = []
    for origin in ORIGINS:
        orig_slice = panel[panel['year'] == origin][['acct', 'value']].copy()
        orig_slice = orig_slice[orig_slice['value'] >= MIN_BASE_VALUE]  # $50K floor
        orig_vals = dict(zip(orig_slice['acct'].values, orig_slice['value'].values))

        for horizon in HORIZONS:
            target_year = origin + horizon
            if target_year > latest_year:
                print(f"\n  Origin={origin}, Horizon={horizon} → target={target_year} > latest {latest_year}, skip")
                continue

            print(f"\n  Origin={origin}, Horizon={horizon} (target={target_year})")
            t0_oh = time.time()

            tgt_slice = panel[panel['year'] == target_year][['acct', 'value']].copy()
            tgt_slice = tgt_slice[tgt_slice['value'] > 0]
            tgt_vals = dict(zip(tgt_slice['acct'].values, tgt_slice['value'].values))

            # Common accts with origin value >= $50K
            common_accts = set(orig_vals.keys()) & set(tgt_vals.keys())

            # Remove renovated
            reno_combined = reno_by_year.get(origin, set()) | reno_by_year.get(target_year, set())
            clean_accts = common_accts - reno_combined

            if len(clean_accts) < 100:
                print(f"    Only {len(clean_accts)} clean accts, skipping")
                continue

            # Build numpy arrays for clean accts
            clean_list = sorted(clean_accts)

            if sample_parcels > 0 and len(clean_list) > sample_parcels:
                import random
                clean_list = sorted(random.sample(clean_list, sample_parcels))

            acct_arr = np.array(clean_list)
            v0_arr = np.array([orig_vals[a] for a in clean_list])
            v1_arr = np.array([tgt_vals[a] for a in clean_list])

            # LOG RETURNS (capped)
            log_growth = np.log(v1_arr / v0_arr)
            log_growth = np.clip(log_growth, -MAX_LOG_RETURN, MAX_LOG_RETURN)

            # MODEL PREDICTED growth (using forecast p50)
            pred_growth = np.full(len(clean_list), np.nan)
            for i, a in enumerate(clean_list):
                pred_val = forecast_lookup.get((a, origin, target_year))
                if pred_val is not None and pred_val > 0:
                    pred_growth[i] = np.log(pred_val / v0_arr[i])

            has_pred = ~np.isnan(pred_growth)
            pred_growth = np.clip(pred_growth, -MAX_LOG_RETURN, MAX_LOG_RETURN)

            acct_idx = {a: i for i, a in enumerate(clean_list)}
            pt_arr = np.array([pt_dict.get(a, '') for a in clean_list])
            z3_arr = np.array([z3_dict.get(a, '') for a in clean_list])

            n_with_preds = int(has_pred.sum())
            print(f"    {len(clean_list):,} clean parcels, {n_with_preds:,} with predictions, log_growth mean={log_growth.mean():.4f}")

            # ── Pre-bucket by (zip3, prop_type) for O(1) comparable lookup ──
            buckets = {}  # (zip3, prop_type) → list of indices
            for i in range(len(clean_list)):
                key = (z3_arr[i], pt_arr[i])
                buckets.setdefault(key, []).append(i)

            n_entities = 0
            for entity, all_accts in icp_entities.items():
                acquired = entity_acquisitions.get(entity, {}).get(origin, set())
                entity_accts = (acquired if acquired else all_accts) & clean_accts
                if len(entity_accts) < 3:
                    continue

                eidx = np.array([acct_idx[a] for a in entity_accts if a in acct_idx])
                if len(eidx) < 2:
                    continue

                # Entity actual log returns
                e_log_growth = log_growth[eidx]
                e_v0 = v0_arr[eidx]
                avg_actual = float(e_log_growth.mean())
                avg_parcel_value = float(e_v0.mean())

                # Entity characteristics
                entity_types = set(pt_arr[eidx]) - {''}
                entity_zips = set(z3_arr[eidx]) - {''}

                # Budget range
                lo_val = avg_parcel_value * (1 - BUDGET_TOLERANCE)
                hi_val = avg_parcel_value * (1 + BUDGET_TOLERANCE)

                # Get comparable indices from pre-bucketed data
                entity_set = set(entity_accts)
                comp_indices = []
                for z3 in entity_zips:
                    for pt in (entity_types if entity_types else {''}) :
                        for idx in buckets.get((z3, pt), []):
                            if clean_list[idx] not in entity_set:
                                if lo_val <= v0_arr[idx] <= hi_val:
                                    comp_indices.append(idx)

                if len(comp_indices) < 5:
                    # Fallback: relax to any zip3 match without prop_type
                    comp_indices = []
                    for z3 in entity_zips:
                        for key, indices in buckets.items():
                            if key[0] == z3:
                                for idx in indices:
                                    if clean_list[idx] not in entity_set:
                                        if lo_val <= v0_arr[idx] <= hi_val:
                                            comp_indices.append(idx)

                if len(comp_indices) < 5:
                    continue

                comp_idx = np.array(comp_indices)
                comp_log_growth = log_growth[comp_idx]
                comp_has_pred = has_pred[comp_idx]
                comp_pred = pred_growth[comp_idx]

                n_picks = min(len(entity_accts), len(comp_idx))

                # MODEL-GUIDED selection: rank by PREDICTED growth, pick top-N
                if comp_has_pred.sum() >= n_picks:
                    # Use model predictions
                    pred_only = comp_pred[comp_has_pred]
                    pred_indices = comp_idx[comp_has_pred]
                    top_pred_idx = np.argpartition(pred_only, -n_picks)[-n_picks:]
                    model_selected = pred_indices[top_pred_idx]
                    avg_model_return = float(log_growth[model_selected].mean())
                    avg_model_predicted = float(pred_only[top_pred_idx].mean())
                    used_model = True
                else:
                    avg_model_return = np.nan
                    avg_model_predicted = np.nan
                    used_model = False

                # Benchmark: median of comparables
                benchmark_return = float(np.median(comp_log_growth))

                # Random picks
                rng = np.random.RandomState(42 + hash(entity) % 10000)
                rand_idx = rng.choice(len(comp_idx), size=min(n_picks, len(comp_idx)), replace=False)
                avg_random_return = float(comp_log_growth[rand_idx].mean())

                # Renovation count
                n_renovated = len(entity_accts & reno_combined)

                results.append({
                    "entity": entity,
                    "origin": origin,
                    "horizon": horizon,
                    "target_year": target_year,
                    "n_parcels": len(entity_accts),
                    "n_comps": len(comp_idx),
                    "n_comps_with_predictions": int(comp_has_pred.sum()),
                    "used_model_predictions": used_model,
                    "actual_log_return": avg_actual,
                    "model_log_return": avg_model_return,
                    "model_predicted_log_return": avg_model_predicted,
                    "benchmark_log_return": benchmark_return,
                    "random_log_return": avg_random_return,
                    "alpha_vs_actual": avg_model_return - avg_actual if used_model else np.nan,
                    "alpha_vs_benchmark": avg_model_return - benchmark_return if used_model else np.nan,
                    "alpha_vs_random": avg_model_return - avg_random_return if used_model else np.nan,
                    "n_renovated_excluded": n_renovated,
                    "avg_parcel_value": avg_parcel_value,
                })
                n_entities += 1

            elapsed = time.time() - t0_oh
            print(f"    Processed {n_entities} entities in {elapsed:.1f}s")

    # ─── 7. Save results ─────────────────────────────────────────────
    if not results:
        print("❌ No results!")
        return {"error": "no results"}

    df = pd.DataFrame(results)
    df_with_model = df[df['used_model_predictions'] == True]

    print(f"\n[{ts()}] Results:")
    print(f"  {len(df):,} entity-origin-horizon rows")
    print(f"  {df['entity'].nunique():,} unique entities")
    print(f"  {len(df_with_model):,} rows with model predictions ({100*len(df_with_model)/len(df):.1f}%)")

    if len(df_with_model) > 0:
        print(f"\n  Alpha vs Actual (model-predicted, log return):")
        print(f"    mean:   {df_with_model['alpha_vs_actual'].mean():.4f}")
        print(f"    median: {df_with_model['alpha_vs_actual'].median():.4f}")
        print(f"    % positive: {(df_with_model['alpha_vs_actual'] > 0).mean()*100:.1f}%")

        print(f"\n  Alpha vs Benchmark:")
        print(f"    mean:   {df_with_model['alpha_vs_benchmark'].mean():.4f}")
        print(f"    median: {df_with_model['alpha_vs_benchmark'].median():.4f}")

        print(f"\n  Log Returns summary:")
        print(f"    Actual:    mean={df_with_model['actual_log_return'].mean():.4f} median={df_with_model['actual_log_return'].median():.4f}")
        print(f"    Model:     mean={df_with_model['model_log_return'].mean():.4f} median={df_with_model['model_log_return'].median():.4f}")
        print(f"    Predicted: mean={df_with_model['model_predicted_log_return'].mean():.4f} median={df_with_model['model_predicted_log_return'].median():.4f}")
        print(f"    Benchmark: mean={df_with_model['benchmark_log_return'].mean():.4f} median={df_with_model['benchmark_log_return'].median():.4f}")
        print(f"    Random:    mean={df_with_model['random_log_return'].mean():.4f} median={df_with_model['random_log_return'].median():.4f}")

    # Save to GCS
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    blob = bucket.blob("entity_backtest_counterfactual/results_v2.parquet")
    blob.upload_from_file(buf, content_type="application/octet-stream")

    # Summary CSV
    if len(df_with_model) > 0:
        summary = df_with_model.groupby(['origin', 'horizon']).agg({
            'entity': 'count',
            'actual_log_return': 'mean',
            'model_log_return': 'mean',
            'benchmark_log_return': 'mean',
            'random_log_return': 'mean',
            'alpha_vs_actual': ['mean', 'median'],
            'alpha_vs_benchmark': 'mean',
            'n_comps_with_predictions': 'mean',
        }).round(4)
        csv_buf = summary.to_csv()
        blob = bucket.blob("entity_backtest_counterfactual/summary_v2.csv")
        blob.upload_from_string(csv_buf)

    print(f"\n[{ts()}] ✅ Saved to gs://properlytic-raw-data/entity_backtest_counterfactual/")
    return {
        "n_results": len(df),
        "n_with_model": len(df_with_model),
        "n_entities": int(df['entity'].nunique()),
        "alpha_mean": float(df_with_model['alpha_vs_actual'].mean()) if len(df_with_model) > 0 else None,
        "alpha_median": float(df_with_model['alpha_vs_actual'].median()) if len(df_with_model) > 0 else None,
    }


@app.local_entrypoint()
def main(sample_entities: int = 500, sample_parcels: int = 0):
    result = run_counterfactual_backtest.remote(
        sample_entities=sample_entities,
        sample_parcels=sample_parcels,
    )
    print(f"\n✅ Counterfactual backtest v2: {result}")
""", "")
