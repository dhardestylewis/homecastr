"""
Counterfactual Entity Backtest v2 — Modal (Cached + Resumable)
===============================================================
Uses MODEL PREDICTIONS (not oracle/actual growth) to select investments.
Compares model-guided selection against actual entity performance.

All expensive intermediate steps are cached to GCS under
`entity_backtest_counterfactual/cache/`. On re-run, cached artifacts
are loaded from GCS and computation is skipped. Per-(origin, horizon)
results are saved incrementally so the run is fully resumable.

Use --force to bypass all caches and recompute everything.

Usage:
    modal run --detach scripts/inference/backtest/entity_backtest_counterfactual_v2_modal.py
    modal run --detach scripts/inference/backtest/entity_backtest_counterfactual_v2_modal.py --force
"""

import os
import modal

app = modal.App("entity-counterfactual-backtest-v2")
image = modal.Image.debian_slim(python_version="3.11").pip_install(
    "pandas", "polars", "numpy", "pyarrow", "google-cloud-storage", "psycopg2-binary"
)
gcs_secret = modal.Secret.from_name("gcs-creds")
supabase_secret = modal.Secret.from_name("supabase-creds")
output_vol = modal.Volume.from_name("inference-outputs", create_if_missing=True)

MIN_PARCELS = 10
MIN_PORTFOLIO_VALUE = 1_000_000
MIN_BASE_VALUE = 50_000          # Floor to exclude land/vacant
BUDGET_TOLERANCE = 0.25          # ±25%
MAX_LOG_RETURN = 1.1             # Cap at ~200% growth
ORIGINS = [2021, 2022, 2023]
HORIZONS = [1, 2]
SUPABASE_SCHEMA = "forecast_20260220_7f31c6e4"
GCS_CACHE_PREFIX = "entity_backtest_counterfactual/cache"


# ─── GCS cache helpers ──────────────────────────────────────────────────

def _gcs_exists(bucket, path):
    return bucket.blob(path).exists()


def _gcs_upload_json(bucket, path, obj):
    import json
    blob = bucket.blob(path)
    blob.upload_from_string(json.dumps(obj), content_type="application/json")


def _gcs_download_json(bucket, path):
    import json
    blob = bucket.blob(path)
    return json.loads(blob.download_as_bytes())


def _gcs_upload_parquet(bucket, path, df):
    import io
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    bucket.blob(path).upload_from_file(buf, content_type="application/octet-stream")


def _gcs_download_parquet(bucket, path):
    import io, pandas as pd
    buf = io.BytesIO()
    bucket.blob(path).download_to_file(buf)
    buf.seek(0)
    return pd.read_parquet(buf)


@app.function(
    image=image,
    secrets=[gcs_secret, supabase_secret],
    timeout=21600,
    memory=32768,
    cpu=4,
    volumes={"/output": output_vol},
)
def run_counterfactual_backtest(sample_entities: int = 0, sample_parcels: int = 0,
                                 force: bool = False):
    import json, io, time, zipfile
    import pandas as pd
    import numpy as np
    import polars as pl
    from google.cloud import storage

    ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")

    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")

    # ─── 1. Load HCAD panel (always needed for values) ───────────────
    print(f"[{ts()}] Loading HCAD panel...")
    t0_panel = time.time()
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
    latest_year = panel['year'].max()

    print(f"  Panel: {len(panel):,} rows, {panel['acct'].nunique():,} accts ({time.time()-t0_panel:.1f}s)")

    # ═══════════════════════════════════════════════════════════════════
    # PHASE 1: Entity portfolios + acquisitions (CACHED)
    # ═══════════════════════════════════════════════════════════════════
    cache_portfolios = f"{GCS_CACHE_PREFIX}/entity_portfolios.json"
    cache_acquisitions = f"{GCS_CACHE_PREFIX}/entity_acquisitions.json"

    if not force and _gcs_exists(bucket, cache_portfolios) and _gcs_exists(bucket, cache_acquisitions):
        print(f"\n[{ts()}] ✅ CACHE HIT: entity portfolios + acquisitions")
        t0 = time.time()
        icp_raw = _gcs_download_json(bucket, cache_portfolios)
        icp_entities = {k: set(v) for k, v in icp_raw.items()}
        acq_raw = _gcs_download_json(bucket, cache_acquisitions)
        entity_acquisitions = {
            owner: {int(yr): set(accts) for yr, accts in years.items()}
            for owner, years in acq_raw.items()
        }
        print(f"  Loaded {len(icp_entities):,} entities, {len(entity_acquisitions):,} with acquisitions ({time.time()-t0:.1f}s)")
    else:
        print(f"\n[{ts()}] ⏳ CACHE MISS: building entity portfolios from owner zips...")
        t0 = time.time()

        owners_by_year = {}
        all_permits = []

        RENO_PERMIT_TYPES = {
            '10', '11', '12',  # New construction
            '20', '21',        # Demolition
            '30', '31', '32',  # Additions, Remodels, Lease Space
            '40', '41',        # Foundation repair
            '50',              # Disaster review
        }

        for blob_item in bucket.list_blobs(prefix="hcad/owner/"):
            if not blob_item.name.endswith(".zip"):
                continue
            parts = blob_item.name.split("/")
            try:
                year = int(parts[2])
            except (IndexError, ValueError):
                continue

            buf = io.BytesIO()
            blob_item.download_to_file(buf)
            buf.seek(0)

            with zipfile.ZipFile(buf) as zf:
                # --- Owner data ---
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

                # --- Permits data ---
                permit_file = next((n for n in zf.namelist() if 'permit' in n.lower()), None)
                n_permits = 0
                if permit_file:
                    try:
                        with zf.open(permit_file) as f:
                            raw_p = f.read().decode("latin-1")
                            pdf = pl.read_csv(io.StringIO(raw_p), separator="\t",
                                              infer_schema_length=0, quote_char=None,
                                              has_header=True)
                        p_acct = next((c for c in pdf.columns if c.lower().strip() == 'acct'), None)
                        p_yr = next((c for c in pdf.columns if c.lower().strip() == 'yr'), None)
                        p_type = next((c for c in pdf.columns if c.lower().strip() == 'permit_type'), None)

                        if p_acct and p_yr and p_type:
                            perm_df = pdf.select([p_acct, p_yr, p_type]).to_pandas()
                            perm_df.columns = ['acct', 'yr', 'permit_type']
                            perm_df['acct'] = perm_df['acct'].astype(str).str.strip()
                            perm_df['yr'] = pd.to_numeric(perm_df['yr'], errors='coerce')
                            perm_df['permit_type'] = perm_df['permit_type'].astype(str).str.strip()
                            perm_df = perm_df.dropna(subset=['yr'])
                            perm_df['yr'] = perm_df['yr'].astype(int)
                            reno_permits = perm_df[perm_df['permit_type'].isin(RENO_PERMIT_TYPES)]
                            n_permits = len(reno_permits)
                            all_permits.append(reno_permits[['acct', 'yr']])
                    except Exception as e:
                        print(f"    ⚠️ Could not parse permits for {year}: {e}")

            print(f"  {year}: {len(owner_map):,} owners, {n_permits:,} reno permits")

        # Build entity acquisitions (new accts per year)
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

        # Filter to ICP entities
        panel_accts = set(panel['acct'].unique())
        icp_entities = {
            owner: accts for owner, accts in entity_all_accts.items()
            if len(accts) >= MIN_PARCELS and len(accts & panel_accts) >= MIN_PARCELS
        }
        latest_vals = panel[panel['year'] == latest_year].set_index('acct')['value'].to_dict()
        icp_entities = {
            owner: accts for owner, accts in icp_entities.items()
            if sum(latest_vals.get(a, 0) for a in accts) >= MIN_PORTFOLIO_VALUE
        }

        print(f"  ICP entities: {len(icp_entities):,} ({time.time()-t0:.1f}s)")

        # Cache to GCS
        _gcs_upload_json(bucket, cache_portfolios,
                         {k: sorted(v) for k, v in icp_entities.items()})
        _gcs_upload_json(bucket, cache_acquisitions,
                         {owner: {str(yr): sorted(accts) for yr, accts in years.items()}
                          for owner, years in entity_acquisitions.items()})
        print(f"  ↑ Cached to GCS: {cache_portfolios}, {cache_acquisitions}")

        # Cache permit data for renovation screening
        if all_permits:
            permit_df_all = pd.concat(all_permits, ignore_index=True)
            _gcs_upload_parquet(bucket, f"{GCS_CACHE_PREFIX}/permit_reno_flags.parquet", permit_df_all)
            print(f"  ↑ Cached permit flags: {len(permit_df_all):,} rows")

    # Sample if requested
    if sample_entities > 0 and len(icp_entities) > sample_entities:
        import random
        sampled_keys = random.sample(list(icp_entities.keys()), sample_entities)
        icp_entities = {k: icp_entities[k] for k in sampled_keys}
        print(f"  Sampled to {sample_entities} entities")

    # ═══════════════════════════════════════════════════════════════════
    # PHASE 2: Renovation flags (CACHED)
    # ═══════════════════════════════════════════════════════════════════
    cache_reno = f"{GCS_CACHE_PREFIX}/renovation_flags_full.parquet"

    if not force and _gcs_exists(bucket, cache_reno):
        print(f"\n[{ts()}] ✅ CACHE HIT: renovation flags")
        t0 = time.time()
        reno_df = _gcs_download_parquet(bucket, cache_reno)
        renovated_parcels = set(zip(reno_df['acct'].values, reno_df['year'].astype(int).values))
        print(f"  Loaded {len(renovated_parcels):,} (acct, year) renovation flags ({time.time()-t0:.1f}s)")
    else:
        print(f"\n[{ts()}] ⏳ CACHE MISS: computing renovation flags...")
        t0 = time.time()
        renovated_parcels = set()

        # PRIMARY: HCAD permit records from cached permit data
        cache_permits_path = f"{GCS_CACHE_PREFIX}/permit_reno_flags.parquet"
        if _gcs_exists(bucket, cache_permits_path):
            permit_df_cached = _gcs_download_parquet(bucket, cache_permits_path)
            permit_keys = set(zip(permit_df_cached['acct'].values, permit_df_cached['yr'].astype(int).values))
            renovated_parcels.update(permit_keys)
            print(f"  HCAD permits: {len(permit_keys):,} (acct,year) pairs")

        # SECONDARY: Panel-based permit screening
        if 'permits_sum_value' in panel.columns:
            PERMIT_VALUE_THRESHOLD = 25_000
            permit_mask = panel['permits_sum_value'].notna() & (panel['permits_sum_value'] > PERMIT_VALUE_THRESHOLD)
            pf = panel.loc[permit_mask, ['acct', 'year']]
            panel_keys = set(zip(pf['acct'].values, pf['year'].astype(int).values))
            n_new = len(panel_keys - renovated_parcels)
            renovated_parcels.update(panel_keys)
            print(f"  Panel permits (>${PERMIT_VALUE_THRESHOLD:,}): {n_new:,} additional")

        # Remodel year flag
        if 'remodel_year' in panel.columns:
            rm = panel.loc[panel['remodel_year'].notna() & (panel['remodel_year'] > 0),
                          ['acct', 'remodel_year']].drop_duplicates()
            yrs = rm['remodel_year'].astype(int).values
            accts = rm['acct'].values
            remo_keys = set(zip(accts, yrs)) | set(zip(accts, yrs + 1))
            n_new = len(remo_keys - renovated_parcels)
            renovated_parcels.update(remo_keys)
            print(f"  Remodel year flag: {n_new:,} additional")

        # Building value jump >20%
        if 'bld_val_lag1' in panel.columns:
            bv = panel[['acct', 'year', 'bld_val_lag1', 'value']].dropna(subset=['bld_val_lag1']).copy()
            bv = bv[bv['bld_val_lag1'] > 10000]
            bv['bld_chg'] = (bv['value'] - bv['bld_val_lag1']) / bv['bld_val_lag1']
            bv_flagged = bv[bv['bld_chg'] > 0.20]
            bv_keys = set(zip(bv_flagged['acct'].values, bv_flagged['year'].astype(int).values))
            n_new = len(bv_keys - renovated_parcels)
            renovated_parcels.update(bv_keys)
            print(f"  Building value jump (>20%): {n_new:,} additional")

        # New construction flag
        if 'new_construction_val_lag1' in panel.columns:
            nc = panel[panel['new_construction_val_lag1'].notna() & (panel['new_construction_val_lag1'] > 0)]
            nc_keys = set(zip(nc['acct'].values, nc['year'].astype(int).values))
            n_new = len(nc_keys - renovated_parcels)
            renovated_parcels.update(nc_keys)
            print(f"  New construction flag: {n_new:,} additional")

        # Development detection (VECTORIZED — was O(N²) Python loop)
        print(f"  Scanning for development parcels (vectorized)...")
        n_dev = 0
        for origin in ORIGINS:
            for horizon in HORIZONS:
                target_year = origin + horizon
                if target_year > latest_year:
                    continue
                orig_s = panel[panel['year'] == origin][['acct', 'value']].set_index('acct')['value']
                tgt_s = panel[panel['year'] == target_year][['acct', 'value']].set_index('acct')['value']
                common = orig_s.index.intersection(tgt_s.index)
                orig_vals_c = orig_s.reindex(common).values
                tgt_vals_c = tgt_s.reindex(common).values
                common_arr = common.values
                # Vectorized mask
                dev_mask = (orig_vals_c < 30_000) & (tgt_vals_c > 100_000)
                dev_accts = common_arr[dev_mask]
                dev_keys = set(zip(dev_accts, [target_year] * len(dev_accts)))
                n_new_dev = len(dev_keys - renovated_parcels)
                renovated_parcels.update(dev_keys)
                n_dev += n_new_dev
        print(f"  Development detection: {n_dev:,} additional")

        print(f"  Total flagged: {len(renovated_parcels):,} ({time.time()-t0:.1f}s)")

        # Cache to GCS
        reno_rows = [{'acct': a, 'year': y} for a, y in renovated_parcels]
        reno_df = pd.DataFrame(reno_rows)
        _gcs_upload_parquet(bucket, cache_reno, reno_df)
        print(f"  ↑ Cached to GCS: {cache_reno}")

    # Pre-index renovated by year for fast lookup
    reno_by_year = {}
    for (a, y) in renovated_parcels:
        reno_by_year.setdefault(y, set()).add(a)

    # ═══════════════════════════════════════════════════════════════════
    # PHASE 3: Load forecasts (CACHED per origin)
    # ═══════════════════════════════════════════════════════════════════
    print(f"\n[{ts()}] Loading model predictions...")

    forecast_lookup = {}  # (acct, origin, target_year) → p50
    for origin in ORIGINS:
        cache_fc = f"{GCS_CACHE_PREFIX}/forecast_lookup_o{origin}.parquet"
        if not force and _gcs_exists(bucket, cache_fc):
            print(f"  ✅ CACHE HIT: forecasts origin={origin}")
            t0 = time.time()
            fc_df = _gcs_download_parquet(bucket, cache_fc)
            fc_df['acct'] = fc_df['acct'].astype(str)
            # Vectorized dict construction
            keys = zip(fc_df['acct'], fc_df['origin_year'].astype(int), fc_df['forecast_year'].astype(int))
            vals = fc_df['p50'].astype(float)
            forecast_lookup.update(dict(zip(keys, vals)))
            print(f"    {len(fc_df):,} entries ({time.time()-t0:.1f}s)")
        else:
            print(f"  ⏳ CACHE MISS: querying Supabase for origin={origin}...")
            t0 = time.time()
            import psycopg2
            db_url = os.environ["SUPABASE_DB_URL"]
            conn = psycopg2.connect(db_url, options="-c statement_timeout=600000")
            query = f"""
                SELECT acct, origin_year, forecast_year, p50
                FROM "{SUPABASE_SCHEMA}"."metrics_parcel_forecast"
                WHERE jurisdiction = 'hcad_houston'
                  AND series_kind = 'forecast'
                  AND origin_year = {origin}
            """
            fc_df = pd.read_sql(query, conn)
            conn.close()

            fc_df['acct'] = fc_df['acct'].astype(str)
            print(f"    {len(fc_df):,} rows from Supabase ({time.time()-t0:.1f}s)")

            # Vectorized dict construction (no iterrows!)
            keys = zip(fc_df['acct'], fc_df['origin_year'].astype(int), fc_df['forecast_year'].astype(int))
            vals = fc_df['p50'].astype(float)
            forecast_lookup.update(dict(zip(keys, vals)))

            # Cache to GCS
            _gcs_upload_parquet(bucket, cache_fc, fc_df)
            print(f"    ↑ Cached to GCS: {cache_fc}")

    print(f"  Total forecast lookup: {len(forecast_lookup):,} entries")

    # ═══════════════════════════════════════════════════════════════════
    # PHASE 4+5: Counterfactual comparison (RESUMABLE per origin×horizon)
    # ═══════════════════════════════════════════════════════════════════
    print(f"\n[{ts()}] Running counterfactual comparisons...")

    # Pre-compute prop_type and zip3 per acct (vectorized)
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

    all_results = []
    for origin in ORIGINS:
        orig_slice = panel[panel['year'] == origin][['acct', 'value']].copy()
        orig_slice = orig_slice[orig_slice['value'] >= MIN_BASE_VALUE]
        orig_vals = dict(zip(orig_slice['acct'].values, orig_slice['value'].values))

        for horizon in HORIZONS:
            target_year = origin + horizon
            if target_year > latest_year:
                print(f"\n  Origin={origin}, Horizon={horizon} → target={target_year} > latest {latest_year}, skip")
                continue

            # ── Check cache for this (origin, horizon) ──
            cache_oh = f"{GCS_CACHE_PREFIX}/results_o{origin}_h{horizon}.parquet"
            if not force and _gcs_exists(bucket, cache_oh):
                print(f"\n  ✅ CACHE HIT: origin={origin} h={horizon}")
                t0 = time.time()
                cached_df = _gcs_download_parquet(bucket, cache_oh)
                all_results.append(cached_df)
                print(f"    {len(cached_df):,} rows loaded ({time.time()-t0:.1f}s)")
                continue

            print(f"\n  ⏳ Computing origin={origin}, horizon={horizon} (target={target_year})")
            t0_oh = time.time()

            tgt_slice = panel[panel['year'] == target_year][['acct', 'value']].copy()
            tgt_slice = tgt_slice[tgt_slice['value'] > 0]
            tgt_vals = dict(zip(tgt_slice['acct'].values, tgt_slice['value'].values))

            common_accts = set(orig_vals.keys()) & set(tgt_vals.keys())
            reno_combined = reno_by_year.get(origin, set()) | reno_by_year.get(target_year, set())
            clean_accts = common_accts - reno_combined

            if len(clean_accts) < 100:
                print(f"    Only {len(clean_accts)} clean accts, skipping")
                continue

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

            # MODEL PREDICTED growth — VECTORIZED (Phase 5 fix)
            pred_vals = np.array([forecast_lookup.get((a, origin, target_year), np.nan) for a in clean_list])
            valid_pred = (~np.isnan(pred_vals)) & (pred_vals > 0)
            pred_growth = np.full(len(clean_list), np.nan)
            pred_growth[valid_pred] = np.log(pred_vals[valid_pred] / v0_arr[valid_pred])
            pred_growth = np.clip(pred_growth, -MAX_LOG_RETURN, MAX_LOG_RETURN)

            acct_idx = {a: i for i, a in enumerate(clean_list)}
            pt_arr = np.array([pt_dict.get(a, '') for a in clean_list])
            z3_arr = np.array([z3_dict.get(a, '') for a in clean_list])

            n_with_preds = int(valid_pred.sum())
            print(f"    {len(clean_list):,} clean parcels, {n_with_preds:,} with predictions, log_growth mean={log_growth.mean():.4f}")

            # Pre-bucket by (zip3, prop_type)
            buckets = {}
            for i in range(len(clean_list)):
                key = (z3_arr[i], pt_arr[i])
                buckets.setdefault(key, []).append(i)

            results = []
            n_entities = 0
            for entity, entity_all_accts in icp_entities.items():
                acquired = entity_acquisitions.get(entity, {}).get(origin, set())
                entity_accts = (acquired if acquired else entity_all_accts) & clean_accts
                if len(entity_accts) < 3:
                    continue

                eidx = np.array([acct_idx[a] for a in entity_accts if a in acct_idx])
                if len(eidx) < 2:
                    continue

                e_log_growth = log_growth[eidx]
                e_v0 = v0_arr[eidx]
                avg_actual = float(e_log_growth.mean())
                avg_parcel_value = float(e_v0.mean())

                entity_types = set(pt_arr[eidx]) - {''}
                entity_zips = set(z3_arr[eidx]) - {''}

                lo_val = avg_parcel_value * (1 - BUDGET_TOLERANCE)
                hi_val = avg_parcel_value * (1 + BUDGET_TOLERANCE)

                entity_set = set(entity_accts)
                comp_indices = []
                for z3 in entity_zips:
                    for pt in (entity_types if entity_types else {''}):
                        for idx in buckets.get((z3, pt), []):
                            if clean_list[idx] not in entity_set:
                                if lo_val <= v0_arr[idx] <= hi_val:
                                    comp_indices.append(idx)

                if len(comp_indices) < 5:
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
                comp_has_pred = valid_pred[comp_idx]
                comp_pred = pred_growth[comp_idx]

                n_picks = min(len(entity_accts), len(comp_idx))

                if comp_has_pred.sum() >= n_picks:
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

                benchmark_return = float(np.median(comp_log_growth))
                rng = np.random.RandomState(42 + hash(entity) % 10000)
                rand_idx = rng.choice(len(comp_idx), size=min(n_picks, len(comp_idx)), replace=False)
                avg_random_return = float(comp_log_growth[rand_idx].mean())
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

            # Save this (origin, horizon) to GCS immediately (resumability!)
            if results:
                oh_df = pd.DataFrame(results)
                _gcs_upload_parquet(bucket, cache_oh, oh_df)
                all_results.append(oh_df)
                print(f"    ↑ Cached {len(oh_df):,} rows to GCS: {cache_oh}")

    # ═══════════════════════════════════════════════════════════════════
    # FINAL: Combine all results
    # ═══════════════════════════════════════════════════════════════════
    if not all_results:
        print("❌ No results!")
        return {"error": "no results"}

    df = pd.concat(all_results, ignore_index=True)
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

    # Save final combined results to GCS
    _gcs_upload_parquet(bucket, "entity_backtest_counterfactual/results_v2.parquet", df)

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
        "n_entities": int(df["entity"].nunique()),
        "alpha_mean": float(df_with_model["alpha_vs_actual"].mean()) if len(df_with_model) > 0 else None,
        "alpha_median": float(df_with_model["alpha_vs_actual"].median()) if len(df_with_model) > 0 else None,
    }


@app.local_entrypoint()
def main(sample_entities: int = 500, sample_parcels: int = 0, force: bool = False):
    result = run_counterfactual_backtest.remote(
        sample_entities=sample_entities,
        sample_parcels=sample_parcels,
        force=force,
    )
    print(f"\n✅ Counterfactual backtest v2: {result}")
