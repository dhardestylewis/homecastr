"""
Entity Backtest v3 — Fully Parallelized + Batched Supabase Reads

Key improvements over v2:
1. Batch Supabase reads: WHERE acct IN (...) with 5K accts per batch
   instead of WHERE jurisdiction='hcad' (full table scan)
2. Parallelize per (origin, horizon) pair — each combo is a separate container
3. Pre-compute owner→acct mapping in build step, share via GCS
4. Upload results to GCS immediately per-batch

Architecture:
  Step 1 (local): Build owner→acct mapping from panel + owner data
  Step 2 (modal.map): For each (origin, horizon), fetch forecasts in batches,
                      compute entity metrics, upload to GCS
  Step 3 (modal): Aggregate results across all (origin, horizon) pairs
"""

import modal
import os

app = modal.App("entity-backtest-v3")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "google-cloud-storage", "numpy", "pandas", "polars", "pyarrow",
        "psycopg2-binary", "scipy",
    )
)

gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
supabase_secret = modal.Secret.from_name("supabase-creds", required_keys=["SUPABASE_DB_URL"])
output_vol = modal.Volume.from_name("entity-backtest-results", create_if_missing=True)


# ─── Step 1: Build entity portfolios (owner → [acct,...]) ─────────────────

@app.function(
    image=image,
    secrets=[gcs_secret],
    timeout=1800,
    memory=16384,
    volumes={"/output": output_vol},
)
def build_entity_portfolios(min_parcels: int = 10, min_portfolio_value: float = 1_000_000):
    """Load panel + owner data, build owner→acct mapping, save to GCS."""
    import json, io, zipfile, tempfile, time
    import pandas as pd
    import polars as pl
    from google.cloud import storage

    ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts()}] Building entity portfolios...")

    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")

    # Load panel to get all HCAD accts
    print(f"[{ts()}] Loading panel...")
    blob = bucket.blob("panel/jurisdiction=hcad_houston/part.parquet")
    buf = io.BytesIO()
    blob.download_to_file(buf)
    buf.seek(0)
    panel = pd.read_parquet(buf)
    acct_col = next(c for c in panel.columns if 'acct' in c.lower())
    all_accts = set(panel[acct_col].astype(str).unique())
    print(f"[{ts()}] Panel: {len(all_accts):,} unique accts")

    # Load owner data for each year (path: hcad/owner/YEAR/Real_acct_owner.zip)
    owners_by_year = {}
    for blob in bucket.list_blobs(prefix="hcad/owner/"):
        if not blob.name.endswith(".zip"):
            continue
        # Extract year from path like hcad/owner/2025/Real_acct_owner.zip
        parts = blob.name.split("/")
        try:
            year = int(parts[2])  # hcad/owner/YEAR/...
        except (IndexError, ValueError):
            continue

        print(f"[{ts()}] Loading owners year={year}...")
        buf = io.BytesIO()
        blob.download_to_file(buf)
        buf.seek(0)

        with zipfile.ZipFile(buf) as zf:
            # Owner data is in owners.txt (tab-separated, has header row)
            # Columns: acct, ln_num, name, aka, pct_own
            owner_file = next((n for n in zf.namelist() if 'owner' in n.lower()), None)
            if not owner_file:
                print(f"  {year}: no owner file found in {zf.namelist()}")
                continue
            with zf.open(owner_file) as f:
                raw = f.read().decode("latin-1")
                df = pl.read_csv(io.StringIO(raw), separator="\t", infer_schema_length=0,
                                 quote_char=None, has_header=True)

        # Use named columns: 'acct' and 'name'
        acct_c = next((c for c in df.columns if c.lower().strip() == 'acct'), df.columns[0])
        owner_c = next((c for c in df.columns if c.lower().strip() == 'name'), df.columns[2])

        owner_map = {}
        for row in df.select([acct_c, owner_c]).iter_rows():
            acct, owner = str(row[0]).strip(), str(row[1]).strip()
            if acct in all_accts and owner and owner != 'nan' and owner != 'None':
                if owner not in owner_map:
                    owner_map[owner] = []
                owner_map[owner].append(acct)

        owners_by_year[year] = owner_map
        print(f"  {year}: {len(owner_map):,} unique owners")

    # Build entity portfolios: owners with >= min_parcels across any year
    all_entities = {}  # owner_name → set(accts)
    for year, owner_map in owners_by_year.items():
        for owner, accts in owner_map.items():
            if owner not in all_entities:
                all_entities[owner] = set()
            all_entities[owner].update(accts)

    # Filter to ICP entities (>= min_parcels)
    icp_entities = {k: sorted(v) for k, v in all_entities.items() if len(v) >= min_parcels}

    # Portfolio value floor: sum assessed values per entity from panel
    if min_portfolio_value > 0:
        val_col = next((c for c in panel.columns if 'tot_appr' in c.lower() or 'tot_mkt' in c.lower() or 'prior_tot' in c.lower()), None)
        if val_col:
            acct_vals = panel.groupby(acct_col)[val_col].max().to_dict()  # latest value per acct
            before = len(icp_entities)
            icp_entities = {
                owner: accts for owner, accts in icp_entities.items()
                if sum(float(acct_vals.get(a, 0) or 0) for a in accts) >= min_portfolio_value
            }
            print(f"  Portfolio value floor ${min_portfolio_value:,.0f}: {before:,} → {len(icp_entities):,} entities")
        else:
            print(f"  ⚠️ No value column found for portfolio floor, skipping value filter")

    all_icp_accts = set()
    for accts in icp_entities.values():
        all_icp_accts.update(accts)

    print(f"\n[{ts()}] Entity portfolios built:")
    print(f"  Total entities: {len(all_entities):,}")
    print(f"  ICP entities (>={min_parcels} parcels, >=${min_portfolio_value:,.0f}): {len(icp_entities):,}")
    print(f"  ICP accts: {len(all_icp_accts):,}")

    # Save to GCS for sharing with worker containers
    result = {
        "entities": icp_entities,
        "all_icp_accts": sorted(all_icp_accts),
        "n_entities": len(icp_entities),
        "n_accts": len(all_icp_accts),
    }
    out_path = "/output/entity_portfolios.json"
    with open(out_path, "w") as f:
        json.dump(result, f)
    output_vol.commit()

    # Also upload to GCS
    blob = bucket.blob("entity_backtest/entity_portfolios.json")
    blob.upload_from_filename(out_path)
    print(f"[{ts()}] → gs://properlytic-raw-data/entity_backtest/entity_portfolios.json")

    return {"n_entities": len(icp_entities), "n_accts": len(all_icp_accts)}


# ─── Step 2: Process one (origin, horizon) pair ──────────────────────────

@app.function(
    image=image,
    secrets=[gcs_secret, supabase_secret],
    timeout=3600,
    memory=16384,
    volumes={"/output": output_vol},
)
def process_origin_horizon(origin: int, horizon: int,
                           schema: str = "forecast_20260220_7f31c6e4"):
    """Fetch forecasts for one (origin, horizon) in batches, compute entity metrics."""
    import json, time
    import pandas as pd
    import numpy as np
    import psycopg2
    from google.cloud import storage

    ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")
    forecast_year = origin + horizon
    print(f"[{ts()}] Processing origin={origin} horizon={horizon} (forecast_year={forecast_year})")

    # Load entity portfolios from volume
    with open("/output/entity_portfolios.json") as f:
        portfolios = json.load(f)
    entities = portfolios["entities"]
    all_accts = portfolios["all_icp_accts"]
    print(f"[{ts()}] {len(entities):,} entities, {len(all_accts):,} accts")

    # Batch-fetch forecasts from Supabase (5K accts per query)
    conn = psycopg2.connect(os.environ["SUPABASE_DB_URL"])
    cur = conn.cursor()
    cur.execute("SET statement_timeout = '60000'")  # 1 min per batch

    BATCH_SIZE = 5000
    forecasts = {}  # acct → {p10, p50, p90}
    n_batches = (len(all_accts) + BATCH_SIZE - 1) // BATCH_SIZE
    
    for i in range(0, len(all_accts), BATCH_SIZE):
        batch = all_accts[i:i+BATCH_SIZE]
        try:
            cur.execute(f"""
                SELECT acct, p10, p50, p90
                FROM {schema}.metrics_parcel_forecast
                WHERE acct = ANY(%s)
                AND origin_year = %s
                AND forecast_year = %s
            """, (batch, origin, forecast_year))
            for acct, p10, p50, p90 in cur.fetchall():
                forecasts[acct] = {"p10": p10, "p50": p50, "p90": p90}
        except Exception as e:
            print(f"[{ts()}] Batch {i//BATCH_SIZE}/{n_batches}: error - {str(e)[:100]}")
            conn.rollback()
            cur.execute("SET statement_timeout = '60000'")

        if (i // BATCH_SIZE) % 10 == 0:
            print(f"[{ts()}] Batch {i//BATCH_SIZE}/{n_batches}: {len(forecasts):,} forecasts so far")

    conn.close()
    print(f"[{ts()}] Fetched {len(forecasts):,} forecasts")

    if not forecasts:
        print(f"[{ts()}] No forecasts found for origin={origin} forecast_year={forecast_year}")
        return {"origin": origin, "horizon": horizon, "n_entities": 0, "n_forecasts": 0}

    # Load actuals from panel
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")

    # Compute entity-level metrics
    entity_rows = []
    for idx, (owner, accts) in enumerate(entities.items()):
        accts_with_forecast = [a for a in accts if a in forecasts]
        if len(accts_with_forecast) < 3:
            continue

        # Aggregate entity-level forecast
        p50_values = [forecasts[a]["p50"] for a in accts_with_forecast if forecasts[a]["p50"]]
        p10_values = [forecasts[a]["p10"] for a in accts_with_forecast if forecasts[a]["p10"]]
        p90_values = [forecasts[a]["p90"] for a in accts_with_forecast if forecasts[a]["p90"]]

        if not p50_values:
            continue

        entity_rows.append({
            "owner_name": owner,
            "origin": origin,
            "horizon": horizon,
            "forecast_year": forecast_year,
            "n_parcels": len(accts),
            "n_with_forecast": len(accts_with_forecast),
            "portfolio_p10": float(np.mean(p10_values)) if p10_values else None,
            "portfolio_p50": float(np.mean(p50_values)),
            "portfolio_p90": float(np.mean(p90_values)) if p90_values else None,
            "portfolio_total_p50": float(np.sum(p50_values)),
        })

        if (idx + 1) % 5000 == 0:
            print(f"[{ts()}] {idx+1:,} entities processed, {len(entity_rows):,} with forecasts")

    print(f"[{ts()}] origin={origin} horizon={horizon}: {len(entity_rows):,} entities with metrics")

    # Save immediately to GCS
    if entity_rows:
        df = pd.DataFrame(entity_rows)
        local_path = f"/output/backtest_o{origin}_h{horizon}.parquet"
        df.to_parquet(local_path, index=False)
        output_vol.commit()

        gcs_path = f"entity_backtest/backtest_o{origin}_h{horizon}.parquet"
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_path)
        print(f"[{ts()}] → gs://properlytic-raw-data/{gcs_path} ({len(entity_rows):,} rows)")

    return {"origin": origin, "horizon": horizon, "n_entities": len(entity_rows),
            "n_forecasts": len(forecasts)}


# ─── Step 3: Aggregate all results ───────────────────────────────────────

@app.function(
    image=image,
    secrets=[gcs_secret],
    timeout=600,
    memory=8192,
    volumes={"/output": output_vol},
)
def aggregate_all(results: list):
    """Combine all per-(origin,horizon) results into final report."""
    import json, time
    import pandas as pd
    from google.cloud import storage

    ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts()}] Aggregating {len(results)} results")

    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")

    # Combine all parquet files
    all_dfs = []
    for blob in bucket.list_blobs(prefix="entity_backtest/backtest_o"):
        if blob.name.endswith(".parquet"):
            buf = __import__("io").BytesIO()
            blob.download_to_file(buf)
            buf.seek(0)
            all_dfs.append(pd.read_parquet(buf))
            print(f"  {blob.name}: {all_dfs[-1].shape}")

    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
        print(f"\n[{ts()}] Combined: {combined.shape}")
        print(f"  Origins: {sorted(combined['origin'].unique())}")
        print(f"  Horizons: {sorted(combined['horizon'].unique())}")
        print(f"  Unique entities: {combined['owner_name'].nunique():,}")

        # Save combined
        out_path = "/output/entity_backtest_combined.parquet"
        combined.to_parquet(out_path, index=False)
        bucket.blob("entity_backtest/entity_backtest_combined.parquet").upload_from_filename(out_path)

        # Summary stats
        summary = combined.groupby(["origin", "horizon"]).agg(
            n_entities=("owner_name", "nunique"),
            avg_portfolio_p50=("portfolio_p50", "mean"),
            total_portfolio_value=("portfolio_total_p50", "sum"),
        ).reset_index()
        print(f"\n{summary.to_string()}")

        summary_path = "/output/entity_backtest_summary.csv"
        summary.to_csv(summary_path, index=False)
        bucket.blob("entity_backtest/entity_backtest_summary.csv").upload_from_filename(summary_path)

    output_vol.commit()
    return {"n_files": len(all_dfs), "n_total": sum(len(d) for d in all_dfs) if all_dfs else 0}


# ─── Orchestrator: runs entirely on Modal cloud (no local connection needed) ──
# Call from local with .spawn() so laptop can close after submission

@app.function(
    image=image,
    secrets=[gcs_secret, supabase_secret],
    timeout=21600,   # 6h — full backtest across all origins/horizons
    memory=4096,
)
def run_full_backtest(origins: list, horizons: list, min_parcels: int = 10, min_portfolio_value: float = 1_000_000):
    """
    Full orchestration that runs on Modal cloud.
    Calls other Modal functions via .remote()/.starmap() from inside Modal —
    this is safe because the connection is cloud→cloud, not laptop→cloud.
    """
    import time
    ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")

    # Step 1: Build entity portfolios
    print(f"[{ts()}] 🏗️  Building entity portfolios (min_parcels={min_parcels}, min_value=${min_portfolio_value:,.0f})...")
    portfolio_info = build_entity_portfolios.remote(min_parcels=min_parcels, min_portfolio_value=min_portfolio_value)
    print(f"[{ts()}]   → {portfolio_info['n_entities']:,} ICP entities, {portfolio_info['n_accts']:,} accts")

    # Step 2: Process all (origin, horizon) pairs
    combos = [(o, h) for o in origins for h in horizons]
    print(f"[{ts()}] 🚀 Launching {len(combos)} (origin, horizon) pairs...")
    for o, h in combos:
        print(f"   origin={o} horizon={h} → forecast_year={o+h}")

    all_results = list(process_origin_horizon.starmap(combos))
    for r in all_results:
        print(f"   origin={r['origin']} h={r['horizon']}: {r['n_entities']:,} entities, {r['n_forecasts']:,} forecasts")

    # Step 3: Aggregate
    print(f"\n[{ts()}] 📊 Aggregating {len(all_results)} results...")
    agg = aggregate_all.remote(all_results)
    print(f"[{ts()}] ✅ Done! {agg['n_total']:,} total entity records across {agg['n_files']} files")
    return agg


# ─── Entry point ─────────────────────────────────────────────────────────
# Uses .spawn() — submits to Modal cloud and exits immediately.
# Safe to close the laptop right after running this.

@app.local_entrypoint()
def main(origins: str = "2023,2024",
         horizons: str = "1,2,3,4,5",
         min_parcels: int = 10,
         min_portfolio_value: float = 1_000_000):
    origin_list = [int(x.strip()) for x in origins.split(",")]
    horizon_list = [int(x.strip()) for x in horizons.split(",")]
    print(f"🚀 Running entity backtest: origins={origin_list} horizons={horizon_list} min_parcels={min_parcels} min_value=${min_portfolio_value:,.0f}")
    result = run_full_backtest.remote(origin_list, horizon_list, min_parcels, min_portfolio_value)
    print(f"✅ Done: {result}")


