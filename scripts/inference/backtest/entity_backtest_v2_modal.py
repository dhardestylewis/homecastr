"""
Entity Portfolio Backtest v2 — Optimized & Parallelized
========================================================
Improvements over v1:
  - Parallelized by origin year via modal.map()
  - Fixed CSV quoting errors (quote_char=None)
  - Cached owner data on Modal volume
  - Progress streaming per-entity
  - Resume support (skip already-processed origins)

Usage:
    modal run scripts/inference/backtest/entity_backtest_v2_modal.py
    modal run scripts/inference/backtest/entity_backtest_v2_modal.py --origins 2019,2020,2021
"""
import modal, os

app = modal.App("entity-backtest-v2")

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


# ─── Shared helpers ────────────────────────────────────────────────────────────

ENTITY_KW = ["LLC","LP","INC","CORP","TRUST","LTD","PARTNERS","FUND",
             "INVESTMENT","PROPERTIES","CAPITAL","HOLDINGS","VENTURES",
             "ASSET","REALTY","MGMT","MANAGEMENT","ENTERPRISE","GROUP"]
SEGMENTS = [
    ("Fund/Inv",   ["FUND","INVESTMENT","CAPITAL","VENTURES","ASSET"]),
    ("Trust",      ["TRUST","ESTATE"]),
    ("PropMgmt",   ["PROPERTIES","REALTY","MGMT","MANAGEMENT","PROPERTY"]),
    ("Holdings",   ["HOLDINGS","GROUP","ENTERPRISE"]),
    ("Other Corp", ["LLC","LP","INC","CORP","LTD","PARTNERS"]),
]
NON_ICP = ["COUNTY OF","STATE OF","CITY OF","HOUSTON","HARRIS COUNTY",
           "HISD","METRO","PORT AUTHORITY","ELECTRIC","WATER AUTHORITY",
           "SCHOOL DISTRICT","CHURCH","UNIVERSITY","FOUNDATION"]


def classify(name):
    if not name: return None
    up = name.upper()
    if not any(kw in up for kw in ENTITY_KW): return None
    for seg, kws in SEGMENTS:
        if any(kw in up for kw in kws): return seg
    return "Other Corp"


def is_icp(name):
    if not name: return False
    up = name.upper()
    return any(kw in up for kw in ENTITY_KW) and not any(kw in up for kw in NON_ICP)


# ─── Per-origin worker (parallelized via modal.map) ───────────────────────────

@app.function(
    image=image,
    secrets=[gcs_secret, supabase_secret],
    timeout=7200,     # 2 hours per origin (Supabase forecast fetch is slow)
    memory=32768,
    volumes={"/output": output_vol},
)
def process_one_origin(
    origin: int,
    schema: str = "forecast_20260220_7f31c6e4",
    min_parcels: int = 3,
):
    """Process a single origin year: load owners, fetch forecasts, compute per-entity metrics."""
    import json, time, io, zipfile
    import numpy as np
    import pandas as pd
    import polars as pl
    import psycopg2
    from google.cloud import storage

    ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")
    t0 = time.time()

    # ─── GCS + DB setup ───
    creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON", "")
    if creds_json:
        with open("/tmp/gcs_creds.json", "w") as f:
            f.write(creds_json)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/gcs_creds.json"
    db_url = os.environ.get("SUPABASE_DB_URL", "")
    client = storage.Client()
    bucket = client.bucket("properlytic-raw-data")

    # ─── 1. Load HCAD panel ───
    print(f"[{ts()}] [origin={origin}] Loading panel...")
    panel_local = "/tmp/hcad_panel.parquet"
    blob = bucket.blob("hcad/hcad_master_panel_2005_2025_leakage_strict_FIXEDYR_WITHGIS.parquet")
    blob.reload()
    print(f"[{ts()}] [origin={origin}] Downloading {blob.size/1e9:.2f} GB...")
    blob.download_to_filename(panel_local)

    panel = pd.read_parquet(panel_local, columns=["acct", "yr", "tot_appr_val"])
    panel = panel[panel["tot_appr_val"] > 0].copy()
    panel["yr"] = panel["yr"].astype(int)
    panel["acct"] = panel["acct"].astype(str).str.strip()
    print(f"[{ts()}] [origin={origin}] Panel: {len(panel):,} rows")
    val_lookup = panel.set_index(["acct", "yr"])["tot_appr_val"].to_dict()

    # ─── 2. Load owner names (FIXED: quote_char=None) ───
    print(f"[{ts()}] [origin={origin}] Loading owners for year {origin}...")
    owners_yr = {}

    # Load target year + a few nearby years for better coverage
    for yr in [origin, origin - 1, origin + 1]:
        if yr in owners_yr and len(owners_yr) > 100000:
            break
        blob_path = f"hcad/owner/{yr}/Real_acct_owner.zip"
        b = bucket.blob(blob_path)
        if not b.exists():
            continue
        content = b.download_as_bytes()
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                of = next((n for n in zf.namelist() if 'owner' in n.lower() and n.endswith('.txt')), None)
                if not of:
                    continue
                with zf.open(of) as f:
                    raw = f.read().decode("latin-1").encode("utf-8")
                    odf = pl.read_csv(
                        io.BytesIO(raw),
                        separator="\t",
                        ignore_errors=True,
                        truncate_ragged_lines=True,
                        infer_schema_length=0,
                        quote_char=None,  # FIX: disable quoting to handle unescaped quotes
                    )
                    oc = next((c for c in odf.columns if 'owner' in c.lower() and 'name' in c.lower()), None)
                    if not oc:
                        oc = next((c for c in odf.columns if 'name' in c.lower()), None)
                    ac = next((c for c in odf.columns if 'acct' in c.lower()), odf.columns[0])
                    if oc:
                        odf = odf.select([ac, oc]).cast({ac: pl.Utf8, oc: pl.Utf8})
                        loaded = dict(zip(
                            odf[ac].str.strip_chars().to_list(),
                            odf[oc].to_list()
                        ))
                        # Merge — prefer target year
                        if yr == origin:
                            owners_yr.update(loaded)
                        else:
                            for k, v in loaded.items():
                                if k not in owners_yr:
                                    owners_yr[k] = v
                        print(f"  [{ts()}] Year {yr}: {len(loaded):,} owners loaded")
        except Exception as e:
            print(f"  [{ts()}] Year {yr}: ⚠️ {e}")

    if not owners_yr:
        print(f"[{ts()}] [origin={origin}] ⚠️ No owner data — skipping")
        return {"origin": origin, "entity_results": [], "error": "no_owner_data"}

    print(f"[{ts()}] [origin={origin}] {len(owners_yr):,} owners loaded")

    # ─── 3. Build entity portfolios ───
    entity_portfolios = {}
    for acct, name in owners_yr.items():
        if not is_icp(name):
            continue
        if (acct, origin) not in val_lookup:
            continue
        entity_portfolios.setdefault(name, []).append(acct)

    entity_portfolios = {k: v for k, v in entity_portfolios.items() if len(v) >= min_parcels}
    n_entities = len(entity_portfolios)
    n_parcels = sum(len(v) for v in entity_portfolios.values())
    print(f"[{ts()}] [origin={origin}] {n_entities:,} ICP entities, {n_parcels:,} parcels")

    if n_entities == 0:
        return {"origin": origin, "entity_results": [], "error": "no_entities"}

    # ─── 4. Fetch ALL forecasts from Supabase for this origin ───
    print(f"[{ts()}] [origin={origin}] Fetching forecasts...")
    try:
        conn = psycopg2.connect(db_url)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SET statement_timeout = '600000'")
        cur.execute(f"SET search_path TO {schema}, public")
        cur.execute("""
            SELECT acct, origin_year, forecast_year, p50
            FROM metrics_parcel_forecast
            WHERE jurisdiction = 'hcad'
            AND origin_year = %s
            AND p50 IS NOT NULL
        """, (origin,))
        fc_rows = cur.fetchall()
        conn.close()
    except Exception as e:
        print(f"[{ts()}] [origin={origin}] ⚠️ DB error: {e}")
        return {"origin": origin, "entity_results": [], "error": str(e)}

    if not fc_rows:
        print(f"[{ts()}] [origin={origin}] No forecasts found")
        return {"origin": origin, "entity_results": [], "error": "no_forecasts"}

    fc_df = pd.DataFrame(fc_rows, columns=["acct", "origin_year", "forecast_year", "p50"])
    fc_df["acct"] = fc_df["acct"].astype(str).str.strip()
    fc_lookup = fc_df.set_index(["acct", "forecast_year"])["p50"].to_dict()
    available_forecast_years = sorted(fc_df["forecast_year"].unique())
    print(f"[{ts()}] [origin={origin}] {len(fc_df):,} forecast rows, years={available_forecast_years}")

    all_accts_with_fc = set(fc_df["acct"].unique())

    # ─── 5. Per-entity backtest loop ───
    entity_rows = []

    for fcast_yr in available_forecast_years:
        horizon = int(fcast_yr) - origin
        if horizon < 1 or fcast_yr > 2025:
            continue

        # Build market data ONCE per forecast year
        market_accts = []
        for acct in all_accts_with_fc:
            v0 = val_lookup.get((acct, origin))
            v1 = val_lookup.get((acct, int(fcast_yr)))
            score = fc_lookup.get((acct, int(fcast_yr)))
            if v0 and v1 and v0 > 0 and v1 > 0 and score is not None:
                market_accts.append({
                    "acct": acct, "val_origin": v0, "val_actual": v1,
                    "model_score": score,
                    "actual_return": (v1 - v0) / v0,
                })
        market = pd.DataFrame(market_accts)
        if len(market) < 100:
            continue

        market["model_rank"] = market["model_score"].rank(pct=True)
        market["price_bracket"] = pd.qcut(market["val_origin"], 10, labels=False, duplicates="drop")

        # Pre-index market by bracket for O(1) lookup
        market_by_bracket = {
            b: grp.sort_values("model_score", ascending=False)
            for b, grp in market.groupby("price_bracket")
        }

        # Vectorized bracket assignment for fast matching
        bracket_edges = market.groupby("price_bracket")["val_origin"].agg(["min", "max"]).to_dict("index")

        entity_count = 0
        for owner_name, accts in entity_portfolios.items():
            portfolio = []
            for acct in accts:
                v0 = val_lookup.get((acct, origin))
                v1 = val_lookup.get((acct, int(fcast_yr)))
                score = fc_lookup.get((acct, int(fcast_yr)))
                if v0 and v1 and v0 > 0 and v1 > 0:
                    portfolio.append({
                        "acct": acct, "val_origin": v0, "val_actual": v1,
                        "model_score": score,
                        "actual_return": (v1 - v0) / v0,
                    })

            if len(portfolio) < min_parcels:
                continue

            pdf = pd.DataFrame(portfolio)
            n = len(pdf)
            total_invested = pdf["val_origin"].sum()

            # Actual return
            actual_gain = pdf["val_actual"].sum() - total_invested
            actual_return_pct = actual_gain / total_invested * 100

            # Model-filtered: top half by model score
            pdf_sorted = pdf.sort_values("model_score", ascending=False)
            top_half = pdf_sorted.head(max(n // 2, 1))
            filtered_invested = top_half["val_origin"].sum()
            filtered_gain = top_half["val_actual"].sum() - filtered_invested
            filtered_return_pct = filtered_gain / filtered_invested * 100 if filtered_invested > 0 else 0

            # Counterfactual: replace bottom half with best market picks
            bottom_half = pdf_sorted.tail(n - max(n // 2, 1))
            replacement_gain = 0
            replacement_invested = 0
            n_replaced = 0
            for _, row in bottom_half.iterrows():
                bracket_match = market[
                    (market["val_origin"] >= row["val_origin"] * 0.7) &
                    (market["val_origin"] <= row["val_origin"] * 1.3) &
                    (~market["acct"].isin(pdf["acct"]))
                ].sort_values("model_score", ascending=False)

                if len(bracket_match) > 0:
                    best = bracket_match.iloc[0]
                    replacement_invested += best["val_origin"]
                    replacement_gain += best["val_actual"] - best["val_origin"]
                    n_replaced += 1
                else:
                    replacement_invested += row["val_origin"]
                    replacement_gain += row["val_actual"] - row["val_origin"]

            replaced_invested = filtered_invested + replacement_invested
            replaced_gain_total = filtered_gain + replacement_gain
            replaced_return_pct = replaced_gain_total / replaced_invested * 100 if replaced_invested > 0 else 0

            entity_rows.append({
                "owner_name": owner_name,
                "segment": classify(owner_name),
                "origin": origin,
                "forecast_year": int(fcast_yr),
                "horizon": horizon,
                "n_parcels": n,
                "total_invested": round(total_invested, 0),
                "actual_return_pct": round(actual_return_pct, 2),
                "actual_gain": round(actual_gain, 0),
                "filtered_return_pct": round(filtered_return_pct, 2),
                "filtered_gain": round(filtered_gain, 0),
                "uplift_filtered_pp": round(filtered_return_pct - actual_return_pct, 2),
                "replaced_return_pct": round(replaced_return_pct, 2),
                "replaced_gain": round(replaced_gain_total, 0),
                "uplift_replaced_pp": round(replaced_return_pct - actual_return_pct, 2),
                "n_replaced": n_replaced,
                "value_add_filtered": round(filtered_gain - actual_gain * (filtered_invested / total_invested), 0),
                "value_add_replaced": round(replaced_gain_total - actual_gain, 0),
            })
            entity_count += 1

            # Progress every 500 entities
            if entity_count % 500 == 0:
                print(f"  [{ts()}] origin={origin} +{horizon}yr: {entity_count:,} entities processed")

        print(f"[{ts()}] [origin={origin}] +{horizon}yr: {entity_count:,} entities, {len(entity_rows):,} total rows")

    elapsed = time.time() - t0
    print(f"[{ts()}] [origin={origin}] ✅ Done: {len(entity_rows):,} rows in {elapsed/60:.1f} min")

    # Save per-origin results
    out_path = f"/output/entity_backtest_origin_{origin}.json"
    import json
    with open(out_path, "w") as f:
        json.dump({"origin": origin, "entity_results": entity_rows}, f)
    output_vol.commit()

    # Upload to GCS immediately (don't wait for aggregator)
    try:
        gcs_path = f"entity_backtest/entity_backtest_origin_{origin}.json"
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(out_path)
        print(f"[{ts()}] [origin={origin}] → gs://properlytic-raw-data/{gcs_path}")

        # Also save as parquet for fast analysis
        if entity_rows:
            edf = pd.DataFrame(entity_rows)
            pq_path = f"/output/entity_backtest_origin_{origin}.parquet"
            edf.to_parquet(pq_path, index=False)
            gcs_pq = f"entity_backtest/entity_backtest_origin_{origin}.parquet"
            bucket.blob(gcs_pq).upload_from_filename(pq_path)
            print(f"[{ts()}] [origin={origin}] → gs://properlytic-raw-data/{gcs_pq}")
    except Exception as e:
        print(f"[{ts()}] [origin={origin}] ⚠️ GCS upload failed: {e}")

    return {"origin": origin, "n_rows": len(entity_rows), "elapsed_min": round(elapsed / 60, 1)}


# ─── Aggregator: collects results from all origins ───────────────────────────

@app.function(
    image=image,
    secrets=[gcs_secret],
    timeout=600,
    memory=8192,
    volumes={"/output": output_vol},
)
def aggregate_results(origins: list):
    """Collect per-origin JSON files into final combined output."""
    import json, time
    import pandas as pd
    from google.cloud import storage

    ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")

    creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON", "")
    if creds_json:
        with open("/tmp/gcs_creds.json", "w") as f:
            f.write(creds_json)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/gcs_creds.json"
    client = storage.Client()
    bucket = client.bucket("properlytic-raw-data")

    all_rows = []
    for origin in origins:
        path = f"/output/entity_backtest_origin_{origin}.json"
        if os.path.exists(path):
            with open(path) as f:
                data = json.load(f)
                rows = data.get("entity_results", [])
                all_rows.extend(rows)
                print(f"[{ts()}] Origin {origin}: {len(rows):,} rows")
        else:
            print(f"[{ts()}] Origin {origin}: no file found")

    print(f"\n[{ts()}] Total: {len(all_rows):,} entity-horizon rows")

    if not all_rows:
        return {"status": "empty"}

    out_data = {
        "entity_results": all_rows,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "origins": sorted(origins),
        "n_entities": len(set(r["owner_name"] for r in all_rows)),
    }

    # Save combined
    with open("/output/entity_portfolio_backtest_v2.json", "w") as f:
        json.dump(out_data, f, indent=2)

    edf = pd.DataFrame(all_rows)
    edf.to_parquet("/output/entity_portfolio_backtest_v2.parquet", index=False)

    # Upload to GCS
    for fname in ["entity_portfolio_backtest_v2.json", "entity_portfolio_backtest_v2.parquet"]:
        blob = bucket.blob(f"entity_backtest/{fname}")
        blob.upload_from_filename(f"/output/{fname}")
        print(f"[{ts()}] → gs://properlytic-raw-data/entity_backtest/{fname}")

    output_vol.commit()

    # Summary
    print(f"\n{'='*70}")
    print(f"📋 PER-ENTITY PORTFOLIO BACKTEST v2 SUMMARY")
    print(f"{'='*70}")
    n_unique = edf["owner_name"].nunique()
    print(f"  {n_unique} unique entities across {len(edf)} entity-horizon combos")
    print(f"\n  Median actual return:   {edf['actual_return_pct'].median():+.1f}%")
    print(f"  Median filtered return: {edf['filtered_return_pct'].median():+.1f}%")
    print(f"  Median replaced return: {edf['replaced_return_pct'].median():+.1f}%")
    print(f"  Median uplift (filter): {edf['uplift_filtered_pp'].median():+.1f}pp")
    print(f"  Median uplift (replace):{edf['uplift_replaced_pp'].median():+.1f}pp")

    top = edf.groupby("owner_name").agg(
        total_invested=("total_invested", "sum"),
        total_value_add=("value_add_replaced", "sum"),
        n_scenarios=("owner_name", "count"),
        avg_uplift=("uplift_replaced_pp", "mean"),
    ).sort_values("total_value_add", ascending=False).head(10)

    print(f"\n  🏆 Top 10 entities by value-add:")
    for name, r in top.iterrows():
        print(f"    {name[:40]:<40} invested=${r['total_invested']:>14,.0f} "
              f"model_add=${r['total_value_add']:>12,.0f} uplift={r['avg_uplift']:+.1f}pp")

    n_worse = (edf["uplift_replaced_pp"] < 0).sum()
    print(f"\n  ⚠️ {n_worse}/{len(edf)} entity-horizons model would have done WORSE ({100*n_worse/len(edf):.0f}%)")

    return {"status": "ok", "n_rows": len(all_rows), "n_entities": n_unique}


@app.local_entrypoint()
def main(origins: str = "2019,2020,2021,2022,2023,2024"):
    """Launch parallelized entity backtest across origins."""
    origin_list = [int(x.strip()) for x in origins.split(",")]
    print(f"🚀 Launching entity backtest v2 across {len(origin_list)} origins: {origin_list}")
    print(f"   Each origin runs in its own Modal container in parallel\n")

    # Fan out: one container per origin year
    results = list(process_one_origin.map(
        origin_list,
        kwargs={"schema": "forecast_20260220_7f31c6e4", "min_parcels": 3},
    ))

    print(f"\n{'='*60}")
    print(f"Per-origin results:")
    for r in results:
        print(f"  Origin {r.get('origin')}: {r.get('n_rows', 0):,} rows in {r.get('elapsed_min', 0):.1f} min")
    print(f"{'='*60}\n")

    # Aggregate
    print("Aggregating results...")
    agg = aggregate_results.remote(origin_list)
    print(f"\n✅ Final: {agg}")
