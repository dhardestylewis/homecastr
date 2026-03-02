"""
Entity Portfolio Backtest — Per-Entity Model Value Analysis
===========================================================
For EACH entity owner:
  1. Identify all parcels they owned at origin year
  2. Compute their actual portfolio weighted return
  3. Build model-guided alternative:
     a) "Filtered" — keep only model top-half of their actual purchases
     b) "Replaced" — swap model bottom-half with best available alternatives
        in same price bracket from the full market
  4. Compute delta: how much better would they have done?

Output: per-entity JSON rows → GCS + Modal volume
  {owner_name, segment, n_parcels, actual_return, filtered_return,
   replaced_return, uplift_filtered_pp, uplift_replaced_pp, value_add, ...}

Usage:
    modal run scripts/inference/backtest/entity_backtest_modal.py
"""
import modal, os, sys

app = modal.App("entity-backtest")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "google-cloud-storage",
        "numpy",
        "pandas",
        "polars",
        "pyarrow",
        "psycopg2-binary",
        "scipy",
    )
)

gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
supabase_secret = modal.Secret.from_name("supabase-creds", required_keys=["SUPABASE_DB_URL"])
output_vol = modal.Volume.from_name("entity-backtest-results", create_if_missing=True)


@app.function(
    image=image,
    secrets=[gcs_secret, supabase_secret],
    timeout=7200,
    memory=32768,
    volumes={"/output": output_vol},
)
def run_entity_backtest(
    bucket_name: str = "properlytic-raw-data",
    panel_blob: str = "hcad/hcad_master_panel_2005_2025_leakage_strict_FIXEDYR_WITHGIS.parquet",
    schema: str = "forecast_20260220_7f31c6e4",
    origins: list = None,
    min_parcels: int = 3,
):
    import json, time, io, zipfile
    import numpy as np
    import pandas as pd
    import polars as pl
    import psycopg2
    from google.cloud import storage

    ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")
    if origins is None:
        origins = [2019, 2020, 2021, 2022, 2023, 2024]

    # ─── GCS setup ───
    creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON", "")
    if creds_json:
        with open("/tmp/gcs_creds.json", "w") as f:
            f.write(creds_json)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/gcs_creds.json"
    db_url = os.environ.get("SUPABASE_DB_URL", "")
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # ─── Entity classification ───
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

    # ═══════════════════════════════════════════════
    # 1. Load HCAD panel
    # ═══════════════════════════════════════════════
    print(f"[{ts()}] Loading panel from GCS...")
    panel_local = "/tmp/hcad_panel.parquet"
    blob = bucket.blob(panel_blob)
    blob.reload()
    print(f"[{ts()}] Downloading {blob.size/1e9:.2f} GB...")
    blob.download_to_filename(panel_local)

    panel = pd.read_parquet(panel_local, columns=["acct", "yr", "tot_appr_val"])
    panel = panel[panel["tot_appr_val"] > 0].copy()
    panel["yr"] = panel["yr"].astype(int)
    panel["acct"] = panel["acct"].astype(str).str.strip()
    print(f"[{ts()}] Panel: {len(panel):,} rows, {panel['acct'].nunique():,} unique parcels")

    # Build fast lookup: (acct, yr) → value
    val_lookup = panel.set_index(["acct", "yr"])["tot_appr_val"].to_dict()

    # ═══════════════════════════════════════════════
    # 2. Load owner names per year from GCS zips
    # ═══════════════════════════════════════════════
    print(f"\n[{ts()}] Loading owner names...")
    # owner_map[yr] = {acct: owner_name}
    owner_map = {}
    for yr in range(2015, 2026):
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
                    odf = pl.read_csv(io.BytesIO(raw), separator="\t",
                                     ignore_errors=True, truncate_ragged_lines=True,
                                     infer_schema_length=0)
                    oc = next((c for c in odf.columns if 'owner' in c.lower() and 'name' in c.lower()), None)
                    if not oc:
                        oc = next((c for c in odf.columns if 'name' in c.lower()), None)
                    ac = next((c for c in odf.columns if 'acct' in c.lower()), odf.columns[0])
                    if oc:
                        odf = odf.select([ac, oc]).cast({ac: pl.Utf8, oc: pl.Utf8})
                        owner_map[yr] = dict(zip(
                            odf[ac].str.strip_chars().to_list(),
                            odf[oc].to_list()
                        ))
                        print(f"  {yr}: {len(owner_map[yr]):,} owners")
        except Exception as e:
            print(f"  {yr}: ⚠️ {e}")

    # ═══════════════════════════════════════════════
    # 3. Per-entity portfolio backtest
    # ═══════════════════════════════════════════════
    all_entity_rows = []

    for origin in sorted(origins):
        owners_yr = owner_map.get(origin, {})
        if not owners_yr:
            print(f"\n⚠️ No owner data for {origin}")
            continue

        # Build entity-to-parcels mapping for this origin year
        # entity_portfolios[owner_name] = [acct1, acct2, ...]
        entity_portfolios = {}
        for acct, name in owners_yr.items():
            if not is_icp(name):
                continue
            # Must have a valuation at origin
            if (acct, origin) not in val_lookup:
                continue
            entity_portfolios.setdefault(name, []).append(acct)

        # Filter to entities with enough parcels
        entity_portfolios = {k: v for k, v in entity_portfolios.items() if len(v) >= min_parcels}
        n_entities = len(entity_portfolios)
        n_parcels = sum(len(v) for v in entity_portfolios.values())
        print(f"\n{'='*60}")
        print(f"Origin {origin} | {n_entities:,} ICP entities | {n_parcels:,} parcels")
        print(f"{'='*60}")

        if n_entities == 0:
            continue

        # Get ALL forecasts from Supabase for this origin (all horizons at once)
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
            print(f"  ⚠️ DB error: {e}")
            continue

        if not fc_rows:
            print(f"  No forecasts for origin={origin}")
            continue

        fc_df = pd.DataFrame(fc_rows, columns=["acct", "origin_year", "forecast_year", "p50"])
        fc_df["acct"] = fc_df["acct"].astype(str).str.strip()
        # p50 lookup: (acct, forecast_year) → model score
        fc_lookup = fc_df.set_index(["acct", "forecast_year"])["p50"].to_dict()
        available_forecast_years = sorted(fc_df["forecast_year"].unique())
        print(f"  Forecasts: {len(fc_df):,} rows, years={available_forecast_years}")

        # For counterfactual: all parcels with forecasts, grouped by value bracket
        all_accts_with_fc = set(fc_df["acct"].unique())

        for fcast_yr in available_forecast_years:
            horizon = int(fcast_yr) - origin
            if horizon < 1 or fcast_yr > 2025:
                continue

            print(f"\n  --- Origin {origin} → {fcast_yr} (+{horizon}yr) ---")

            # All market parcels with both origin val, actual val, and forecast
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

            # Pre-compute market quartile boundaries for model score
            market["model_rank"] = market["model_score"].rank(pct=True)

            # Price brackets for counterfactual matching
            market["price_bracket"] = pd.qcut(market["val_origin"], 10, labels=False, duplicates="drop")

            # Index for fast counterfactual lookup
            market_by_bracket = {
                b: grp.sort_values("model_score", ascending=False)
                for b, grp in market.groupby("price_bracket")
            }

            entity_count = 0
            for owner_name, accts in entity_portfolios.items():
                # Filter to parcels with complete data for this horizon
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

                # ── Actual portfolio return (value-weighted) ──
                actual_gain = pdf["val_actual"].sum() - total_invested
                actual_return_pct = actual_gain / total_invested * 100

                # ── Model-filtered: keep only top-half by model score ──
                pdf_sorted = pdf.sort_values("model_score", ascending=False)
                top_half = pdf_sorted.head(max(n // 2, 1))
                filtered_invested = top_half["val_origin"].sum()
                filtered_gain = top_half["val_actual"].sum() - filtered_invested
                filtered_return_pct = filtered_gain / filtered_invested * 100 if filtered_invested > 0 else 0

                # ── Counterfactual: replace bottom-half with market's best in same bracket ──
                bottom_half = pdf_sorted.tail(n - max(n // 2, 1))
                replacement_gain = 0
                replacement_invested = 0
                n_replaced = 0
                for _, row in bottom_half.iterrows():
                    # Find parcel's price bracket in market
                    bracket_match = market[
                        (market["val_origin"] >= row["val_origin"] * 0.7) &
                        (market["val_origin"] <= row["val_origin"] * 1.3) &
                        (~market["acct"].isin(pdf["acct"]))  # can't pick own parcels
                    ].sort_values("model_score", ascending=False)

                    if len(bracket_match) > 0:
                        best = bracket_match.iloc[0]
                        replacement_invested += best["val_origin"]
                        replacement_gain += best["val_actual"] - best["val_origin"]
                        n_replaced += 1
                    else:
                        # No match — keep original
                        replacement_invested += row["val_origin"]
                        replacement_gain += row["val_actual"] - row["val_origin"]

                # Combined: top-half actual + replaced bottom-half
                replaced_invested = filtered_invested + replacement_invested
                replaced_gain_total = filtered_gain + replacement_gain
                replaced_return_pct = replaced_gain_total / replaced_invested * 100 if replaced_invested > 0 else 0

                entity_row = {
                    "owner_name": owner_name,
                    "segment": classify(owner_name),
                    "origin": origin,
                    "forecast_year": int(fcast_yr),
                    "horizon": horizon,
                    "n_parcels": n,
                    "total_invested": round(total_invested, 0),
                    # Actual
                    "actual_return_pct": round(actual_return_pct, 2),
                    "actual_gain": round(actual_gain, 0),
                    # Filtered (keep top-half only)
                    "filtered_return_pct": round(filtered_return_pct, 2),
                    "filtered_gain": round(filtered_gain, 0),
                    "uplift_filtered_pp": round(filtered_return_pct - actual_return_pct, 2),
                    # Replaced (swap bottom-half with model picks)
                    "replaced_return_pct": round(replaced_return_pct, 2),
                    "replaced_gain": round(replaced_gain_total, 0),
                    "uplift_replaced_pp": round(replaced_return_pct - actual_return_pct, 2),
                    "n_replaced": n_replaced,
                    # Value-add
                    "value_add_filtered": round(filtered_gain - actual_gain * (filtered_invested / total_invested), 0),
                    "value_add_replaced": round(replaced_gain_total - actual_gain, 0),
                }
                all_entity_rows.append(entity_row)
                entity_count += 1

            print(f"  {entity_count} entities processed for +{horizon}yr")

    # ═══════════════════════════════════════════════
    # 4. Save results
    # ═══════════════════════════════════════════════
    print(f"\n[{ts()}] Total entity-horizon rows: {len(all_entity_rows):,}")

    out_data = {
        "entity_results": all_entity_rows,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "schema": schema,
        "origins": sorted(origins),
        "min_parcels": min_parcels,
        "n_entities": len(set(r["owner_name"] for r in all_entity_rows)),
    }

    # Save to Modal volume
    out_path = "/output/entity_portfolio_backtest.json"
    with open(out_path, "w") as f:
        json.dump(out_data, f, indent=2)
    print(f"[{ts()}] Saved to {out_path}")

    # Also save as parquet for analysis
    if all_entity_rows:
        edf = pd.DataFrame(all_entity_rows)
        parquet_path = "/output/entity_portfolio_backtest.parquet"
        edf.to_parquet(parquet_path, index=False)
        print(f"[{ts()}] Parquet: {parquet_path}")

        # Upload to GCS
        for fname in ["entity_portfolio_backtest.json", "entity_portfolio_backtest.parquet"]:
            blob = bucket.blob(f"entity_backtest/{fname}")
            blob.upload_from_filename(f"/output/{fname}")
            print(f"[{ts()}] → gs://{bucket_name}/entity_backtest/{fname}")

        # ── Summary ──
        print(f"\n{'='*70}")
        print(f"📋 PER-ENTITY PORTFOLIO BACKTEST SUMMARY")
        print(f"{'='*70}")
        n_unique = edf["owner_name"].nunique()
        print(f"  {n_unique} unique entities across {len(edf)} entity-horizon combos")
        print(f"\n  Median actual return:   {edf['actual_return_pct'].median():+.1f}%")
        print(f"  Median filtered return: {edf['filtered_return_pct'].median():+.1f}%")
        print(f"  Median replaced return: {edf['replaced_return_pct'].median():+.1f}%")
        print(f"  Median uplift (filter): {edf['uplift_filtered_pp'].median():+.1f}pp")
        print(f"  Median uplift (replace):{edf['uplift_replaced_pp'].median():+.1f}pp")

        # Top 10 entities by value-add
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

        # Entities that would have done WORSE with model (honesty check)
        n_worse = (edf["uplift_replaced_pp"] < 0).sum()
        print(f"\n  ⚠️ {n_worse}/{len(edf)} entity-horizons model would have done WORSE ({100*n_worse/len(edf):.0f}%)")

    print(f"\n[{ts()}] ✅ Done!")
    return out_data


@app.local_entrypoint()
def main():
    print("🚀 Launching per-entity portfolio backtest on Modal...")
    result = run_entity_backtest.remote()
    n = len(result.get("entity_results", []))
    ne = result.get("n_entities", 0)
    print(f"\n✅ {n} entity-horizon rows for {ne} unique entities")
    print(f"   Results at gs://properlytic-raw-data/entity_backtest/")
