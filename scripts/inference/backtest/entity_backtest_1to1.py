"""
Entity Backtest — Model as Selection Filter (Apples-to-Apples)
==============================================================
Question: If entities had used our model to FILTER their purchase
decisions, would they have achieved better returns?

Method:
 1. Take every parcel an entity actually bought
 2. Look up the model's P50 forecast for each of those parcels
 3. Split entity purchases into model quartiles (Q1=worst, Q4=best)
 4. Compare ACTUAL returns across quartiles

If Q4 entity buys outperform Q1, the model adds selection signal.

Pitch: "You bought 1,000 properties. The ones our model liked
returned +30%. The ones it didn't like returned +15%. Filter through
us and capture that 15pp difference."

Usage:
    python scripts/inference/entity_backtest_1to1.py
"""
import os, json, tempfile
import numpy as np
import pandas as pd
import psycopg2
from scipy.stats import spearmanr

DB_URL = os.environ["SUPABASE_DB_URL"]
SCHEMA = "forecast_20260220_7f31c6e4"
GCS_BUCKET = "properlytic-raw-data"
PANEL_BLOB = "hcad/hcad_master_panel_2005_2025_leakage_strict_FIXEDYR_WITHGIS.parquet"
OUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "..", "logs", "entity_backtest_1to1.json")


def main():
    print("=" * 70)
    print("🎯 MODEL AS SELECTION FILTER — Would entities do better with us?")
    print("=" * 70)

    # ═══════════════════════════════════════════════
    # 1. Load panel
    # ═══════════════════════════════════════════════
    print("\n📦 Loading HCAD master panel...")
    local_path = os.path.join(tempfile.gettempdir(), "hcad_panel.parquet")
    if not (os.path.exists(local_path) and os.path.getsize(local_path) > 2e9):
        from google.cloud import storage
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET)
        blob = bucket.blob(PANEL_BLOB)
        blob.reload()
        print(f"   Downloading {blob.size/1e9:.2f} GB...")
        blob.download_to_filename(local_path)

    cols = ["acct", "yr", "tot_appr_val", "owners_any_entity"]
    panel = pd.read_parquet(local_path, columns=cols)
    panel = panel[panel["tot_appr_val"] > 0].copy()
    print(f"   Panel: {len(panel):,} rows")

    ent_by_year = {}
    for yr in panel["yr"].unique():
        ents = set(panel[(panel["yr"] == yr) & (panel["owners_any_entity"] == True)]["acct"].unique())
        if ents:
            ent_by_year[int(yr)] = ents

    val_lookup = panel.set_index(["acct", panel["yr"].astype(int)])["tot_appr_val"].to_dict()
    print(f"   Valuations: {len(val_lookup):,}")
    del panel

    # ═══════════════════════════════════════════════
    # 2. For each origin × horizon:
    #    Get model forecast for ALL parcels (to establish quartiles)
    #    Then look at entity purchases through that lens
    # ═══════════════════════════════════════════════
    results = []
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)

    for origin in [2022, 2023, 2024]:
        ent_set = ent_by_year.get(origin, set())
        if len(ent_set) < 100:
            continue

        for fcast_yr in range(origin + 1, min(origin + 6, 2026)):
            horizon = fcast_yr - origin
            print(f"\n{'='*60}")
            print(f"Origin {origin}, +{horizon}yr → {fcast_yr} | {len(ent_set):,} entities")
            print(f"{'='*60}")

            # Fetch ALL parcels' forecasts to establish quartile boundaries
            conn = psycopg2.connect(DB_URL)
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("SET statement_timeout = '600000'")
            cur.execute(f"SET search_path TO {SCHEMA}, public")

            print(f"  Fetching all forecasts...")
            cur.execute("""
                SELECT acct, p50 FROM metrics_parcel_forecast
                WHERE jurisdiction = 'hcad'
                AND origin_year = %s AND forecast_year = %s
                AND p50 IS NOT NULL
            """, (origin, fcast_yr))
            rows = cur.fetchall()
            conn.close()
            print(f"  → {len(rows):,} total parcels with forecasts")

            if len(rows) < 1000:
                continue

            # Build df with model forecast + actual valuation + entity flag
            df = pd.DataFrame(rows, columns=["acct", "p50"])
            df["val_origin"] = df["acct"].map(lambda a: val_lookup.get((a, origin), np.nan))
            df["val_actual"] = df["acct"].map(lambda a: val_lookup.get((a, fcast_yr), np.nan))
            df = df.dropna()
            df = df[(df["val_origin"] > 0) & (df["val_actual"] > 0)]
            df["actual_return"] = (df["val_actual"] - df["val_origin"]) / df["val_origin"] * 100
            df["is_entity"] = df["acct"].isin(ent_set)

            n_total = len(df)
            n_ent = df["is_entity"].sum()
            print(f"  Pool: {n_total:,} parcels | Entity: {n_ent:,}")

            if n_ent < 50:
                continue

            # ── Establish model quartiles across ALL parcels ──
            df["model_quartile"] = pd.qcut(df["p50"], 4, labels=["Q1_worst", "Q2", "Q3", "Q4_best"])

            # ── Entity purchases stratified by model quartile ──
            ent_df = df[df["is_entity"]]
            
            print(f"\n  {'Quartile':<12} {'n':>7} {'Median':>9} {'Weighted':>9} {'Invested':>14}")
            print(f"  {'─'*12} {'─'*7} {'─'*9} {'─'*9} {'─'*14}")
            
            quartile_results = {}
            for q in ["Q1_worst", "Q2", "Q3", "Q4_best"]:
                eq = ent_df[ent_df["model_quartile"] == q]
                if len(eq) < 5:
                    continue
                med = eq["actual_return"].median()
                wt = (eq["val_actual"].sum() - eq["val_origin"].sum()) / eq["val_origin"].sum() * 100
                inv = eq["val_origin"].sum()
                quartile_results[q] = {"n": len(eq), "median": med, "weighted": wt, "invested": inv}
                print(f"  {q:<12} {len(eq):>7,} {med:>+8.1f}% {wt:>+8.1f}% ${inv:>12,.0f}")

            # ── All entities combined ──
            all_med = ent_df["actual_return"].median()
            all_wt = (ent_df["val_actual"].sum() - ent_df["val_origin"].sum()) / ent_df["val_origin"].sum() * 100
            print(f"  {'ALL ENTITY':<12} {n_ent:>7,} {all_med:>+8.1f}% {all_wt:>+8.1f}%")

            # ── The key metric: Q4 - Q1 spread ──
            q4 = quartile_results.get("Q4_best", {})
            q1 = quartile_results.get("Q1_worst", {})
            if q4 and q1:
                spread = q4["weighted"] - q1["weighted"]
                print(f"\n  📊 MODEL SELECTION VALUE:")
                print(f"     Q4 (model liked) return: {q4['weighted']:+.1f}%")
                print(f"     Q1 (model disliked) return: {q1['weighted']:+.1f}%")
                indicator = "✅" if spread > 0 else "⚠️"
                print(f"     Spread (Q4-Q1): {spread:+.1f}pp {indicator}")
                print(f"     → If entity only bought model-top-half: {(quartile_results.get('Q3',{}).get('weighted',0)+q4['weighted'])/2:+.1f}%")

            # ── Spearman between model forecast and actual return on entity parcels ──
            rho, pval = spearmanr(ent_df["p50"], ent_df["actual_return"])
            print(f"  ρ(model, actual) on entity parcels: {rho:.3f} (p={pval:.2e})")

            # ── Counterfactual: had entity filtered to model top-half ──
            top_half = ent_df[ent_df["model_quartile"].isin(["Q3", "Q4_best"])]
            if len(top_half) > 10:
                cf_wt = (top_half["val_actual"].sum() - top_half["val_origin"].sum()) / top_half["val_origin"].sum() * 100
                value_add = top_half["val_origin"].sum() * (cf_wt - all_wt) / 100
                print(f"\n  🎯 COUNTERFACTUAL:")
                print(f"     Entity actual (all buys): {all_wt:+.1f}%")
                print(f"     Had they filtered to model top-half: {cf_wt:+.1f}%")
                uplift = cf_wt - all_wt
                print(f"     Additional return: {uplift:+.1f}pp → ${value_add:,.0f}")

            row = {
                "origin": origin, "horizon": horizon,
                "n_entity": int(n_ent),
                "entity_return_all": round(all_wt, 2),
                "entity_return_q1": round(q1.get("weighted", 0), 2),
                "entity_return_q4": round(q4.get("weighted", 0), 2),
                "q4_q1_spread": round(q4.get("weighted", 0) - q1.get("weighted", 0), 2),
                "rho_entity": round(rho, 3),
                "n_q1": q1.get("n", 0), "n_q4": q4.get("n", 0),
            }
            if len(top_half) > 10:
                row["counterfactual_top_half"] = round(cf_wt, 2)
                row["uplift_pp"] = round(cf_wt - all_wt, 2)
                row["value_add"] = round(value_add, 0)
            results.append(row)

            with open(OUT_PATH, "w") as f:
                json.dump(results, f, indent=2)
            print(f"  💾 Saved ({len(results)} scenarios)")

    # ═══════════════════════════════════════════════
    # Summary
    # ═══════════════════════════════════════════════
    if not results:
        print("\n⚠️ No results")
        return

    rdf = pd.DataFrame(results)
    print(f"\n\n{'='*70}")
    print("📋 SELECTION FILTER SUMMARY")
    print(f"{'='*70}")
    print(f"\n{'Orig':>5} {'H':>3} | {'All':>8} {'Q1':>8} {'Q4':>8} {'Spread':>8} | {'TopHalf':>8} {'Uplift':>8} {'$':>14}")
    print("-" * 80)
    for _, r in rdf.iterrows():
        print(f"{int(r['origin']):>5} {int(r['horizon']):>2}yr | "
              f"{r['entity_return_all']:>+7.1f}% {r['entity_return_q1']:>+7.1f}% {r['entity_return_q4']:>+7.1f}% {r['q4_q1_spread']:>+7.1f}pp | "
              f"{r.get('counterfactual_top_half', 0):>+7.1f}% {r.get('uplift_pp', 0):>+7.1f}pp ${r.get('value_add', 0):>13,.0f}")

    avg_spread = rdf["q4_q1_spread"].mean()
    avg_uplift = rdf.get("uplift_pp", pd.Series([0])).mean()
    total_value = rdf.get("value_add", pd.Series([0])).sum()
    print(f"\n🎯 Avg Q4-Q1 spread: {avg_spread:+.1f}pp")
    print(f"   Avg top-half uplift: {avg_uplift:+.1f}pp")
    print(f"   Total value-add: ${total_value:,.0f}")
    print(f"\n   PITCH: 'Among properties you actually bought, the ones our model")
    print(f"    ranked highest returned {avg_spread:+.1f}pp more than the ones it ranked")
    print(f"    lowest. If you had filtered your purchases through our forecasts,")
    print(f"    you would have earned an additional ${total_value:,.0f}.'")
    print(f"\n✅ Done!")


if __name__ == "__main__":
    main()
