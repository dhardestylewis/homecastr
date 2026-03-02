"""
Model-vs-Entity Backtest — SQL-side aggregation
================================================
Uses live Supabase forecasts + GCS master panel.
Does heavy lifting in SQL to avoid pulling 3M+ rows per origin.

Strategy:
 1. Upload entity flags to a temp table
 2. JOIN forecasts with panel valuations and entity flags — all in SQL
 3. Pull only summary stats (~100 rows per origin/horizon)

Usage:
    python scripts/inference/entity_backtest_model_vs.py
"""
import os, sys, json, tempfile
import numpy as np
import pandas as pd
import psycopg2

DB_URL = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"
SCHEMA = "forecast_20260220_7f31c6e4"
GCS_BUCKET = "properlytic-raw-data"
PANEL_BLOB = "hcad/hcad_master_panel_2005_2025_leakage_strict_FIXEDYR_WITHGIS.parquet"
OUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "..", "logs", "entity_backtest_model_vs.json")


def main():
    print("=" * 70)
    print("🎯 MODEL vs ENTITY — Live Supabase Inference Backtest (SQL-side)")
    print("=" * 70)

    # ═══════════════════════════════════════════════
    # 1. Load master panel for entity flags + valuations
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
    print(f"   Panel: {len(panel):,} rows, years {panel['yr'].min()}-{panel['yr'].max()}")

    # Get list of entity accounts per year  
    entity_accounts_by_year = {}
    for yr in panel["yr"].unique():
        ents = set(panel[(panel["yr"] == yr) & (panel["owners_any_entity"] == True)]["acct"].unique())
        entity_accounts_by_year[int(yr)] = ents
    print(f"   Entity accts by year: {', '.join(f'{k}:{len(v):,}' for k,v in sorted(entity_accounts_by_year.items()) if len(v) > 0)}")

    # Build valuation lookup: (acct, yr) → val
    print("   Building valuation lookup...")
    val = panel[["acct", "yr", "tot_appr_val"]].copy()
    val["yr"] = val["yr"].astype(int)
    val_lookup = val.set_index(["acct", "yr"])["tot_appr_val"].to_dict()
    print(f"   Lookup: {len(val_lookup):,} entries")
    del panel  # free memory

    # ═══════════════════════════════════════════════
    # 2. Process one origin at a time using Supabase
    #    Pull ONLY the P50 forecast per (acct, forecast_year)
    #    then join locally with entity flags + valuations
    # ═══════════════════════════════════════════════
    results = []
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)

    for origin_year in [2022, 2023, 2024]:
        ent_set = entity_accounts_by_year.get(origin_year, set())
        if len(ent_set) < 100:
            print(f"\n⚠️ Origin {origin_year}: only {len(ent_set)} entities, skipping")
            continue

        print(f"\n{'='*70}")
        print(f"📊 Origin {origin_year} — {len(ent_set):,} entity accounts")
        print(f"{'='*70}")

        # Connect fresh per origin to avoid timeout issues
        conn = psycopg2.connect(DB_URL)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SET statement_timeout = '600000'")
        cur.execute(f"SET search_path TO {SCHEMA}, public")

        # Pull forecasts for each target year
        for fcast_yr in range(origin_year + 1, min(origin_year + 4, 2026)):
            horizon = fcast_yr - origin_year
            print(f"\n  ── +{horizon}yr → {fcast_yr} ──")

            # Fetch ONLY acct + p50 for this origin/forecast_year
            print(f"  Fetching forecasts (origin={origin_year}, forecast_year={fcast_yr})...")
            cur.execute("""
                SELECT acct, p50 FROM metrics_parcel_forecast
                WHERE jurisdiction = 'hcad'
                AND origin_year = %s AND forecast_year = %s
                AND p50 IS NOT NULL
            """, (origin_year, fcast_yr))
            rows = cur.fetchall()
            print(f"  → {len(rows):,} parcels with P50 forecast")

            if len(rows) < 1000:
                print(f"  ⚠️ Too few forecasts, skipping")
                continue

            # Build dataframe with forecast + actual valuations + entity flag
            fcasts = pd.DataFrame(rows, columns=["acct", "p50"])
            fcasts["val_origin"] = fcasts["acct"].map(lambda a: val_lookup.get((a, origin_year), np.nan))
            fcasts["val_actual"] = fcasts["acct"].map(lambda a: val_lookup.get((a, fcast_yr), np.nan))
            fcasts = fcasts.dropna(subset=["val_origin", "val_actual"])
            fcasts = fcasts[(fcasts["val_origin"] > 0) & (fcasts["val_actual"] > 0)]

            if len(fcasts) < 100:
                print(f"  ⚠️ Too few with both valuations")
                continue

            # Actual return
            fcasts["actual_pct"] = (fcasts["val_actual"] - fcasts["val_origin"]) / fcasts["val_origin"] * 100
            fcasts["is_entity"] = fcasts["acct"].isin(ent_set)

            n_total = len(fcasts)
            n_entity = fcasts["is_entity"].sum()
            print(f"  Pool: {n_total:,} parcels | Entity: {n_entity:,}")

            if n_entity < 10:
                continue

            # ── A. Entity portfolio ──
            ent = fcasts[fcasts["is_entity"]]
            ent_weighted = (ent["val_actual"].sum() - ent["val_origin"].sum()) / ent["val_origin"].sum() * 100
            ent_median = ent["actual_pct"].median()
            ent_invested = ent["val_origin"].sum()

            # ── B. Model's top-N (same count, same price bracket) ──
            val_lo = ent["val_origin"].quantile(0.05)
            val_hi = ent["val_origin"].quantile(0.95)
            comparable = fcasts[(fcasts["val_origin"] >= val_lo) & (fcasts["val_origin"] <= val_hi)]
            model_picks = comparable.nlargest(n_entity, "p50")
            model_weighted = (model_picks["val_actual"].sum() - model_picks["val_origin"].sum()) / model_picks["val_origin"].sum() * 100
            model_median = model_picks["actual_pct"].median()

            # ── C. Random baseline (10 draws) ──
            rng = np.random.default_rng(42)
            rand_returns = []
            for _ in range(10):
                s = comparable.sample(n=min(n_entity, len(comparable)), replace=False, random_state=rng)
                r = (s["val_actual"].sum() - s["val_origin"].sum()) / s["val_origin"].sum() * 100
                rand_returns.append(r)
            rand_return = np.median(rand_returns)

            # ── D. Screening efficiency ──
            fcasts["rank_pct"] = fcasts["p50"].rank(pct=True)
            ent_ranks = fcasts[fcasts["is_entity"]]["rank_pct"]
            q1 = (ent_ranks < 0.25).mean() * 100
            q4 = (ent_ranks >= 0.75).mean() * 100

            # ── E. Spearman ──
            from scipy.stats import spearmanr
            rho, _ = spearmanr(fcasts["p50"], fcasts["actual_pct"])

            uplift = model_weighted - ent_weighted
            gap = ent_invested * uplift / 100

            print(f"  {'Strategy':<28} {'Weighted':>9} {'Median':>9}")
            print(f"  {'─'*28} {'─'*9} {'─'*9}")
            print(f"  {'Random baseline':<28} {rand_return:>+8.1f}%")
            print(f"  {'Entity actual':<28} {ent_weighted:>+8.1f}% {ent_median:>+8.1f}%")
            print(f"  {'Model top-N (P50)':<28} {model_weighted:>+8.1f}% {model_median:>+8.1f}%")
            indicator = "✅" if uplift > 0 else "⚠️"
            print(f"  Uplift: {uplift:+.1f}pp → ${gap:,.0f} {indicator}")
            print(f"  ρ={rho:.3f} | Screening: Q1={q1:.0f}% Q4={q4:.0f}%")

            row = {
                "origin": origin_year, "horizon": horizon,
                "n_entity": int(n_entity), "entity_invested": float(ent_invested),
                "entity_return": round(ent_weighted, 2),
                "model_return": round(model_weighted, 2),
                "random_return": round(rand_return, 2),
                "uplift_pp": round(uplift, 2),
                "value_gap": round(gap, 0),
                "rho": round(rho, 3),
                "q1_pct": round(q1, 1), "q4_pct": round(q4, 1),
            }
            results.append(row)

            # ── SAVE INCREMENTALLY ──
            with open(OUT_PATH, "w") as f:
                json.dump(results, f, indent=2)
            print(f"  💾 Saved ({len(results)} scenarios so far)")

        conn.close()

    # ═══════════════════════════════════════════════
    # 3. Summary
    # ═══════════════════════════════════════════════
    if not results:
        print("\n⚠️ No results. Check forecast availability.")
        return

    rdf = pd.DataFrame(results)
    print(f"\n\n{'='*70}")
    print("📋 SUMMARY")
    print(f"{'='*70}")
    print(f"\n{'Orig':>5} {'H':>3} | {'Entity':>8} {'Model':>8} {'Rnd':>8} | {'Uplift':>8} {'$ Gap':>14} | {'ρ':>5} {'Q4%':>4}")
    print("-" * 78)
    for _, r in rdf.iterrows():
        print(f"{int(r['origin']):>5} {int(r['horizon']):>2}yr | "
              f"{r['entity_return']:>+7.1f}% {r['model_return']:>+7.1f}% {r['random_return']:>+7.1f}% | "
              f"{r['uplift_pp']:>+7.1f}pp ${r['value_gap']:>13,.0f} | "
              f"{r['rho']:>5.3f} {r['q4_pct']:>3.0f}%")

    avg_uplift = rdf["uplift_pp"].mean()
    total_gap = rdf["value_gap"].sum()
    n_wins = (rdf["uplift_pp"] > 0).sum()
    print(f"\n🎯 Model wins {n_wins}/{len(rdf)} | Avg uplift: {avg_uplift:+.1f}pp | Total gap: ${total_gap:,.0f}")
    print(f"\n✅ Done! Results: {OUT_PATH}")


if __name__ == "__main__":
    main()
