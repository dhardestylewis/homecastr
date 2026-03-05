"""
Model-vs-Entity Backtest — Using LIVE Supabase Inference Results
================================================================
Compares what entities ACTUALLY bought vs what our model WOULD HAVE
recommended, using real inference forecasts stored in Supabase.

Steps:
 1. Pull model forecasts from metrics_parcel_forecast (Supabase)
 2. Pull entity flags + valuations from HCAD master panel (GCS)
 3. For each entity purchase: what did the model forecast for that parcel?
 4. Rank all parcels by model forecast → build model's "recommended portfolio"
 5. Compare entity portfolio returns vs model portfolio returns at same budget

Usage:
    python scripts/inference/entity_backtest_model_vs.py
"""
import os, sys, tempfile, json
import numpy as np
import pandas as pd
import psycopg2

DB_URL = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"
SCHEMA = "forecast_20260220_7f31c6e4"
GCS_BUCKET = "properlytic-raw-data"
PANEL_BLOB = "hcad/hcad_master_panel_2005_2025_leakage_strict_FIXEDYR_WITHGIS.parquet"


def main():
    print("=" * 70)
    print("🎯 MODEL vs ENTITY — Live Supabase Inference Backtest")
    print("=" * 70)

    # ═══════════════════════════════════════════════
    # 1. Load model forecasts from Supabase
    # ═══════════════════════════════════════════════
    print("\n📡 Loading model forecasts from Supabase...")
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SET statement_timeout = '600000'")
    cur.execute(f"SET search_path TO {SCHEMA}, public")

    # Load forecasts per-origin directly (skip COUNT to avoid timeout)
    # Only origins 2022-2024 have actual data to compare against (panel goes to 2025)
    available_origins = [2022, 2023, 2024]
    fcast_frames = []
    for orig in available_origins:
        print(f"  Loading origin {orig} forecasts (forecast_year <= 2025)...")
        cur.execute("""
            SELECT acct, origin_year, forecast_year, p50
            FROM metrics_parcel_forecast 
            WHERE jurisdiction = 'hcad' 
            AND origin_year = %s
            AND forecast_year <= 2025
            AND p50 IS NOT NULL
        """, (orig,))
        rows = cur.fetchall()
        if rows:
            df = pd.DataFrame(rows, columns=["acct", "origin_year", "forecast_year", "p50"])
            fcast_frames.append(df)
            print(f"    ✅ {len(df):,} rows (years: {sorted(df['forecast_year'].unique())})")
        else:
            print(f"    ⚠️ No rows")
    conn.close()

    if not fcast_frames:
        print("  ⚠️ No forecast data found")
        return
    fcast_df = pd.concat(fcast_frames, ignore_index=True)
    print(f"  Total: {len(fcast_df):,} forecast rows")

    # ═══════════════════════════════════════════════
    # 2. Load master panel (valuations + entity flags)
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

    cols = ["acct", "yr", "tot_appr_val", "owners_any_entity", "Neighborhood_Code"]
    panel = pd.read_parquet(local_path, columns=cols)
    panel = panel[panel["tot_appr_val"] > 0]
    print(f"   Panel: {len(panel):,} rows, years {panel['yr'].min()}-{panel['yr'].max()}")

    # Entity accounts per year
    entity_by_year = {}
    for yr in panel["yr"].unique():
        ents = set(panel[(panel["yr"] == yr) & (panel["owners_any_entity"] == True)]["acct"])
        entity_by_year[yr] = ents
    total_ent = len(set().union(*entity_by_year.values()))
    print(f"   Entity accounts (ever): {total_ent:,}")

    # Build valuation dict: (acct, yr) → tot_appr_val
    val_dict = {}
    for _, row in panel[["acct", "yr", "tot_appr_val"]].iterrows():
        val_dict[(row["acct"], int(row["yr"]))] = row["tot_appr_val"]
    # More efficient approach
    val_lookup = panel.set_index(["acct", "yr"])["tot_appr_val"].to_dict()
    del val_dict
    print(f"   Valuation lookup: {len(val_lookup):,} entries")

    # ═══════════════════════════════════════════════
    # 3. For each origin: rank parcels by model P50 forecast
    #    and compare entity portfolio vs model's picks
    # ═══════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("📊 RESULTS: Entity Actual vs Model-Guided Selection")
    print("=" * 70)

    results = []
    for origin_year in sorted(fcast_df["origin_year"].unique()):
        origin_year = int(origin_year)
        ent_at_origin = entity_by_year.get(origin_year, set())
        if len(ent_at_origin) < 100:
            print(f"\n  ⚠️ Origin {origin_year}: only {len(ent_at_origin)} entity accounts, skipping")
            continue

        # Get forecasts for this origin
        origin_fcasts = fcast_df[fcast_df["origin_year"] == origin_year].copy()

        # For each forecast year (horizon), do the comparison
        for fcast_yr in sorted(origin_fcasts["forecast_year"].unique()):
            fcast_yr = int(fcast_yr)
            horizon = fcast_yr - origin_year
            if horizon < 1 or horizon > 5:
                continue

            # Model's P50 forecast per parcel
            yr_fcasts = origin_fcasts[origin_fcasts["forecast_year"] == fcast_yr][["acct", "p50"]].copy()
            yr_fcasts = yr_fcasts.drop_duplicates(subset=["acct"])

            # Get actual valuations at origin and forecast year
            yr_fcasts["val_origin"] = yr_fcasts["acct"].map(lambda a: val_lookup.get((a, origin_year), np.nan))
            yr_fcasts["val_actual"] = yr_fcasts["acct"].map(lambda a: val_lookup.get((a, fcast_yr), np.nan))
            yr_fcasts = yr_fcasts.dropna(subset=["val_origin", "val_actual"])
            yr_fcasts = yr_fcasts[(yr_fcasts["val_origin"] > 0) & (yr_fcasts["val_actual"] > 0)]

            # Actual return
            yr_fcasts["actual_return_pct"] = (yr_fcasts["val_actual"] - yr_fcasts["val_origin"]) / yr_fcasts["val_origin"] * 100
            yr_fcasts["is_entity"] = yr_fcasts["acct"].isin(ent_at_origin)

            n_total = len(yr_fcasts)
            n_entity = yr_fcasts["is_entity"].sum()
            if n_entity < 10 or n_total < 100:
                continue

            # ── A. Entity portfolio: what entities actually held ──
            ent = yr_fcasts[yr_fcasts["is_entity"]]
            ent_weighted_return = (ent["val_actual"].sum() - ent["val_origin"].sum()) / ent["val_origin"].sum() * 100
            ent_median_return = ent["actual_return_pct"].median()
            ent_total_invested = ent["val_origin"].sum()

            # ── B. MODEL's recommended portfolio ──
            # Rank ALL parcels by model's P50 forecast (higher = model thinks bigger growth)
            # Pick top N parcels (same count as entity holdings)
            # Constrain to same value bracket as entities (P5-P95 of entity values)
            ent_val_lo = ent["val_origin"].quantile(0.05)
            ent_val_hi = ent["val_origin"].quantile(0.95)
            comparable = yr_fcasts[
                (yr_fcasts["val_origin"] >= ent_val_lo) & 
                (yr_fcasts["val_origin"] <= ent_val_hi)
            ]

            # Model's top-N by P50 forecast
            model_picks = comparable.nlargest(n_entity, "p50")
            model_weighted_return = (model_picks["val_actual"].sum() - model_picks["val_origin"].sum()) / model_picks["val_origin"].sum() * 100
            model_median_return = model_picks["actual_return_pct"].median()
            model_total_invested = model_picks["val_origin"].sum()

            # ── C. Random baseline ──
            random_returns = []
            for _ in range(30):
                sample = comparable.sample(n=min(n_entity, len(comparable)), replace=False)
                r = (sample["val_actual"].sum() - sample["val_origin"].sum()) / sample["val_origin"].sum() * 100
                random_returns.append(r)
            random_return = np.median(random_returns)

            # ── D. Screening efficiency ──
            # What quartile of the model's ranking do entity purchases fall in?
            yr_fcasts["model_rank"] = yr_fcasts["p50"].rank(pct=True)
            ent_ranks = yr_fcasts[yr_fcasts["is_entity"]]["model_rank"]
            q1 = (ent_ranks < 0.25).mean() * 100  # bottom quartile
            q2 = ((ent_ranks >= 0.25) & (ent_ranks < 0.50)).mean() * 100
            q3 = ((ent_ranks >= 0.50) & (ent_ranks < 0.75)).mean() * 100
            q4 = (ent_ranks >= 0.75).mean() * 100  # top quartile (model's best)

            # Value gap
            model_uplift = model_weighted_return - ent_weighted_return
            value_gap = ent_total_invested * model_uplift / 100

            # Spearman correlation (model forecast vs actual)
            from scipy.stats import spearmanr
            rho, _ = spearmanr(yr_fcasts["p50"], yr_fcasts["actual_return_pct"])

            print(f"\n  ── Origin {origin_year}, +{horizon}yr → {fcast_yr} ──")
            print(f"  Pool: {n_total:,} parcels | Entity: {n_entity:,} | Model ρ={rho:.3f}")
            print(f"  Value bracket: ${ent_val_lo/1e3:.0f}K–${ent_val_hi/1e3:.0f}K")
            print(f"")
            print(f"  {'Strategy':<30} {'Weighted':>9} {'Median':>9} {'n':>7}")
            print(f"  {'─'*30} {'─'*9} {'─'*9} {'─'*7}")
            print(f"  {'Random baseline':<30} {random_return:>+8.1f}% {'':>9} {n_entity:>7,}")
            print(f"  {'Entity actual':<30} {ent_weighted_return:>+8.1f}% {ent_median_return:>+8.1f}% {n_entity:>7,}")
            print(f"  {'Model top-N (our forecast)':<30} {model_weighted_return:>+8.1f}% {model_median_return:>+8.1f}% {n_entity:>7,}")
            print(f"")
            indicator = "✅" if model_uplift > 0 else "⚠️"
            print(f"  Model uplift: {model_uplift:+.1f}pp → ${value_gap:,.0f} {indicator}")
            print(f"  📊 Screening: Q1={q1:.0f}% Q2={q2:.0f}% Q3={q3:.0f}% Q4(best)={q4:.0f}%")
            if q4 > 25:
                print(f"     → Entities already cluster in model's top quartile ({q4:.0f}% vs 25% expected)")

            results.append({
                "origin": origin_year, "horizon": horizon,
                "n_entity": int(n_entity), "entity_invested": float(ent_total_invested),
                "entity_return": round(ent_weighted_return, 2),
                "model_return": round(model_weighted_return, 2),
                "random_return": round(random_return, 2),
                "model_uplift_pp": round(model_uplift, 2),
                "value_gap": round(value_gap, 0),
                "rho": round(rho, 3),
                "screening_q1": round(q1, 1), "screening_q2": round(q2, 1),
                "screening_q3": round(q3, 1), "screening_q4": round(q4, 1),
            })

    # ═══════════════════════════════════════════════
    # 4. Summary
    # ═══════════════════════════════════════════════
    if not results:
        print("\n⚠️ No results produced. Check data availability.")
        return

    rdf = pd.DataFrame(results)
    print("\n\n" + "=" * 70)
    print("📋 SUMMARY — Model vs Entity Across All Scenarios")
    print("=" * 70)
    print(f"\n{'Orig':>5} {'H':>2} | {'Entity':>8} {'Model':>8} {'Random':>8} | {'Uplift':>8} {'$ Gap':>14} | {'ρ':>5} {'Q4%':>4}")
    print("-" * 80)
    for _, r in rdf.iterrows():
        print(f"{int(r['origin']):>5} {int(r['horizon']):>2}yr | "
              f"{r['entity_return']:>+7.1f}% {r['model_return']:>+7.1f}% {r['random_return']:>+7.1f}% | "
              f"{r['model_uplift_pp']:>+7.1f}pp ${r['value_gap']:>13,.0f} | "
              f"{r['rho']:>5.3f} {r['screening_q4']:>3.0f}%")

    # ── THE PITCH ──
    print("\n\n" + "=" * 70)
    print("🎯 THE PITCH")
    print("=" * 70)
    avg_uplift = rdf["model_uplift_pp"].mean()
    total_gap = rdf["value_gap"].sum()
    avg_rho = rdf["rho"].mean()
    n_wins = (rdf["model_uplift_pp"] > 0).sum()
    n_total = len(rdf)
    print(f"\n  Model beats entity selections: {n_wins}/{n_total} scenarios")
    print(f"  Average model uplift: {avg_uplift:+.1f}pp")
    print(f"  Average forecast rank correlation (ρ): {avg_rho:.3f}")
    print(f"  Total value-add across all scenarios: ${total_gap:,.0f}")
    print(f"\n  'If Houston's institutional investors had used Properlytic's")
    print(f"   forecasts to guide their property selection, they would have")
    print(f"   earned {avg_uplift:+.1f}pp more on average per scenario.'")

    # Save
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "..", "logs", "entity_backtest_model_vs.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    rdf.to_json(out_path, orient="records", indent=2)
    print(f"\n📎 Saved to {out_path}")
    print("\n✅ Model-vs-Entity backtest complete!")


if __name__ == "__main__":
    main()
