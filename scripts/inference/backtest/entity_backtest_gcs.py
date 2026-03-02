"""
Entity Backtest — Adapted for GCS Master Panel
Uses the pre-computed `owners_any_entity` flag in the HCAD master panel
instead of parsing owner zip files.

Computes:
1. Entity (LLC/Corp/Trust) portfolio valuation changes vs county-wide baseline
2. Top/bottom entity neighborhoods by appreciation
3. Total addressable value-add for sales pitch

Usage:
    python scripts/inference/entity_backtest_gcs.py
"""
import os
import sys
import tempfile
import numpy as np
import pandas as pd
from google.cloud import storage

GCS_BUCKET = "properlytic-raw-data"
PANEL_BLOB = "hcad/hcad_master_panel_2005_2025_leakage_strict_FIXEDYR_WITHGIS.parquet"

HOLD_HORIZONS = [1, 2, 3, 5]
YEARS = list(range(2015, 2026))  # Focus on recent years


def main():
    print("=" * 70)
    print("📊 ENTITY PORTFOLIO BACKTEST — GCS Master Panel")
    print("=" * 70)

    # ── 1. Download panel from GCS ──
    print("\n📦 Downloading HCAD master panel from GCS...")
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(PANEL_BLOB)
    blob.reload()
    size_gb = blob.size / 1e9
    print(f"   Size: {size_gb:.2f} GB")

    local_path = os.path.join(tempfile.gettempdir(), "hcad_panel.parquet")
    if os.path.exists(local_path) and os.path.getsize(local_path) > 2e9:
        print(f"   Using cached: {local_path}")
    else:
        print(f"   Downloading to {local_path}...")
        blob.download_to_filename(local_path)
    print("   ✅ Downloaded")

    # ── 2. Load relevant columns ──
    print("\n📊 Loading panel data...")
    cols = ["acct", "yr", "tot_appr_val", "owners_any_entity",
            "Neighborhood_Code", "Neighborhood_Grp",
            "deeds_last_sale_price", "deeds_last_event_year",
            "gis_lat", "gis_lon"]
    df = pd.read_parquet(local_path, columns=cols)
    df = df[df["yr"].isin(YEARS)]
    print(f"   Loaded: {len(df):,} rows, years {df['yr'].min()}-{df['yr'].max()}")
    print(f"   Entity parcels: {df['owners_any_entity'].sum():,} / {len(df):,}")

    # ── 3. Build valuation time series ──
    print("\n📈 Building valuation time series...")
    # Pivot: acct → year → tot_appr_val
    vals = df[["acct", "yr", "tot_appr_val"]].dropna()
    vals = vals[vals["tot_appr_val"] > 0]

    # Identify entity parcels (any year with entity=True)
    entity_accts = set(
        df[df["owners_any_entity"] == True]["acct"].unique()
    )
    print(f"   Entity accounts (ever): {len(entity_accts):,}")

    # ── 4. Compute returns per hold horizon ──
    print("\n💰 Computing returns per hold horizon...")
    all_entity_returns = []
    all_county_returns = []

    for horizon in HOLD_HORIZONS:
        # Join current year values with future year values
        current = vals.rename(columns={"tot_appr_val": "buy_val"})
        future = vals.copy()
        future["yr"] = future["yr"] - horizon  # shift back to match on buy year
        future = future.rename(columns={"tot_appr_val": "future_val"})

        paired = current.merge(future[["acct", "yr", "future_val"]], on=["acct", "yr"], how="inner")
        paired = paired[(paired["buy_val"] > 0) & (paired["future_val"] > 0)]
        paired["return_pct"] = (paired["future_val"] - paired["buy_val"]) / paired["buy_val"] * 100
        paired["is_entity"] = paired["acct"].isin(entity_accts)

        # Entity returns
        ent = paired[paired["is_entity"]]
        non_ent = paired[~paired["is_entity"]]

        ent_median = ent["return_pct"].median()
        ent_mean = ent["return_pct"].mean()
        ent_total_buy = ent["buy_val"].sum()
        ent_total_future = ent["future_val"].sum()
        ent_weighted = (ent_total_future - ent_total_buy) / ent_total_buy * 100

        # County-wide baseline
        county_median = paired["return_pct"].median()
        county_mean = paired["return_pct"].mean()
        county_p75 = paired["return_pct"].quantile(0.75)
        county_p90 = paired["return_pct"].quantile(0.90)

        alpha = ent_weighted - county_median
        potential_uplift = county_p75 - ent_weighted
        missed_dollars = ent_total_buy * max(0, potential_uplift) / 100

        indicator = "✅" if alpha > 0 else "⚠️"
        print(f"\n  ── {horizon}-YEAR HOLD ──")
        print(f"  Entity weighted return: {ent_weighted:+.1f}%")
        print(f"  County median: {county_median:+.1f}% | P75: {county_p75:+.1f}% | P90: {county_p90:+.1f}%")
        print(f"  Entity vs market alpha: {alpha:+.1f}% {indicator}")
        print(f"  Forecast uplift (to P75): {potential_uplift:+.1f}%")
        print(f"  💰 Missed value (P75 uplift): ${missed_dollars:,.0f}")
        print(f"  Entity data points: {len(ent):,} | County: {len(paired):,}")

        all_entity_returns.append({
            "horizon": horizon,
            "entity_return": ent_weighted,
            "county_median": county_median,
            "county_p75": county_p75,
            "alpha": alpha,
            "potential_uplift": potential_uplift,
            "missed_dollars": missed_dollars,
            "entity_total_invested": ent_total_buy,
            "n_entity": len(ent),
            "n_county": len(paired),
        })

    # ── 5. Neighborhood-level analysis ──
    print("\n\n" + "=" * 70)
    print("🏘️  NEIGHBORHOOD-LEVEL ENTITY ANALYSIS")
    print("=" * 70)

    # Focus on 3-year horizon
    horizon = 3
    current = vals.rename(columns={"tot_appr_val": "buy_val"})
    future = vals.copy()
    future["yr"] = future["yr"] - horizon
    future = future.rename(columns={"tot_appr_val": "future_val"})
    paired = current.merge(future[["acct", "yr", "future_val"]], on=["acct", "yr"], how="inner")
    paired = paired[(paired["buy_val"] > 0) & (paired["future_val"] > 0)]
    paired["return_pct"] = (paired["future_val"] - paired["buy_val"]) / paired["buy_val"] * 100
    paired["is_entity"] = paired["acct"].isin(entity_accts)

    # Add neighborhood info
    nbhd = df[["acct", "yr", "Neighborhood_Code", "Neighborhood_Grp"]].drop_duplicates()
    paired = paired.merge(nbhd, on=["acct", "yr"], how="left")

    # Entity parcels by neighborhood
    ent_by_nbhd = (
        paired[paired["is_entity"]]
        .groupby("Neighborhood_Code")
        .agg(
            n_entity=("acct", "count"),
            total_invested=("buy_val", "sum"),
            median_return=("return_pct", "median"),
            mean_return=("return_pct", "mean"),
        )
        .reset_index()
    )
    ent_by_nbhd = ent_by_nbhd[ent_by_nbhd["n_entity"] >= 20]
    ent_by_nbhd = ent_by_nbhd.sort_values("median_return", ascending=False)

    county_p75 = paired["return_pct"].quantile(0.75)
    ent_by_nbhd["uplift_to_p75"] = county_p75 - ent_by_nbhd["median_return"]
    ent_by_nbhd["missed_dollars"] = ent_by_nbhd["total_invested"] * ent_by_nbhd["uplift_to_p75"].clip(lower=0) / 100

    print(f"\n  County P75 target: {county_p75:+.1f}% (3yr)")
    print(f"  Neighborhoods with ≥20 entity parcels: {len(ent_by_nbhd)}")

    print(f"\n  🏆 Top 10 entity neighborhoods (highest returns):")
    for _, row in ent_by_nbhd.head(10).iterrows():
        print(f"    {row['Neighborhood_Code']:>6s}: {row['median_return']:+6.1f}% "
              f"({row['n_entity']:>4} entities, ${row['total_invested']:>12,.0f} invested)")

    print(f"\n  📉 Bottom 10 entity neighborhoods (biggest opportunity):")
    bottom = ent_by_nbhd.sort_values("uplift_to_p75", ascending=False).head(10)
    for _, row in bottom.iterrows():
        print(f"    {row['Neighborhood_Code']:>6s}: {row['median_return']:+6.1f}% "
              f"(uplift: {row['uplift_to_p75']:+.1f}%, ${row['missed_dollars']:>10,.0f} missed, "
              f"{row['n_entity']:>4} entities)")

    # ── 6. The Pitch ──
    print("\n\n" + "=" * 70)
    print("🎯 THE PITCH — TOTAL VALUE OUR PRODUCT COULD ADD")
    print("=" * 70)

    for r in all_entity_returns:
        h = r["horizon"]
        print(f"\n  {h}-year hold horizon:")
        print(f"    Entity capital tracked: ${r['entity_total_invested']:,.0f}")
        print(f"    Current entity return: {r['entity_return']:+.1f}%")
        print(f"    County P75 target: {r['county_p75']:+.1f}%")
        print(f"    💰 POTENTIAL VALUE-ADD (P75 uplift): ${r['missed_dollars']:,.0f}")
        if r['entity_total_invested'] > 0:
            pct = r['missed_dollars'] / r['entity_total_invested'] * 100
            print(f"    📈 Uplift per dollar: {pct:.2f}%")

    # ── 7. Save results ──
    results_df = pd.DataFrame(all_entity_returns)
    results_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs", "entity_backtest_results.json")
    os.makedirs(os.path.dirname(results_path), exist_ok=True)
    results_df.to_json(results_path, orient="records", indent=2)
    print(f"\n📎 Results saved to {results_path}")

    nbhd_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs", "entity_backtest_neighborhoods.csv")
    ent_by_nbhd.to_csv(nbhd_path, index=False)
    print(f"📎 Neighborhood data saved to {nbhd_path}")

    print("\n✅ Entity backtest complete!")


if __name__ == "__main__":
    main()
