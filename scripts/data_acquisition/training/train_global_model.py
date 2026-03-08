"""
Train a cross-country property price model on verified data:
  - France DVF: ~3,000 per-property transactions (EUR, lat/lon, area)
  - Taiwan Real Price: ~300 Taipei transactions (TWD, district, area)
  - Pakistan Zameen: ~200 listings (PKR, city, type)

Steps:
  1. Pull fresh France DVF data
  2. Load sweep7_verified.csv (Taiwan + Pakistan)
  3. Normalize all prices to USD
  4. Engineer features (area, country encoding, property type)
  5. Train LightGBM with pooled CV and leave-one-country-out
"""
import os, json, time, io, csv, warnings
import requests
import pandas as pd
import numpy as np
from sklearn.model_selection import KFold, cross_val_score
from sklearn.preprocessing import LabelEncoder
warnings.filterwarnings("ignore")

try:
    import lightgbm as lgb
    HAS_LGB = True
except ImportError:
    from sklearn.ensemble import GradientBoostingRegressor
    HAS_LGB = False

OUT_DIR = os.path.dirname(__file__)

# ─── Exchange rates (approximate) ───
FX = {"EUR": 1.08, "TWD": 0.031, "PKR": 0.0036, "USD": 1.0}

def pull_france_dvf(n_depts=15, max_rows=3000):
    """Pull per-property transactions from France DVF API."""
    print(">>> Pulling France DVF...")
    rows = []
    depts = ["75","92","93","94","91","78","95","77",
             "69","13","31","33","59","44","34"][:n_depts]
    for dept in depts:
        if len(rows) >= max_rows:
            break
        try:
            url = f"https://api.cquest.org/dvf?code_departement={dept}&nature_mutation=Vente&limit=300"
            r = requests.get(url, timeout=20)
            if r.status_code != 200:
                continue
            data = r.json()
            results = data.get("resultats", [])
            for rec in results:
                price = rec.get("valeur_fonciere")
                lat = rec.get("latitude")
                lon = rec.get("longitude")
                area = rec.get("surface_reelle_bati")
                if price and price > 0 and lat and lon:
                    rows.append({
                        "price_usd": float(price) * FX["EUR"],
                        "lat": float(lat),
                        "lon": float(lon),
                        "area_m2": float(area) if area else np.nan,
                        "country": "France",
                        "city": rec.get("commune",""),
                        "property_type": rec.get("type_local",""),
                    })
            print(f"  Dept {dept}: {len(results)} → total {len(rows)}")
        except Exception as e:
            print(f"  Dept {dept}: {e}")
    print(f"  France total: {len(rows)} transactions")
    return rows

def load_taiwan_pakistan():
    """Load verified sweep7 data."""
    path = os.path.join(OUT_DIR, "sweep7_verified.csv")
    if not os.path.exists(path):
        print("  sweep7_verified.csv not found!")
        return []
    
    df = pd.read_csv(path)
    rows = []
    
    # Taiwan
    tw = df[df["country"] == "Taiwan"].copy()
    tw = tw[tw["price"].notna() & (tw["price"] > 100000)]  # filter noise
    for _, r in tw.iterrows():
        rows.append({
            "price_usd": float(r["price"]) * FX["TWD"],
            "lat": np.nan,  # no coords
            "lon": np.nan,
            "area_m2": float(r["area_m2"]) if pd.notna(r.get("area_m2")) else np.nan,
            "country": "Taiwan",
            "city": str(r.get("district","")) or str(r.get("city","")),
            "property_type": str(r.get("property_type","")) if pd.notna(r.get("property_type")) else "",
        })
    print(f"  Taiwan: {len([r for r in rows if r['country']=='Taiwan'])} transactions")
    
    # Pakistan
    pk = df[df["country"] == "Pakistan"].copy()
    pk = pk[pk["price"].notna() & (pk["price"] > 100000)]  # filter noise
    for _, r in pk.iterrows():
        rows.append({
            "price_usd": float(r["price"]) * FX["PKR"],
            "lat": np.nan,
            "lon": np.nan,
            "area_m2": float(r["area_m2"]) if pd.notna(r.get("area_m2")) else np.nan,
            "country": "Pakistan",
            "city": str(r.get("city","")) if pd.notna(r.get("city")) else "",
            "property_type": str(r.get("property_type","")) if pd.notna(r.get("property_type")) else "",
        })
    print(f"  Pakistan: {len([r for r in rows if r['country']=='Pakistan'])} listings")
    
    return rows

def train_model(df):
    """Train LightGBM/GBR with pooled CV and leave-one-country-out."""
    print("\n" + "="*60)
    print("MODEL TRAINING")
    print("="*60)
    
    # Target is log price
    df = df[df["price_usd"] > 100].copy()
    df["log_price"] = np.log1p(df["price_usd"])
    
    # Features
    # Encode country
    le_country = LabelEncoder()
    df["country_enc"] = le_country.fit_transform(df["country"])
    
    # Encode city (top N)
    top_cities = df["city"].value_counts().head(30).index
    df["city_enc"] = df["city"].apply(lambda x: top_cities.get_loc(x) if x in top_cities else -1)
    
    # Encode property type
    le_type = LabelEncoder()
    df["type_enc"] = le_type.fit_transform(df["property_type"].fillna("unknown"))
    
    # Has coordinates flag
    df["has_coords"] = (~df["lat"].isna()).astype(int)
    
    feature_cols = ["country_enc", "city_enc", "type_enc", "area_m2", "lat", "lon", "has_coords"]
    
    # Fill NAs
    df["area_m2"] = df["area_m2"].fillna(df["area_m2"].median())
    df["lat"] = df["lat"].fillna(0)
    df["lon"] = df["lon"].fillna(0)
    
    X = df[feature_cols].values
    y = df["log_price"].values
    
    print(f"\nDataset: {len(df)} rows, {len(feature_cols)} features")
    print(f"Countries: {df['country'].value_counts().to_dict()}")
    print(f"Price USD range: ${df['price_usd'].min():.0f} - ${df['price_usd'].max():.0f}")
    print(f"Median price: ${df['price_usd'].median():.0f}")
    
    # ─── Pooled 5-fold CV ───
    print("\n--- Pooled 5-Fold CV ---")
    if HAS_LGB:
        model = lgb.LGBMRegressor(n_estimators=200, max_depth=6, learning_rate=0.1,
                                   num_leaves=31, verbose=-1)
    else:
        model = GradientBoostingRegressor(n_estimators=200, max_depth=6, learning_rate=0.1)
    
    cv = KFold(n_splits=5, shuffle=True, random_state=42)
    scores = cross_val_score(model, X, y, cv=cv, scoring="r2")
    print(f"  R² per fold: {[f'{s:.3f}' for s in scores]}")
    print(f"  Mean R²: {scores.mean():.3f} ± {scores.std():.3f}")
    
    # ─── Per-country model ───
    print("\n--- Per-Country Models ---")
    country_results = {}
    for country in df["country"].unique():
        mask = df["country"] == country
        Xc = X[mask]
        yc = y[mask]
        if len(Xc) < 20:
            print(f"  {country}: {len(Xc)} rows (too few)")
            continue
        
        if HAS_LGB:
            m = lgb.LGBMRegressor(n_estimators=100, max_depth=5, verbose=-1)
        else:
            m = GradientBoostingRegressor(n_estimators=100, max_depth=5)
        
        n_splits = min(5, len(Xc) // 10) if len(Xc) >= 30 else 3
        cv2 = KFold(n_splits=max(2, n_splits), shuffle=True, random_state=42)
        sc = cross_val_score(m, Xc, yc, cv=cv2, scoring="r2")
        r2 = sc.mean()
        country_results[country] = {"n": int(mask.sum()), "r2": float(r2)}
        print(f"  {country:12s}  n={mask.sum():5d}  R²={r2:.3f}")
    
    # ─── Leave-One-Country-Out ───
    print("\n--- Leave-One-Country-Out ---")
    loco_results = {}
    countries = df["country"].unique()
    for test_country in countries:
        test_mask = df["country"] == test_country
        train_mask = ~test_mask
        
        if test_mask.sum() < 10 or train_mask.sum() < 10:
            continue
        
        X_train, y_train = X[train_mask], y[train_mask]
        X_test, y_test = X[test_mask], y[test_mask]
        
        if HAS_LGB:
            m = lgb.LGBMRegressor(n_estimators=200, max_depth=6, verbose=-1)
        else:
            m = GradientBoostingRegressor(n_estimators=200, max_depth=6)
        
        m.fit(X_train, y_train)
        preds = m.predict(X_test)
        
        ss_res = np.sum((y_test - preds) ** 2)
        ss_tot = np.sum((y_test - y_test.mean()) ** 2)
        r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0
        
        # Also compute MAPE on actual prices
        actual = np.expm1(y_test)
        predicted = np.expm1(preds)
        mape = np.median(np.abs(actual - predicted) / actual) * 100
        
        loco_results[test_country] = {"n": int(test_mask.sum()), 
                                       "r2": float(r2), 
                                       "median_ape": float(mape)}
        print(f"  Train on {'+'.join(c for c in countries if c != test_country)}")
        print(f"  Test on {test_country:12s}: n={test_mask.sum():5d}  R²={r2:.3f}  MedAPE={mape:.1f}%")
    
    # ─── Feature importance ───
    print("\n--- Feature Importance ---")
    if HAS_LGB:
        final_model = lgb.LGBMRegressor(n_estimators=200, max_depth=6, verbose=-1)
    else:
        final_model = GradientBoostingRegressor(n_estimators=200, max_depth=6)
    final_model.fit(X, y)
    importances = final_model.feature_importances_
    for fname, imp in sorted(zip(feature_cols, importances), key=lambda x: -x[1]):
        bar = "█" * int(imp / max(importances) * 30)
        print(f"  {fname:15s} {imp:6.0f}  {bar}")
    
    return {
        "pooled_cv": {"mean_r2": float(scores.mean()), "std": float(scores.std()),
                      "per_fold": [float(s) for s in scores]},
        "per_country": country_results,
        "loco": loco_results,
        "n_rows": len(df),
        "n_countries": len(countries),
        "features": feature_cols,
    }


def main():
    t0 = time.time()
    print("="*60)
    print("GLOBAL CROSS-COUNTRY MODEL — VERIFIED DATA ONLY")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # Pull data
    france_rows = pull_france_dvf()
    tw_pk_rows = load_taiwan_pakistan()
    
    all_rows = france_rows + tw_pk_rows
    df = pd.DataFrame(all_rows)
    
    print(f"\n  Combined: {len(df)} rows")
    print(f"  Countries: {df['country'].value_counts().to_dict()}")
    
    # Save combined panel
    panel_path = os.path.join(OUT_DIR, "global_panel_verified.csv")
    df.to_csv(panel_path, index=False)
    print(f"  Saved: {panel_path}")
    
    # Train
    results = train_model(df)
    
    # Save results
    results["elapsed"] = round(time.time() - t0, 1)
    results_path = os.path.join(OUT_DIR, "global_model_results.json")
    with open(results_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n  Results: {results_path}")
    print(f"  Completed in {results['elapsed']:.1f}s")

if __name__ == "__main__":
    main()
