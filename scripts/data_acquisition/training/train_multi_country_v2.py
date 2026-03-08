"""
Multi-country model: France DVF + Taiwan + Pakistan
With temporal, spatial, and random CV.
Pulls fresh France DVF with retries, combines with sweep7 verified data.
"""
import os, json, time, warnings, csv, io
import requests
import pandas as pd
import numpy as np
from sklearn.model_selection import cross_val_score, KFold
from sklearn.preprocessing import LabelEncoder
warnings.filterwarnings('ignore')
import lightgbm as lgb

OUT = os.path.dirname(__file__)
FX = {"EUR": 1.08, "TWD": 0.031, "PKR": 0.0036}

TW_DISTRICTS = {
    '內湖區': (25.083, 121.590), '中山區': (25.064, 121.532),
    '北投區': (25.132, 121.500), '士林區': (25.093, 121.525),
    '信義區': (25.031, 121.572), '大安區': (25.026, 121.544),
    '松山區': (25.050, 121.558), '文山區': (24.989, 121.570),
    '萬華區': (25.034, 121.498), '大同區': (25.063, 121.513),
    '南港區': (25.055, 121.606), '中正區': (25.032, 121.517),
}


def pull_france_dvf(target=2000):
    """Pull France DVF per-property transactions with retry logic."""
    print(">>> Pulling France DVF...")
    rows = []
    depts = ["75","92","93","94","91","78","95","77",
             "69","13","31","33","59","44","34","06",
             "67","38","57","35","76","45","54","21"]
    
    for dept in depts:
        if len(rows) >= target:
            break
        for attempt in range(3):
            try:
                url = f"https://api.cquest.org/dvf?code_departement={dept}&nature_mutation=Vente&limit=200"
                r = requests.get(url, timeout=25)
                if r.status_code != 200:
                    continue
                data = r.json()
                results = data.get("resultats", [])
                for rec in results:
                    price = rec.get("valeur_fonciere")
                    lat = rec.get("latitude")
                    lon = rec.get("longitude")
                    area = rec.get("surface_reelle_bati")
                    date = rec.get("date_mutation", "")
                    if price and price > 10000 and lat and lon:
                        rows.append({
                            "price": float(price),
                            "price_usd": float(price) * FX["EUR"],
                            "currency": "EUR",
                            "country": "France",
                            "lat": float(lat),
                            "lon": float(lon),
                            "area_m2": float(area) if area else np.nan,
                            "property_type": rec.get("type_local", ""),
                            "city": rec.get("commune", ""),
                            "date": date,
                            "yr": int(date[:4]) if date and len(date) >= 4 else np.nan,
                            "month": int(date[5:7]) if date and len(date) >= 7 else np.nan,
                        })
                print(f"  Dept {dept}: {len(results)} → total {len(rows)}")
                break
            except requests.exceptions.Timeout:
                print(f"  Dept {dept}: timeout (attempt {attempt+1})")
                time.sleep(2)
            except Exception as e:
                print(f"  Dept {dept}: {e}")
                break
    
    print(f"  France total: {len(rows)} transactions")
    return pd.DataFrame(rows)


def load_taiwan():
    """Load Taiwan from sweep7, enrich with geocoded district centroids."""
    df = pd.read_csv(os.path.join(OUT, 'sweep7_verified.csv'))
    tw = df[df['country'] == 'Taiwan'].copy()
    tw = tw[tw['price'].notna() & (tw['price'] > 100000)]
    tw['price_usd'] = tw['price'] * FX['TWD']
    tw['currency'] = 'TWD'
    tw['dist'] = tw['district'].fillna('unknown')
    tw['lat'] = tw['dist'].map(lambda d: TW_DISTRICTS.get(d, (25.04, 121.54))[0])
    tw['lon'] = tw['dist'].map(lambda d: TW_DISTRICTS.get(d, (25.04, 121.54))[1])
    # Add jitter for within-district variation
    np.random.seed(42)
    tw['lat'] += np.random.normal(0, 0.004, len(tw))
    tw['lon'] += np.random.normal(0, 0.004, len(tw))
    tw['area_m2'] = tw['area_m2'].astype(float)
    tw['yr'] = 2024
    tw['month'] = np.random.randint(1, 13, len(tw))
    tw['date'] = tw.apply(lambda r: f"2024-{int(r['month']):02d}-01", axis=1)
    return tw


def load_pakistan():
    """Load Pakistan from sweep7, enrich with geocoded city centroids."""
    PK = {
        'Islamabad': (33.684, 73.048), 'Lahore': (31.520, 74.359),
        'Karachi': (24.861, 67.001), 'Rawalpindi': (33.565, 73.017),
        'Faisalabad': (31.450, 73.135),
    }
    df = pd.read_csv(os.path.join(OUT, 'sweep7_verified.csv'))
    pk = df[df['country'] == 'Pakistan'].copy()
    pk = pk[pk['price'].notna() & (pk['price'] > 100000)]
    pk['price_usd'] = pk['price'] * FX['PKR']
    pk['currency'] = 'PKR'
    pk['lat'] = pk['city'].map(lambda c: PK.get(c, (30, 70))[0])
    pk['lon'] = pk['city'].map(lambda c: PK.get(c, (30, 70))[1])
    np.random.seed(42)
    pk['lat'] += np.random.normal(0, 0.008, len(pk))
    pk['lon'] += np.random.normal(0, 0.008, len(pk))
    pk['yr'] = 2024
    pk['month'] = np.random.randint(1, 13, len(pk))
    pk['date'] = pk.apply(lambda r: f"2024-{int(r['month']):02d}-01", axis=1)
    return pk


def evaluate(X_train, y_train, X_test, y_test):
    m = lgb.LGBMRegressor(n_estimators=200, max_depth=6, verbose=-1,
                           num_leaves=31, learning_rate=0.08, min_child_samples=5)
    m.fit(X_train, y_train)
    preds = m.predict(X_test)
    ss_res = np.sum((y_test - preds) ** 2)
    ss_tot = np.sum((y_test - y_test.mean()) ** 2)
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0
    actual = np.expm1(y_test)
    predicted = np.expm1(preds)
    mape = np.median(np.abs(actual - predicted) / actual) * 100
    return round(r2, 3), round(mape, 1), m


def main():
    t0 = time.time()
    print("=" * 60)
    print("MULTI-COUNTRY MODEL — FRANCE + TAIWAN + PAKISTAN")
    print("=" * 60)
    
    fr = pull_france_dvf(target=2000)
    tw = load_taiwan()
    pk = load_pakistan()
    
    # Standardize columns
    common_cols = ['price', 'price_usd', 'currency', 'country', 'lat', 'lon',
                   'area_m2', 'property_type', 'city', 'yr', 'month', 'date']
    
    for col in common_cols:
        for df in [fr, tw, pk]:
            if col not in df.columns:
                df[col] = np.nan
    
    combined = pd.concat([fr[common_cols], tw[common_cols], pk[common_cols]], ignore_index=True)
    combined = combined[combined['price_usd'].notna() & (combined['price_usd'] > 100)]
    
    print(f"\n  Combined: {len(combined)} rows")
    print(f"  By country: {combined['country'].value_counts().to_dict()}")
    print(f"  Price USD: ${combined['price_usd'].min():.0f} - ${combined['price_usd'].max():.0f}")
    
    # Save panel
    combined.to_csv(os.path.join(OUT, 'multi_country_panel.csv'), index=False)
    
    # ═══ Prepare features ═══
    combined['log_price'] = np.log1p(combined['price_usd'])
    combined['area_fill'] = combined['area_m2'].fillna(combined['area_m2'].median())
    
    le_country = LabelEncoder()
    combined['country_enc'] = le_country.fit_transform(combined['country'])
    le_type = LabelEncoder()
    combined['type_enc'] = le_type.fit_transform(combined['property_type'].fillna('unknown'))
    
    feat_base = ['lat', 'lon', 'area_fill']
    feat_full = ['lat', 'lon', 'area_fill', 'country_enc', 'type_enc']
    
    y = combined['log_price'].values
    results = {}
    
    # ═══ 1. RANDOM 5-FOLD CV ═══
    print(f"\n{'='*50}")
    print("1. RANDOM 5-FOLD CV")
    print(f"{'='*50}")
    for fname, fcols in [('base (lat/lon/area)', feat_base), ('full', feat_full)]:
        X = combined[fcols].values
        m = lgb.LGBMRegressor(n_estimators=200, max_depth=6, verbose=-1, min_child_samples=5)
        s = cross_val_score(m, X, y, cv=5, scoring='r2')
        results[f'random_cv_{fname}'] = round(s.mean(), 3)
        print(f"  {fname:30s}: R2={s.mean():.3f} +/- {s.std():.3f}")
    
    # ═══ 2. PER-COUNTRY RANDOM CV ═══
    print(f"\n{'='*50}")
    print("2. PER-COUNTRY RANDOM CV")
    print(f"{'='*50}")
    per_country = {}
    for country in combined['country'].unique():
        mask = combined['country'] == country
        Xc = combined.loc[mask, feat_full].values
        yc = y[mask]
        if len(Xc) < 20:
            print(f"  {country}: too few ({len(Xc)})")
            continue
        m = lgb.LGBMRegressor(n_estimators=200, max_depth=6, verbose=-1, min_child_samples=3)
        n_cv = min(5, len(Xc) // 10)
        if n_cv < 2:
            n_cv = 2
        s = cross_val_score(m, Xc, yc, cv=n_cv, scoring='r2')
        per_country[country] = {'r2': round(s.mean(), 3), 'n': int(mask.sum())}
        print(f"  {country:12s} (n={mask.sum():5d}): R2={s.mean():.3f} +/- {s.std():.3f}")
    results['per_country_cv'] = per_country
    
    # ═══ 3. LEAVE-ONE-COUNTRY-OUT ═══
    print(f"\n{'='*50}")
    print("3. LEAVE-ONE-COUNTRY-OUT")
    print(f"{'='*50}")
    loco = {}
    X_all = combined[feat_full].values
    for test_country in combined['country'].unique():
        test = combined['country'] == test_country
        train = ~test
        if test.sum() < 10:
            continue
        r2, mape, _ = evaluate(X_all[train], y[train], X_all[test], y[test])
        loco[test_country] = {'r2': r2, 'mape': mape, 'n': int(test.sum())}
        others = '+'.join(c for c in combined['country'].unique() if c != test_country)
        print(f"  Train {others} → Test {test_country}: R2={r2:.3f}  MedAPE={mape:.1f}%")
    results['loco'] = loco
    
    # ═══ 4. SPATIAL CV — Leave-One-Region-Out (within France) ═══
    if 'France' in combined['country'].values:
        print(f"\n{'='*50}")
        print("4. FRANCE — Leave-One-Department-Out")
        print(f"{'='*50}")
        fr_mask = combined['country'] == 'France'
        fr_sub = combined[fr_mask].copy()
        
        if len(fr_sub) > 50:
            # Cluster cities into ~5 groups by lat
            fr_sub['lat_bin'] = pd.qcut(fr_sub['lat'], q=min(5, len(fr_sub)//20), labels=False, duplicates='drop')
            bins = fr_sub['lat_bin'].value_counts()
            
            fr_spatial = {}
            for test_bin in bins[bins >= 10].index:
                test = fr_sub['lat_bin'] == test_bin
                train = ~test
                if train.sum() < 10:
                    continue
                r2, mape, _ = evaluate(
                    fr_sub.loc[train, feat_full].values, fr_sub.loc[train, 'log_price'].values,
                    fr_sub.loc[test, feat_full].values, fr_sub.loc[test, 'log_price'].values)
                fr_spatial[f'region_{test_bin}'] = {'r2': r2, 'mape': mape, 'n': int(test.sum())}
                print(f"  Hold out region {test_bin} (n={test.sum():4d}): R2={r2:.3f}  MedAPE={mape:.1f}%")
            results['france_spatial'] = fr_spatial
    
    # ═══ 5. TEMPORAL CV (France has real dates) ═══
    if 'France' in combined['country'].values:
        print(f"\n{'='*50}")
        print("5. TEMPORAL CV — France (real dates)")
        print(f"{'='*50}")
        fr_sub = combined[combined['country'] == 'France'].copy()
        fr_sub = fr_sub[fr_sub['yr'].notna()]
        
        if len(fr_sub) > 50:
            # Sort by date and split 70/30
            fr_sub = fr_sub.sort_values('date')
            split = int(len(fr_sub) * 0.7)
            train_idx = fr_sub.index[:split]
            test_idx = fr_sub.index[split:]
            
            r2, mape, model = evaluate(
                combined.loc[train_idx, feat_full].values, y[train_idx],
                combined.loc[test_idx, feat_full].values, y[test_idx])
            results['france_temporal'] = {'r2': r2, 'mape': mape,
                                           'train_n': len(train_idx), 'test_n': len(test_idx)}
            print(f"  Train first 70% → Test last 30%:")
            print(f"  Train: {len(train_idx)} rows, Test: {len(test_idx)} rows")
            print(f"  R2={r2:.3f}  MedAPE={mape:.1f}%")
            
            # Feature importance from this model
            print(f"\n  Feature Importance:")
            for fn, imp in sorted(zip(feat_full, model.feature_importances_), key=lambda x: -x[1]):
                bar = "█" * int(imp / max(model.feature_importances_) * 25)
                print(f"    {fn:15s} {imp:5.0f}  {bar}")
    
    # ═══ 6. COMBINED: Train France → Test Taiwan ═══
    print(f"\n{'='*50}")
    print("6. CROSS-GEOGRAPHY TRANSFER")
    print(f"{'='*50}")
    countries = combined['country'].unique()
    for source in countries:
        for target in countries:
            if source == target:
                continue
            src = combined['country'] == source
            tgt = combined['country'] == target
            if src.sum() < 20 or tgt.sum() < 10:
                continue
            r2, mape, _ = evaluate(X_all[src], y[src], X_all[tgt], y[tgt])
            print(f"  {source:12s} → {target:12s}: R2={r2:.3f}  MedAPE={mape:.1f}%")
    
    # ═══ SUMMARY ═══
    elapsed = round(time.time() - t0, 1)
    results['elapsed'] = elapsed
    results['n_total'] = len(combined)
    results['n_countries'] = len(countries)
    
    with open(os.path.join(OUT, 'multi_country_results.json'), 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n{'='*50}")
    print(f"DONE in {elapsed:.0f}s — {len(combined)} rows, {len(countries)} countries")
    print(f"Saved: multi_country_panel.csv, multi_country_results.json")
    print(f"{'='*50}")


if __name__ == '__main__':
    main()
