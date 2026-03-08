"""Enriched leave-one-region-out: geocode + area → retrain."""
import pandas as pd, numpy as np, json, warnings, os
warnings.filterwarnings('ignore')
import lightgbm as lgb
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import cross_val_score

TW_DISTRICTS = {
    '內湖區': (25.083, 121.590), '中山區': (25.064, 121.532),
    '北投區': (25.132, 121.500), '士林區': (25.093, 121.525),
    '信義區': (25.031, 121.572), '大安區': (25.026, 121.544),
    '松山區': (25.050, 121.558), '文山區': (24.989, 121.570),
    '萬華區': (25.034, 121.498), '大同區': (25.063, 121.513),
    '南港區': (25.055, 121.606), '中正區': (25.032, 121.517),
}
PK_CITIES = {
    'Islamabad': (33.684, 73.048), 'Lahore': (31.520, 74.359),
    'Karachi': (24.861, 67.001), 'Rawalpindi': (33.565, 73.017),
    'Faisalabad': (31.450, 73.135),
}

OUT = os.path.dirname(__file__)
df = pd.read_csv(os.path.join(OUT, 'sweep7_verified.csv'))

# Taiwan
tw = df[df['country'] == 'Taiwan'].copy()
tw = tw[tw['price'].notna() & (tw['price'] > 100000)]
tw['log_price'] = np.log1p(tw['price'])
tw['dist'] = tw['district'].fillna('unknown')
tw['lat'] = tw['dist'].map(lambda d: TW_DISTRICTS.get(d, (0, 0))[0])
tw['lon'] = tw['dist'].map(lambda d: TW_DISTRICTS.get(d, (0, 0))[1])
tw['area_fill'] = tw['area_m2'].fillna(tw['area_m2'].median())
le = LabelEncoder()
tw['dist_enc'] = le.fit_transform(tw['dist'])

# Pakistan
pk = df[df['country'] == 'Pakistan'].copy()
pk = pk[pk['price'].notna() & (pk['price'] > 100000)]
pk['log_price'] = np.log1p(pk['price'])
pk['lat'] = pk['city'].map(lambda c: PK_CITIES.get(c, (0, 0))[0])
pk['lon'] = pk['city'].map(lambda c: PK_CITIES.get(c, (0, 0))[1])
le2 = LabelEncoder()
pk['type_enc'] = le2.fit_transform(pk['property_type'].fillna('?'))

geocoded_tw = tw['lat'].gt(0).sum()
geocoded_pk = pk['lat'].gt(0).sum()
print("=== Enrichment Summary ===")
print(f"  Taiwan: {len(tw)} rows, {geocoded_tw} geocoded")
print(f"  Pakistan: {len(pk)} rows, {geocoded_pk} geocoded")

# ── TAIWAN ENRICHED ──
print("\n=== TAIWAN ENRICHED — Leave-One-District-Out ===")
feat_tw = ['lat', 'lon', 'area_fill']
districts = tw['dist'].value_counts()
results_tw_before = {}
results_tw_after = {}

for test_dist in districts[districts >= 5].index:
    test = tw['dist'] == test_dist
    train = ~test
    if train.sum() < 5:
        continue

    # BEFORE (district_enc + area only)
    m1 = lgb.LGBMRegressor(n_estimators=150, max_depth=6, verbose=-1)
    m1.fit(tw.loc[train, ['dist_enc', 'area_fill']].values, tw.loc[train, 'log_price'].values)
    p1 = m1.predict(tw.loc[test, ['dist_enc', 'area_fill']].values)
    yt = tw.loc[test, 'log_price'].values
    r2_b = 1 - np.sum((yt - p1) ** 2) / np.sum((yt - yt.mean()) ** 2)

    # AFTER (lat/lon + area)
    m2 = lgb.LGBMRegressor(n_estimators=150, max_depth=6, verbose=-1)
    m2.fit(tw.loc[train, feat_tw].values, tw.loc[train, 'log_price'].values)
    p2 = m2.predict(tw.loc[test, feat_tw].values)
    r2_a = 1 - np.sum((yt - p2) ** 2) / np.sum((yt - yt.mean()) ** 2)

    actual = np.expm1(yt)
    predicted = np.expm1(p2)
    mape = np.median(np.abs(actual - predicted) / actual) * 100

    results_tw_before[test_dist] = round(r2_b, 3)
    results_tw_after[test_dist] = {'r2': round(r2_a, 3), 'mape': round(mape, 1), 'n': int(test.sum())}
    delta = r2_a - r2_b
    arrow = "▲" if delta > 0 else "▼"
    print(f"  {test_dist} (n={test.sum():3d}): BEFORE R²={r2_b:.3f} → AFTER R²={r2_a:.3f} {arrow}{abs(delta):.3f}  MedAPE={mape:.1f}%")

avg_before = np.mean(list(results_tw_before.values()))
avg_after = np.mean([v['r2'] for v in results_tw_after.values()])
print(f"  AVERAGE: BEFORE={avg_before:.3f} → AFTER={avg_after:.3f}")

# ── PAKISTAN ENRICHED ──
print("\n=== PAKISTAN ENRICHED — Leave-One-City-Out ===")
feat_pk = ['lat', 'lon', 'type_enc']
cities = pk['city'].value_counts()
results_pk_before = {}
results_pk_after = {}

le3 = LabelEncoder()
pk['city_enc'] = le3.fit_transform(pk['city'].fillna('?'))

for test_city in cities[cities >= 5].index:
    test = pk['city'] == test_city
    train = ~test
    if train.sum() < 5:
        continue

    yt = pk.loc[test, 'log_price'].values

    # BEFORE (city_enc + type only)
    m1 = lgb.LGBMRegressor(n_estimators=150, max_depth=6, verbose=-1)
    m1.fit(pk.loc[train, ['city_enc', 'type_enc']].values, pk.loc[train, 'log_price'].values)
    p1 = m1.predict(pk.loc[test, ['city_enc', 'type_enc']].values)
    r2_b = 1 - np.sum((yt - p1) ** 2) / np.sum((yt - yt.mean()) ** 2)

    # AFTER (lat/lon + type)
    m2 = lgb.LGBMRegressor(n_estimators=150, max_depth=6, verbose=-1)
    m2.fit(pk.loc[train, feat_pk].values, pk.loc[train, 'log_price'].values)
    p2 = m2.predict(pk.loc[test, feat_pk].values)
    r2_a = 1 - np.sum((yt - p2) ** 2) / np.sum((yt - yt.mean()) ** 2)

    actual = np.expm1(yt)
    predicted = np.expm1(p2)
    mape = np.median(np.abs(actual - predicted) / actual) * 100

    results_pk_before[test_city] = round(r2_b, 3)
    results_pk_after[test_city] = {'r2': round(r2_a, 3), 'mape': round(mape, 1), 'n': int(test.sum())}
    delta = r2_a - r2_b
    arrow = "▲" if delta > 0 else "▼"
    print(f"  {test_city:15s} (n={test.sum():3d}): BEFORE R²={r2_b:.3f} → AFTER R²={r2_a:.3f} {arrow}{abs(delta):.3f}  MedAPE={mape:.1f}%")

avg_pk_b = np.mean(list(results_pk_before.values()))
avg_pk_a = np.mean([v['r2'] for v in results_pk_after.values()])
print(f"  AVERAGE: BEFORE={avg_pk_b:.3f} → AFTER={avg_pk_a:.3f}")

# ── CROSS-COUNTRY with coords ──
print("\n=== CROSS-COUNTRY ENRICHED ===")
FX = {'TWD': 0.031, 'PKR': 0.0036}
tw['price_usd'] = tw['price'] * FX['TWD']
pk['price_usd'] = pk['price'] * FX['PKR']
tw['log_usd'] = np.log1p(tw['price_usd'])
pk['log_usd'] = np.log1p(pk['price_usd'])

combined = pd.concat([
    tw[['lat', 'lon', 'area_fill', 'log_usd']].assign(country=0),
    pk[['lat', 'lon', 'log_usd']].assign(area_fill=50, country=1),
], ignore_index=True)

X = combined[['lat', 'lon', 'area_fill', 'country']].values
y = combined['log_usd'].values

m = lgb.LGBMRegressor(n_estimators=150, max_depth=6, verbose=-1)
scores = cross_val_score(m, X, y, cv=5, scoring='r2')
print(f"  Pooled CV R²: {scores.mean():.3f} ± {scores.std():.3f}")

# LOCO
tw_idx = combined['country'] == 0
pk_idx = combined['country'] == 1
m.fit(X[tw_idx], y[tw_idx])
p = m.predict(X[pk_idx])
r2_tw2pk = 1 - np.sum((y[pk_idx] - p) ** 2) / np.sum((y[pk_idx] - y[pk_idx].mean()) ** 2)

m.fit(X[pk_idx], y[pk_idx])
p = m.predict(X[tw_idx])
r2_pk2tw = 1 - np.sum((y[tw_idx] - p) ** 2) / np.sum((y[tw_idx] - y[tw_idx].mean()) ** 2)
print(f"  Train Taiwan → Test Pakistan: R²={r2_tw2pk:.3f}")
print(f"  Train Pakistan → Test Taiwan: R²={r2_pk2tw:.3f}")

# Save
results = {
    'taiwan_enriched': {k: {'before': results_tw_before[k], 'after': v} for k, v in results_tw_after.items()},
    'pakistan_enriched': {k: {'before': results_pk_before[k], 'after': v} for k, v in results_pk_after.items()},
    'taiwan_avg': {'before': round(avg_before, 3), 'after': round(avg_after, 3)},
    'pakistan_avg': {'before': round(avg_pk_b, 3), 'after': round(avg_pk_a, 3)},
    'cross_country': {
        'pooled_cv_r2': round(scores.mean(), 3),
        'tw_to_pk_r2': round(r2_tw2pk, 3),
        'pk_to_tw_r2': round(r2_pk2tw, 3),
    }
}
with open(os.path.join(OUT, 'enriched_results.json'), 'w', encoding='utf-8') as f:
    json.dump(results, f, indent=2, ensure_ascii=False)
print("\nSaved: enriched_results.json")
