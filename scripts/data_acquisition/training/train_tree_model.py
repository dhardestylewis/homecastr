"""Train a tree model on the global trainable panel."""
import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import cross_val_score, train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
import json

df = pd.read_csv("scripts/data_acquisition/global_trainable_panel.csv")
print("Panel: {} rows, {} columns".format(len(df), len(df.columns)))
print("Countries:", df["country"].unique().tolist())
print("Cities:", df["city"].nunique(), "unique")
print("\nPrice stats:")
print(df["price_local"].describe())
print("\nArea stats:")
print(df["area_m2"].describe())
yr_min = df["yr"].min()
yr_max = df["yr"].max()
print("\nYear range: {}-{}".format(yr_min, yr_max))

mask_price = df["price_local"].notna() & df["lat"].notna() & df["lon"].notna()
print("Rows with price+lat+lon:", mask_price.sum())
mask_area = mask_price & df["area_m2"].notna()
print("Rows with price+area:", mask_area.sum())

# ── Model A: lat + lon + yr → price (all rows w/ coords) ──
print("\n" + "="*60)
print("MODEL A: lat + lon + yr -> log(price)")
print("="*60)
dfa = df[mask_price].copy()
dfa = dfa[(dfa["price_local"] > 10_000) & (dfa["price_local"] < 5_000_000)]
X_a = dfa[["lat", "lon", "yr"]].fillna(2023)
y_a = np.log1p(dfa["price_local"])

X_tr, X_te, y_tr, y_te = train_test_split(X_a, y_a, test_size=0.2, random_state=42)
gb = GradientBoostingRegressor(n_estimators=200, max_depth=4, learning_rate=0.1, random_state=42)
gb.fit(X_tr, y_tr)
yp = gb.predict(X_te)

r2 = r2_score(y_te, yp)
mae_log = mean_absolute_error(y_te, yp)
mae_eur = mean_absolute_error(np.expm1(y_te), np.expm1(yp))
cv = cross_val_score(gb, X_a, y_a, cv=5, scoring="r2")

print("  Train/Test: {}/{}".format(len(X_tr), len(X_te)))
print("  R2 (test): {:.4f}".format(r2))
print("  MAE (log): {:.4f}".format(mae_log))
print("  MAE (EUR): {:,.0f}".format(mae_eur))
print("  CV R2 (5-fold): {:.4f} +/- {:.4f}".format(cv.mean(), cv.std()))
fi_a = dict(zip(X_a.columns, gb.feature_importances_.round(3)))
print("  Feature importance:", fi_a)

# ── Model B: lat + lon + area + rooms → price ──
print("\n" + "="*60)
print("MODEL B: lat + lon + yr + area -> log(price)")
print("="*60)
dfb = df[mask_area].copy()
dfb = dfb[(dfb["price_local"] > 10_000) & (dfb["price_local"] < 5_000_000)]
dfb = dfb[dfb["area_m2"] > 0]

feat_cols = ["lat", "lon", "yr", "area_m2"]
dfb["n_rooms_f"] = pd.to_numeric(dfb.get("n_rooms"), errors="coerce")
if dfb["n_rooms_f"].notna().sum() > 30:
    feat_cols.append("n_rooms_f")
dfb["terrain_f"] = pd.to_numeric(dfb.get("terrain_m2"), errors="coerce")
if dfb["terrain_f"].notna().sum() > 30:
    feat_cols.append("terrain_f")

X_b = dfb[feat_cols].fillna(0)
y_b = np.log1p(dfb["price_local"])

X_tr2, X_te2, y_tr2, y_te2 = train_test_split(X_b, y_b, test_size=0.2, random_state=42)
gb2 = GradientBoostingRegressor(n_estimators=300, max_depth=5, learning_rate=0.1, random_state=42)
gb2.fit(X_tr2, y_tr2)
yp2 = gb2.predict(X_te2)

r2_b = r2_score(y_te2, yp2)
mae_b = mean_absolute_error(np.expm1(y_te2), np.expm1(yp2))
cv2 = cross_val_score(gb2, X_b, y_b, cv=5, scoring="r2")

print("  Train/Test: {}/{}".format(len(X_tr2), len(X_te2)))
print("  R2 (test): {:.4f}".format(r2_b))
print("  MAE (EUR): {:,.0f}".format(mae_b))
print("  CV R2 (5-fold): {:.4f} +/- {:.4f}".format(cv2.mean(), cv2.std()))
fi_b = dict(zip(X_b.columns, gb2.feature_importances_.round(3)))
print("  Feature importance:", fi_b)

# Save results
results = {
    "model_a": {
        "features": ["lat", "lon", "yr"],
        "n_rows": len(dfa), "r2": round(r2, 4),
        "mae_eur": round(mae_eur),
        "cv_r2_mean": round(cv.mean(), 4), "cv_r2_std": round(cv.std(), 4),
    },
    "model_b": {
        "features": feat_cols,
        "n_rows": len(dfb), "r2": round(r2_b, 4),
        "mae_eur": round(mae_b),
        "cv_r2_mean": round(cv2.mean(), 4), "cv_r2_std": round(cv2.std(), 4),
    },
}
with open("scripts/data_acquisition/model_results.json", "w") as f:
    json.dump(results, f, indent=2)
print("\nResults saved to scripts/data_acquisition/model_results.json")
