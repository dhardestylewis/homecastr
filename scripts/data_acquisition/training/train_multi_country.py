"""
Multi-Country Model Training Pipeline
=======================================
Takes the panel from pull_all_jurisdictions.py,
enriches with OSM POI + raster features,
trains LightGBM with cross-country validation.
"""
import pandas as pd
import numpy as np
import json, time, os, math, random, requests

OUT_DIR = os.path.dirname(__file__)

def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    dlat = math.radians(lat2-lat1)
    dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))


def enrich_osm(df, n=80):
    """Add POI distances via Overpass API."""
    print("\n[OSM POI ENRICHMENT]")
    for col in ["dist_school","dist_hospital","dist_restaurant","dist_park","dist_transit"]:
        df[col] = np.nan

    has_ll = df["lat"].notna() & df["lon"].notna()
    if has_ll.sum() == 0:
        print("  No coordinates — skip")
        return df

    sample_idx = df[has_ll].sample(min(n, has_ll.sum()), random_state=42).index
    poi_cache = {}
    poi_types = {"school":'"amenity"="school"', "hospital":'"amenity"="hospital"',
                 "restaurant":'"amenity"="restaurant"', "park":'"leisure"="park"',
                 "transit":'"railway"="station"'}
    done = 0
    for idx in sample_idx:
        lat, lon = df.loc[idx,"lat"], df.loc[idx,"lon"]
        grid = f"{round(lat*10)}_{round(lon*10)}"
        for pn, pt in poi_types.items():
            ck = f"{grid}_{pn}"
            if ck not in poi_cache:
                bbox = f"{lat-0.02},{lon-0.02},{lat+0.02},{lon+0.02}"
                q = f'[out:json][timeout:8];node[{pt}]({bbox});out center 15;'
                try:
                    r = requests.get("https://overpass-api.de/api/interpreter",
                                    params={"data":q}, timeout=10)
                    if r and r.status_code == 200:
                        poi_cache[ck] = [(e["lat"],e["lon"]) for e in r.json().get("elements",[]) if "lat" in e]
                    else:
                        poi_cache[ck] = []
                except:
                    poi_cache[ck] = []
            pois = poi_cache[ck]
            if pois:
                df.loc[idx, f"dist_{pn}"] = min(haversine(lat,lon,p[0],p[1]) for p in pois)
        done += 1
        if done % 20 == 0:
            print(f"  OSM: {done}/{len(sample_idx)}")
            time.sleep(0.5)
    print(f"  Done: {done} points, {df[['dist_school','dist_hospital']].notna().any(axis=1).sum()} enriched")
    return df


def enrich_rasters(df, n=60):
    """Add LULC class and elevation via Planetary Computer."""
    print("\n[RASTER ENRICHMENT]")
    df["lulc_class"] = np.nan
    df["elevation_m"] = np.nan
    has_ll = df["lat"].notna() & df["lon"].notna()
    if has_ll.sum() == 0: return df
    sample_idx = df[has_ll].sample(min(n, has_ll.sum()), random_state=42).index
    done = 0
    try:
        import rasterio
        import planetary_computer as pc
    except ImportError:
        print("  rasterio/planetary_computer not available, using tile IDs only")
        for idx in sample_idx:
            lat, lon = df.loc[idx,"lat"], df.loc[idx,"lon"]
            try:
                r = requests.post("https://planetarycomputer.microsoft.com/api/stac/v1/search",
                    json={"collections":["esa-worldcover"],"intersects":{"type":"Point","coordinates":[lon,lat]},"limit":1}, timeout=8)
                if r and r.status_code == 200:
                    feats = r.json().get("features",[])
                    if feats: df.loc[idx,"lulc_class"] = -1
            except: pass
            done += 1
        print(f"  Tile-only: {done} points")
        return df

    for idx in sample_idx:
        lat, lon = df.loc[idx,"lat"], df.loc[idx,"lon"]
        # ESA WorldCover
        try:
            r = requests.post("https://planetarycomputer.microsoft.com/api/stac/v1/search",
                json={"collections":["esa-worldcover"],"intersects":{"type":"Point","coordinates":[lon,lat]},"limit":1}, timeout=8)
            if r and r.status_code == 200:
                feats = r.json().get("features",[])
                if feats:
                    href = feats[0].get("assets",{}).get("map",{}).get("href","")
                    if href:
                        signed = pc.sign(href)
                        with rasterio.open(signed) as src:
                            vals = list(src.sample([(lon,lat)]))
                            if vals and len(vals[0])>0:
                                df.loc[idx,"lulc_class"] = int(vals[0][0])
        except: pass
        # Copernicus DEM
        try:
            r2 = requests.post("https://planetarycomputer.microsoft.com/api/stac/v1/search",
                json={"collections":["cop-dem-glo-30"],"intersects":{"type":"Point","coordinates":[lon,lat]},"limit":1}, timeout=8)
            if r2 and r2.status_code == 200:
                feats2 = r2.json().get("features",[])
                if feats2:
                    href2 = feats2[0].get("assets",{}).get("data",{}).get("href","")
                    if href2:
                        signed2 = pc.sign(href2)
                        with rasterio.open(signed2) as src:
                            vals2 = list(src.sample([(lon,lat)]))
                            if vals2 and len(vals2[0])>0:
                                df.loc[idx,"elevation_m"] = float(vals2[0][0])
        except: pass
        done += 1
        if done % 15 == 0: print(f"  Raster: {done}/{len(sample_idx)}")
    print(f"  Raster done: {df['lulc_class'].notna().sum()} LULC, {df['elevation_m'].notna().sum()} elev")
    return df


def enrich_bis_hpi(df):
    """Add BIS HPI for each country."""
    print("\n[BIS HPI]")
    df["bis_hpi"] = np.nan
    for iso in df["iso"].dropna().unique():
        sid = f"Q{iso}N628BIS"
        try:
            r = requests.get(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}&cosd=2020-01-01", timeout=8)
            if r and r.status_code == 200:
                lines = r.text.strip().split("\n")
                if len(lines)>1:
                    parts = lines[-1].split(",")
                    if len(parts)==2 and parts[1]!=".":
                        df.loc[df["iso"]==iso,"bis_hpi"] = float(parts[1])
                        print(f"  {iso}: {parts[1]}")
        except: pass
    return df


def train_model(df):
    """Train LightGBM with cross-country holdout."""
    print("\n" + "="*70)
    print("LIGHTGBM CROSS-COUNTRY MODEL")
    print("="*70)
    import lightgbm as lgb
    from sklearn.model_selection import cross_val_score
    from sklearn.metrics import r2_score, mean_absolute_error

    # Only rows with price > 0 (real prices, not indices)
    mask = df["price"].notna() & (df["price"] > 0)
    if "lat" in df.columns:
        mask = mask & df["lat"].notna() & df["lon"].notna()
    dft = df[mask].copy()

    # Log price
    dft["log_price"] = np.log1p(dft["price"])

    # Feature columns
    all_feats = ["lat","lon","yr","area_m2","bis_hpi",
                 "dist_school","dist_hospital","dist_restaurant","dist_park","dist_transit",
                 "lulc_class","elevation_m"]
    avail = [c for c in all_feats if c in dft.columns and dft[c].notna().sum() > 5]
    print(f"  Trainable rows: {len(dft)}")
    print(f"  Countries: {dft['country'].nunique()} ({', '.join(sorted(dft['country'].unique()))})")
    print(f"  Features: {avail}")

    if len(dft) < 50:
        print("  Too few rows for meaningful model")
        return {"error": "insufficient data", "n_rows": len(dft)}

    X = dft[avail].copy()
    y = dft["log_price"]

    params = {"n_estimators":300,"max_depth":6,"learning_rate":0.1,
              "num_leaves":31,"reg_alpha":0.1,"reg_lambda":1.0,
              "random_state":42,"verbose":-1,"n_jobs":-1}

    # Pooled CV
    print("\n  A) Pooled 5-fold CV")
    model = lgb.LGBMRegressor(**params)
    cv = cross_val_score(model, X, y, cv=5, scoring="r2")
    print(f"     R2: {cv.mean():.4f} +/- {cv.std():.4f}")
    print(f"     Per-fold: {[round(s,3) for s in cv]}")

    model.fit(X, y)
    fi = dict(zip(avail, model.feature_importances_))
    fi_sorted = sorted(fi.items(), key=lambda x: -x[1])
    print(f"     Importance: {fi_sorted}")

    # Leave-one-country-out
    print("\n  B) Leave-One-Country-Out")
    loco = {}
    for country in sorted(dft["country"].unique()):
        tr = dft["country"] != country
        te = dft["country"] == country
        if te.sum() < 5 or tr.sum() < 20: continue
        m = lgb.LGBMRegressor(**params)
        m.fit(dft.loc[tr, avail], dft.loc[tr, "log_price"])
        yp = m.predict(dft.loc[te, avail])
        r2 = r2_score(dft.loc[te, "log_price"], yp)
        mae = mean_absolute_error(np.expm1(dft.loc[te, "log_price"]), np.expm1(yp))
        loco[country] = {"r2": round(r2, 4), "mae": round(mae), "n": int(te.sum())}
        print(f"     {country}: R2={r2:.3f}, MAE={mae:,.0f}, N={te.sum()}")

    # With country encoding
    print("\n  C) With country_code feature")
    dft["country_code"] = pd.Categorical(dft["country"]).codes
    avail_c = avail + ["country_code"]
    X_c = dft[avail_c]
    cv_c = cross_val_score(lgb.LGBMRegressor(**params), X_c, y, cv=5, scoring="r2")
    print(f"     R2: {cv_c.mean():.4f} +/- {cv_c.std():.4f}")

    return {
        "pooled_cv": {"r2_mean": round(cv.mean(),4), "r2_std": round(cv.std(),4), "features": avail},
        "loco": loco,
        "with_country": {"r2_mean": round(cv_c.mean(),4), "r2_std": round(cv_c.std(),4)},
        "n_rows": len(dft), "n_countries": int(dft["country"].nunique()),
        "feature_importance": {k: int(v) for k,v in fi_sorted},
    }


def main():
    t0 = time.time()
    print("="*70)
    print("MULTI-COUNTRY MODEL PIPELINE")
    print("="*70)

    # Load panel
    csv_path = os.path.join(OUT_DIR, "all_jurisdictions_panel.csv")
    if not os.path.exists(csv_path):
        print(f"  ERROR: {csv_path} not found")
        return
    df = pd.read_csv(csv_path)
    print(f"  Loaded {len(df)} rows")
    print(f"  Columns: {df.columns.tolist()}")
    print(f"  Countries: {df['country'].unique().tolist()}")

    # Ensure columns
    for col in ["lat","lon","yr","area_m2","bis_hpi",
                "dist_school","dist_hospital","dist_restaurant","dist_park","dist_transit",
                "lulc_class","elevation_m"]:
        if col not in df.columns: df[col] = None

    # Enrichment
    df = enrich_osm(df, n=80)
    df = enrich_rasters(df, n=60)
    df = enrich_bis_hpi(df)

    # Save enriched panel
    enriched_csv = os.path.join(OUT_DIR, "multi_country_enriched.csv")
    df.to_csv(enriched_csv, index=False)
    print(f"\n  Enriched panel: {enriched_csv} ({len(df)} rows)")

    # Panel summary
    print("\n  PANEL:")
    for c, g in df.groupby("country"):
        hp = g["price"].notna().sum()
        hl = (g["lat"].notna()&g["lon"].notna()).sum()
        print(f"    {c}: {len(g)} rows, {hp} prices, {hl} coords")

    # Train
    results = train_model(df)
    results["elapsed"] = round(time.time()-t0, 1)

    out = os.path.join(OUT_DIR, "multi_country_model.json")
    with open(out, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n  Results: {out}")
    print(f"  Completed in {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
