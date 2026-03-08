"""
Scaled Multi-Country Panel — Fast Pipeline
=============================================
Maximizes data volume and country coverage. No slow OSM.
Pulls real price values from every working API.

Strategy:
  1. France DVF: ALL departments (5000+ transactions with lat/lon)
  2. Eurostat HPI: Parse actual TSV values (27+ EU countries, quarterly)
  3. Austria Statistik: Parse CSV (monthly property price index)
  4. Norway SSB: Quarterly dwelling prices via JSON-stat
  5. Netherlands CBS: Avg sale prices via OData
  6. Spain INE: Regional HPI with actual values
  7. BIS HPI: 40+ countries via FRED (quarterly since 2015)
  8. STAC tiles: Quick lookup (no rasterio), batch

No OSM Overpass. All enrichment via fast APIs.
Target: 3000+ rows, 30+ countries, model-ready.
"""
import requests, json, time, csv, io, gzip, os, math, random
import pandas as pd
import numpy as np

random.seed(42)
OUT_DIR = os.path.dirname(__file__)
HEADERS = {"User-Agent": "Mozilla/5.0 Properlytic/6.0"}

def get(url, timeout=25, **kw):
    try:
        h = kw.pop("headers", HEADERS)
        return requests.get(url, timeout=timeout, headers=h, allow_redirects=True, **kw)
    except: return None

def post(url, timeout=20, **kw):
    try:
        h = kw.pop("headers", HEADERS)
        return requests.post(url, timeout=timeout, headers=h, **kw)
    except: return None


# ═══════════════════════════════════════════════════════════════
# 1. FRANCE DVF — All major departments
# ═══════════════════════════════════════════════════════════════
def pull_france_full(target_per_dept=200, max_total=3000):
    print("\n[1] FRANCE DVF — Full Sweep")
    rows = []
    depts = [
        ("75","Paris"),("69","Lyon/Rhône"),("13","Marseille/BdR"),
        ("31","Toulouse/HG"),("33","Bordeaux/Gironde"),("06","Nice/AM"),
        ("59","Lille/Nord"),("44","Nantes/LA"),("67","Strasbourg/BR"),
        ("34","Montpellier/Hérault"),("35","Rennes/IV"),("21","Dijon/CO"),
        ("38","Grenoble/Isère"),("76","Rouen/SM"),("57","Metz/Moselle"),
        ("54","Nancy/MM"),("42","St-Étienne/Loire"),("29","Brest/Finistère"),
        ("37","Tours/IL"),("63","Clermont/PdD"),("14","Caen/Calvados"),
        ("971","Guadeloupe"),("972","Martinique"),("974","Réunion"),
    ]
    for dept, name in depts:
        if len(rows) >= max_total: break
        for year in ["2023","2022"]:
            url = f"https://files.data.gouv.fr/geo-dvf/latest/csv/{year}/departements/{dept}.csv.gz"
            r = get(url, timeout=40, stream=True)
            if not r or r.status_code != 200: continue
            try:
                content = b""
                for chunk in r.iter_content(256*1024):
                    content += chunk
                    if len(content) > 3*1024*1024: break
                try: text = gzip.decompress(content).decode("utf-8",errors="replace")
                except: text = content.decode("utf-8",errors="replace")
                reader = csv.DictReader(io.StringIO(text.replace("\r\n","\n")))
                n = 0
                for rec in reader:
                    try:
                        price = float(rec.get("valeur_fonciere","").replace(",","."))
                        lat = float(rec.get("latitude","0"))
                        lon = float(rec.get("longitude","0"))
                        if price<=0 or price>10e6 or lat==0: continue
                        lt = rec.get("type_local","")
                        if lt and lt not in ["Maison","Appartement"]: continue
                        area = rec.get("surface_reelle_bati","")
                        rooms = rec.get("nombre_pieces_principales","")
                        terrain = rec.get("surface_terrain","")
                        date = rec.get("date_mutation","")
                        rows.append({
                            "lat":round(lat,6),"lon":round(lon,6),
                            "price":price,"currency":"EUR",
                            "area_m2":float(area) if area else None,
                            "n_rooms":int(rooms) if rooms and rooms.isdigit() else None,
                            "terrain_m2":float(terrain) if terrain else None,
                            "property_type":lt,
                            "yr":int(date[:4]) if date else int(year),
                            "city":rec.get("nom_commune",name),
                            "dept":dept,
                            "country":"France","iso":"FR","source":"france_dvf",
                        })
                        n += 1
                    except: continue
                    if n >= target_per_dept: break
                if n > 0:
                    print(f"  {name}: {n} txns (total: {len(rows)})")
                    break
            except: pass
    print(f"  FRANCE TOTAL: {len(rows)} transactions")
    return rows


# ═══════════════════════════════════════════════════════════════
# 2. EUROSTAT HPI — Parse actual TSV values
# ═══════════════════════════════════════════════════════════════
def pull_eurostat_hpi():
    print("\n[2] EUROSTAT HPI — All EU Countries")
    rows = []
    url = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/prc_hpi_q?format=TSV&startPeriod=2015-Q1"
    r = get(url, timeout=25)
    if not r or r.status_code != 200:
        print(f"  FAIL: {r.status_code if r else 'timeout'}")
        return rows

    lines = r.text.strip().split("\n")
    header = lines[0].split("\t")
    # Columns are time periods like "2015-Q1", "2015-Q2", etc.
    time_cols = header[1:]  # Skip first col (dimensions)

    iso_map = {"AT":"Austria","BE":"Belgium","BG":"Bulgaria","CY":"Cyprus",
               "CZ":"Czechia","DE":"Germany","DK":"Denmark","EE":"Estonia",
               "EL":"Greece","ES":"Spain","FI":"Finland","FR":"France",
               "HR":"Croatia","HU":"Hungary","IE":"Ireland","IT":"Italy",
               "LT":"Lithuania","LU":"Luxembourg","LV":"Latvia","MT":"Malta",
               "NL":"Netherlands","PL":"Poland","PT":"Portugal","RO":"Romania",
               "SE":"Sweden","SI":"Slovenia","SK":"Slovakia","NO":"Norway",
               "UK":"United Kingdom","IS":"Iceland","CH":"Switzerland","TR":"Turkey"}

    for line in lines[1:]:
        parts = line.split("\t")
        if len(parts) < 2: continue
        dims = parts[0]
        # Parse dimension: freq,unit,purchase,geo
        dim_parts = dims.split(",")
        if len(dim_parts) < 4: continue
        geo = dim_parts[-1].strip()
        unit = dim_parts[1].strip() if len(dim_parts) > 1 else ""
        purchase = dim_parts[2].strip() if len(dim_parts) > 2 else ""
        # Only total purchases, index_2015=100
        if purchase not in ["TOTAL",""]: continue
        if unit not in ["I15_NSA","I15_SA","INX_Q",""]: continue

        country = iso_map.get(geo, "")
        if not country: continue

        for i, val_str in enumerate(parts[1:]):
            val_str = val_str.strip().replace(" ","")
            if not val_str or val_str in [":",".","..","na"]: continue
            # Remove flags like "p", "e", "b"
            val_clean = ""
            for ch in val_str:
                if ch.isdigit() or ch == ".": val_clean += ch
            if not val_clean: continue
            try:
                val = float(val_clean)
                if val <= 0: continue
                period = time_cols[i].strip() if i < len(time_cols) else ""
                yr = int(period[:4]) if period and period[:4].isdigit() else None
                rows.append({
                    "price":val, "currency":"index_2015",
                    "country":country, "iso":geo, "source":"eurostat_hpi",
                    "yr":yr, "quarter":period, "property_type":"all_dwellings",
                    "unit":unit,
                })
            except: continue

    # Deduplicate (keep one unit per country-quarter)
    seen = set()
    deduped = []
    for r in rows:
        key = f"{r['iso']}_{r.get('quarter','')}_{r.get('unit','')}"
        if key not in seen:
            seen.add(key)
            deduped.append(r)
    rows = deduped

    countries = set(r["country"] for r in rows)
    print(f"  EUROSTAT: {len(rows)} observations across {len(countries)} countries")
    return rows


# ═══════════════════════════════════════════════════════════════
# 3. AUSTRIA — Parse actual CSV values
# ═══════════════════════════════════════════════════════════════
def pull_austria():
    print("\n[3] AUSTRIA Statistik CSV")
    rows = []
    r = get("https://data.statistik.gv.at/opendata/OGD_gpi_ext_GPIMonat_1.csv", timeout=20)
    if not r or r.status_code != 200:
        print(f"  FAIL: {r.status_code if r else 'timeout'}")
        return rows
    try:
        reader = csv.DictReader(io.StringIO(r.text), delimiter=";")
        for rec in reader:
            val = None
            for k, v in rec.items():
                if v and k != list(rec.keys())[0]:
                    try:
                        val = float(v.replace(",","."))
                        break
                    except: continue
            if val and val > 0:
                period = rec.get(list(rec.keys())[0],"")
                rows.append({"price":val,"currency":"index",
                    "country":"Austria","iso":"AT","source":"at_statistik",
                    "yr":int(period[:4]) if period[:4].isdigit() else None,
                    "property_type":"all"})
    except Exception as e:
        print(f"  Parse error: {e}")
    print(f"  AUSTRIA: {len(rows)} monthly price index values")
    return rows


# ═══════════════════════════════════════════════════════════════
# 4. NORWAY SSB — Quarterly dwelling prices
# ═══════════════════════════════════════════════════════════════
def pull_norway():
    print("\n[4] NORWAY SSB")
    rows = []
    url = "https://data.ssb.no/api/v0/en/table/07241"
    query = {"query":[
        {"code":"Boligtype","selection":{"filter":"item","values":["00","02","03"]}},
        {"code":"ContentsCode","selection":{"filter":"item","values":["KvPris"]}},
        {"code":"Tid","selection":{"filter":"top","values":["40"]}}],
        "response":{"format":"json-stat2"}}
    r = post(url, json=query, timeout=20)
    if r and r.status_code == 200:
        try:
            data = r.json()
            values = data.get("value",[])
            dims = data.get("dimension",{})
            time_cat = dims.get("Tid",{}).get("category",{})
            time_labels = list(time_cat.get("label",{}).values())
            type_cat = dims.get("Boligtype",{}).get("category",{})
            type_labels = list(type_cat.get("label",{}).values())
            n_times = len(time_labels)
            n_types = len(type_labels)
            for t in range(n_types):
                for q in range(n_times):
                    idx = t * n_times + q
                    if idx < len(values) and values[idx]:
                        rows.append({"price":float(values[idx])*1000,"currency":"NOK",
                            "country":"Norway","iso":"NO","source":"no_ssb",
                            "yr":int(time_labels[q][:4]) if time_labels[q][:4].isdigit() else None,
                            "quarter":time_labels[q],"property_type":type_labels[t]})
        except Exception as e:
            print(f"  Parse error: {e}")
    print(f"  NORWAY: {len(rows)} quarterly price observations")
    return rows


# ═══════════════════════════════════════════════════════════════
# 5. NETHERLANDS CBS — Dwelling sale prices
# ═══════════════════════════════════════════════════════════════
def pull_netherlands():
    print("\n[5] NETHERLANDS CBS")
    rows = []
    url = "https://opendata.cbs.nl/ODataApi/OData/83625NED/TypedDataSet?$top=500&$format=json"
    r = get(url, timeout=20)
    if r and r.status_code == 200:
        try:
            vals = r.json().get("value",[])
            for v in vals:
                price = None
                for k in ["VerkoopprijsGemiddelde_1","GemiddeldeVerkoopprijs_1",
                           "Verkoopprijs_1","PrijsindexVerkoopprijzen_1"]:
                    if v.get(k) and float(v[k]) > 0:
                        price = float(v[k]); break
                if not price: continue
                period = v.get("Perioden","")
                region = v.get("RegioS","")
                rows.append({"price":price,"currency":"EUR",
                    "country":"Netherlands","iso":"NL","source":"nl_cbs",
                    "yr":int(period[:4]) if period and period[:4].isdigit() else None,
                    "city":region,"quarter":period})
        except: pass
    print(f"  NETHERLANDS: {len(rows)} avg price observations")
    return rows


# ═══════════════════════════════════════════════════════════════
# 6. SPAIN INE — Regional HPI
# ═══════════════════════════════════════════════════════════════
def pull_spain():
    print("\n[6] SPAIN INE")
    rows = []
    r = get("https://servicios.ine.es/wstempus/js/EN/DATOS_TABLA/25171?tip=AM", timeout=20)
    if r and r.status_code == 200:
        try:
            data = r.json()
            for item in data:
                nombre = item.get("Nombre","")
                for dato in item.get("Data",[]):
                    val = dato.get("Valor")
                    if val:
                        rows.append({"price":float(val),"currency":"EUR_per_m2",
                            "country":"Spain","iso":"ES","source":"es_ine",
                            "city":nombre,"yr":dato.get("Anyo"),
                            "quarter":str(dato.get("FK_Periodo",""))})
        except: pass
    print(f"  SPAIN: {len(rows)} regional HPI observations")
    return rows


# ═══════════════════════════════════════════════════════════════
# 7. BIS HPI via FRED — 40+ countries, quarterly since 2015
# ═══════════════════════════════════════════════════════════════
def pull_bis_hpi():
    print("\n[7] BIS HPI via FRED — 40+ Countries")
    rows = []
    countries = {
        "US":"United States","GB":"United Kingdom","FR":"France","DE":"Germany",
        "JP":"Japan","AU":"Australia","CA":"Canada","NZ":"New Zealand",
        "SG":"Singapore","KR":"South Korea","IN":"India","BR":"Brazil",
        "MX":"Mexico","ZA":"South Africa","TR":"Turkey","IL":"Israel",
        "CL":"Chile","CO":"Colombia","TH":"Thailand","MY":"Malaysia",
        "ID":"Indonesia","PH":"Philippines","PE":"Peru","CN":"China",
        "HK":"Hong Kong","TW":"Taiwan","SA":"Saudi Arabia","AE":"UAE",
        "RU":"Russia","NO":"Norway","SE":"Sweden","DK":"Denmark",
        "FI":"Finland","NL":"Netherlands","BE":"Belgium","AT":"Austria",
        "CH":"Switzerland","IE":"Ireland","PT":"Portugal","ES":"Spain",
        "IT":"Italy","PL":"Poland","CZ":"Czechia","HU":"Hungary",
        "HR":"Croatia","RO":"Romania","BG":"Bulgaria","SI":"Slovenia",
        "SK":"Slovakia","LT":"Lithuania","LV":"Latvia","EE":"Estonia",
    }
    for iso, name in countries.items():
        sid = f"Q{iso}N628BIS"
        r = get(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}&cosd=2015-01-01", timeout=8)
        if r and r.status_code == 200 and len(r.content) > 50:
            lines = r.text.strip().split("\n")[1:]
            n = 0
            for line in lines:
                parts = line.split(",")
                if len(parts)==2 and parts[1]!=".":
                    try:
                        rows.append({"price":float(parts[1]),"currency":"index_2010",
                            "country":name,"iso":iso,"source":"bis_hpi_fred",
                            "yr":int(parts[0][:4]),"quarter":parts[0][:7]})
                        n += 1
                    except: pass
            if n > 0:
                print(f"  {name}: {n} quarters")
    print(f"  BIS TOTAL: {len(rows)} country-quarter observations, {len(set(r['iso'] for r in rows))} countries")
    return rows


# ═══════════════════════════════════════════════════════════════
# 8. STAC — Quick tile lookup (batch, no rasterio)
# ═══════════════════════════════════════════════════════════════
def enrich_stac(df, n=80):
    print(f"\n[8] STAC Tile Enrichment ({n} points)")
    df["esa_tile"] = None
    df["dem_tile"] = None
    has_ll = df["lat"].notna() & df["lon"].notna()
    if has_ll.sum() == 0: return df
    sample_idx = df[has_ll].sample(min(n, has_ll.sum()), random_state=42).index
    done = 0
    for idx in sample_idx:
        lat,lon = df.loc[idx,"lat"],df.loc[idx,"lon"]
        try:
            r = requests.post("https://planetarycomputer.microsoft.com/api/stac/v1/search",
                json={"collections":["esa-worldcover"],"intersects":{"type":"Point","coordinates":[lon,lat]},"limit":1},timeout=6)
            if r and r.status_code==200:
                feats = r.json().get("features",[])
                if feats: df.loc[idx,"esa_tile"] = feats[0].get("id","")
        except: pass
        try:
            r2 = requests.post("https://planetarycomputer.microsoft.com/api/stac/v1/search",
                json={"collections":["cop-dem-glo-30"],"intersects":{"type":"Point","coordinates":[lon,lat]},"limit":1},timeout=6)
            if r2 and r2.status_code==200:
                feats2 = r2.json().get("features",[])
                if feats2: df.loc[idx,"dem_tile"] = feats2[0].get("id","")
        except: pass
        done += 1
        if done % 20 == 0: print(f"  STAC: {done}/{n}")
    esa = df["esa_tile"].notna().sum()
    dem = df["dem_tile"].notna().sum()
    print(f"  STAC done: {esa} LULC tiles, {dem} DEM tiles")
    return df


# ═══════════════════════════════════════════════════════════════
# 9. TRAIN LIGHTGBM
# ═══════════════════════════════════════════════════════════════
def train(df):
    print("\n" + "="*70)
    print("LIGHTGBM TRAINING")
    print("="*70)
    import lightgbm as lgb
    from sklearn.model_selection import cross_val_score
    from sklearn.metrics import r2_score, mean_absolute_error

    mask = df["price"].notna() & (df["price"]>0)
    dft = df[mask].copy()
    dft["log_price"] = np.log1p(dft["price"])
    # Encode country
    dft["country_code"] = pd.Categorical(dft["country"]).codes
    # Encode source
    dft["source_code"] = pd.Categorical(dft["source"]).codes

    feat_cols = ["country_code","source_code","yr"]
    if "lat" in dft.columns and dft["lat"].notna().sum() > 50:
        feat_cols.extend(["lat","lon"])
    if "area_m2" in dft.columns and dft["area_m2"].notna().sum() > 50:
        feat_cols.append("area_m2")
    if "n_rooms" in dft.columns and dft["n_rooms"].notna().sum() > 50:
        feat_cols.append("n_rooms")
    if "terrain_m2" in dft.columns and dft["terrain_m2"].notna().sum() > 50:
        feat_cols.append("terrain_m2")

    print(f"  Rows: {len(dft)}")
    print(f"  Countries: {dft['country'].nunique()} ({', '.join(sorted(dft['country'].unique())[:20])}{'...' if dft['country'].nunique()>20 else ''})")
    print(f"  Sources: {dft['source'].nunique()} ({', '.join(sorted(dft['source'].unique()))})")
    print(f"  Features: {feat_cols}")
    print(f"  Year range: {dft['yr'].dropna().min():.0f}-{dft['yr'].dropna().max():.0f}")

    X = dft[feat_cols].fillna(0)
    y = dft["log_price"]
    params = {"n_estimators":300,"max_depth":6,"learning_rate":0.08,
              "num_leaves":31,"reg_alpha":0.1,"reg_lambda":1.0,
              "random_state":42,"verbose":-1,"n_jobs":-1}

    # A) Pooled CV
    print("\n  A) Pooled 5-fold CV (all sources)")
    model = lgb.LGBMRegressor(**params)
    cv = cross_val_score(model, X, y, cv=5, scoring="r2")
    print(f"     R2: {cv.mean():.4f} +/- {cv.std():.4f}  [{', '.join(f'{s:.3f}' for s in cv)}]")
    model.fit(X, y)
    fi = sorted(zip(feat_cols, model.feature_importances_), key=lambda x:-x[1])
    print(f"     Importance: {fi}")

    # B) France-only (per-property level)
    fr_mask = dft["source"] == "france_dvf"
    if fr_mask.sum() > 100:
        print(f"\n  B) France DVF Only ({fr_mask.sum()} rows)")
        fr_feats = [c for c in ["lat","lon","yr","area_m2","n_rooms","terrain_m2"] if c in dft.columns and dft.loc[fr_mask,c].notna().sum()>30]
        X_fr = dft.loc[fr_mask, fr_feats].fillna(0)
        y_fr = dft.loc[fr_mask, "log_price"]
        cv_fr = cross_val_score(lgb.LGBMRegressor(**params), X_fr, y_fr, cv=5, scoring="r2")
        print(f"     R2: {cv_fr.mean():.4f} +/- {cv_fr.std():.4f}  [{', '.join(f'{s:.3f}' for s in cv_fr)}]")
        m_fr = lgb.LGBMRegressor(**params)
        m_fr.fit(X_fr, y_fr)
        fi_fr = sorted(zip(fr_feats, m_fr.feature_importances_), key=lambda x:-x[1])
        print(f"     Importance: {fi_fr}")

    # C) Leave-one-country-out (for countries with 20+ rows)
    print(f"\n  C) Leave-One-Country-Out")
    loco = {}
    for c in sorted(dft["country"].unique()):
        te = dft["country"]==c
        tr = ~te
        if te.sum() < 15 or tr.sum() < 50: continue
        m = lgb.LGBMRegressor(**params)
        m.fit(dft.loc[tr,feat_cols].fillna(0), dft.loc[tr,"log_price"])
        yp = m.predict(dft.loc[te,feat_cols].fillna(0))
        r2 = r2_score(dft.loc[te,"log_price"], yp)
        mae = mean_absolute_error(np.expm1(dft.loc[te,"log_price"]), np.expm1(yp))
        loco[c] = {"r2":round(r2,4),"mae":round(mae),"n":int(te.sum())}
        print(f"     {c}: R2={r2:.3f}, MAE={mae:,.0f}, N={te.sum()}")

    return {"pooled_r2":round(cv.mean(),4),"pooled_std":round(cv.std(),4),
            "features":feat_cols,"fi":[(k,int(v)) for k,v in fi],
            "n_rows":len(dft),"n_countries":int(dft["country"].nunique()),
            "loco":loco}


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    t0 = time.time()
    print("="*70)
    print("SCALED MULTI-COUNTRY PANEL — FAST PIPELINE")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

    all_rows = []
    all_rows.extend(pull_france_full(target_per_dept=200, max_total=3000))
    all_rows.extend(pull_eurostat_hpi())
    all_rows.extend(pull_austria())
    all_rows.extend(pull_norway())
    all_rows.extend(pull_netherlands())
    all_rows.extend(pull_spain())
    all_rows.extend(pull_bis_hpi())

    df = pd.DataFrame(all_rows)
    for col in ["lat","lon","area_m2","n_rooms","terrain_m2","esa_tile","dem_tile"]:
        if col not in df.columns: df[col] = None

    print(f"\n  Total raw rows: {len(df)}")
    print(f"  Countries: {df['country'].nunique()}")

    # STAC enrichment (quick, only for rows with coords)
    df = enrich_stac(df, n=60)

    # Save
    out_csv = os.path.join(OUT_DIR, "scaled_panel.csv")
    df.to_csv(out_csv, index=False, encoding="utf-8")

    # Summary
    print("\n" + "="*70)
    print("PANEL SUMMARY")
    print("="*70)
    print(f"  Total: {len(df)} rows, {df['country'].nunique()} countries, {df['source'].nunique()} sources")
    for src, g in df.groupby("source"):
        nc = g["country"].nunique()
        has_ll = (g["lat"].notna()&g["lon"].notna()).sum()
        print(f"  {src}: {len(g)} rows, {nc} countries, {has_ll} with coords")

    # Train
    results = train(df)
    results["elapsed"] = round(time.time()-t0, 1)
    results["total_rows"] = len(df)
    results["sources"] = sorted(df["source"].unique().tolist())

    out_json = os.path.join(OUT_DIR, "scaled_model_results.json")
    with open(out_json, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n  CSV: {out_csv}")
    print(f"  Results: {out_json}")
    print(f"  Completed in {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
