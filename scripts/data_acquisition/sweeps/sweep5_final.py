"""
Sweep 5: ALL Remaining Untried Countries
==========================================
Final sweep covering every country not yet attempted.
"""
import requests, json, time, csv, io, os
import pandas as pd, numpy as np
OUT_DIR = os.path.dirname(__file__)
H = {"User-Agent": "Mozilla/5.0 Properlytic/9.0 (Research)"}
R = {}

def get(url, timeout=15, **kw):
    try:
        h = kw.pop("headers", H)
        return requests.get(url, timeout=timeout, headers=h, allow_redirects=True, **kw)
    except: return None

def post(url, timeout=15, **kw):
    try:
        h = kw.pop("headers", H)
        return requests.post(url, timeout=timeout, headers=h, **kw)
    except: return None

def log(country, src, rows, detail):
    R[f"{country}|{src}"] = {"country":country,"source":src,"n":len(rows),"detail":detail}
    print(f"  [{'PASS' if rows else 'FAIL'}] {country}: {len(rows)} — {detail}")
    return rows

# ═══════════════ NORTH AMERICA ═══════════════

def try_canada_bc():
    """Canada BC — BC Assessment open data (per-property valuations!)"""
    print("\n>>> CANADA BC — BC Assessment")
    rows = []
    # BC Assessment open data via CKAN
    apis = [
        "https://catalogue.data.gov.bc.ca/api/3/action/package_search?q=property+assessment&rows=10",
        "https://catalogue.data.gov.bc.ca/api/3/action/package_search?q=BC+Assessment&rows=10",
        "https://catalogue.data.gov.bc.ca/api/3/action/package_search?q=land+value&rows=10",
    ]
    for url in apis:
        r = get(url, timeout=20)
        if r and r.status_code == 200:
            try:
                data = r.json()
                results = data.get("result",{}).get("results",[])
                if results:
                    for pkg in results[:5]:
                        title = pkg.get("title","")
                        resources = pkg.get("resources",[])
                        # Try to download actual data
                        for res in resources[:2]:
                            dl_url = res.get("url","")
                            fmt = res.get("format","").lower()
                            if fmt in ["csv","json","geojson"] and dl_url:
                                r2 = get(dl_url, timeout=20)
                                if r2 and r2.status_code == 200 and len(r2.content) > 500:
                                    if fmt == "csv":
                                        try:
                                            reader = csv.DictReader(io.StringIO(r2.text))
                                            for rec in reader:
                                                val = rec.get("ASSESSED_VALUE") or rec.get("assessed_value") or rec.get("LAND_VALUE") or rec.get("land_value") or rec.get("TOTAL_VALUE")
                                                if val and str(val).replace(",","").replace(".","").isdigit() and float(str(val).replace(",","")) > 0:
                                                    rows.append({
                                                        "price":float(str(val).replace(",","")),"currency":"CAD",
                                                        "country":"Canada","iso":"CA","source":"ca_bc_assessment",
                                                        "city":rec.get("MUNICIPALITY","") or rec.get("municipality",""),
                                                        "area_m2":float(rec.get("LOT_SIZE",0)) if rec.get("LOT_SIZE") else None,
                                                        "property_type":rec.get("PROPERTY_CLASS","") or rec.get("property_class",""),
                                                    })
                                                if len(rows) >= 200: break
                                        except: pass
                                    elif fmt in ["json","geojson"]:
                                        try:
                                            data2 = r2.json()
                                            features = data2.get("features",[]) if isinstance(data2,dict) else []
                                            for f in features[:200]:
                                                props = f.get("properties",{})
                                                val = props.get("ASSESSED_VALUE") or props.get("LAND_VALUE")
                                                if val:
                                                    geom = f.get("geometry",{})
                                                    coords = geom.get("coordinates",[]) if geom else []
                                                    while isinstance(coords,list) and coords and isinstance(coords[0],list):
                                                        coords = coords[0]
                                                    rows.append({
                                                        "price":float(val),"currency":"CAD",
                                                        "lat":coords[1] if len(coords)>=2 else None,
                                                        "lon":coords[0] if len(coords)>=2 else None,
                                                        "country":"Canada","iso":"CA","source":"ca_bc_assessment",
                                                    })
                                        except: pass
                                    if rows: break
                        if rows: break
                    if not rows:
                        titles = [p.get("title","")[:50] for p in results[:5]]
                        return log("Canada BC","BC Assessment",[],f"Found: {'; '.join(titles)}")
            except: continue
    return log("Canada BC","BC Assessment",rows,
               f"{len(rows)} per-property valuations" if rows else "Timeout")

def try_puerto_rico():
    """Puerto Rico — CRIM property tax data"""
    print("\n>>> PUERTO RICO — CRIM")
    r = get("https://datos.pr.gov/api/3/action/package_search?q=propiedad", timeout=15)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            if results:
                return log("Puerto Rico","CRIM/datos.pr",[],f"Found {len(results)} datasets")
        except: pass
    return log("Puerto Rico","CRIM",[],"Timeout")

# ═══════════════ CARIBBEAN / CENTRAL AMERICA ═══════════════

def try_costa_rica():
    """Costa Rica — Registro Nacional property data"""
    print("\n>>> COSTA RICA — Open Data")
    r = get("https://api.datos.go.cr/api/3/action/package_search?q=catastro", timeout=15)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            if results:
                return log("Costa Rica","datos.go.cr",[],f"Found {len(results)} datasets")
        except: pass
    return log("Costa Rica","Open Data",[],"Timeout")

def try_panama():
    """Panama — Property registry"""
    print("\n>>> PANAMA — Open Data")
    r = get("https://www.datosabiertos.gob.pa/api/3/action/package_search?q=propiedad", timeout=15)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            if results:
                return log("Panama","Open Data",[],f"Found {len(results)} datasets")
        except: pass
    return log("Panama","Open Data",[],"Timeout")

def try_jamaica():
    """Jamaica — National Land Agency"""
    print("\n>>> JAMAICA — National Land Agency")
    r = get("https://data.gov.jm/api/3/action/package_search?q=land+property", timeout=15)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            if results:
                return log("Jamaica","data.gov.jm",[],f"Found {len(results)} datasets")
        except: pass
    return log("Jamaica","NLA",[],"Timeout")

def try_dominican_republic():
    """Dominican Republic"""
    print("\n>>> DOMINICAN REPUBLIC")
    r = get("https://datos.gob.do/api/3/action/package_search?q=catastro", timeout=15)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            if results:
                return log("Dominican Republic","datos.gob.do",[],f"Found {len(results)} datasets")
        except: pass
    return log("Dominican Republic","Open Data",[],"Timeout")

def try_trinidad():
    """Trinidad & Tobago"""
    print("\n>>> TRINIDAD & TOBAGO")
    r = get("https://data.gov.tt/api/3/action/package_search?q=property", timeout=15)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            if results:
                return log("Trinidad & Tobago","data.gov.tt",[],f"Found {len(results)} datasets")
        except: pass
    return log("Trinidad & Tobago","Open Data",[],"Timeout")

def try_guatemala():
    """Guatemala"""
    print("\n>>> GUATEMALA")
    r = get("https://datos.gob.gt/api/3/action/package_search?q=catastro", timeout=15)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            if results:
                return log("Guatemala","datos.gob.gt",[],f"Found {len(results)} datasets")
        except: pass
    return log("Guatemala","Open Data",[],"Timeout")

# ═══════════════ MIDDLE EAST ═══════════════

def try_kuwait():
    print("\n>>> KUWAIT")
    r = get("https://data.gov.kw/api/3/action/package_search?q=real+estate", timeout=15)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            if results:
                return log("Kuwait","data.gov.kw",[],f"Found {len(results)} datasets")
        except: pass
    return log("Kuwait","Open Data",[],"Timeout")

def try_qatar():
    print("\n>>> QATAR")
    r = get("https://www.data.gov.qa/api/datasets/1.0/search?q=real+estate", timeout=15)
    if r and r.status_code == 200:
        try:
            data = r.json()
            datasets = data.get("datasets",[])
            if datasets:
                return log("Qatar","data.gov.qa",[],f"Found {len(datasets)} datasets")
        except: pass
    return log("Qatar","Open Data",[],"Timeout")

def try_bahrain():
    print("\n>>> BAHRAIN")
    r = get("https://data.gov.bh/api/3/action/package_search?q=property", timeout=15)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            if results:
                return log("Bahrain","data.gov.bh",[],f"Found {len(results)} datasets")
        except: pass
    return log("Bahrain","Open Data",[],"Timeout")

def try_oman():
    print("\n>>> OMAN")
    r = get("https://data.gov.om/api/3/action/package_search?q=real+estate", timeout=15)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            if results:
                return log("Oman","data.gov.om",[],f"Found {len(results)} datasets")
        except: pass
    return log("Oman","NCSI",[],"Timeout")

def try_jordan():
    print("\n>>> JORDAN")
    r = get("https://data.jordan.gov.jo/api/3/action/package_search?q=property", timeout=15)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            if results:
                return log("Jordan","data.jordan.gov.jo",[],f"Found {len(results)} datasets")
        except: pass
    return log("Jordan","Open Data",[],"Timeout")

# ═══════════════ CAUCASUS / CENTRAL ASIA ═══════════════

def try_georgia():
    """Georgia (Caucasus) — NAPR has open WFS for property"""
    print("\n>>> GEORGIA — NAPR Property Registry")
    rows = []
    # Try WFS
    wfs = "https://maps.napr.gov.ge/geoserver/wfs?service=WFS&version=2.0.0&request=GetFeature&typeName=cadastre:parcels&outputFormat=application/json&count=50&srsName=EPSG:4326"
    r = get(wfs, timeout=20)
    if r and r.status_code == 200:
        try:
            data = r.json()
            features = data.get("features",[])
            for f in features:
                props = f.get("properties",{})
                geom = f.get("geometry",{})
                coords = geom.get("coordinates",[]) if geom else []
                while isinstance(coords,list) and coords and isinstance(coords[0],list):
                    coords = coords[0]
                rows.append({
                    "country":"Georgia","iso":"GE","source":"ge_napr",
                    "lat":coords[1] if len(coords)>=2 else None,
                    "lon":coords[0] if len(coords)>=2 else None,
                    "area_m2":float(props.get("area",0)) if props.get("area") else None,
                })
            if rows:
                return log("Georgia","NAPR WFS",rows,f"{len(rows)} cadastral parcels with geometry")
        except: pass
    # Try data portal
    r2 = get("https://data.gov.ge/api/3/action/package_search?q=property", timeout=15)
    if r2 and r2.status_code == 200:
        try:
            results = r2.json().get("result",{}).get("results",[])
            if results:
                return log("Georgia","data.gov.ge",[],f"Found {len(results)} datasets")
        except: pass
    return log("Georgia","NAPR",[],"Timeout")

def try_kazakhstan():
    print("\n>>> KAZAKHSTAN")
    r = get("https://data.egov.kz/api/v4/datasets?search=real+estate&limit=10", timeout=15)
    if r and r.status_code == 200:
        try:
            data = r.json()
            if data:
                return log("Kazakhstan","data.egov.kz",[],f"API returned {type(data).__name__}")
        except: pass
    return log("Kazakhstan","Open Data",[],"Timeout")

def try_armenia():
    print("\n>>> ARMENIA")
    r = get("https://data.gov.am/api/3/action/package_search?q=property", timeout=15)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            if results:
                return log("Armenia","data.gov.am",[],f"Found {len(results)} datasets")
        except: pass
    return log("Armenia","Open Data",[],"Timeout")

# ═══════════════ SOUTH/SE ASIA ═══════════════

def try_sri_lanka():
    print("\n>>> SRI LANKA")
    r = get("https://data.gov.lk/api/3/action/package_search?q=property+land", timeout=15)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            if results:
                return log("Sri Lanka","data.gov.lk",[],f"Found {len(results)} datasets")
        except: pass
    return log("Sri Lanka","Open Data",[],"Timeout")

def try_nepal():
    print("\n>>> NEPAL")
    r = get("https://data.gov.np/api/3/action/package_search?q=land", timeout=15)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            if results:
                return log("Nepal","data.gov.np",[],f"Found {len(results)} datasets")
        except: pass
    return log("Nepal","Open Data",[],"Timeout")

def try_cambodia():
    print("\n>>> CAMBODIA")
    r = get("https://data.opendevelopmentcambodia.net/api/3/action/package_search?q=land+price", timeout=15)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            if results:
                return log("Cambodia","ODC",[],f"Found {len(results)} datasets")
        except: pass
    return log("Cambodia","Open Data",[],"Timeout")

# ═══════════════ AFRICA ═══════════════

def try_tunisia():
    print("\n>>> TUNISIA")
    r = get("https://data.gov.tn/api/3/action/package_search?q=immobilier", timeout=15)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            if results:
                return log("Tunisia","data.gov.tn",[],f"Found {len(results)} datasets")
        except: pass
    return log("Tunisia","Open Data",[],"Timeout")

def try_ethiopia():
    print("\n>>> ETHIOPIA")
    r = get("https://data.gov.et/api/3/action/package_search?q=housing+land", timeout=15)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            if results:
                return log("Ethiopia","data.gov.et",[],f"Found {len(results)} datasets")
        except: pass
    return log("Ethiopia","Open Data",[],"Timeout")

def try_mauritius():
    print("\n>>> MAURITIUS")
    r = get("https://data.govmu.org/api/3/action/package_search?q=property+land", timeout=15)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            if results:
                return log("Mauritius","data.govmu.org",[],f"Found {len(results)} datasets")
        except: pass
    return log("Mauritius","Open Data",[],"Timeout")

def try_botswana():
    print("\n>>> BOTSWANA")
    r = get("https://data.gov.bw/api/3/action/package_search?q=land+property", timeout=15)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            if results:
                return log("Botswana","data.gov.bw",[],f"Found {len(results)} datasets")
        except: pass
    return log("Botswana","Open Data",[],"Timeout")

def try_senegal():
    print("\n>>> SENEGAL")
    r = get("https://www.data.gouv.sn/api/1/datasets/?q=foncier", timeout=15)
    if r and r.status_code == 200:
        try:
            data = r.json()
            datasets = data.get("data",[])
            if datasets:
                return log("Senegal","data.gouv.sn",[],f"Found {len(datasets)} datasets")
        except: pass
    return log("Senegal","Open Data",[],"Timeout")


# ═══════════════ MAIN ═══════════════

def main():
    t0 = time.time()
    print("="*70)
    print("SWEEP 5: ALL REMAINING UNTRIED COUNTRIES")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

    all_rows = []
    # North America
    all_rows.extend(try_canada_bc())
    all_rows.extend(try_puerto_rico())
    # Caribbean/Central America
    all_rows.extend(try_costa_rica())
    all_rows.extend(try_panama())
    all_rows.extend(try_jamaica())
    all_rows.extend(try_dominican_republic())
    all_rows.extend(try_trinidad())
    all_rows.extend(try_guatemala())
    # Middle East
    all_rows.extend(try_kuwait())
    all_rows.extend(try_qatar())
    all_rows.extend(try_bahrain())
    all_rows.extend(try_oman())
    all_rows.extend(try_jordan())
    # Caucasus/Central Asia
    all_rows.extend(try_georgia())
    all_rows.extend(try_kazakhstan())
    all_rows.extend(try_armenia())
    # South/SE Asia
    all_rows.extend(try_sri_lanka())
    all_rows.extend(try_nepal())
    all_rows.extend(try_cambodia())
    # Africa
    all_rows.extend(try_tunisia())
    all_rows.extend(try_ethiopia())
    all_rows.extend(try_mauritius())
    all_rows.extend(try_botswana())
    all_rows.extend(try_senegal())

    print("\n" + "="*70)
    print("SWEEP 5 RESULTS")
    print("="*70)
    passes = sum(1 for r in R.values() if r["n"]>0)
    accessible = sum(1 for r in R.values() if "Found" in r["detail"] or "accessible" in r["detail"] or "Portal" in r["detail"] or "API returned" in r["detail"])
    total = sum(r["n"] for r in R.values())
    print(f"  Sources: {len(R)}")
    print(f"  With parsed data: {passes}")
    print(f"  Portals found: {accessible}")
    print(f"  Total rows: {total}")

    print("\n  DATA RETURNED:")
    for k,r in sorted(R.items()):
        if r["n"]>0:
            print(f"    {r['country']:20s} {r['n']:5d} rows  {r['detail']}")
    print("\n  PORTALS/DATASETS FOUND:")
    for k,r in sorted(R.items()):
        if r["n"]==0 and ("Found" in r["detail"] or "accessible" in r["detail"] or "API returned" in r["detail"]):
            print(f"    {r['country']:20s} {r['detail']}")
    print("\n  TIMEOUT/FAIL:")
    for k,r in sorted(R.items()):
        if r["n"]==0 and "Found" not in r["detail"] and "accessible" not in r["detail"] and "API returned" not in r["detail"]:
            print(f"    {r['country']:20s} {r['detail']}")

    if all_rows:
        df = pd.DataFrame(all_rows)
        out = os.path.join(OUT_DIR, "sweep5_panel.csv")
        df.to_csv(out, index=False)
        print(f"\n  CSV: {out}")

    out_json = os.path.join(OUT_DIR, "sweep5_results.json")
    with open(out_json, "w") as f:
        json.dump({"elapsed":round(time.time()-t0,1),"n_sources":len(R),
                   "n_with_data":passes,"n_accessible":accessible,
                   "total_rows":total,"results":R}, f, indent=2)
    print(f"  JSON: {out_json}")
    print(f"  Completed in {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
