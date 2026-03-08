"""
Sweep 4: South America, Africa, Asia, Oceania — Per-Property
==============================================================
Targets actual per-property transaction or cadastral data
in regions not yet properly attempted.
"""
import requests, json, time, csv, io, os, random
import pandas as pd, numpy as np

random.seed(42)
OUT_DIR = os.path.dirname(__file__)
H = {"User-Agent": "Mozilla/5.0 Properlytic/8.0 (Research)"}
R = {}

def get(url, timeout=20, **kw):
    try:
        h = kw.pop("headers", H)
        return requests.get(url, timeout=timeout, headers=h, allow_redirects=True, **kw)
    except: return None

def post(url, timeout=20, **kw):
    try:
        h = kw.pop("headers", H)
        return requests.post(url, timeout=timeout, headers=h, **kw)
    except: return None

def log(country, src, rows, detail):
    R[f"{country}|{src}"] = {"country":country,"source":src,"n":len(rows),"detail":detail}
    print(f"  [{'PASS' if rows else 'FAIL'}] {country}: {len(rows)} — {detail}")
    return rows

# ═══════════════ SOUTH AMERICA ═══════════════

def try_chile_sii():
    """Chile SII — Tax authority property valuations (per-property)"""
    print("\n>>> CHILE SII — Property Tax Valuations")
    rows = []
    # SII has avalúo fiscal (tax valuations) per property
    apis = [
        "https://www.sii.cl/servicios_online/BC_Impuesto/propiedades.html",
        "https://zeus.sii.cl/cvc_cgi/stc/getstc",
        "https://datos.gob.cl/api/3/action/package_search?q=bienes+raices",
        "https://datos.gob.cl/api/3/action/package_search?q=avaluo+propiedad",
    ]
    for url in apis:
        r = get(url, timeout=15)
        if r and r.status_code == 200 and len(r.content) > 200:
            try:
                if "json" in url:
                    data = r.json()
                    results = data.get("result",{}).get("results",[])
                    if results:
                        pkgs = [p.get("title","") for p in results[:5]]
                        return log("Chile","SII/datos.gob.cl",[],"Found datasets: "+"; ".join(pkgs))
                else:
                    return log("Chile","SII Portal",[],f"Portal accessible ({len(r.content)} bytes)")
            except: pass
    return log("Chile","SII",[],"Timeout")

def try_colombia_igac():
    """Colombia IGAC — Geographic institute cadastral data"""
    print("\n>>> COLOMBIA IGAC — Cadastral Data")
    rows = []
    apis = [
        "https://geoportal.igac.gov.co/es/contenido/datos-abiertos-catastro",
        "https://datos.gov.co/api/views/metadata/v1?search=catastro&limit=10",
        "https://www.datos.gov.co/resource/wk4y-mfhb.json?$limit=100",  # Bogota cadastral
        "https://datosabiertos.bogota.gov.co/api/3/action/package_search?q=catastro",
    ]
    for url in apis:
        r = get(url, timeout=15)
        if r and r.status_code == 200 and len(r.content) > 200:
            try:
                data = r.json()
                if isinstance(data, list) and len(data) > 0:
                    for rec in data[:100]:
                        val = rec.get("valor_comercial") or rec.get("avaluo") or rec.get("valor_catastral")
                        if val:
                            rows.append({"price":float(str(val).replace(",","")),"currency":"COP",
                                "country":"Colombia","iso":"CO","source":"co_igac",
                                "area_m2":float(rec.get("area",0)) if rec.get("area") else None,
                                "lat":float(rec.get("latitud",0)) if rec.get("latitud") else None,
                                "lon":float(rec.get("longitud",0)) if rec.get("longitud") else None,
                                "city":"Bogota"})
                    if rows: break
                elif isinstance(data,dict):
                    results = data.get("result",{}).get("results",[])
                    if results:
                        pkgs = [p.get("title","")[:60] for p in results[:5]]
                        return log("Colombia","IGAC/datos.gov.co",[],"Found: "+"; ".join(pkgs))
            except: continue
    return log("Colombia","IGAC cadastral",rows,
               f"{len(rows)} cadastral records" if rows else "API needs different endpoint")

def try_ecuador():
    """Ecuador — Municipal property data"""
    print("\n>>> ECUADOR — Municipal Open Data")
    rows = []
    apis = [
        "https://datosabiertos.quito.gob.ec/api/3/action/package_search?q=catastro",
        "https://www.datosabiertos.gob.ec/api/3/action/package_search?q=propiedad",
    ]
    for url in apis:
        r = get(url, timeout=15)
        if r and r.status_code == 200:
            try:
                data = r.json()
                results = data.get("result",{}).get("results",[])
                if results:
                    return log("Ecuador","Quito Open Data",[],
                              f"Found {len(results)} datasets: {results[0].get('title','')[:60]}")
            except: pass
    return log("Ecuador","Municipal",[],"Timeout/No datasets")

def try_uruguay():
    """Uruguay — DGR property register / catastro"""
    print("\n>>> URUGUAY — Catastro")
    rows = []
    apis = [
        "https://catastro.gub.uy/",
        "https://catalogodatos.gub.uy/api/3/action/package_search?q=inmueble",
        "https://catalogodatos.gub.uy/api/3/action/package_search?q=catastro",
    ]
    for url in apis:
        r = get(url, timeout=15)
        if r and r.status_code == 200 and len(r.content) > 200:
            try:
                if "json" in url:
                    data = r.json()
                    results = data.get("result",{}).get("results",[])
                    if results:
                        titles = [p.get("title","")[:50] for p in results[:5]]
                        return log("Uruguay","Catálogo Datos",[],f"Found: "+"; ".join(titles))
                else:
                    return log("Uruguay","Catastro Portal",[],f"Portal accessible")
            except: pass
    return log("Uruguay","Catastro",[],"Timeout")

def try_paraguay():
    """Paraguay — STP or cadastre"""
    print("\n>>> PARAGUAY — Open Data")
    r = get("https://www.datos.gov.py/api/3/action/package_search?q=inmueble", timeout=15)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            if results:
                return log("Paraguay","datos.gov.py",[],f"Found {len(results)} datasets")
        except: pass
    return log("Paraguay","Open Data",[],"Timeout")

# ═══════════════ AFRICA ═══════════════

def try_rwanda():
    """Rwanda RLMUA — Land Registry (surprisingly open, WFS)"""
    print("\n>>> RWANDA — RLMUA Land Registry")
    rows = []
    wfs = "https://geonode.rlma.rw/geoserver/wfs?service=WFS&version=2.0.0&request=GetFeature&typeName=geonode:parcels&outputFormat=application/json&count=50"
    r = get(wfs, timeout=20)
    if r and r.status_code == 200:
        try:
            data = r.json()
            features = data.get("features",[])
            for f in features:
                props = f.get("properties",{})
                geom = f.get("geometry",{})
                coords = geom.get("coordinates",[]) if geom else []
                # Flatten
                while isinstance(coords, list) and coords and isinstance(coords[0], list):
                    coords = coords[0]
                val = props.get("land_value") or props.get("market_value") or props.get("value")
                rows.append({
                    "country":"Rwanda","iso":"RW","source":"rw_rlma",
                    "area_m2":float(props.get("area",0)) if props.get("area") else None,
                    "lat":coords[1] if len(coords)>=2 else None,
                    "lon":coords[0] if len(coords)>=2 else None,
                    "price":float(val) if val else None,"currency":"RWF",
                    "property_type":props.get("land_use",""),
                })
            if features:
                return log("Rwanda","RLMA WFS",rows,f"{len(rows)} parcels with geometry")
        except: pass
    # Try geonode catalog
    r2 = get("https://geonode.rlma.rw/api/v2/layers/?search=parcel", timeout=15)
    if r2 and r2.status_code == 200:
        return log("Rwanda","RLMA Geonode",[],f"Geonode API accessible ({len(r2.content)} bytes)")
    return log("Rwanda","RLMA",[],"Timeout")

def try_south_africa():
    """South Africa — Deeds Office / Cape Town Open Data"""
    print("\n>>> SOUTH AFRICA — Deeds/Cape Town Open Data")
    rows = []
    apis = [
        "https://odp-cctegis.opendata.arcgis.com/api/v3/datasets?q=property&sort=-modified",
        "https://web1.capetown.gov.za/web1/opendataportal/AllDatasets",
        "https://data.gov.za/api/3/action/package_search?q=property+valuation",
    ]
    for url in apis:
        r = get(url, timeout=15)
        if r and r.status_code == 200 and len(r.content) > 200:
            try:
                data = r.json()
                datasets = data.get("data",[]) or data.get("result",{}).get("results",[])
                if datasets:
                    titles = [d.get("attributes",{}).get("name","")[:50] or d.get("title","")[:50] for d in datasets[:5]]
                    return log("South Africa","Open Data Portal",[],f"Found: {'; '.join(t for t in titles if t)}")
            except: pass
    # Try Cape Town property valuations GeoJSON
    r2 = get("https://odp-cctegis.opendata.arcgis.com/api/v3/datasets?q=valuation", timeout=15)
    if r2 and r2.status_code == 200:
        try:
            data = r2.json()
            datasets = data.get("data",[])
            if datasets:
                return log("South Africa","Cape Town ArcGIS",[],f"Found {len(datasets)} valuation datasets")
        except: pass
    return log("South Africa","Deeds/Open Data",[],"Timeout")

def try_kenya():
    """Kenya — Nairobi / KNBS property data"""
    print("\n>>> KENYA — Nairobi Open Data")
    rows = []
    r = get("https://kenya.opendataforafrica.org/api/1.0/data?dataset=kenya-housing-statistics&limit=100", timeout=15)
    if r and r.status_code == 200:
        try:
            data = r.json()
            if data:
                return log("Kenya","OpenDataForAfrica",[],f"API returned {len(str(data))} chars")
        except: pass
    r2 = get("https://opendata.go.ke/api/3/action/package_search?q=housing", timeout=15)
    if r2 and r2.status_code == 200:
        try:
            results = r2.json().get("result",{}).get("results",[])
            if results:
                return log("Kenya","OpenData Portal",[],f"Found {len(results)} housing datasets")
        except: pass
    return log("Kenya","Open Data",[],"Timeout")

def try_ghana():
    """Ghana — Lands Commission"""
    print("\n>>> GHANA — Lands Commission")
    r = get("https://data.gov.gh/api/3/action/package_search?q=land", timeout=15)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            if results:
                return log("Ghana","data.gov.gh",[],f"Found {len(results)} land datasets")
        except: pass
    return log("Ghana","Lands Commission",[],"Timeout")

def try_tanzania():
    """Tanzania — Open Data"""
    print("\n>>> TANZANIA — Open Data")
    r = get("https://opendata.go.tz/api/3/action/package_search?q=housing", timeout=15)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            if results:
                return log("Tanzania","Open Data Portal",[],f"Found {len(results)} datasets")
        except: pass
    return log("Tanzania","Open Data",[],"Timeout")

# ═══════════════ ASIA ═══════════════

def try_vietnam():
    """Vietnam — Ministry of Construction / Property data"""
    print("\n>>> VIETNAM — Ministry of Construction")
    r = get("https://data.gov.vn/api/3/action/package_search?q=nha+dat", timeout=15)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            if results:
                return log("Vietnam","data.gov.vn",[],f"Found {len(results)} property datasets")
        except: pass
    return log("Vietnam","data.gov.vn",[],"Timeout")

def try_thailand_dol():
    """Thailand — Department of Lands transaction data"""
    print("\n>>> THAILAND — Department of Lands")
    apis = [
        "https://data.go.th/api/3/action/package_search?q=land+price",
        "https://data.go.th/api/3/action/package_search?q=real+estate",
        "https://catalog.nso.go.th/api/3/action/package_search?q=housing",
    ]
    for url in apis:
        r = get(url, timeout=15)
        if r and r.status_code == 200:
            try:
                results = r.json().get("result",{}).get("results",[])
                if results:
                    titles = [p.get("title","")[:50] for p in results[:3]]
                    return log("Thailand","data.go.th",[],f"Found: {'; '.join(titles)}")
            except: pass
    return log("Thailand","DOL",[],"Timeout")

def try_malaysia_napic():
    """Malaysia — NAPIC property data"""
    print("\n>>> MALAYSIA — NAPIC/data.gov.my")
    apis = [
        "https://data.gov.my/data-catalogue?search=property",
        "https://api.data.gov.my/data-catalogue?id=property_prices&limit=100",
    ]
    for url in apis:
        r = get(url, timeout=15)
        if r and r.status_code == 200 and len(r.content) > 200:
            try:
                data = r.json()
                if isinstance(data, list) and data:
                    for rec in data[:100]:
                        price = rec.get("price") or rec.get("value")
                        if price:
                            rows = [{"price":float(price),"currency":"MYR",
                                "country":"Malaysia","iso":"MY","source":"my_datagovmy"}]
                            return log("Malaysia","data.gov.my",rows,f"Got price data")
                return log("Malaysia","data.gov.my",[],f"API returned {type(data).__name__}")
            except:
                return log("Malaysia","data.gov.my",[],f"Page accessible ({len(r.content)//1024}KB)")
    return log("Malaysia","NAPIC",[],"Timeout")

def try_philippines_hlurb():
    """Philippines — HLURB/PSA property data"""
    print("\n>>> PHILIPPINES — PSA/Data Portal")
    r = get("https://openstat.psa.gov.ph/api/3/action/package_search?q=housing+price", timeout=15)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            if results:
                return log("Philippines","PSA OpenStat",[],f"Found {len(results)} datasets")
        except: pass
    return log("Philippines","PSA",[],"Timeout")

def try_bangladesh():
    """Bangladesh — Open Data"""
    print("\n>>> BANGLADESH — Open Data")
    r = get("https://data.gov.bd/api/3/action/package_search?q=land", timeout=15)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            return log("Bangladesh","data.gov.bd",[],f"Found {len(results)} datasets" if results else "No results")
        except: pass
    return log("Bangladesh","Open Data",[],"Timeout")

def try_pakistan():
    """Pakistan — Open Data"""
    print("\n>>> PAKISTAN — Open Data")
    r = get("https://opendata.com.pk/api/3/action/package_search?q=property", timeout=15)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            return log("Pakistan","opendata.com.pk",[],f"Found {len(results)} datasets" if results else "No results")
        except: pass
    return log("Pakistan","Open Data",[],"Timeout")

def try_mongolia():
    """Mongolia — Open Data"""
    print("\n>>> MONGOLIA — Open Data")
    r = get("https://opendata.gov.mn/api/3/action/package_search?q=real+estate", timeout=15)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            return log("Mongolia","opendata.gov.mn",[],f"Found {len(results)} datasets" if results else "No results")
        except: pass
    return log("Mongolia","Open Data",[],"Timeout")

# ═══════════════ OCEANIA ═══════════════

def try_australia_vic():
    """Australia Victoria — DELWP property data"""
    print("\n>>> AUSTRALIA VIC — DELWP")
    rows = []
    apis = [
        "https://discover.data.vic.gov.au/api/3/action/package_search?q=property+sale",
        "https://services.land.vic.gov.au/catalogue/publicpriceithink",
        "https://discover.data.vic.gov.au/api/3/action/package_search?q=valuation",
    ]
    for url in apis:
        r = get(url, timeout=15)
        if r and r.status_code == 200 and len(r.content) > 200:
            try:
                data = r.json()
                results = data.get("result",{}).get("results",[])
                if results:
                    titles = [p.get("title","")[:60] for p in results[:5]]
                    return log("Australia VIC","DELWP/data.vic",[],f"Found: {'; '.join(titles)}")
            except: pass
    return log("Australia VIC","DELWP",[],"Timeout")

def try_australia_qld():
    """Australia Queensland — Property data"""
    print("\n>>> AUSTRALIA QLD — Property Data")
    apis = [
        "https://data.qld.gov.au/api/3/action/package_search?q=property+sale",
        "https://data.qld.gov.au/api/3/action/package_search?q=valuation",
    ]
    for url in apis:
        r = get(url, timeout=15)
        if r and r.status_code == 200:
            try:
                results = r.json().get("result",{}).get("results",[])
                if results:
                    titles = [p.get("title","")[:60] for p in results[:5]]
                    return log("Australia QLD","data.qld",[],f"Found: {'; '.join(titles)}")
            except: pass
    return log("Australia QLD","data.qld",[],"Timeout")

def try_fiji():
    """Fiji — Property data"""
    print("\n>>> FIJI — Open Data")
    r = get("https://www.opendata.com.fj/api/3/action/package_search?q=property", timeout=15)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            return log("Fiji","Open Data",[],f"Found {len(results)} datasets" if results else "No results")
        except: pass
    return log("Fiji","Open Data",[],"Timeout")

def try_png():
    """Papua New Guinea — National Research Institute"""
    print("\n>>> PAPUA NEW GUINEA")
    r = get("https://pngiportal.org/api/v1/datasets?search=land", timeout=15)
    if r and r.status_code == 200:
        return log("Papua New Guinea","iPortal",[],f"Portal accessible ({len(r.content)} bytes)")
    return log("Papua New Guinea","Open Data",[],"Timeout")


# ═══════════════ MAIN ═══════════════

def main():
    t0 = time.time()
    print("="*70)
    print("SWEEP 4: SOUTH AMERICA, AFRICA, ASIA, OCEANIA")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

    all_rows = []
    # South America
    all_rows.extend(try_chile_sii())
    all_rows.extend(try_colombia_igac())
    all_rows.extend(try_ecuador())
    all_rows.extend(try_uruguay())
    all_rows.extend(try_paraguay())
    # Africa
    all_rows.extend(try_rwanda())
    all_rows.extend(try_south_africa())
    all_rows.extend(try_kenya())
    all_rows.extend(try_ghana())
    all_rows.extend(try_tanzania())
    # Asia
    all_rows.extend(try_vietnam())
    all_rows.extend(try_thailand_dol())
    all_rows.extend(try_malaysia_napic())
    all_rows.extend(try_philippines_hlurb())
    all_rows.extend(try_bangladesh())
    all_rows.extend(try_pakistan())
    all_rows.extend(try_mongolia())
    # Oceania
    all_rows.extend(try_australia_vic())
    all_rows.extend(try_australia_qld())
    all_rows.extend(try_fiji())
    all_rows.extend(try_png())

    print("\n" + "="*70)
    print("SWEEP 4 RESULTS")
    print("="*70)
    passes = sum(1 for r in R.values() if r["n"]>0)
    accessible = sum(1 for r in R.values() if "Found" in r["detail"] or "accessible" in r["detail"] or "Portal" in r["detail"])
    total = sum(r["n"] for r in R.values())
    print(f"  Sources tested: {len(R)}")
    print(f"  With parsed data: {passes}")
    print(f"  Portals/datasets found: {accessible}")
    print(f"  Total rows: {total}")

    print("\n  DATA RETURNED:")
    for k,r in sorted(R.items()):
        if r["n"]>0:
            print(f"    {r['country']:20s} {r['n']:5d} rows  {r['detail']}")
    print("\n  DATASETS FOUND (no price parsed):")
    for k,r in sorted(R.items()):
        if r["n"]==0 and ("Found" in r["detail"] or "accessible" in r["detail"]):
            print(f"    {r['country']:20s} {r['detail']}")
    print("\n  FAILED/TIMEOUT:")
    for k,r in sorted(R.items()):
        if r["n"]==0 and "Found" not in r["detail"] and "accessible" not in r["detail"]:
            print(f"    {r['country']:20s} {r['detail']}")

    if all_rows:
        df = pd.DataFrame(all_rows)
        out = os.path.join(OUT_DIR, "sweep4_panel.csv")
        df.to_csv(out, index=False)
        print(f"\n  CSV: {out}")

    out_json = os.path.join(OUT_DIR, "sweep4_results.json")
    with open(out_json, "w") as f:
        json.dump({"elapsed":round(time.time()-t0,1),"n_sources":len(R),
                   "n_with_data":passes,"n_accessible":accessible,
                   "total_rows":total,"results":R}, f, indent=2)
    print(f"  JSON: {out_json}")
    print(f"  Completed in {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
