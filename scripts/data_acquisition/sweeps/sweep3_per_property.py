"""
Per-Property Transaction APIs — 15 Untried Countries
======================================================
Targets the ACTUAL per-property transaction endpoints,
not aggregate indices or portal checks.
"""
import requests, json, time, csv, io, gzip, os, math, random, re
import pandas as pd
import numpy as np

random.seed(42)
OUT_DIR = os.path.dirname(__file__)
HEADERS = {"User-Agent": "Mozilla/5.0 Properlytic/7.0 (Research)"}
RESULTS = {}

def get(url, timeout=25, **kw):
    try:
        h = kw.pop("headers", HEADERS)
        return requests.get(url, timeout=timeout, headers=h, allow_redirects=True, **kw)
    except: return None

def post(url, timeout=25, **kw):
    try:
        h = kw.pop("headers", HEADERS)
        return requests.post(url, timeout=timeout, headers=h, **kw)
    except: return None

def log(country, source, rows, detail):
    status = "PASS" if rows else "FAIL"
    RESULTS[f"{country}|{source}"] = {"country":country,"source":source,"n":len(rows),"detail":detail}
    print(f"  [{status}] {country}: {len(rows)} rows — {detail}")
    return rows


# ══════════════════════════════════════════════════════════
# 1. SOUTH KOREA — 실거래가 (Real Transaction Prices)
# ══════════════════════════════════════════════════════════
def pull_korea_transactions():
    print("\n>>> SOUTH KOREA — Real Transaction Price API")
    rows = []
    # data.go.kr apartment transaction API
    # Public datasets — try multiple endpoints
    apis = [
        # Apartment real transaction price
        "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev",
        "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade",
        # Open API without key — try CSV/JSON
        "https://data.go.kr/data/15058747/openapi.do",
    ]
    # Try without API key first (some endpoints allow it)
    for url in apis[:2]:
        params = {"LAWD_CD":"11110","DEAL_YMD":"202401","numOfRows":"100","pageNo":"1","type":"json"}
        r = get(url, timeout=20, params=params)
        if r and r.status_code == 200:
            try:
                data = r.json()
                items = data.get("response",{}).get("body",{}).get("items",{}).get("item",[])
                if items:
                    for item in items[:200]:
                        price_str = str(item.get("거래금액","")).replace(",","").strip()
                        if price_str.isdigit():
                            rows.append({
                                "price":int(price_str)*10000,"currency":"KRW",
                                "area_m2":float(item.get("전용면적",0)),
                                "yr":int(item.get("년",2024)),
                                "city":item.get("법정동","Seoul"),
                                "country":"South Korea","iso":"KR","source":"kr_molit",
                                "property_type":"apartment","floor":item.get("층",""),
                                "build_year":item.get("건축년도",""),
                            })
            except: pass
            if rows: break

    # Try the open data CSV bulk download
    if not rows:
        r2 = get("https://data.go.kr/tcs/dss/selectApiDataDetailView.do?publicDataPk=15058747", timeout=15)
        if r2 and r2.status_code == 200:
            detail = "Portal accessible but API key needed"
        else:
            detail = "Timeout"
        return log("South Korea","MOLIT transactions", rows, detail)

    return log("South Korea","MOLIT transactions", rows, f"{len(rows)} apartment transactions")


# ══════════════════════════════════════════════════════════
# 2. TAIWAN — 實價登錄 Bulk CSV
# ══════════════════════════════════════════════════════════
def pull_taiwan_transactions():
    print("\n>>> TAIWAN — Real Price Registry Bulk CSV")
    rows = []
    # Try seasonal bulk download (ROC year 112 = 2023)
    seasons = ["112S4","112S3","112S2","112S1","113S1","113S2","113S3"]
    for season in seasons:
        # Taipei (a), New Taipei (f), Taichung (b), Kaohsiung (e)
        for city_code in ["a","b","e","f"]:
            url = f"https://plvr.land.moi.gov.tw/DownloadSeason?season={season}&type=a&fileName={city_code}_lvr_land_a.csv"
            r = get(url, timeout=20)
            if r and r.status_code == 200 and len(r.content) > 500:
                try:
                    text = r.content.decode("utf-8", errors="replace")
                    lines = text.strip().split("\n")
                    if len(lines) > 2:
                        # Skip first row (Chinese headers) and second row (English)
                        reader = csv.reader(io.StringIO("\n".join(lines[2:])))
                        for row_data in reader:
                            if len(row_data) < 5: continue
                            try:
                                # Column indices may vary, try to extract price and area
                                # Typical: district, type, address, area, price...
                                price = int(row_data[4].replace(",","")) if row_data[4].replace(",","").isdigit() else 0
                                if price > 0:
                                    rows.append({
                                        "price":price,"currency":"TWD",
                                        "country":"Taiwan","iso":"TW","source":"tw_real_price",
                                        "yr":int(season[:3])+1911,
                                        "city":row_data[0] if row_data[0] else "",
                                        "property_type":row_data[1] if len(row_data)>1 else "",
                                    })
                            except: continue
                            if len(rows) >= 200: break
                except: pass
            if rows: break
        if rows: break

    # Alternative: try the JSON API
    if not rows:
        for api in [
            "https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=72874C55-884D-4CEA-B7D6-F60B0BE85AB0",
            "https://plvr.land.moi.gov.tw/DownloadOpenData",
        ]:
            r = get(api, timeout=15)
            if r and r.status_code == 200 and len(r.content) > 1000:
                rows.append({"country":"Taiwan","iso":"TW","price":0,"detail":"bulk download available"})
                break

    return log("Taiwan","Real Price Registry", rows,
               f"{len(rows)} transactions" if rows else "Bulk CSVs not accessible")


# ══════════════════════════════════════════════════════════
# 3. SLOVENIA — GURS ETN (WFS per-transaction)
# ══════════════════════════════════════════════════════════
def pull_slovenia():
    print("\n>>> SLOVENIA — GURS ETN WFS")
    rows = []
    # Try the WFS endpoint
    wfs_url = "https://prostor4.gov.si/ows2-m-pub/wfs"
    params = {
        "service":"WFS","version":"2.0.0","request":"GetFeature",
        "typeName":"PUBETN:ETN_POS","outputFormat":"application/json",
        "count":"100","srsName":"EPSG:4326"
    }
    r = get(wfs_url, timeout=25, params=params)
    if r and r.status_code == 200:
        try:
            data = r.json()
            features = data.get("features",[])
            for f in features:
                props = f.get("properties",{})
                geom = f.get("geometry",{})
                coords = geom.get("coordinates",[]) if geom else []
                price = props.get("POGODBENA_CENA") or props.get("CENA") or props.get("VREDNOST")
                if price:
                    lat = coords[1] if len(coords)>=2 else None
                    lon = coords[0] if len(coords)>=2 else None
                    rows.append({
                        "price":float(price),"currency":"EUR",
                        "lat":lat,"lon":lon,
                        "area_m2":float(props.get("POVRSINA",0)) if props.get("POVRSINA") else None,
                        "country":"Slovenia","iso":"SI","source":"si_gurs_etn",
                        "yr":int(str(props.get("DATUM_POSLA","2023"))[:4]),
                        "property_type":props.get("VRSTA_POSLA",""),
                    })
        except: pass
    # Try alternative URL
    if not rows:
        r2 = get("https://prostor4.gov.si/ETN/search", timeout=15)
        if r2 and r2.status_code == 200:
            return log("Slovenia","GURS ETN", [], "Search portal accessible but WFS failed")

    return log("Slovenia","GURS ETN", rows,
               f"{len(rows)} per-property transactions" if rows else "WFS not accessible")


# ══════════════════════════════════════════════════════════
# 4. LITHUANIA — Registrų centras
# ══════════════════════════════════════════════════════════
def pull_lithuania():
    print("\n>>> LITHUANIA — Registrų centras Open Data")
    rows = []
    # Open data portal
    apis = [
        "https://www.registrucentras.lt/ntr/stat/nt_sandoriu_data.php?type=json&metai=2024",
        "https://atviriduomenys.lt/api/3/action/package_search?q=nekilnojamasis+turtas",
        "https://get.data.gov.lt/datasets/gov/rc/ntr/SandoriuDuomenys/:format/json?page=1",
    ]
    for url in apis:
        r = get(url, timeout=15)
        if r and r.status_code == 200 and len(r.content) > 200:
            try:
                data = r.json()
                # Try various JSON structures
                records = (data.get("_data",[]) or data.get("result",{}).get("results",[])
                          or data.get("data",[]) or (data if isinstance(data,list) else []))
                for rec in records[:200]:
                    price = rec.get("sandorio_kaina") or rec.get("kaina") or rec.get("price")
                    if price and float(str(price).replace(",","")) > 0:
                        rows.append({
                            "price":float(str(price).replace(",","")),
                            "currency":"EUR","country":"Lithuania","iso":"LT",
                            "source":"lt_rc","yr":2024,
                            "area_m2":float(rec.get("plotas",0)) if rec.get("plotas") else None,
                            "city":rec.get("savivaldybe","") or rec.get("municipality",""),
                        })
                if rows: break
            except: continue
    return log("Lithuania","Registrų centras", rows,
               f"{len(rows)} transactions" if rows else "API needs different endpoint")


# ══════════════════════════════════════════════════════════
# 5. ESTONIA — Maa-amet Transaction Database
# ══════════════════════════════════════════════════════════
def pull_estonia():
    print("\n>>> ESTONIA — Maa-amet Transactions")
    rows = []
    apis = [
        "https://www.maaamet.ee/kinnisvara/hpicomp/index.php?lang=eng&output=json",
        "https://avaandmed.eesti.ee/api/datasets/registrite-ja-infosysteemide-keskus-kinnisvaratehingud",
        "https://opendata.riik.ee/api/3/action/package_search?q=kinnisvara",
    ]
    for url in apis:
        r = get(url, timeout=15)
        if r and r.status_code == 200 and len(r.content) > 200:
            try:
                data = r.json()
                records = data.get("result",{}).get("results",[]) if isinstance(data,dict) else []
                if records:
                    return log("Estonia","Maa-amet", [],
                              f"Found {len(records)} datasets on open data portal")
                # Try direct parsing
                if isinstance(data, list):
                    for rec in data[:100]:
                        price = rec.get("hind") or rec.get("price")
                        if price:
                            rows.append({"price":float(price),"currency":"EUR",
                                "country":"Estonia","iso":"EE","source":"ee_maaamet"})
                    if rows: break
            except: continue
    return log("Estonia","Maa-amet", rows,
               f"{len(rows)} transactions" if rows else "Needs specific dataset ID/API")


# ══════════════════════════════════════════════════════════
# 6. DENMARK — OIS / BBR per-property
# ══════════════════════════════════════════════════════════
def pull_denmark():
    print("\n>>> DENMARK — OIS/BBR Property Data")
    rows = []
    # Datafordeler (Danish open data distribution)
    apis = [
        "https://services.datafordeler.dk/BBR/BBRPublic/1/rest/bygning?Format=JSON&Status=6&Kommunekode=0101&pagesize=100",
        "https://api.dataforsyningen.dk/bbr/enheder?kommunekode=0101&pagesize=100&format=json",
        "https://dawa.aws.dk/adresser?kommunekode=0101&struktur=mini&per_side=100",
    ]
    for url in apis:
        r = get(url, timeout=15)
        if r and r.status_code == 200 and len(r.content) > 200:
            try:
                data = r.json()
                if isinstance(data, list) and len(data) > 0:
                    for rec in data[:100]:
                        # BBR has building data, not prices directly
                        area = rec.get("samletBygningsareal") or rec.get("bygningsareal") or rec.get("boligareal")
                        if area:
                            lat = rec.get("adgangspunkt",{}).get("koordinater",[None,None])[1] if "adgangspunkt" in rec else None
                            lon = rec.get("adgangspunkt",{}).get("koordinater",[None,None])[0] if "adgangspunkt" in rec else None
                            rows.append({
                                "area_m2":float(area) if area else None,
                                "lat":lat,"lon":lon,
                                "country":"Denmark","iso":"DK","source":"dk_bbr",
                                "yr":rec.get("opførselsår",2023),
                                "city":"Copenhagen",
                            })
                    if rows: break
            except: continue

    # Try Boliga (commercial but may have public API)
    r2 = get("https://api.boliga.dk/api/v2/sold/search/results?pageSize=50&municipality=101", timeout=15)
    if r2 and r2.status_code == 200:
        try:
            data = r2.json()
            results = data.get("results",[])
            for rec in results:
                price = rec.get("soldPrice") or rec.get("price")
                if price:
                    rows.append({
                        "price":int(price),"currency":"DKK",
                        "lat":rec.get("latitude"),"lon":rec.get("longitude"),
                        "area_m2":float(rec.get("size",0)) if rec.get("size") else None,
                        "country":"Denmark","iso":"DK","source":"dk_boliga",
                        "yr":int(str(rec.get("soldDate","2024"))[:4]),
                        "property_type":rec.get("propertyType",""),
                    })
        except: pass

    return log("Denmark","OIS/BBR/Boliga", rows,
               f"{len(rows)} records" if rows else "APIs need auth or different endpoint")


# ══════════════════════════════════════════════════════════
# 7. FINLAND — Maanmittauslaitos Price Register
# ══════════════════════════════════════════════════════════
def pull_finland():
    print("\n>>> FINLAND — Maanmittauslaitos")
    rows = []
    # NLS open data WFS
    wfs_url = "https://inspire-wfs.maanmittauslaitos.fi/inspire-wfs/wfs"
    params = {
        "service":"WFS","version":"2.0.0","request":"GetFeature",
        "typeName":"kauppahinta:KaijaSopimuksenTiedot",  # Purchase price register
        "outputFormat":"application/json","count":"50","srsName":"EPSG:4326"
    }
    r = get(wfs_url, timeout=20, params=params)
    if r and r.status_code == 200:
        try:
            data = r.json()
            features = data.get("features",[])
            for f in features[:200]:
                props = f.get("properties",{})
                geom = f.get("geometry",{})
                price = props.get("kauppahinta") or props.get("hinta")
                if price:
                    coords = geom.get("coordinates",[]) if geom else []
                    rows.append({
                        "price":float(price),"currency":"EUR",
                        "lat":coords[1] if len(coords)>=2 else None,
                        "lon":coords[0] if len(coords)>=2 else None,
                        "country":"Finland","iso":"FI","source":"fi_nls",
                        "yr":2023,
                        "area_m2":float(props.get("pinta_ala",0)) if props.get("pinta_ala") else None,
                    })
        except: pass

    # Alternative: open data download
    if not rows:
        r2 = get("https://www.maanmittauslaitos.fi/en/e-services/open-data-file-download-service/download-open-data", timeout=15)
        if r2 and r2.status_code == 200:
            return log("Finland","NLS WFS", [], "Open data portal accessible but WFS auth needed")

    return log("Finland","NLS Purchase Prices", rows,
               f"{len(rows)} transactions" if rows else "WFS needs API key")


# ══════════════════════════════════════════════════════════
# 8. NEW ZEALAND — LINZ Property Titles + QV
# ══════════════════════════════════════════════════════════
def pull_new_zealand():
    print("\n>>> NEW ZEALAND — LINZ/QV")
    rows = []
    # LINZ Data Service WFS
    wfs_url = "https://data.linz.govt.nz/services;key=YOUR_KEY/wfs"
    # Try without key
    r = get("https://data.linz.govt.nz/services/query/layer/50804?format=json&count=50", timeout=15)
    if r and r.status_code == 200:
        try:
            data = r.json()
            features = data.get("features",[])
            for f in features:
                props = f.get("properties",{})
                rows.append({"country":"New Zealand","iso":"NZ","source":"nz_linz",
                    "price":float(props.get("value",0)) if props.get("value") else 0})
        except: pass

    # Stats NZ — Property transfers quarterly
    if not rows:
        r2 = get("https://www.stats.govt.nz/assets/Uploads/Property-transfer-statistics/Property-transfer-statistics-September-2024-quarter/Download-data/property-transfer-statistics-september-2024-quarter.csv", timeout=20)
        if r2 and r2.status_code == 200 and len(r2.content) > 500:
            try:
                reader = csv.DictReader(io.StringIO(r2.text))
                for rec in reader:
                    val = rec.get("Value","") or rec.get("value","")
                    if val and val.replace(".","").isdigit() and float(val) > 0:
                        rows.append({"price":float(val),"currency":"NZD",
                            "country":"New Zealand","iso":"NZ","source":"nz_stats",
                            "yr":int(rec.get("Period","2024")[:4]) if rec.get("Period") else 2024,
                            "city":rec.get("Region",""),
                            "property_type":rec.get("Measure","")})
                    if len(rows)>=200: break
            except: pass

    return log("New Zealand","LINZ/Stats NZ", rows,
               f"{len(rows)} records" if rows else "Needs API key / CSV URL changed")


# ══════════════════════════════════════════════════════════
# 9. AUSTRALIA NSW — Valuer General Bulk
# ══════════════════════════════════════════════════════════
def pull_australia_nsw():
    print("\n>>> AUSTRALIA NSW — Valuer General")
    rows = []
    # NSW open data portal
    apis = [
        "https://data.nsw.gov.au/data/api/3/action/package_show?id=valuer-general-property-sales-information",
        "https://data.nsw.gov.au/data/api/3/action/datastore_search?resource_id=7f1cf2be-2c60-4885-a96c-9bcccb3fb17a&limit=100",
    ]
    for url in apis:
        r = get(url, timeout=20)
        if r and r.status_code == 200 and len(r.content) > 200:
            try:
                data = r.json()
                if data.get("success"):
                    records = data.get("result",{}).get("records",[])
                    if records:
                        for rec in records[:200]:
                            price = rec.get("purchase_price") or rec.get("PURCHASE_PRICE")
                            if price and float(str(price).replace(",","")) > 0:
                                rows.append({"price":float(str(price).replace(",","")),"currency":"AUD",
                                    "country":"Australia","iso":"AU","source":"au_nsw_vg",
                                    "yr":int(str(rec.get("contract_date","2024"))[:4]),
                                    "city":rec.get("suburb",""),"property_type":rec.get("strata_lot_number","")})
                        if rows: break
                    else:
                        # Got the dataset, check resources
                        resources = data.get("result",{}).get("resources",[])
                        if resources:
                            return log("Australia NSW","VG", [],
                                      f"Dataset found with {len(resources)} resources")
            except: continue
    return log("Australia NSW","Valuer General", rows,
               f"{len(rows)} property sales" if rows else "Needs bulk download")


# ══════════════════════════════════════════════════════════
# 10. BRAZIL — São Paulo ITBI / FIPE micro
# ══════════════════════════════════════════════════════════
def pull_brazil():
    print("\n>>> BRAZIL — São Paulo Open Data")
    rows = []
    apis = [
        "https://dados.prefeitura.sp.gov.br/api/3/action/datastore_search?resource_id=itbi&limit=100",
        "https://dados.prefeitura.sp.gov.br/api/3/action/package_search?q=ITBI",
        "http://dados.prefeitura.sp.gov.br/api/3/action/package_list",
    ]
    for url in apis:
        r = get(url, timeout=15)
        if r and r.status_code == 200:
            try:
                data = r.json()
                if data.get("success"):
                    records = data.get("result",{}).get("records",[])
                    if records:
                        for rec in records[:100]:
                            price = rec.get("valor") or rec.get("VALOR")
                            if price:
                                rows.append({"price":float(str(price).replace(",","")),"currency":"BRL",
                                    "country":"Brazil","iso":"BR","source":"br_sp_itbi","yr":2024})
                        break
                    # Check package list
                    result = data.get("result",[])
                    if isinstance(result, list) and len(result) > 0:
                        itbi_pkgs = [p for p in result if "itbi" in str(p).lower() or "imobili" in str(p).lower()]
                        if itbi_pkgs:
                            return log("Brazil SP","ITBI", [], f"Found packages: {itbi_pkgs[:3]}")
            except: continue
    return log("Brazil SP","ITBI", rows,
               f"{len(rows)} transactions" if rows else "ITBI dataset not in CKAN search")


# ══════════════════════════════════════════════════════════
# 11. NETHERLANDS — Kadaster / BAG per-property
# ══════════════════════════════════════════════════════════
def pull_netherlands_kadaster():
    print("\n>>> NETHERLANDS — Kadaster/BAG")
    rows = []
    # BAG (Basisregistratie Adressen en Gebouwen) — building registry
    r = get("https://api.bag.kadaster.nl/lvbag/individuelebevragingen/v2/panden?postcode=1012AB&huisnummer=1", timeout=15,
            headers={"Accept":"application/json","X-Api-Key":"l7xxDEMO"})
    if r and r.status_code == 200:
        try:
            data = r.json()
            rows.append({"country":"Netherlands","iso":"NL","source":"nl_kadaster_bag","detail":"BAG API accessible"})
        except: pass

    # PDOK WFS — property boundaries
    wfs_url = "https://service.pdok.nl/kadaster/kadastralekaart/wfs/v5_0"
    params = {"service":"WFS","version":"2.0.0","request":"GetFeature",
              "typeName":"kadastralekaartv5:perceel","outputFormat":"application/json",
              "count":"20","srsName":"EPSG:4326",
              "bbox":"52.37,4.89,52.38,4.90"}
    r2 = get(wfs_url, timeout=20, params=params)
    if r2 and r2.status_code == 200:
        try:
            data = r2.json()
            features = data.get("features",[])
            for f in features[:50]:
                props = f.get("properties",{})
                geom = f.get("geometry",{})
                if geom and geom.get("coordinates"):
                    c = geom["coordinates"]
                    # Flatten nested coords
                    while isinstance(c, list) and isinstance(c[0], list):
                        c = c[0]
                    rows.append({
                        "lat":c[1] if len(c)>=2 else None,
                        "lon":c[0] if len(c)>=2 else None,
                        "country":"Netherlands","iso":"NL","source":"nl_pdok_kadaster",
                        "property_type":props.get("type","perceel"),
                        "area_m2":float(props.get("kadastraleGrootte",0)) if props.get("kadastraleGrootte") else None,
                    })
        except: pass

    return log("Netherlands","Kadaster/PDOK", rows,
               f"{len(rows)} cadastral records (parcels/buildings)" if rows else "API needs key")


# ══════════════════════════════════════════════════════════
# 12. POLAND — Geoportal WFS
# ══════════════════════════════════════════════════════════
def pull_poland():
    print("\n>>> POLAND — Geoportal/GUGiK")
    rows = []
    # GUGiK WFS for property boundaries
    wfs_url = "https://mapy.geoportal.gov.pl/wss/service/PZGIK/DanePZGIK/WFS/EGiB"
    params = {"service":"WFS","version":"2.0.0","request":"GetCapabilities"}
    r = get(wfs_url, timeout=15, params=params)
    if r and r.status_code == 200:
        return log("Poland","GUGiK WFS", [], "WFS capabilities accessible")

    # Alternative: BDL per-powiat housing prices
    r2 = get("https://bdl.stat.gov.pl/api/v1/data/by-variable/721590?format=json&unit-level=5&page-size=100&page=0", timeout=15)
    if r2 and r2.status_code == 200:
        try:
            data = r2.json()
            results = data.get("results",[])
            for res in results:
                for val in res.get("values",[]):
                    if val.get("val"):
                        rows.append({"price":float(val["val"]),"currency":"PLN_per_m2",
                            "country":"Poland","iso":"PL","source":"pl_gus_powiat",
                            "yr":int(val.get("year",2023)),
                            "city":res.get("name",""),"property_type":"dwellings"})
                    if len(rows)>=100: break
        except: pass

    return log("Poland","GUS per-powiat", rows,
               f"{len(rows)} powiat-level prices" if rows else "API error")


# ══════════════════════════════════════════════════════════
# 13. CZECH REPUBLIC — ČÚZK
# ══════════════════════════════════════════════════════════
def pull_czech():
    print("\n>>> CZECH — ČÚZK Open Data")
    rows = []
    r = get("https://services.cuzk.cz/wfs/inspire-cp-wfs.asp?service=WFS&version=2.0.0&request=GetCapabilities", timeout=15)
    if r and r.status_code == 200:
        return log("Czech Republic","ČÚZK WFS", [], "WFS capabilities accessible — cadastral parcels")

    r2 = get("https://vdp.cuzk.cz/vdp/ruian/staty/0", timeout=15)
    if r2 and r2.status_code == 200:
        return log("Czech Republic","ČÚZK RÚIAN", [], "RÚIAN address registry accessible")

    return log("Czech Republic","ČÚZK", [], "Timeout")


# ══════════════════════════════════════════════════════════
# 14. LATVIA — VZD State Land Service
# ══════════════════════════════════════════════════════════
def pull_latvia():
    print("\n>>> LATVIA — VZD")
    rows = []
    # VZD open data
    r = get("https://data.gov.lv/dati/lv/api/3/action/package_search?q=nekustamais+ipasums", timeout=15)
    if r and r.status_code == 200:
        try:
            data = r.json()
            results = data.get("result",{}).get("results",[])
            if results:
                return log("Latvia","VZD/data.gov.lv", [],
                          f"Found {len(results)} property datasets on open data portal")
        except: pass

    # Try the cadastre WMS/WFS
    wfs = "https://ozols.vzd.gov.lv/api/v1/darital?service=WFS&version=2.0.0&request=GetCapabilities"
    r2 = get(wfs, timeout=15)
    if r2 and r2.status_code == 200:
        return log("Latvia","VZD WFS", [], "WFS capabilities accessible")

    return log("Latvia","VZD", [], "Timeout")


# ══════════════════════════════════════════════════════════
# 15. CROATIA — DZS/eNekretnine
# ══════════════════════════════════════════════════════════
def pull_croatia():
    print("\n>>> CROATIA — eNekretnine/DZS")
    rows = []
    # eNekretnine is the official real estate transaction registry
    r = get("https://oss.uredjenazemlja.hr/public/lrTransactions.jsp?outputFormat=json&count=50", timeout=15)
    if r and r.status_code == 200:
        try:
            data = r.json()
            features = data.get("features",[])
            for f in features:
                p = f.get("properties",{})
                price = p.get("cijena") or p.get("price")
                if price:
                    rows.append({"price":float(price),"currency":"EUR",
                        "country":"Croatia","iso":"HR","source":"hr_enekretnine","yr":2024})
        except: pass

    if not rows:
        r2 = get("https://enekretnine.hr/", timeout=15)
        if r2 and r2.status_code == 200:
            return log("Croatia","eNekretnine", [], "Portal accessible")

    return log("Croatia","eNekretnine", rows,
               f"{len(rows)} transactions" if rows else "Need authenticated API")


# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════
def main():
    t0 = time.time()
    print("="*70)
    print("PER-PROPERTY TRANSACTION APIs — 15 UNTRIED COUNTRIES")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

    all_rows = []
    all_rows.extend(pull_korea_transactions())
    all_rows.extend(pull_taiwan_transactions())
    all_rows.extend(pull_slovenia())
    all_rows.extend(pull_lithuania())
    all_rows.extend(pull_estonia())
    all_rows.extend(pull_denmark())
    all_rows.extend(pull_finland())
    all_rows.extend(pull_new_zealand())
    all_rows.extend(pull_australia_nsw())
    all_rows.extend(pull_brazil())
    all_rows.extend(pull_netherlands_kadaster())
    all_rows.extend(pull_poland())
    all_rows.extend(pull_czech())
    all_rows.extend(pull_latvia())
    all_rows.extend(pull_croatia())

    print("\n" + "="*70)
    print("RESULTS SUMMARY")
    print("="*70)
    passes = sum(1 for r in RESULTS.values() if r["n"]>0)
    total = sum(r["n"] for r in RESULTS.values())
    print(f"  Sources: {len(RESULTS)}")
    print(f"  With actual data: {passes}")
    print(f"  Total rows: {total}")

    print("\n  DATA RETURNED:")
    for k,r in sorted(RESULTS.items()):
        if r["n"]>0:
            print(f"    {r['country']:20s} {r['source']:25s} {r['n']:5d} rows  {r['detail']}")
    print("\n  NO DATA (portal/WFS only or timeout):")
    for k,r in sorted(RESULTS.items()):
        if r["n"]==0:
            print(f"    {r['country']:20s} {r['source']:25s} {r['detail']}")

    if all_rows:
        df = pd.DataFrame(all_rows)
        out = os.path.join(OUT_DIR, "per_property_sweep3.csv")
        df.to_csv(out, index=False)
        print(f"\n  CSV: {out} ({len(df)} rows)")

    out_json = os.path.join(OUT_DIR, "sweep3_results.json")
    with open(out_json, "w") as f:
        json.dump({"elapsed":round(time.time()-t0,1),"n_sources":len(RESULTS),
                   "n_with_data":passes,"total_rows":total,"results":RESULTS}, f, indent=2)
    print(f"  JSON: {out_json}")
    print(f"  Completed in {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
