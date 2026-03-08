"""
Sweep 6: Actually Download Everything Found in Catalogs
=========================================================
Follow through on ALL catalog entries and bulk downloads
that were discovered but never actually downloaded.
"""
import requests, json, time, csv, io, gzip, os, zipfile
import pandas as pd, numpy as np

OUT_DIR = os.path.dirname(__file__)
H = {"User-Agent": "Mozilla/5.0 Properlytic/10.0 (Research)"}
RESULTS = {}

def get(url, timeout=30, **kw):
    try:
        h = kw.pop("headers", H)
        return requests.get(url, timeout=timeout, headers=h, allow_redirects=True, **kw)
    except Exception as e:
        return None

def log(country, src, rows, detail):
    RESULTS[f"{country}|{src}"] = {"country":country,"source":src,"n":len(rows),"detail":detail}
    tag = "PASS" if rows else "FAIL"
    print(f"  [{tag}] {country}: {len(rows)} rows — {detail}")
    return rows

def try_ckan_package(base_url, package_id, country, source, timeout=20):
    """Fetch CKAN package metadata → find resources → download first CSV/JSON."""
    print(f"  Fetching CKAN package: {package_id}")
    r = get(f"{base_url}/api/3/action/package_show?id={package_id}", timeout=timeout)
    if not r or r.status_code != 200:
        return []
    try:
        pkg = r.json().get("result", {})
    except:
        return []
    
    resources = pkg.get("resources", [])
    print(f"  Found {len(resources)} resources in package '{pkg.get('title','')}'")
    
    rows = []
    for res in resources:
        fmt = (res.get("format","") or "").lower()
        url = res.get("url","")
        name = res.get("name","") or res.get("description","")
        size = res.get("size")
        print(f"    Resource: {name[:60]} [{fmt}] {f'{size//1024}KB' if size else '?'} → {url[:80]}")
        
        if fmt not in ["csv","json","geojson","xlsx","zip","tsv","txt"] or not url:
            continue
        
        # Attempt download (30s timeout, max 5MB)
        r2 = get(url, timeout=30, stream=True)
        if not r2 or r2.status_code != 200:
            print(f"      Download: HTTP {r2.status_code if r2 else 'timeout'}")
            continue
        
        content = b""
        for chunk in r2.iter_content(256*1024):
            content += chunk
            if len(content) > 5*1024*1024:
                break
        print(f"      Downloaded: {len(content)//1024}KB")
        
        if len(content) < 100:
            continue
            
        # Try to parse
        if fmt == "csv" or fmt == "tsv" or fmt == "txt":
            text = content.decode("utf-8", errors="replace")
            sep = "\t" if fmt == "tsv" else ","
            try:
                reader = csv.DictReader(io.StringIO(text), delimiter=sep)
                fields = reader.fieldnames or []
                print(f"      Columns: {fields[:10]}")
                n = 0
                for rec in reader:
                    # Look for any price-like column
                    price = None
                    for k in ["ASSESSED_VALUE","assessed_value","LAND_VALUE","land_value",
                              "TOTAL_VALUE","total_value","SALE_PRICE","sale_price",
                              "price","Price","valor","VALOR","value","VALUE",
                              "purchase_price","PURCHASE_PRICE","FMV","fmv",
                              "PROPERTY_VALUE","property_value","amount","AMOUNT",
                              "CONSIDERATION","consideration","soldPrice"]:
                        v = rec.get(k)
                        if v:
                            try:
                                pv = float(str(v).replace(",","").replace("$","").strip())
                                if pv > 0:
                                    price = pv
                                    break
                            except: pass
                    
                    row = {"country":country,"source":source,"price":price}
                    # Extract lat/lon
                    for lk in ["lat","LAT","latitude","LATITUDE","y","Y"]:
                        if rec.get(lk):
                            try: row["lat"] = float(rec[lk])
                            except: pass
                    for lk in ["lon","LON","lng","LNG","longitude","LONGITUDE","x","X"]:
                        if rec.get(lk):
                            try: row["lon"] = float(rec[lk])
                            except: pass
                    # Area
                    for ak in ["area","AREA","area_m2","LOT_SIZE","lot_size","size","SIZE",
                               "TOTAL_AREA","total_area","LAND_SIZE","land_size"]:
                        if rec.get(ak):
                            try: row["area_m2"] = float(str(rec[ak]).replace(",",""))
                            except: pass
                    # City/address
                    for ck in ["city","CITY","municipality","MUNICIPALITY","suburb","SUBURB",
                               "address","ADDRESS","district","DISTRICT","region","REGION","location"]:
                        if rec.get(ck):
                            row["city"] = str(rec[ck])[:50]
                            break
                    
                    if price or row.get("lat"):
                        rows.append(row)
                        n += 1
                    if n >= 300:
                        break
                print(f"      Parsed: {n} rows with data")
            except Exception as e:
                print(f"      CSV parse error: {e}")
                
        elif fmt in ["json","geojson"]:
            try:
                data = json.loads(content)
                features = data.get("features",[]) if isinstance(data,dict) else (data if isinstance(data,list) else [])
                n = 0
                for f in features[:300]:
                    props = f.get("properties",{}) if isinstance(f,dict) and "properties" in f else f
                    price = None
                    for k in ["price","value","ASSESSED_VALUE","sale_price","amount","valor"]:
                        v = props.get(k)
                        if v:
                            try:
                                price = float(str(v).replace(",",""))
                                if price > 0: break
                            except: pass
                    geom = f.get("geometry",{}) if isinstance(f,dict) else {}
                    coords = geom.get("coordinates",[])
                    while isinstance(coords,list) and coords and isinstance(coords[0],list):
                        coords = coords[0]
                    row = {"country":country,"source":source,"price":price}
                    if len(coords) >= 2:
                        row["lat"] = coords[1]
                        row["lon"] = coords[0]
                    if price or row.get("lat"):
                        rows.append(row)
                        n += 1
                print(f"      Parsed: {n} records")
            except Exception as e:
                print(f"      JSON parse error: {e}")
                
        elif fmt == "zip":
            try:
                zf = zipfile.ZipFile(io.BytesIO(content))
                names = zf.namelist()
                print(f"      ZIP contents: {names[:5]}")
                for zn in names:
                    if zn.lower().endswith(".csv"):
                        with zf.open(zn) as cf:
                            text = cf.read().decode("utf-8",errors="replace")
                            reader = csv.DictReader(io.StringIO(text))
                            n = 0
                            for rec in reader:
                                price = None
                                for k in ["price","value","ASSESSED_VALUE","sale_price","amount","valor","PRECIO"]:
                                    v = rec.get(k)
                                    if v:
                                        try:
                                            price = float(str(v).replace(",",""))
                                            if price > 0: break
                                        except: pass
                                if price:
                                    rows.append({"country":country,"source":source,"price":price})
                                    n += 1
                                if n >= 300: break
                            print(f"      {zn}: {n} rows")
                            break
            except Exception as e:
                print(f"      ZIP error: {e}")
        
        if rows:
            break  # Got data from first working resource
    
    return rows


# ══════════════════════════════════════════════════════════
# 1. CANADA BC — All 5 found packages
# ══════════════════════════════════════════════════════════
def try_canada_bc():
    print("\n>>> CANADA BC — Downloading Assessment Data")
    base = "https://catalogue.data.gov.bc.ca"
    packages = [
        "bc-assessment-property-sales-view",
        "property-transfer-tax-data-2019",
        "bc-assessment-property-values-view",
        "bc-assessment-property-descriptions-view",
        "metadata-for-bc-property-assessment",
    ]
    for pkg_id in packages:
        rows = try_ckan_package(base, pkg_id, "Canada BC", f"bc_{pkg_id}")
        if rows:
            return log("Canada BC", pkg_id, rows, f"{len(rows)} records from {pkg_id}")
    
    # Try direct search for downloadable resources
    r = get(f"{base}/api/3/action/package_search?q=property+assessment&rows=5", timeout=20)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            for pkg in results:
                pid = pkg.get("name","")
                resources = pkg.get("resources",[])
                for res in resources:
                    fmt = (res.get("format","") or "").lower()
                    url = res.get("url","")
                    if fmt in ["csv","json","geojson","zip"] and url:
                        print(f"  Trying: {pid} → {res.get('name','')[:40]} [{fmt}]")
                        r2 = get(url, timeout=30, stream=True)
                        if r2 and r2.status_code == 200 and len(r2.content) > 500:
                            # parse CSV
                            text = r2.content[:3*1024*1024].decode("utf-8",errors="replace")
                            rows = []
                            try:
                                reader = csv.DictReader(io.StringIO(text))
                                print(f"    Columns: {reader.fieldnames[:8]}")
                                for rec in reader:
                                    for k in ["ASSESSED_VALUE","LAND_VALUE","TOTAL_VALUE","sale_price","SALE_PRICE"]:
                                        v = rec.get(k)
                                        if v:
                                            try:
                                                pv = float(str(v).replace(",","").replace("$",""))
                                                if pv > 0:
                                                    rows.append({"price":pv,"currency":"CAD",
                                                        "country":"Canada","iso":"CA","source":"ca_bc"})
                                                    break
                                            except: pass
                                    if len(rows) >= 200: break
                            except: pass
                            if rows:
                                return log("Canada BC", pid, rows, f"{len(rows)} from {pid}")
        except: pass
    return log("Canada BC","Assessment",[],"Catalog found but resources not directly downloadable")


# ══════════════════════════════════════════════════════════
# 2. AUSTRALIA NSW — Valuer General
# ══════════════════════════════════════════════════════════
def try_australia_nsw():
    print("\n>>> AUSTRALIA NSW — Valuer General Downloads")
    base = "https://data.nsw.gov.au"
    # Search for the actual package
    r = get(f"{base}/data/api/3/action/package_search?q=valuer+general+property+sales&rows=5", timeout=20)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            for pkg in results:
                rows = try_ckan_package(base+"/data", pkg.get("name",""), "Australia NSW", "nsw_vg")
                if rows:
                    return log("Australia NSW", "VG", rows, f"{len(rows)} property sales")
        except: pass
    
    # Try direct datastore search
    r2 = get(f"{base}/data/api/3/action/datastore_search?q=property+sales&limit=100", timeout=20)
    if r2 and r2.status_code == 200:
        try:
            records = r2.json().get("result",{}).get("records",[])
            if records:
                rows = []
                for rec in records[:200]:
                    price = rec.get("purchase_price") or rec.get("PURCHASE_PRICE") or rec.get("consideration")
                    if price:
                        rows.append({"price":float(str(price).replace(",","")),"currency":"AUD",
                            "country":"Australia","iso":"AU","source":"au_nsw_vg",
                            "city":rec.get("suburb","")})
                if rows:
                    return log("Australia NSW","VG datastore",rows,f"{len(rows)} sales")
        except: pass
    return log("Australia NSW","VG",[],"Package search returned no downloadable resources")


# ══════════════════════════════════════════════════════════
# 3. AUSTRALIA QLD — Valuation Property Boundaries
# ══════════════════════════════════════════════════════════
def try_australia_qld():
    print("\n>>> AUSTRALIA QLD — Valuation Boundaries")
    base = "https://data.qld.gov.au"
    # Search for valuation datasets
    r = get(f"{base}/api/3/action/package_search?q=valuation+property+boundaries&rows=5", timeout=20)
    if r and r.status_code == 200:
        try:
            results = r.json().get("result",{}).get("results",[])
            for pkg in results:
                rows = try_ckan_package(base, pkg.get("name",""), "Australia QLD", "qld_val")
                if rows:
                    return log("Australia QLD","Valuations",rows,f"{len(rows)} valuations")
        except: pass
    return log("Australia QLD","Valuations",[],"No downloadable resources found")


# ══════════════════════════════════════════════════════════
# 4. TAIWAN — All 4 cities, multiple seasons
# ══════════════════════════════════════════════════════════
def try_taiwan():
    print("\n>>> TAIWAN — Real Price Registry Bulk CSVs (All Cities)")
    rows = []
    cities = {"a":"Taipei","b":"Taichung","e":"Kaohsiung","f":"New Taipei",
              "c":"Keelung","d":"Tainan","g":"Yilan","h":"Hsinchu"}
    seasons = ["113S3","113S2","113S1","112S4","112S3","112S2","112S1"]
    types = ["a","b"]  # a=sales, b=presale
    
    for season in seasons:
        if len(rows) >= 200: break
        for city_code, city_name in cities.items():
            if len(rows) >= 200: break
            for sale_type in types:
                url = f"https://plvr.land.moi.gov.tw/DownloadSeason?season={season}&type={sale_type}&fileName={city_code}_lvr_land_{sale_type}.csv"
                r = get(url, timeout=20)
                if not r or r.status_code != 200 or len(r.content) < 200:
                    continue
                try:
                    text = r.content.decode("utf-8", errors="replace")
                    lines = text.strip().split("\n")
                    if len(lines) < 3:
                        continue
                    # Parse CSV: skip header rows (Chinese + English)
                    # Try to find price column by examining first data row
                    header_row = lines[0]
                    # The CSV typically has columns separated by commas
                    for start_row in [1, 2]:  # Try skipping 1 or 2 header rows
                        try:
                            reader = csv.reader(io.StringIO("\n".join(lines[start_row:])))
                            header = lines[start_row-1].split(",") if start_row > 0 else []
                            for data_row in reader:
                                if len(data_row) < 3:
                                    continue
                                # Try each column for a price-like value (large number)
                                for i, val in enumerate(data_row):
                                    val_clean = val.replace(",","").strip()
                                    if val_clean.isdigit() and len(val_clean) >= 5:
                                        price = int(val_clean)
                                        if price >= 100000:  # At least 100K TWD
                                            row = {
                                                "price":price,"currency":"TWD",
                                                "country":"Taiwan","iso":"TW","source":"tw_real_price",
                                                "city":city_name,
                                                "yr":int(season[:3])+1911,
                                                "property_type":"sale" if sale_type=="a" else "presale",
                                            }
                                            # Try to get area (usually a smaller number with decimal)
                                            for j, v2 in enumerate(data_row):
                                                if j != i:
                                                    try:
                                                        area = float(v2.replace(",",""))
                                                        if 5 < area < 5000:
                                                            row["area_m2"] = area
                                                            break
                                                    except: pass
                                            rows.append(row)
                                            break
                                if len(rows) >= 200: break
                            if rows: break
                        except: continue
                except: pass
                if rows:
                    print(f"  {city_name} {season}: {len(rows)} transactions so far")
                    break
        if rows and len(rows) > 10:
            break
    
    return log("Taiwan","Real Price CSV",rows,f"{len(rows)} transactions" if rows else "CSVs not parseable")


# ══════════════════════════════════════════════════════════
# 5-10. ALL PORTAL-FOUND CATALOGS
# ══════════════════════════════════════════════════════════
def try_portal_downloads(country, base_url, search_query, source_name):
    print(f"\n>>> {country.upper()} — Attempting Data Downloads")
    rows = []
    r = get(f"{base_url}/api/3/action/package_search?q={search_query}&rows=5", timeout=20)
    if not r or r.status_code != 200:
        # Try alternative API formats
        for alt in [
            f"{base_url}/api/datasets/1.0/search?q={search_query}&rows=5",
            f"{base_url}/api/v2/catalog/datasets?search={search_query}&limit=5",
        ]:
            r = get(alt, timeout=15)
            if r and r.status_code == 200:
                break
        if not r or r.status_code != 200:
            return log(country, source_name, [], "API timeout")
    
    try:
        data = r.json()
        # Handle CKAN format
        results = data.get("result",{}).get("results",[])
        # Handle OpenDataSoft format
        if not results:
            results = data.get("datasets",[])
        
        for pkg in results[:5]:
            pkg_id = pkg.get("name","") or pkg.get("datasetid","")
            title = pkg.get("title","") or pkg.get("metas",{}).get("default",{}).get("title","")
            print(f"  Package: {title[:60]} (id={pkg_id})")
            
            # Try CKAN resources
            resources = pkg.get("resources",[])
            for res in resources[:3]:
                fmt = (res.get("format","") or "").lower()
                url = res.get("url","")
                if fmt in ["csv","json","geojson","xlsx","zip"] and url:
                    print(f"    Downloading: {res.get('name','')[:40]} [{fmt}]")
                    r2 = get(url, timeout=30, stream=True)
                    if r2 and r2.status_code == 200:
                        content = b""
                        for chunk in r2.iter_content(256*1024):
                            content += chunk
                            if len(content) > 3*1024*1024: break
                        print(f"    Got {len(content)//1024}KB")
                        if len(content) > 200:
                            if fmt == "csv":
                                try:
                                    text = content.decode("utf-8",errors="replace")
                                    reader = csv.DictReader(io.StringIO(text))
                                    print(f"    Columns: {(reader.fieldnames or [])[:8]}")
                                    n = 0
                                    for rec in reader:
                                        for k in rec:
                                            v = rec[k]
                                            if v:
                                                try:
                                                    pv = float(str(v).replace(",","").replace("$",""))
                                                    if pv > 1000:
                                                        rows.append({"price":pv,"country":country,
                                                            "source":source_name,"detail":k})
                                                        n += 1
                                                        break
                                                except: pass
                                        if n >= 100: break
                                    print(f"    Parsed: {n} rows")
                                except: pass
                            elif fmt in ["json","geojson"]:
                                try:
                                    jd = json.loads(content)
                                    feats = jd.get("features",[]) if isinstance(jd,dict) else jd if isinstance(jd,list) else []
                                    if feats:
                                        print(f"    {len(feats)} features")
                                        rows.append({"country":country,"source":source_name,
                                                    "detail":f"{len(feats)} features in GeoJSON"})
                                except: pass
                        if rows: break
            
            # OpenDataSoft: try export
            if not rows and pkg.get("datasetid"):
                dsid = pkg["datasetid"]
                export_url = f"{base_url}/api/records/1.0/search/?dataset={dsid}&rows=100"
                r3 = get(export_url, timeout=15)
                if r3 and r3.status_code == 200:
                    try:
                        records = r3.json().get("records",[])
                        for rec in records:
                            fields = rec.get("fields",{})
                            if fields:
                                rows.append({"country":country,"source":source_name,
                                            "detail":str(list(fields.keys())[:5])})
                        if rows:
                            print(f"    OpenDataSoft: {len(rows)} records")
                    except: pass
            
            if rows: break
    except Exception as e:
        print(f"  Error: {e}")
    
    return log(country, source_name, rows,
               f"{len(rows)} records downloaded" if rows else "No downloadable data found")


# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════
def main():
    t0 = time.time()
    print("="*70)
    print("SWEEP 6: DOWNLOAD EVERYTHING FROM CONFIRMED CATALOGS")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

    all_rows = []
    
    # Bulk downloads
    all_rows.extend(try_canada_bc())
    all_rows.extend(try_australia_nsw())
    all_rows.extend(try_australia_qld())
    all_rows.extend(try_taiwan())
    
    # Portal/catalog downloads
    all_rows.extend(try_portal_downloads("Panama",
        "https://www.datosabiertos.gob.pa", "propiedad", "pa_open_data"))
    all_rows.extend(try_portal_downloads("Cambodia",
        "https://data.opendevelopmentcambodia.net", "land+price", "kh_odc"))
    all_rows.extend(try_portal_downloads("Pakistan",
        "https://opendata.com.pk", "property", "pk_open_data"))
    all_rows.extend(try_portal_downloads("Latvia",
        "https://data.gov.lv/dati/lv", "nekustamais+ipasums", "lv_data_gov"))
    all_rows.extend(try_portal_downloads("Dominican Republic",
        "https://datos.gob.do", "catastro", "do_datos"))
    all_rows.extend(try_portal_downloads("Qatar",
        "https://www.data.gov.qa", "real+estate", "qa_data_gov"))

    # Summary
    print("\n" + "="*70)
    print("SWEEP 6 RESULTS — ACTUAL DOWNLOADS")
    print("="*70)
    passes = sum(1 for r in RESULTS.values() if r["n"]>0)
    total = sum(r["n"] for r in RESULTS.values())
    print(f"  Sources attempted: {len(RESULTS)}")
    print(f"  With actual data: {passes}")
    print(f"  Total rows: {total}")
    
    print("\n  DATA DOWNLOADED:")
    for k,r in sorted(RESULTS.items()):
        if r["n"]>0:
            print(f"    {r['country']:20s} {r['source']:30s} {r['n']:5d} rows  {r['detail']}")
    print("\n  NO DATA:")
    for k,r in sorted(RESULTS.items()):
        if r["n"]==0:
            print(f"    {r['country']:20s} {r['source']:30s} {r['detail']}")

    if all_rows:
        df = pd.DataFrame(all_rows)
        out = os.path.join(OUT_DIR, "sweep6_downloads.csv")
        df.to_csv(out, index=False)
        print(f"\n  CSV: {out} ({len(df)} rows)")

    out_json = os.path.join(OUT_DIR, "sweep6_results.json")
    with open(out_json, "w") as f:
        json.dump({"elapsed":round(time.time()-t0,1),"n_sources":len(RESULTS),
                   "n_with_data":passes,"total_rows":total,"results":RESULTS}, f, indent=2)
    print(f"  JSON: {out_json}")
    print(f"  Completed in {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
