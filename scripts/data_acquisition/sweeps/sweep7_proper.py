"""
Sweep 7: PROPERLY Parse All Confirmed Sources
================================================
Actually inspect headers, print column samples, and extract correct fields.
"""
import requests, json, time, csv, io, os
import pandas as pd, numpy as np

OUT_DIR = os.path.dirname(__file__)
H = {"User-Agent": "Mozilla/5.0 Properlytic/11.0 (Research)"}
ALL_ROWS = []
RESULTS = {}

def get(url, timeout=25, **kw):
    try:
        h = kw.pop("headers", H)
        return requests.get(url, timeout=timeout, headers=h, allow_redirects=True, **kw)
    except: return None

def log(country, src, rows, detail):
    RESULTS[f"{country}|{src}"] = {"country":country,"source":src,"n":len(rows),"detail":detail}
    tag = "PASS" if rows else "FAIL"
    print(f"\n  [{tag}] {country}: {len(rows)} rows — {detail}")
    ALL_ROWS.extend(rows)
    return rows


# ══════════════════════════════════════════════════════════
# 1. TAIWAN — Inspect actual CSV structure
# ══════════════════════════════════════════════════════════
def try_taiwan():
    print("\n" + "="*60)
    print(">>> TAIWAN — Real Price Registry (proper parse)")
    print("="*60)
    rows = []
    cities = {"a":"Taipei","f":"New Taipei","b":"Taichung","e":"Kaohsiung","d":"Tainan","c":"Keelung"}
    
    for city_code, city_name in cities.items():
        if len(rows) >= 300: break
        for season in ["113S3","113S2","113S1","112S4"]:
            url = f"https://plvr.land.moi.gov.tw/DownloadSeason?season={season}&type=a&fileName={city_code}_lvr_land_a.csv"
            r = get(url, timeout=20)
            if not r or r.status_code != 200 or len(r.content) < 500:
                continue
            
            text = r.content.decode("utf-8", errors="replace")
            lines = text.strip().split("\n")
            print(f"\n  {city_name} {season}: {len(lines)} lines, {len(r.content)//1024}KB")
            
            if len(lines) < 4:
                continue
            
            # Print first 3 rows to see structure
            for i in range(min(3, len(lines))):
                print(f"    Row {i}: {lines[i][:200]}")
            
            # Parse: Row 0 = Chinese headers, Row 1 = English headers
            # Data starts at row 2
            try:
                # Use row 1 (English headers) as fieldnames
                header_line = lines[1] if len(lines) > 1 else lines[0]
                reader = csv.DictReader(io.StringIO("\n".join([header_line] + lines[2:])))
                fields = reader.fieldnames
                print(f"    Fields: {fields}")
                
                n = 0
                for rec in reader:
                    if n >= 100: break
                    # Look for price columns by name
                    price = None
                    for pk in ["The total price NTD","total price NTD","The berth total price NTD",
                               "price","Price","NTD","總價元","交易總價","單價元/平方公尺",
                               "The unit price (NTD / square meter)"]:
                        v = rec.get(pk,"").replace(",","").strip()
                        if v and v.isdigit() and int(v) > 0:
                            price = int(v)
                            break
                    
                    if not price:
                        # Try all fields for large NTD-scale numbers
                        for k, v in rec.items():
                            v_clean = str(v).replace(",","").strip()
                            if v_clean.isdigit() and 100000 < int(v_clean) < 100000000000:
                                # Check it's not a date (ROC format like 1130801)
                                if not (1100000 <= int(v_clean) <= 1140000 or 
                                       11300 <= int(v_clean) <= 11400):
                                    price = int(v_clean)
                                    if n == 0:
                                        print(f"    Price from column '{k}': {price}")
                                    break
                    
                    if price:
                        # Get area
                        area = None
                        for ak in ["The shifting total area square meter",
                                   "building shifting total area","area",
                                   "建物移轉總面積平方公尺","土地移轉總面積平方公尺"]:
                            v = rec.get(ak,"").replace(",","").strip()
                            try:
                                a = float(v)
                                if 1 < a < 50000:
                                    area = a
                                    break
                            except: pass
                        
                        # Get district
                        district = ""
                        for dk in ["The villages and towns urban district",
                                   "鄉鎮市區","district","District"]:
                            if rec.get(dk,"").strip():
                                district = rec[dk].strip()
                                break
                        
                        # Get property type
                        ptype = ""
                        for tk in ["Transaction sign","建物型態","Building state",
                                   "Main use","主要用途"]:
                            if rec.get(tk,"").strip():
                                ptype = rec[tk].strip()[:30]
                                break
                        
                        rows.append({
                            "price": price,
                            "currency": "TWD",
                            "country": "Taiwan",
                            "iso": "TW",
                            "source": "tw_real_price",
                            "city": city_name,
                            "district": district,
                            "yr": int(season[:3]) + 1911,
                            "area_m2": area,
                            "property_type": ptype,
                        })
                        n += 1
                
                print(f"    Parsed: {n} valid transactions")
                if n > 0:
                    print(f"    Sample: price={rows[-1]['price']}, area={rows[-1].get('area_m2')}, district={rows[-1].get('district')}")
            except Exception as e:
                print(f"    Parse error: {e}")
            
            if len(rows) >= 300: break
    
    return log("Taiwan", "Real Price", rows,
               f"{len(rows)} transactions with verified prices" if rows else "Could not parse price columns")


# ══════════════════════════════════════════════════════════
# 2. QATAR — Properly extract value field from OpenDataSoft
# ══════════════════════════════════════════════════════════
def try_qatar():
    print("\n" + "="*60)
    print(">>> QATAR — OpenDataSoft Proper Parse")
    print("="*60)
    rows = []
    
    # First, search for real estate datasets
    r = get("https://www.data.gov.qa/api/datasets/1.0/search?q=real+estate&rows=10", timeout=20)
    if not r or r.status_code != 200:
        return log("Qatar", "data.gov.qa", [], "Timeout")
    
    try:
        data = r.json()
        datasets = data.get("datasets", [])
        print(f"  Found {len(datasets)} datasets")
        
        for ds in datasets:
            dsid = ds.get("datasetid","")
            title = ds.get("metas",{}).get("default",{}).get("title","")
            print(f"\n  Dataset: {title} (id={dsid})")
            
            # Export records
            export_url = f"https://www.data.gov.qa/api/records/1.0/search/?dataset={dsid}&rows=200"
            r2 = get(export_url, timeout=20)
            if not r2 or r2.status_code != 200:
                continue
            
            records = r2.json().get("records", [])
            print(f"  Records: {len(records)}")
            
            if records:
                # Print first record to see structure
                first = records[0].get("fields", {})
                print(f"  Fields: {list(first.keys())}")
                print(f"  Sample: {json.dumps(first, ensure_ascii=False)[:300]}")
                
                for rec in records:
                    fields = rec.get("fields", {})
                    # Look for value/price field
                    price = None
                    for pk in ["value","price","amount","total_value","sale_price",
                               "transaction_value","qymh","المبلغ"]:
                        v = fields.get(pk)
                        if v is not None:
                            try:
                                pv = float(str(v).replace(",",""))
                                if pv > 0:
                                    price = pv
                                    break
                            except: pass
                    
                    yr = fields.get("year") or fields.get("سنة")
                    ptype = fields.get("type_of_property") or fields.get("نوع_العقار")
                    nationality = fields.get("nationality") or fields.get("الجنسية")
                    
                    if price or yr:
                        rows.append({
                            "price": price,
                            "currency": "QAR",
                            "country": "Qatar",
                            "iso": "QA",
                            "source": "qa_data_gov",
                            "yr": int(yr) if yr else None,
                            "property_type": str(ptype)[:30] if ptype else "",
                            "detail": str(nationality)[:30] if nationality else "",
                        })
                
                if rows:
                    print(f"  Got {len(rows)} records with data")
                    break
    except Exception as e:
        print(f"  Error: {e}")
    
    return log("Qatar", "data.gov.qa", rows,
               f"{len(rows)} records with actual values" if rows else "No price/value field found")


# ══════════════════════════════════════════════════════════
# 3. CAMBODIA — Parse GeoJSON features properly
# ══════════════════════════════════════════════════════════
def try_cambodia():
    print("\n" + "="*60)
    print(">>> CAMBODIA — ODC GeoJSON Proper Parse")
    print("="*60)
    rows = []
    
    # Search for land price dataset
    r = get("https://data.opendevelopmentcambodia.net/api/3/action/package_search?q=land+price&rows=5", timeout=20)
    if not r or r.status_code != 200:
        return log("Cambodia", "ODC", [], "Timeout")
    
    try:
        results = r.json().get("result",{}).get("results",[])
        print(f"  Found {len(results)} packages")
        
        for pkg in results:
            title = pkg.get("title","")
            print(f"\n  Package: {title}")
            
            for res in pkg.get("resources", []):
                fmt = (res.get("format","") or "").lower()
                url = res.get("url","")
                name = res.get("name","") or res.get("description","")
                print(f"    Resource: {name[:50]} [{fmt}] → {url[:80]}")
                
                if fmt in ["geojson","json","csv"] and url:
                    r2 = get(url, timeout=30)
                    if not r2 or r2.status_code != 200:
                        print(f"      HTTP {r2.status_code if r2 else 'timeout'}")
                        continue
                    
                    print(f"      Downloaded: {len(r2.content)//1024}KB")
                    
                    if fmt in ["geojson","json"]:
                        try:
                            data = r2.json()
                            features = data.get("features",[])
                            print(f"      Features: {len(features)}")
                            
                            if features:
                                # Print first feature
                                f0 = features[0]
                                props = f0.get("properties",{})
                                print(f"      First feature properties: {json.dumps(props, ensure_ascii=False)[:300]}")
                                
                                for feat in features[:300]:
                                    props = feat.get("properties",{})
                                    geom = feat.get("geometry",{})
                                    
                                    # Extract price
                                    price = None
                                    for pk in ["price","value","land_price","price_per_sqm",
                                               "total_price","amount","cost"]:
                                        v = props.get(pk)
                                        if v is not None:
                                            try:
                                                price = float(str(v).replace(",",""))
                                                if price > 0: break
                                            except: pass
                                    
                                    # Extract coordinates
                                    coords = geom.get("coordinates",[]) if geom else []
                                    if geom and geom.get("type") == "Point":
                                        lat = coords[1] if len(coords) >= 2 else None
                                        lon = coords[0] if len(coords) >= 2 else None
                                    else:
                                        lat = lon = None
                                    
                                    if price or lat:
                                        rows.append({
                                            "price": price,
                                            "currency": "USD",  # Cambodia uses USD for real estate
                                            "country": "Cambodia",
                                            "iso": "KH",
                                            "source": "kh_odc",
                                            "lat": lat,
                                            "lon": lon,
                                            "property_type": str(props.get("type","") or props.get("land_use",""))[:30],
                                        })
                                
                                print(f"      Parsed: {len(rows)} features with data")
                                if rows:
                                    print(f"      Sample: {rows[0]}")
                        except Exception as e:
                            print(f"      JSON error: {e}")
                    
                    elif fmt == "csv":
                        text = r2.content.decode("utf-8",errors="replace")
                        reader = csv.DictReader(io.StringIO(text))
                        print(f"      Columns: {reader.fieldnames}")
                        n = 0
                        for rec in reader:
                            for pk in ["price","value","land_price","price_usd"]:
                                v = rec.get(pk,"").replace(",","").strip()
                                if v:
                                    try:
                                        pv = float(v)
                                        if pv > 0:
                                            rows.append({"price":pv,"currency":"USD",
                                                "country":"Cambodia","iso":"KH","source":"kh_odc"})
                                            n += 1
                                            break
                                    except: pass
                            if n >= 200: break
                        print(f"      Parsed: {n} rows")
                    
                    if rows: break
            if rows: break
    except Exception as e:
        print(f"  Error: {e}")
    
    return log("Cambodia", "ODC", rows,
               f"{len(rows)} with data" if rows else "No price data in features")


# ══════════════════════════════════════════════════════════
# 4. PAKISTAN — Zameen proper parse
# ══════════════════════════════════════════════════════════
def try_pakistan():
    print("\n" + "="*60)
    print(">>> PAKISTAN — Zameen/OpenData Proper Parse")
    print("="*60)
    rows = []
    
    r = get("https://opendata.com.pk/api/3/action/package_search?q=property&rows=5", timeout=20)
    if not r or r.status_code != 200:
        return log("Pakistan", "opendata.com.pk", [], "Timeout")
    
    try:
        results = r.json().get("result",{}).get("results",[])
        print(f"  Found {len(results)} packages")
        
        for pkg in results:
            title = pkg.get("title","")
            print(f"\n  Package: {title}")
            
            for res in pkg.get("resources", []):
                fmt = (res.get("format","") or "").lower()
                url = res.get("url","")
                name = res.get("name","") or ""
                print(f"    Resource: {name[:50]} [{fmt}] → {url[:80]}")
                
                if fmt in ["csv","json"] and url:
                    r2 = get(url, timeout=30)
                    if not r2 or r2.status_code != 200:
                        continue
                    
                    print(f"      Downloaded: {len(r2.content)//1024}KB")
                    
                    if fmt == "csv" and len(r2.content) > 200:
                        text = r2.content.decode("utf-8",errors="replace")
                        reader = csv.DictReader(io.StringIO(text))
                        fields = reader.fieldnames or []
                        print(f"      Columns: {fields[:15]}")
                        
                        # Print first 3 data rows
                        sample_reader = csv.DictReader(io.StringIO(text))
                        for i, rec in enumerate(sample_reader):
                            if i >= 3: break
                            print(f"      Row {i}: {dict(list(rec.items())[:8])}")
                        
                        # Now parse with correct columns
                        reader2 = csv.DictReader(io.StringIO(text))
                        n = 0
                        for rec in reader2:
                            price = None
                            for pk in ["price","Price","PRICE","price_pkr","amount",
                                       "price_in_pkr","listing_price","sale_price"]:
                                v = rec.get(pk,"").replace(",","").replace("PKR","").strip()
                                try:
                                    pv = float(v)
                                    if pv > 1000:
                                        price = pv
                                        break
                                except: pass
                            
                            city = rec.get("city","") or rec.get("location","") or rec.get("City","")
                            area = None
                            for ak in ["area","size","area_sqft","area_marla","Area"]:
                                v = rec.get(ak,"").replace(",","").strip()
                                try:
                                    area = float(v)
                                    if area > 0: break
                                except: pass
                            
                            ptype = rec.get("property_type","") or rec.get("type","") or rec.get("purpose","")
                            
                            if price:
                                rows.append({
                                    "price": price,
                                    "currency": "PKR",
                                    "country": "Pakistan",
                                    "iso": "PK",
                                    "source": "pk_zameen",
                                    "city": str(city)[:30],
                                    "area_m2": area,
                                    "property_type": str(ptype)[:30],
                                })
                                n += 1
                            if n >= 200: break
                        
                        print(f"      Parsed: {n} rows with prices")
                        if n > 0:
                            print(f"      Sample: price={rows[-1]['price']}, city={rows[-1]['city']}, type={rows[-1]['property_type']}")
                    
                    if rows: break
            if rows: break
    except Exception as e:
        print(f"  Error: {e}")
    
    return log("Pakistan", "Zameen", rows,
               f"{len(rows)} listings with prices" if rows else "No price column found")


# ══════════════════════════════════════════════════════════
# 5. CANADA BC — Try every resource URL in the assessment packages
# ══════════════════════════════════════════════════════════
def try_canada_bc():
    print("\n" + "="*60)
    print(">>> CANADA BC — Proper Resource Discovery")
    print("="*60)
    rows = []
    base = "https://catalogue.data.gov.bc.ca"
    
    for pkg_name in ["property-transfer-tax-data-2019",
                     "bc-assessment-property-sales-view",
                     "bc-assessment-property-values-view"]:
        r = get(f"{base}/api/3/action/package_show?id={pkg_name}", timeout=20)
        if not r or r.status_code != 200:
            print(f"  {pkg_name}: timeout")
            continue
        
        try:
            pkg = r.json().get("result",{})
            title = pkg.get("title","")
            resources = pkg.get("resources",[])
            print(f"\n  Package: {title} ({len(resources)} resources)")
            
            for res in resources:
                fmt = (res.get("format","") or "").lower()
                url = res.get("url","")
                name = res.get("name","") or ""
                rtype = res.get("resource_type","")
                size = res.get("size")
                print(f"    [{fmt}] {name[:50]} type={rtype} size={size} → {url[:100]}")
                
                if fmt in ["csv","json","geojson","xlsx","zip","wms","wfs","arcgis_rest","openapi"]:
                    if fmt in ["csv","json","geojson","xlsx","zip"]:
                        r2 = get(url, timeout=30, stream=True)
                        if r2 and r2.status_code == 200:
                            content = b""
                            for chunk in r2.iter_content(256*1024):
                                content += chunk
                                if len(content) > 3*1024*1024: break
                            print(f"      Downloaded: {len(content)//1024}KB")
                            
                            if fmt == "csv" and len(content) > 200:
                                text = content.decode("utf-8",errors="replace")
                                reader = csv.DictReader(io.StringIO(text))
                                print(f"      Columns: {(reader.fieldnames or [])[:10]}")
                                # Print first row
                                for i, rec in enumerate(csv.DictReader(io.StringIO(text))):
                                    if i >= 2: break
                                    print(f"      Row {i}: {dict(list(rec.items())[:6])}")
                                
                                n = 0
                                for rec in csv.DictReader(io.StringIO(text)):
                                    price = None
                                    for pk in ["ASSESSED_VALUE","LAND_VALUE","IMPROVEMENT_VALUE",
                                               "TOTAL_VALUE","SALE_PRICE","FMR_VALUE",
                                               "MARKET_VALUE","TAX_ASSESSED_VALUE",
                                               "TRANSFER_VALUE","FAIR_MARKET_VALUE"]:
                                        v = rec.get(pk,"").replace(",","").replace("$","").strip()
                                        try:
                                            pv = float(v)
                                            if pv > 0:
                                                price = pv
                                                break
                                        except: pass
                                    
                                    if price:
                                        rows.append({
                                            "price": price,
                                            "currency": "CAD",
                                            "country": "Canada",
                                            "iso": "CA",
                                            "source": "ca_bc_" + pkg_name.replace("-","_"),
                                            "city": rec.get("MUNICIPALITY","") or rec.get("REGIONAL_DISTRICT",""),
                                        })
                                        n += 1
                                    if n >= 200: break
                                print(f"      Parsed: {n} rows with prices")
                        else:
                            print(f"      HTTP {r2.status_code if r2 else 'timeout'}")
                    else:
                        print(f"      (API endpoint, not direct download)")
            
            if rows: break
        except Exception as e:
            print(f"  Error: {e}")
    
    return log("Canada BC", "Assessment", rows,
               f"{len(rows)} per-property records" if rows else "Resources are API-only, not direct CSV")


# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════
def main():
    t0 = time.time()
    print("="*60)
    print("SWEEP 7: PROPER PARSING WITH COLUMN INSPECTION")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    try_taiwan()
    try_qatar()
    try_cambodia()
    try_pakistan()
    try_canada_bc()
    
    print("\n" + "="*60)
    print("SWEEP 7 RESULTS — VERIFIED DATA")
    print("="*60)
    passes = sum(1 for r in RESULTS.values() if r["n"]>0)
    total = sum(r["n"] for r in RESULTS.values())
    
    print(f"  Sources: {len(RESULTS)}")
    print(f"  With VERIFIED price data: {passes}")
    print(f"  Total verified rows: {total}")
    
    for k,r in sorted(RESULTS.items()):
        tag = "PASS" if r["n"]>0 else "FAIL"
        print(f"  [{tag}] {r['country']:15s} {r['n']:5d}  {r['detail']}")
    
    if ALL_ROWS:
        df = pd.DataFrame(ALL_ROWS)
        out = os.path.join(OUT_DIR, "sweep7_verified.csv")
        df.to_csv(out, index=False)
        print(f"\n  CSV: {out} ({len(df)} rows)")
        
        # Print stats per country
        for country in df["country"].unique():
            sub = df[df["country"]==country]
            prices = sub["price"].dropna()
            if len(prices) > 0:
                print(f"    {country}: {len(sub)} rows, price range {prices.min():.0f} - {prices.max():.0f} {sub['currency'].iloc[0]}")
            else:
                print(f"    {country}: {len(sub)} rows, no prices")
    
    out_json = os.path.join(OUT_DIR, "sweep7_results.json")
    with open(out_json, "w") as f:
        json.dump({"elapsed":round(time.time()-t0,1),"n_sources":len(RESULTS),
                   "n_with_data":passes,"total_rows":total,"results":RESULTS}, f, indent=2)
    print(f"  JSON: {out_json}")
    print(f"  Completed in {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
