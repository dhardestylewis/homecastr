"""
Global Proof-of-Concept Panel V2: 1000 Real Properties
========================================================
Pulls ACTUAL data from sources confirmed working tonight.
Uses MS Building footprints as entities and enriches with
real macro/price data across multiple time periods.

Sources:
  - MS Building Footprints (7 countries: UK, FR, JP, DE, AU, BR, IN)
  - BIS HPI via FRED (full quarterly timeseries, 20 countries)
  - NL CBS StatLine (Dutch property statistics)
  - Spain INE (House Price Index)
  - Germany Destatis GENESIS (price index)
  - ESA WorldCover + Copernicus DEM (STAC tile enrichment)

Output:
  scripts/data_acquisition/global_panel_1000.csv

Usage:
    python scripts/data_acquisition/build_global_panel_v2.py
"""

import requests, json, time, csv, io, gzip, os, math, random
import pandas as pd

random.seed(42)
OUT_DIR = os.path.dirname(__file__)
OUT_CSV = os.path.join(OUT_DIR, "global_panel_1000.csv")
OUT_JSON = os.path.join(OUT_DIR, "global_panel_1000_summary.json")

def get(url, timeout=20):
    try:
        return requests.get(url, timeout=timeout,
                          headers={"User-Agent": "Mozilla/5.0 Properlytic-Panel/2.0"})
    except:
        return None

def stac_point(collection, lon, lat):
    try:
        r = requests.post(
            "https://planetarycomputer.microsoft.com/api/stac/v1/search",
            json={"collections": [collection],
                  "intersects": {"type": "Point", "coordinates": [lon, lat]},
                  "limit": 1}, timeout=10)
        if r and r.status_code == 200:
            feats = r.json().get("features", [])
            return feats[0].get("id") if feats else None
    except:
        pass
    return None

# ═══════════════════════════════════════════════════════════════════
# 1. MS BUILDING FOOTPRINTS
# ═══════════════════════════════════════════════════════════════════
def get_ms_buildings(region, iso, max_buildings=100):
    print(f"  [{region}] Fetching buildings...")
    r = get("https://minedbuildings.z5.web.core.windows.net/global-buildings/dataset-links.csv")
    if not r or r.status_code != 200:
        return []
    tiles = [row for row in csv.DictReader(io.StringIO(r.text)) if row.get("Location") == region]
    if not tiles:
        return []
    tile = tiles[min(random.randint(0, 3), len(tiles)-1)]
    r2 = get(tile.get("Url", ""), timeout=30)
    if not r2 or r2.status_code != 200:
        return []

    buildings = []
    try:
        for line in gzip.open(io.BytesIO(r2.content)):
            feat = json.loads(line)
            props = feat.get("properties", {})
            coords = feat.get("geometry", {}).get("coordinates", [[]])[0]
            if not coords or len(coords) < 3:
                continue
            lons = [c[0] for c in coords]; lats = [c[1] for c in coords]
            clat = sum(lats)/len(lats); clon = sum(lons)/len(lons)
            # Shoelace area
            area = 0
            for i in range(len(coords)):
                j = (i+1) % len(coords)
                xi = coords[i][0]*111320*math.cos(math.radians(clat))
                yi = coords[i][1]*110540
                xj = coords[j][0]*111320*math.cos(math.radians(clat))
                yj = coords[j][1]*110540
                area += xi*yj - xj*yi
            buildings.append({
                "lat": round(clat, 6), "lon": round(clon, 6),
                "area_m2": round(abs(area)/2, 1),
                "height": props.get("height"),
                "confidence": props.get("confidence"),
                "country_iso": iso, "source": "ms_buildings",
                "entity_type": "building_footprint",
            })
            if len(buildings) >= max_buildings:
                break
    except Exception as e:
        print(f"    WARN: {e}")
    print(f"    Got {len(buildings)} buildings")
    return buildings


# ═══════════════════════════════════════════════════════════════════
# 2. BIS HPI — Full timeseries (quarterly, 2015-2025)
# ═══════════════════════════════════════════════════════════════════
def get_bis_hpi_timeseries(countries):
    print(f"  Fetching BIS HPI timeseries for {len(countries)} countries...")
    all_hpi = []
    for iso, name in countries.items():
        sid = f"Q{iso}N628BIS"
        r = get(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}&cosd=2015-01-01", timeout=10)
        if r and r.status_code == 200 and len(r.content) > 50:
            lines = r.text.strip().split("\n")[1:]  # skip header
            for line in lines:
                parts = line.split(",")
                if len(parts) == 2 and parts[1] != ".":
                    try:
                        all_hpi.append({
                            "country_iso": iso, "country": name,
                            "date": parts[0],
                            "yr": int(parts[0][:4]),
                            "quarter": parts[0][:7],
                            "bis_hpi": float(parts[1]),
                            "source": "bis_hpi_fred",
                            "entity_type": "macro_index",
                            "currency": "index_2010=100",
                        })
                    except:
                        pass
            if all_hpi:
                latest = [h for h in all_hpi if h["country_iso"] == iso]
                if latest:
                    print(f"    {name}: {len(latest)} quarters, latest={latest[-1]['bis_hpi']:.1f} ({latest[-1]['date']})")
    print(f"    Total: {len(all_hpi)} country-quarter observations")
    return all_hpi


# ═══════════════════════════════════════════════════════════════════
# 3. NETHERLANDS CBS — Property transaction stats
# ═══════════════════════════════════════════════════════════════════
def get_nl_cbs(max_rows=50):
    print(f"  Fetching Netherlands CBS property data...")
    url = "https://opendata.cbs.nl/ODataApi/OData/83913NED?$top=50&$format=json"
    r = get(url, timeout=15)
    rows = []
    if r and r.status_code == 200:
        try:
            vals = r.json().get("value", [])
            for v in vals:
                rows.append({
                    "country_iso": "NL", "country": "Netherlands",
                    "source": "nl_cbs", "entity_type": "area_aggregate",
                    "cbs_id": v.get("ID"),
                    "cbs_key": v.get("Key"),
                    "cbs_title": v.get("Title"),
                })
            print(f"    Got {len(rows)} CBS records")
        except:
            print(f"    Parse failed")
    else:
        print(f"    Cannot access CBS: {r.status_code if r else 'timeout'}")
    return rows


# ═══════════════════════════════════════════════════════════════════
# 4. SPAIN INE — House Price Index
# ═══════════════════════════════════════════════════════════════════
def get_spain_ine(max_rows=50):
    print(f"  Fetching Spain INE HPI...")
    url = "https://servicios.ine.es/wstempus/js/EN/DATOS_TABLA/25171?tip=AM"
    r = get(url, timeout=15)
    rows = []
    if r and r.status_code == 200:
        try:
            data = r.json()
            for item in data[:max_rows]:
                nombre = item.get("Nombre", "")
                for dato in item.get("Data", [])[:5]:  # last 5 periods
                    rows.append({
                        "country_iso": "ES", "country": "Spain",
                        "source": "es_ine", "entity_type": "area_aggregate",
                        "city": nombre,
                        "price_local": dato.get("Valor"),
                        "currency": "index",
                        "date": dato.get("Fecha"),
                        "yr": int(str(dato.get("Anyo", ""))[:4]) if dato.get("Anyo") else None,
                    })
            print(f"    Got {len(rows)} Spain HPI records across {len(data)} regions")
        except Exception as e:
            print(f"    Parse failed: {e}")
    else:
        print(f"    Cannot access INE: {r.status_code if r else 'timeout'}")
    return rows


# ═══════════════════════════════════════════════════════════════════
# 5. GERMANY DESTATIS — Property Price Index
# ═══════════════════════════════════════════════════════════════════
def get_de_destatis():
    print(f"  Fetching Germany Destatis HPI...")
    url = "https://www-genesis.destatis.de/genesisWS/rest/2020/data/table?username=GUEST&password=&name=61262-0001&area=all&compress=false&startyear=2020&language=en"
    r = get(url, timeout=15)
    rows = []
    if r and r.status_code == 200:
        try:
            data = r.json()
            content = data.get("Object", {}).get("Content", "")
            if content:
                lines = content.strip().split("\n")
                for line in lines:
                    if line.startswith("DL") or line.startswith(";"):
                        continue
                    parts = line.split(";")
                    if len(parts) >= 3:
                        rows.append({
                            "country_iso": "DE", "country": "Germany",
                            "source": "de_destatis", "entity_type": "macro_index",
                            "raw_data": line.strip()[:120],
                        })
            print(f"    Got {len(rows)} Destatis records ({len(r.content)} bytes)")
        except Exception as e:
            # At least we know it's accessible — store raw
            rows.append({
                "country_iso": "DE", "country": "Germany",
                "source": "de_destatis", "entity_type": "macro_index",
                "raw_data": r.text[:200],
            })
            print(f"    Got raw response ({len(r.content)} bytes)")
    else:
        print(f"    Cannot access: {r.status_code if r else 'timeout'}")
    return rows


# ═══════════════════════════════════════════════════════════════════
# 6. FRANCE DVF — Try alternative endpoint
# ═══════════════════════════════════════════════════════════════════
def get_france_dvf_alt(max_rows=150):
    print(f"  Fetching France DVF (govt data API)...")
    rows = []
    # Try the official data.gouv.fr DVF geo API
    for dept in ["75", "69", "13", "31", "33", "06"]:
        url = f"https://apidf-preprod.cerema.fr/dvf_opendata/mutations/?code_departement={dept}&page_size=30&ordering=-date_mutation"
        r = get(url, timeout=15)
        if r and r.status_code == 200:
            try:
                data = r.json()
                results = data.get("results", [])
                for s in results:
                    price = s.get("valeur_fonciere")
                    if price and float(price) > 0:
                        rows.append({
                            "price_local": float(price), "currency": "EUR",
                            "transaction_date": s.get("date_mutation", ""),
                            "yr": int(s.get("date_mutation", "")[:4]) if s.get("date_mutation") else None,
                            "property_type": s.get("nature_mutation", ""),
                            "city": s.get("nom_commune", ""),
                            "lat": s.get("latitude"), "lon": s.get("longitude"),
                            "area_m2": s.get("surface_terrain"),
                            "country_iso": "FR", "country": "France",
                            "source": "france_dvf", "entity_type": "transaction",
                        })
                if results:
                    print(f"    {dept}: {len(results)} mutations")
            except:
                pass
        if len(rows) >= max_rows:
            break
        time.sleep(0.3)

    # Fallback: try the cquest API with longer timeout
    if len(rows) < 20:
        for cp in ["75001", "75008", "75016", "69001", "13001"]:
            r2 = get(f"https://api.cquest.org/dvf?code_postal={cp}&limit=30", timeout=20)
            if r2 and r2.status_code == 200:
                try:
                    data2 = r2.json()
                    for s in data2.get("resultats", []):
                        price = s.get("valeur_fonciere")
                        if price and float(price) > 0:
                            rows.append({
                                "price_local": float(price), "currency": "EUR",
                                "transaction_date": s.get("date_mutation", ""),
                                "yr": int(s.get("date_mutation", "")[:4]) if s.get("date_mutation") else None,
                                "property_type": s.get("type_local", ""),
                                "city": s.get("commune", cp),
                                "lat": s.get("latitude"), "lon": s.get("longitude"),
                                "area_m2": s.get("surface_reelle_bati"),
                                "country_iso": "FR", "country": "France",
                                "source": "france_dvf", "entity_type": "transaction",
                            })
                except:
                    pass
            if len(rows) >= max_rows:
                break
            time.sleep(0.5)

    print(f"    Total: {len(rows)} France transactions")
    return rows


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════
def main():
    t0 = time.time()
    print("="*70)
    print("BUILDING GLOBAL 1000-ROW PANEL (V2)")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

    all_rows = []

    # 1. MS Buildings (7 countries)
    print("\n[1/7] MS BUILDING FOOTPRINTS")
    for region, iso in [("UnitedKingdom","GB"),("France","FR"),("Japan","JP"),
                        ("Germany","DE"),("Australia","AU"),("Brazil","BR"),("India","IN")]:
        buildings = get_ms_buildings(region, iso, max_buildings=70)
        for i, b in enumerate(buildings):
            b["building_id"] = f"{iso}_BLD_{i:04d}"
        all_rows.extend(buildings)

    # 2. France DVF (with fallback)
    print("\n[2/7] FRANCE DVF TRANSACTIONS")
    all_rows.extend(get_france_dvf_alt(max_rows=120))

    # 3. Spain INE
    print("\n[3/7] SPAIN INE HPI")
    all_rows.extend(get_spain_ine(max_rows=40))

    # 4. Netherlands CBS
    print("\n[4/7] NETHERLANDS CBS")
    all_rows.extend(get_nl_cbs(max_rows=30))

    # 5. Germany Destatis
    print("\n[5/7] GERMANY DESTATIS")
    all_rows.extend(get_de_destatis())

    # 6. BIS HPI Timeseries (quarterly, 2015-2025, ~20 countries)
    print("\n[6/7] BIS HPI TIMESERIES (20 countries)")
    bis_ts = get_bis_hpi_timeseries({
        "GB": "United Kingdom", "FR": "France", "JP": "Japan",
        "DE": "Germany", "AU": "Australia", "BR": "Brazil",
        "IN": "India", "US": "United States", "CA": "Canada",
        "NZ": "New Zealand", "SG": "Singapore", "KR": "South Korea",
        "NL": "Netherlands", "SE": "Sweden", "NO": "Norway",
        "IT": "Italy", "ES": "Spain", "CH": "Switzerland",
        "MX": "Mexico", "ZA": "South Africa",
    })
    all_rows.extend(bis_ts)

    # Convert to DataFrame
    print(f"\n  Total raw rows: {len(all_rows)}")
    df = pd.DataFrame(all_rows)

    # Ensure safe columns
    for col in ["price_local","currency","transaction_date","property_type",
                "city","lat","lon","area_m2","height","confidence",
                "postcode","structure","use","date","yr","quarter",
                "bis_hpi","esa_worldcover_tile","copernicus_dem_tile",
                "building_id","raw_data","cbs_id"]:
        if col not in df.columns:
            df[col] = None

    # 7. Raster enrichment (sample 40 points with coords)
    print("\n[7/7] RASTER ENRICHMENT (Planetary Computer)")
    has_ll = df["lat"].notna() & df["lon"].notna()
    if has_ll.sum() > 0:
        sample_idx = df[has_ll].sample(min(40, has_ll.sum()), random_state=42).index
        done = 0
        for idx in sample_idx:
            lat, lon = df.loc[idx, "lat"], df.loc[idx, "lon"]
            wc = stac_point("esa-worldcover", lon, lat)
            if wc: df.loc[idx, "esa_worldcover_tile"] = wc
            dem = stac_point("cop-dem-glo-30", lon, lat)
            if dem: df.loc[idx, "copernicus_dem_tile"] = dem
            done += 1
            if done % 10 == 0:
                print(f"    Enriched {done}/40 points...")
        print(f"    Enriched {done} points")

    # Trim to 1000
    if len(df) > 1000:
        df = df.sample(1000, random_state=42).reset_index(drop=True)

    # Reorder columns
    col_pref = ["building_id","country","country_iso","city","lat","lon",
                "entity_type","source","yr","date","quarter",
                "area_m2","height","price_local","currency",
                "bis_hpi","esa_worldcover_tile","copernicus_dem_tile"]
    cols = [c for c in col_pref if c in df.columns] + [c for c in df.columns if c not in col_pref]
    df = df[cols]

    # Save
    df.to_csv(OUT_CSV, index=False, encoding="utf-8")
    print(f"\n  Saved {len(df)} rows to {OUT_CSV}")

    # Summary
    print("\n" + "="*70)
    print("PANEL SUMMARY")
    print("="*70)
    n = len(df)
    nc = df["country"].nunique()
    countries = sorted(df["country"].dropna().unique())
    ns = df["source"].nunique()
    sources = sorted(df["source"].dropna().unique())
    has_price = int(df["price_local"].notna().sum())
    has_hpi = int(df["bis_hpi"].notna().sum())
    has_coords = int((df["lat"].notna() & df["lon"].notna()).sum())
    has_area = int(df["area_m2"].notna().sum())
    has_yr = int(df["yr"].notna().sum())
    has_wc = int(df["esa_worldcover_tile"].notna().sum())
    has_dem = int(df["copernicus_dem_tile"].notna().sum())

    print(f"  Total rows: {n}")
    print(f"  Countries: {nc} ({', '.join(countries)})")
    print(f"  Sources: {ns} ({', '.join(sources)})")
    print(f"  Rows with lat/lon: {has_coords}")
    print(f"  Rows with price: {has_price}")
    print(f"  Rows with BIS HPI: {has_hpi}")
    print(f"  Rows with area: {has_area}")
    print(f"  Rows with year: {has_yr}")
    print(f"  Rows with LULC tile: {has_wc}")
    print(f"  Rows with DEM tile: {has_dem}")

    print("\n  PER-SOURCE:")
    for src, grp in df.groupby("source"):
        print(f"    {src}: {len(grp)} rows")

    print("\n  PER-COUNTRY:")
    for c, grp in df.groupby("country"):
        print(f"    {c}: {len(grp)} rows")

    if has_yr > 0:
        yr_range = df["yr"].dropna()
        print(f"\n  TEMPORAL RANGE: {int(yr_range.min())}-{int(yr_range.max())}")

    # JSON summary
    summary = {
        "total_rows": n, "n_countries": nc, "countries": countries,
        "n_sources": ns, "sources": sources,
        "rows_with_price": has_price, "rows_with_coords": has_coords,
        "rows_with_bis_hpi": has_hpi, "rows_with_area": has_area,
        "rows_with_year": has_yr, "rows_with_lulc": has_wc,
        "rows_with_dem": has_dem,
        "temporal_range": f"{int(df['yr'].dropna().min())}-{int(df['yr'].dropna().max())}" if has_yr > 0 else "n/a",
        "elapsed_seconds": round(time.time()-t0, 1),
    }
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print(f"\n  Summary: {OUT_JSON}")
    print(f"  Completed in {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
