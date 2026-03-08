"""
Multi-Country Trainable Panel with Full Feature Enrichment
============================================================
Pulls per-property transaction data from 5+ countries,
enriches with OSM POI distances + raster features,
trains LightGBM, and validates with cross-country holdout.

Sources (transaction-level prices):
  [1] France DVF — bulk CSV (lat/lon + price + area + rooms)
  [2] Singapore HDB — data.gov.sg (resale price + floor area + geocoded)
  [3] Japan MLIT — API (price + area + type + city ward)
  [4] UK PPD — linked data API (price + postcode + type)
  [5] Ireland PPR — property register (price + county)

Enrichment features:
  [6] OSM POI distances (school, hospital, restaurant, park)
  [7] ESA WorldCover via rasterio (LULC class at each point)
  [8] Copernicus DEM via rasterio (elevation at each point)
  [9] BIS HPI via FRED (country-level price index)

Model:
  [10] LightGBM with cross-country holdout validation

Output:
  scripts/data_acquisition/multi_country_panel.csv
  scripts/data_acquisition/multi_country_model.json
"""

import requests, json, time, csv, io, gzip, os, math, random
import pandas as pd
import numpy as np

random.seed(42)
OUT_DIR = os.path.dirname(__file__)
HEADERS = {"User-Agent": "Mozilla/5.0 Properlytic-Global/3.0 (Research)"}

def get(url, timeout=30, **kw):
    try:
        h = kw.pop("headers", HEADERS)
        return requests.get(url, timeout=timeout, headers=h, allow_redirects=True, **kw)
    except:
        return None


# ═══════════════════════════════════════════════════════════════════
# 1. FRANCE DVF
# ═══════════════════════════════════════════════════════════════════
def pull_france(target=600):
    print("\n[1] FRANCE DVF")
    rows = []
    for dept, city in [("75","Paris"),("69","Lyon"),("13","Marseille"),
                       ("31","Toulouse"),("33","Bordeaux"),("06","Nice"),
                       ("59","Lille"),("44","Nantes"),("67","Strasbourg"),
                       ("34","Montpellier"),("35","Rennes"),("21","Dijon")]:
        if len(rows) >= target: break
        for year in ["2023","2022"]:
            url = f"https://files.data.gouv.fr/geo-dvf/latest/csv/{year}/departements/{dept}.csv.gz"
            print(f"  {city} ({dept}) {year}...", end=" ")
            r = get(url, timeout=45, stream=True)
            if not r or r.status_code != 200:
                print(f"HTTP {r.status_code if r else 'timeout'}")
                continue
            try:
                content = b""
                for chunk in r.iter_content(256*1024):
                    content += chunk
                    if len(content) > 2*1024*1024: break
                try: text = gzip.decompress(content).decode("utf-8", errors="replace")
                except: text = content.decode("utf-8", errors="replace")
                lines = text.split("\n")
                reader = csv.DictReader(io.StringIO("\n".join(lines[:3000])))
                n = 0
                for rec in reader:
                    try:
                        price = float(rec.get("valeur_fonciere","").replace(",","."))
                        lat = float(rec.get("latitude",""))
                        lon = float(rec.get("longitude",""))
                        area = rec.get("surface_reelle_bati","")
                        lt = rec.get("type_local","")
                        date = rec.get("date_mutation","")
                        if price<=0 or price>10e6 or lat==0 or lon==0: continue
                        if lt and lt not in ["Maison","Appartement","Dépendance"]: continue
                        rooms = rec.get("nombre_pieces_principales","")
                        rows.append({
                            "lat":round(lat,6),"lon":round(lon,6),
                            "price_local":price,"currency":"EUR",
                            "area_m2":float(area) if area else None,
                            "property_type":lt,"yr":int(date[:4]) if date else int(year),
                            "city":rec.get("nom_commune",city),
                            "country":"France","country_iso":"FR","source":"france_dvf",
                            "n_rooms":int(rooms) if rooms and rooms.isdigit() else None,
                            "terrain_m2":float(rec.get("surface_terrain",0)) if rec.get("surface_terrain") else None,
                        })
                        n += 1
                    except: continue
                    if n >= 100 or len(rows) >= target: break
                print(f"{n} txns")
                if n > 0: break
            except Exception as e:
                print(f"error: {e}")
    print(f"  FRANCE TOTAL: {len(rows)}")
    return rows


# ═══════════════════════════════════════════════════════════════════
# 2. SINGAPORE HDB RESALE
# ═══════════════════════════════════════════════════════════════════
def pull_singapore(target=300):
    print("\n[2] SINGAPORE HDB RESALE")
    rows = []
    # data.gov.sg resale flat prices (2017 onwards)
    resource_ids = [
        "f1765b54-a209-4718-8d38-a39237f502b3",  # 2017-onwards
    ]
    for rid in resource_ids:
        if len(rows) >= target: break
        url = f"https://data.gov.sg/api/action/datastore_search?resource_id={rid}&limit={target}&sort=month+desc"
        print(f"  data.gov.sg resale...", end=" ")
        r = get(url, timeout=30)
        if not r or r.status_code != 200:
            print(f"HTTP {r.status_code if r else 'timeout'}")
            continue
        try:
            records = r.json().get("result", {}).get("records", [])
            print(f"{len(records)} records")
            for rec in records:
                price = rec.get("resale_price")
                area = rec.get("floor_area_sqm")
                if not price: continue
                addr = f"{rec.get('block','')} {rec.get('street_name','')}"
                rows.append({
                    "price_local": float(price),
                    "currency": "SGD",
                    "area_m2": float(area) if area else None,
                    "property_type": rec.get("flat_type", ""),
                    "city": rec.get("town", "Singapore"),
                    "country": "Singapore", "country_iso": "SG",
                    "source": "sg_hdb",
                    "yr": int(rec.get("month","")[:4]) if rec.get("month") else None,
                    "n_rooms": int(rec.get("flat_type","")[0]) if rec.get("flat_type","")[0].isdigit() else None,
                    "structure": rec.get("flat_model", ""),
                    "build_year": rec.get("lease_commence_date", ""),
                    "storey": rec.get("storey_range", ""),
                    "address": addr,
                })
        except Exception as e:
            print(f"  Error: {e}")

    # Geocode Singapore addresses using OneMap
    if rows:
        print(f"  Geocoding {len(rows)} Singapore addresses...")
        geocoded = 0
        seen = {}
        for row in rows:
            addr = row.get("address", "")
            if addr in seen:
                row["lat"], row["lon"] = seen[addr]
                geocoded += 1
                continue
            try:
                gr = get(f"https://www.onemap.gov.sg/api/common/elastic/search?searchVal={addr}&returnGeom=Y&getAddrDetails=N", timeout=5)
                if gr and gr.status_code == 200:
                    results = gr.json().get("results", [])
                    if results:
                        row["lat"] = float(results[0]["LATITUDE"])
                        row["lon"] = float(results[0]["LONGITUDE"])
                        seen[addr] = (row["lat"], row["lon"])
                        geocoded += 1
            except:
                pass
            if geocoded % 50 == 0 and geocoded > 0:
                print(f"    Geocoded {geocoded}...")
            time.sleep(0.1)  # Rate limit
        print(f"  Geocoded {geocoded}/{len(rows)} addresses")

    print(f"  SINGAPORE TOTAL: {len(rows)}")
    return rows


# ═══════════════════════════════════════════════════════════════════
# 3. JAPAN MLIT
# ═══════════════════════════════════════════════════════════════════
def pull_japan(target=300):
    print("\n[3] JAPAN MLIT")
    rows = []
    # Ward-level centroids for approximate geocoding
    ward_coords = {
        "Tokyo-Chiyoda": (35.694, 139.754), "Tokyo-Chuo": (35.671, 139.774),
        "Tokyo-Minato": (35.658, 139.752), "Tokyo-Shinjuku": (35.694, 139.703),
        "Tokyo-Bunkyo": (35.708, 139.752), "Tokyo-Shibuya": (35.662, 139.704),
        "Osaka-Kita": (34.705, 135.500), "Osaka-Chuo": (34.684, 135.520),
        "Yokohama-Nishi": (35.460, 139.622), "Nagoya-Naka": (35.172, 136.902),
    }
    queries = [
        ("13","13101","Tokyo-Chiyoda"), ("13","13102","Tokyo-Chuo"),
        ("13","13103","Tokyo-Minato"),  ("13","13104","Tokyo-Shinjuku"),
        ("13","13105","Tokyo-Bunkyo"),  ("13","13113","Tokyo-Shibuya"),
        ("27","27102","Osaka-Kita"),    ("27","27104","Osaka-Chuo"),
        ("14","14103","Yokohama-Nishi"),("23","23106","Nagoya-Naka"),
    ]
    for area, city_code, name in queries:
        if len(rows) >= target: break
        for prange in [("20231","20244"),("20221","20224")]:
            url = f"https://www.land.mlit.go.jp/webland/api/TradeListSearch?from={prange[0]}&to={prange[1]}&area={area}&city={city_code}"
            print(f"  {name}...", end=" ")
            r = get(url, timeout=45)
            if not r or r.status_code != 200:
                print(f"HTTP {r.status_code if r else 'timeout'}")
                continue
            try:
                items = r.json().get("data", [])
                n = 0
                base_lat, base_lon = ward_coords.get(name, (35.68, 139.77))
                for s in items:
                    price = s.get("TradePrice")
                    if not price: continue
                    # Add small jitter to ward centroid for per-property coords
                    rows.append({
                        "lat": round(base_lat + random.uniform(-0.01, 0.01), 6),
                        "lon": round(base_lon + random.uniform(-0.01, 0.01), 6),
                        "price_local": int(price), "currency": "JPY",
                        "area_m2": float(s.get("Area",0)) if s.get("Area") else None,
                        "property_type": s.get("Type", ""),
                        "city": name, "country": "Japan", "country_iso": "JP",
                        "source": "japan_mlit",
                        "yr": int(s.get("Period","")[:4]) if s.get("Period") else None,
                        "structure": s.get("Structure", ""),
                        "build_year": s.get("BuildingYear", ""),
                        "n_rooms": None,
                    })
                    n += 1
                    if n >= 60 or len(rows) >= target: break
                print(f"{n} txns")
                if n > 0: break
            except Exception as e:
                print(f"error: {e}")
    print(f"  JAPAN TOTAL: {len(rows)}")
    return rows


# ═══════════════════════════════════════════════════════════════════
# 4. UK PPD — Linked Data API
# ═══════════════════════════════════════════════════════════════════
def pull_uk(target=300):
    print("\n[4] UK PPD (Linked Data)")
    rows = []
    # UK postcode centroids (approximate)
    pc_coords = {}

    # Try the Land Registry linked data CSV download
    url = "https://landregistry.data.gov.uk/app/ppd/ppd_data.csv?et%5B%5D=lrcommon%3Afreehold&et%5B%5D=lrcommon%3Aleasehold&limit=500&min_date=2024-01-01&header=true"
    print(f"  Linked Data CSV...", end=" ")
    r = get(url, timeout=45)
    if r and r.status_code == 200 and len(r.content) > 200:
        try:
            reader = csv.DictReader(io.StringIO(r.text))
            for rec in reader:
                try:
                    price = int(rec.get("pricepaid", rec.get("Price Paid", "0")).replace(",",""))
                    if price <= 0: continue
                    date = rec.get("date", rec.get("Date", ""))
                    pc = rec.get("postcode", rec.get("Postcode", ""))
                    town = rec.get("town", rec.get("Town/City", ""))
                    ptype = rec.get("propertytype", rec.get("Property Type", ""))
                    rows.append({
                        "price_local": price, "currency": "GBP",
                        "yr": int(date[:4]) if date and len(date)>=4 else None,
                        "transaction_date": date,
                        "postcode": pc, "property_type": ptype,
                        "city": town, "country": "United Kingdom", "country_iso": "GB",
                        "source": "uk_ppd",
                    })
                except: continue
                if len(rows) >= target: break
            print(f"{len(rows)} txns")
        except Exception as e:
            print(f"error: {e}")
    else:
        print(f"HTTP {r.status_code if r else 'timeout'}")

    # Geocode UK postcodes using postcodes.io
    if rows:
        print(f"  Geocoding {len(rows)} UK postcodes...")
        geocoded = 0
        seen = {}
        batch = []
        for row in rows:
            pc = row.get("postcode", "")
            if not pc: continue
            if pc in seen:
                row["lat"], row["lon"] = seen[pc]
                geocoded += 1
                continue
            batch.append((row, pc))

        # Batch geocode via postcodes.io (up to 100 at a time)
        for i in range(0, len(batch), 100):
            chunk = batch[i:i+100]
            pcs = [b[1] for b in chunk]
            try:
                gr = requests.post("https://api.postcodes.io/postcodes",
                                  json={"postcodes": pcs}, timeout=15)
                if gr and gr.status_code == 200:
                    results = gr.json().get("result", [])
                    for j, res in enumerate(results):
                        if res and res.get("result"):
                            lat = res["result"]["latitude"]
                            lon = res["result"]["longitude"]
                            chunk[j][0]["lat"] = lat
                            chunk[j][0]["lon"] = lon
                            seen[chunk[j][1]] = (lat, lon)
                            geocoded += 1
            except: pass
        print(f"  Geocoded {geocoded}/{len(rows)}")

    print(f"  UK TOTAL: {len(rows)}")
    return rows


# ═══════════════════════════════════════════════════════════════════
# 5. IRELAND PPR
# ═══════════════════════════════════════════════════════════════════
def pull_ireland(target=200):
    print("\n[5] IRELAND PPR")
    rows = []
    # Try the PPR CSV
    url = "https://www.propertypriceregister.ie/website/npsra/ppr/npsra-ppr.nsf/Downloads/PPR-ALL.zip/$FILE/PPR-ALL.zip"
    print(f"  Downloading PPR zip...", end=" ")
    r = get(url, timeout=45)
    if r and r.status_code == 200 and len(r.content) > 1000:
        print(f"{len(r.content)//1024} KB")
        try:
            import zipfile
            # County centroids for approximate geocoding
            county_coords = {
                "Dublin": (53.349, -6.260), "Cork": (51.897, -8.470),
                "Galway": (53.270, -9.057), "Limerick": (52.668, -8.627),
                "Waterford": (52.259, -7.110), "Kerry": (52.059, -9.504),
                "Meath": (53.606, -6.656), "Kildare": (53.159, -6.910),
                "Wicklow": (52.981, -6.045), "Louth": (53.849, -6.534),
            }
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                for name in z.namelist():
                    if name.endswith(".csv"):
                        with z.open(name) as f:
                            text = f.read().decode("utf-8", errors="replace")
                            reader = csv.DictReader(io.StringIO(text))
                            for rec in reader:
                                try:
                                    date = rec.get("Date of Sale (dd/mm/yyyy)", "")
                                    price_str = rec.get("Price (\x80)", rec.get("Price", ""))
                                    price_str = price_str.replace(",","").replace("\x80","").replace("€","").strip()
                                    price = float(price_str)
                                    county = rec.get("County","").strip()
                                    if price<=0 or not county: continue
                                    yr = int(date.split("/")[-1]) if "/" in date else None
                                    if yr and yr < 2020: continue
                                    coords = county_coords.get(county, (53.35, -6.26))
                                    rows.append({
                                        "lat": round(coords[0]+random.uniform(-0.05,0.05), 6),
                                        "lon": round(coords[1]+random.uniform(-0.05,0.05), 6),
                                        "price_local": price, "currency": "EUR",
                                        "yr": yr, "transaction_date": date,
                                        "city": county, "country": "Ireland", "country_iso": "IE",
                                        "source": "ireland_ppr",
                                        "property_type": rec.get("Description of Property",""),
                                    })
                                except: continue
                                if len(rows) >= target: break
                        break
        except Exception as e:
            print(f"error: {e}")
    else:
        print(f"HTTP {r.status_code if r else 'timeout'}")
    print(f"  IRELAND TOTAL: {len(rows)}")
    return rows


# ═══════════════════════════════════════════════════════════════════
# 6. OSM POI DISTANCES (via Overpass)
# ═══════════════════════════════════════════════════════════════════
def enrich_osm(df, sample_n=200):
    """Compute distance to nearest school, hospital, restaurant, park."""
    print("\n[6] OSM POI ENRICHMENT")
    has_ll = df["lat"].notna() & df["lon"].notna()
    if has_ll.sum() == 0:
        print("  No coordinates — skipping")
        return df

    for col in ["dist_school","dist_hospital","dist_restaurant","dist_park","dist_transit"]:
        df[col] = np.nan

    # Sample points grouped by city to reduce API calls
    sample_idx = df[has_ll].sample(min(sample_n, has_ll.sum()), random_state=42).index
    done = 0

    # Group by approximate location (0.1 deg grid)
    df.loc[sample_idx, "_grid"] = (
        (df.loc[sample_idx, "lat"]*10).round().astype(str) + "_" +
        (df.loc[sample_idx, "lon"]*10).round().astype(str)
    )

    poi_types = {
        "school": '"amenity"="school"',
        "hospital": '"amenity"="hospital"',
        "restaurant": '"amenity"="restaurant"',
        "park": '"leisure"="park"',
        "transit": '"railway"="station"',
    }

    # Cache POIs per grid cell
    poi_cache = {}

    for idx in sample_idx:
        lat, lon = df.loc[idx, "lat"], df.loc[idx, "lon"]
        grid = df.loc[idx, "_grid"]

        for poi_name, poi_tag in poi_types.items():
            cache_key = f"{grid}_{poi_name}"
            if cache_key not in poi_cache:
                # Query Overpass for this grid cell
                bbox = f"{lat-0.02},{lon-0.02},{lat+0.02},{lon+0.02}"
                query = f'[out:json][timeout:10];node[{poi_tag}]({bbox});out center 20;'
                try:
                    r = requests.get(
                        "https://overpass-api.de/api/interpreter",
                        params={"data": query}, timeout=12)
                    if r and r.status_code == 200:
                        elements = r.json().get("elements", [])
                        poi_cache[cache_key] = [(e["lat"], e["lon"]) for e in elements if "lat" in e]
                    else:
                        poi_cache[cache_key] = []
                except:
                    poi_cache[cache_key] = []

            # Compute distance to nearest
            pois = poi_cache[cache_key]
            if pois:
                dists = [haversine(lat, lon, p[0], p[1]) for p in pois]
                df.loc[idx, f"dist_{poi_name}"] = min(dists)

        done += 1
        if done % 30 == 0:
            print(f"  Enriched {done}/{len(sample_idx)} points")
            time.sleep(1)  # Overpass rate limit

    if "_grid" in df.columns:
        df = df.drop(columns=["_grid"])

    enriched = df[["dist_school","dist_hospital","dist_restaurant","dist_park","dist_transit"]].notna().any(axis=1).sum()
    print(f"  OSM enrichment: {enriched} rows have at least one POI distance")
    return df


def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    dlat = math.radians(lat2-lat1)
    dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))


# ═══════════════════════════════════════════════════════════════════
# 7. RASTER ENRICHMENT (ESA WorldCover + DEM)
# ═══════════════════════════════════════════════════════════════════
def enrich_rasters(df, sample_n=150):
    """Extract ESA WorldCover class and Copernicus DEM elevation at each point."""
    print("\n[7] RASTER ENRICHMENT")
    has_ll = df["lat"].notna() & df["lon"].notna()
    if has_ll.sum() == 0:
        print("  No coordinates — skipping")
        return df

    df["lulc_class"] = np.nan
    df["elevation_m"] = np.nan

    sample_idx = df[has_ll].sample(min(sample_n, has_ll.sum()), random_state=42).index
    done = 0

    try:
        import rasterio
        from rasterio.io import MemoryFile

        # Use Planetary Computer's TiTiler for pixel queries
        pc_base = "https://planetarycomputer.microsoft.com/api/data/v1"

        for idx in sample_idx:
            lat, lon = df.loc[idx, "lat"], df.loc[idx, "lon"]

            # ESA WorldCover — query via STAC + COG
            try:
                r = requests.post(
                    "https://planetarycomputer.microsoft.com/api/stac/v1/search",
                    json={"collections": ["esa-worldcover"],
                          "intersects": {"type": "Point", "coordinates": [lon, lat]},
                          "limit": 1}, timeout=8)
                if r and r.status_code == 200:
                    feats = r.json().get("features", [])
                    if feats:
                        # Get the COG URL and read pixel
                        asset_href = feats[0].get("assets", {}).get("map", {}).get("href", "")
                        if asset_href:
                            import planetary_computer as pc
                            signed = pc.sign(asset_href)
                            with rasterio.open(signed) as src:
                                vals = list(src.sample([(lon, lat)]))
                                if vals and len(vals[0]) > 0:
                                    df.loc[idx, "lulc_class"] = int(vals[0][0])
            except:
                pass

            # Copernicus DEM
            try:
                r2 = requests.post(
                    "https://planetarycomputer.microsoft.com/api/stac/v1/search",
                    json={"collections": ["cop-dem-glo-30"],
                          "intersects": {"type": "Point", "coordinates": [lon, lat]},
                          "limit": 1}, timeout=8)
                if r2 and r2.status_code == 200:
                    feats2 = r2.json().get("features", [])
                    if feats2:
                        asset_href2 = feats2[0].get("assets", {}).get("data", {}).get("href", "")
                        if asset_href2:
                            import planetary_computer as pc
                            signed2 = pc.sign(asset_href2)
                            with rasterio.open(signed2) as src:
                                vals2 = list(src.sample([(lon, lat)]))
                                if vals2 and len(vals2[0]) > 0:
                                    df.loc[idx, "elevation_m"] = float(vals2[0][0])
            except:
                pass

            done += 1
            if done % 30 == 0:
                print(f"  Raster: {done}/{len(sample_idx)} done")

    except ImportError:
        print("  rasterio or planetary_computer not available — using STAC tile IDs only")
        for idx in sample_idx:
            lat, lon = df.loc[idx, "lat"], df.loc[idx, "lon"]
            try:
                r = requests.post(
                    "https://planetarycomputer.microsoft.com/api/stac/v1/search",
                    json={"collections": ["esa-worldcover"],
                          "intersects": {"type": "Point", "coordinates": [lon, lat]},
                          "limit": 1}, timeout=8)
                if r and r.status_code == 200:
                    feats = r.json().get("features", [])
                    if feats:
                        df.loc[idx, "lulc_class"] = -1  # Tile exists, pixel unknown
            except: pass
            done += 1

    lulc_n = df["lulc_class"].notna().sum()
    elev_n = df["elevation_m"].notna().sum()
    print(f"  Raster enrichment: {lulc_n} LULC, {elev_n} elevation values")
    return df


# ═══════════════════════════════════════════════════════════════════
# 8. BIS HPI
# ═══════════════════════════════════════════════════════════════════
def enrich_bis_hpi(df):
    print("\n[8] BIS HPI ENRICHMENT")
    df["bis_hpi"] = np.nan
    countries = df["country_iso"].unique()
    for iso in countries:
        sid = f"Q{iso}N628BIS"
        r = get(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}&cosd=2020-01-01", timeout=8)
        if r and r.status_code == 200 and len(r.content) > 50:
            lines = r.text.strip().split("\n")
            if len(lines) > 1:
                parts = lines[-1].split(",")
                if len(parts)==2 and parts[1]!=".":
                    val = float(parts[1])
                    df.loc[df["country_iso"]==iso, "bis_hpi"] = val
                    print(f"  {iso}: {val}")
    return df


# ═══════════════════════════════════════════════════════════════════
# 9. NORMALIZE PRICES TO USD
# ═══════════════════════════════════════════════════════════════════
def normalize_prices(df):
    """Convert all prices to USD for cross-country comparison."""
    print("\n[9] PRICE NORMALIZATION TO USD")
    # Approximate exchange rates (2024 avg)
    fx = {"EUR": 1.08, "GBP": 1.27, "JPY": 0.0067, "SGD": 0.74, "USD": 1.0}
    df["price_usd"] = df.apply(
        lambda r: r["price_local"] * fx.get(r["currency"], 1.0)
        if pd.notna(r["price_local"]) else None, axis=1)
    # Log price for modeling
    df["log_price_usd"] = np.log1p(df["price_usd"])
    print(f"  Converted {df['price_usd'].notna().sum()} prices to USD")
    return df


# ═══════════════════════════════════════════════════════════════════
# 10. TRAIN + CROSS-COUNTRY VALIDATION
# ═══════════════════════════════════════════════════════════════════
def train_and_validate(df):
    print("\n" + "="*70)
    print("[10] LIGHTGBM TRAINING + CROSS-COUNTRY VALIDATION")
    print("="*70)

    import lightgbm as lgb
    from sklearn.model_selection import cross_val_score, GroupKFold
    from sklearn.metrics import r2_score, mean_absolute_error

    # Feature columns
    feat_cols = ["lat", "lon", "yr", "area_m2", "n_rooms",
                 "dist_school", "dist_hospital", "dist_restaurant",
                 "dist_park", "dist_transit",
                 "lulc_class", "elevation_m", "bis_hpi"]

    target = "log_price_usd"

    # Only rows with price + coords
    mask = df["log_price_usd"].notna() & df["lat"].notna() & df["lon"].notna()
    dft = df[mask].copy()
    print(f"  Trainable rows: {len(dft)}")
    print(f"  Countries: {dft['country'].nunique()} ({', '.join(sorted(dft['country'].unique()))})")

    # Only keep features that exist
    avail = [c for c in feat_cols if c in dft.columns and dft[c].notna().sum() > 10]
    print(f"  Features with data: {avail}")

    X = dft[avail].copy()
    y = dft[target].copy()
    groups = dft["country"].copy()

    # ── Model A: All-country pooled (5-fold CV) ──
    print("\n  MODEL A: Pooled 5-fold CV")
    params = {"n_estimators": 300, "max_depth": 6, "learning_rate": 0.1,
              "num_leaves": 31, "reg_alpha": 0.1, "reg_lambda": 1.0,
              "random_state": 42, "verbose": -1, "n_jobs": -1}
    model = lgb.LGBMRegressor(**params)
    cv_scores = cross_val_score(model, X, y, cv=5, scoring="r2")
    print(f"    CV R2: {cv_scores.mean():.4f} +/- {cv_scores.std():.4f}")
    print(f"    Per-fold: {[round(s,3) for s in cv_scores]}")

    # Fit on all data for feature importance
    model.fit(X, y)
    fi = dict(zip(avail, model.feature_importances_))
    fi_sorted = sorted(fi.items(), key=lambda x: -x[1])
    print(f"    Feature importance: {fi_sorted}")

    # ── Model B: Leave-one-country-out ──
    print("\n  MODEL B: Leave-One-Country-Out")
    loco_results = {}
    for country in sorted(dft["country"].unique()):
        train_mask = dft["country"] != country
        test_mask = dft["country"] == country
        if test_mask.sum() < 10 or train_mask.sum() < 50:
            print(f"    Skip {country} (too few rows: {test_mask.sum()} test, {train_mask.sum()} train)")
            continue

        X_tr = dft.loc[train_mask, avail]
        y_tr = dft.loc[train_mask, target]
        X_te = dft.loc[test_mask, avail]
        y_te = dft.loc[test_mask, target]

        m = lgb.LGBMRegressor(**params)
        m.fit(X_tr, y_tr)
        yp = m.predict(X_te)

        r2 = r2_score(y_te, yp)
        mae_usd = mean_absolute_error(np.expm1(y_te), np.expm1(yp))
        mape = np.mean(np.abs(np.expm1(y_te) - np.expm1(yp)) / np.expm1(y_te)) * 100

        loco_results[country] = {"r2": round(r2, 4), "mae_usd": round(mae_usd),
                                 "mape": round(mape, 1), "n_test": int(test_mask.sum())}
        print(f"    {country}: R2={r2:.3f}, MAE=${mae_usd:,.0f}, MAPE={mape:.1f}%, N={test_mask.sum()}")

    # ── Model C: Country as feature (generalization test) ──
    print("\n  MODEL C: With country_iso encoded")
    dft["country_code"] = pd.Categorical(dft["country_iso"]).codes
    avail_c = avail + ["country_code"]
    X_c = dft[avail_c].copy()
    model_c = lgb.LGBMRegressor(**params)
    cv_c = cross_val_score(model_c, X_c, y, cv=5, scoring="r2")
    print(f"    CV R2: {cv_c.mean():.4f} +/- {cv_c.std():.4f}")

    results = {
        "model_a_pooled": {
            "features": avail, "cv_r2_mean": round(cv_scores.mean(), 4),
            "cv_r2_std": round(cv_scores.std(), 4),
            "feature_importance": {k: int(v) for k, v in fi_sorted},
        },
        "model_b_loco": loco_results,
        "model_c_with_country": {
            "features": avail_c, "cv_r2_mean": round(cv_c.mean(), 4),
            "cv_r2_std": round(cv_c.std(), 4),
        },
        "panel_stats": {
            "total_rows": len(dft), "n_countries": int(dft["country"].nunique()),
            "countries": sorted(dft["country"].unique().tolist()),
        }
    }
    return results


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════
def main():
    t0 = time.time()
    print("="*70)
    print("MULTI-COUNTRY TRAINABLE PANEL")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

    # Pull transactions
    all_rows = []
    all_rows.extend(pull_france(target=600))
    all_rows.extend(pull_singapore(target=300))
    all_rows.extend(pull_japan(target=300))
    all_rows.extend(pull_uk(target=300))
    all_rows.extend(pull_ireland(target=200))

    print(f"\n  Total transaction rows: {len(all_rows)}")
    df = pd.DataFrame(all_rows)

    # Ensure columns
    for col in ["lat","lon","price_local","currency","area_m2","property_type",
                "yr","city","country","country_iso","source","n_rooms",
                "terrain_m2","postcode","structure","build_year",
                "dist_school","dist_hospital","dist_restaurant","dist_park","dist_transit",
                "lulc_class","elevation_m","bis_hpi"]:
        if col not in df.columns:
            df[col] = None

    # Enrichment
    df = enrich_osm(df, sample_n=150)
    df = enrich_rasters(df, sample_n=100)
    df = enrich_bis_hpi(df)
    df = normalize_prices(df)

    # Save panel
    out_csv = os.path.join(OUT_DIR, "multi_country_panel.csv")
    df.to_csv(out_csv, index=False, encoding="utf-8")
    print(f"\n  Panel saved: {out_csv} ({len(df)} rows)")

    # Panel summary
    print("\n" + "="*70)
    print("PANEL SUMMARY")
    print("="*70)
    print(f"  Total rows: {len(df)}")
    print(f"  Countries: {df['country'].nunique()}")
    for c, g in df.groupby("country"):
        hp = g["price_local"].notna().sum()
        hl = (g["lat"].notna()&g["lon"].notna()).sum()
        ha = g["area_m2"].notna().sum()
        print(f"    {c}: {len(g)} rows, {hp} prices, {hl} coords, {ha} areas")

    # Train model
    results = train_and_validate(df)
    results["elapsed"] = round(time.time()-t0, 1)

    out_json = os.path.join(OUT_DIR, "multi_country_model.json")
    with open(out_json, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n  Model results: {out_json}")
    print(f"  Completed in {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
