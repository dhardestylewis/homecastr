"""
Global Trainable Panel: Transaction-Level Prices
==================================================
Downloads ACTUAL per-property transaction data with prices,
enriches with macro indicators, and outputs a panel ready
for tree-based model training.

Target: 2000+ rows with features + price label.

Sources that have per-building transaction prices:
  - France DVF: Bulk CSV from data.gouv.fr (lat/lon + price + area)
  - Japan MLIT: API (price + area + type)
  - UK PPD: Land Registry (price + postcode + type)
  - Singapore URA: data.gov.sg
  - Ireland CSO: Residential property register

Output:
  scripts/data_acquisition/global_trainable_panel.csv

Usage:
    python scripts/data_acquisition/build_trainable_panel.py
"""

import requests, json, time, csv, io, gzip, os, math, random
import pandas as pd

random.seed(42)
OUT_DIR = os.path.dirname(__file__)
OUT_CSV = os.path.join(OUT_DIR, "global_trainable_panel.csv")

HEADERS = {"User-Agent": "Mozilla/5.0 Properlytic-Panel/3.0 (Research)"}

def get(url, timeout=30, stream=False):
    try:
        return requests.get(url, timeout=timeout, headers=HEADERS,
                          stream=stream, allow_redirects=True)
    except:
        return None


# ═══════════════════════════════════════════════════════════════════
# 1. FRANCE DVF — Departmental Bulk CSVs (Gold Standard)
#    Each row = one notarized property sale with lat/lon + price
# ═══════════════════════════════════════════════════════════════════
def get_france_dvf(target_rows=800):
    """Download France DVF by department. Has lat, lon, price, area."""
    print("\n" + "="*70)
    print("[1] FRANCE DVF — Notarized Sale Prices")
    print("="*70)

    rows = []
    # Department codes: Paris=75, Rhone/Lyon=69, BdR/Marseille=13,
    # Haute-Garonne/Toulouse=31, Gironde/Bordeaux=33, Alpes-Maritimes/Nice=06,
    # Nord/Lille=59, Loire-Atlantique/Nantes=44
    departments = [
        ("75", "Paris"), ("69", "Lyon"), ("13", "Marseille"),
        ("31", "Toulouse"), ("33", "Bordeaux"), ("06", "Nice"),
        ("59", "Lille"), ("44", "Nantes"), ("67", "Strasbourg"),
        ("34", "Montpellier"),
    ]

    for dept_code, city in departments:
        if len(rows) >= target_rows:
            break

        # Try 2023 first, then 2022
        for year in ["2023", "2022"]:
            url = f"https://files.data.gouv.fr/geo-dvf/latest/csv/{year}/departements/{dept_code}.csv.gz"
            print(f"  Trying DVF {city} ({dept_code}) {year}...")

            r = get(url, timeout=45, stream=True)
            if not r or r.status_code != 200:
                print(f"    HTTP {r.status_code if r else 'timeout'}")
                continue

            try:
                # Download first chunk only (avoid downloading entire file)
                content = b""
                for chunk in r.iter_content(chunk_size=256*1024):
                    content += chunk
                    if len(content) > 2*1024*1024:  # Max 2MB per dept
                        break

                # Decompress and parse
                try:
                    text = gzip.decompress(content).decode("utf-8", errors="replace")
                except:
                    text = content.decode("utf-8", errors="replace")

                lines = text.split("\n")
                if len(lines) < 2:
                    continue

                reader = csv.DictReader(io.StringIO("\n".join(lines[:2000])))
                dept_rows = 0
                for rec in reader:
                    try:
                        price = rec.get("valeur_fonciere", "").replace(",", ".")
                        lat = rec.get("latitude", "")
                        lon = rec.get("longitude", "")
                        area = rec.get("surface_reelle_bati", "")
                        local_type = rec.get("type_local", "")
                        date = rec.get("date_mutation", "")
                        commune = rec.get("nom_commune", "")

                        if not price or not lat or not lon:
                            continue
                        price_f = float(price)
                        lat_f = float(lat)
                        lon_f = float(lon)

                        if price_f <= 0 or price_f > 50_000_000:
                            continue
                        if lat_f == 0 or lon_f == 0:
                            continue
                        # Only keep residential
                        if local_type and local_type not in ["Maison", "Appartement", "Dépendance"]:
                            continue

                        rows.append({
                            "lat": round(lat_f, 6),
                            "lon": round(lon_f, 6),
                            "price_local": price_f,
                            "currency": "EUR",
                            "area_m2": float(area) if area else None,
                            "property_type": local_type,
                            "transaction_date": date,
                            "yr": int(date[:4]) if date and len(date) >= 4 else int(year),
                            "city": commune or city,
                            "country": "France",
                            "country_iso": "FR",
                            "source": "france_dvf",
                            "n_rooms": rec.get("nombre_pieces_principales", None),
                            "terrain_m2": float(rec.get("surface_terrain", 0)) if rec.get("surface_terrain") else None,
                        })
                        dept_rows += 1
                    except (ValueError, TypeError):
                        continue

                    if dept_rows >= 200 or len(rows) >= target_rows:
                        break

                print(f"    {city}: {dept_rows} transactions (total: {len(rows)})")
                if dept_rows > 0:
                    break  # Got data for this year, skip older year

            except Exception as e:
                print(f"    Parse error: {e}")

    print(f"  TOTAL FRANCE: {len(rows)} transactions with lat/lon + price")
    return rows


# ═══════════════════════════════════════════════════════════════════
# 2. JAPAN MLIT — Transaction Prices
#    Each row = surveyed property sale with price + area + type
# ═══════════════════════════════════════════════════════════════════
def get_japan_mlit(target_rows=400):
    """Fetch Japan MLIT. Has price, area, type but limited coords."""
    print("\n" + "="*70)
    print("[2] JAPAN MLIT — Transaction Prices")
    print("="*70)

    rows = []
    # area=13: Tokyo, 27: Osaka, 14: Kanagawa, 23: Aichi/Nagoya, 01: Hokkaido
    queries = [
        ("13", "13101", "Tokyo-Chiyoda"), ("13", "13102", "Tokyo-Chuo"),
        ("13", "13103", "Tokyo-Minato"),  ("13", "13104", "Tokyo-Shinjuku"),
        ("13", "13105", "Tokyo-Bunkyo"),  ("13", "13113", "Tokyo-Shibuya"),
        ("27", "27102", "Osaka-Kita"),    ("27", "27104", "Osaka-Nishi"),
        ("27", "27127", "Osaka-Naniwa"),  ("14", "14101", "Yokohama-Tsurumi"),
        ("14", "14102", "Yokohama-Kanagawa"), ("23", "23101", "Nagoya-Chikusa"),
    ]

    for area, city_code, name in queries:
        if len(rows) >= target_rows:
            break

        for period_range in [("20231", "20244"), ("20221", "20224")]:
            url = (f"https://www.land.mlit.go.jp/webland/api/TradeListSearch"
                   f"?from={period_range[0]}&to={period_range[1]}"
                   f"&area={area}&city={city_code}")
            print(f"  Trying MLIT {name}...")
            r = get(url, timeout=30)

            if r and r.status_code == 200:
                try:
                    items = r.json().get("data", [])
                    ward_rows = 0
                    for s in items:
                        price = s.get("TradePrice")
                        if not price:
                            continue
                        price_i = int(price)
                        if price_i <= 0:
                            continue

                        period = s.get("Period", "")
                        # Period format: "20231Q" → year=2023, quarter=1
                        yr = int(period[:4]) if period and len(period) >= 4 else None

                        rows.append({
                            "price_local": price_i,
                            "currency": "JPY",
                            "area_m2": float(s.get("Area", 0)) if s.get("Area") else None,
                            "property_type": s.get("Type", ""),
                            "yr": yr,
                            "transaction_date": period,
                            "city": name,
                            "country": "Japan",
                            "country_iso": "JP",
                            "source": "japan_mlit",
                            "structure": s.get("Structure", ""),
                            "use": s.get("Use", ""),
                            "build_year": s.get("BuildingYear", ""),
                            "floor_plan": s.get("FloorPlan", ""),
                            "district": s.get("DistrictName", ""),
                        })
                        ward_rows += 1
                        if ward_rows >= 80 or len(rows) >= target_rows:
                            break

                    if ward_rows > 0:
                        print(f"    {name}: {ward_rows} transactions (total: {len(rows)})")
                        break  # Got data, skip older period
                except Exception as e:
                    print(f"    Parse error: {e}")
            else:
                print(f"    HTTP {r.status_code if r else 'timeout'}")

    print(f"  TOTAL JAPAN: {len(rows)} transactions")
    return rows


# ═══════════════════════════════════════════════════════════════════
# 3. UK PPD — Try multiple access methods
# ═══════════════════════════════════════════════════════════════════
def get_uk_ppd(target_rows=400):
    """Try multiple UK PPD access methods."""
    print("\n" + "="*70)
    print("[3] UK PPD — Land Registry Price Paid Data")
    print("="*70)

    rows = []

    # Method 1: Direct download (may have DNS issues)
    urls = [
        "https://prod.publicdata.landregistry.gov.uk/pp-monthly-update-new-version.csv",
        "http://prod.publicdata.landregistry.gov.uk/pp-monthly-update-new-version.csv",
    ]

    for url in urls:
        print(f"  Trying: {url[:60]}...")
        r = get(url, timeout=30)
        if r and r.status_code == 200 and len(r.content) > 500:
            print(f"    Got {len(r.content)} bytes!")
            text = r.text[:1_000_000]  # First 1MB
            for line in text.split("\n"):
                if not line.strip():
                    continue
                fields = line.strip().replace('"', '').split(",")
                if len(fields) < 14:
                    continue
                try:
                    price = int(fields[1])
                    date = fields[2]
                    postcode = fields[3].strip()
                    prop_type = fields[4]
                    town = fields[11]

                    if price <= 0 or price > 50_000_000:
                        continue

                    rows.append({
                        "price_local": price,
                        "currency": "GBP",
                        "transaction_date": date,
                        "yr": int(date[:4]) if date and len(date) >= 4 else None,
                        "postcode": postcode,
                        "property_type": prop_type,
                        "city": town,
                        "country": "United Kingdom",
                        "country_iso": "GB",
                        "source": "uk_ppd",
                    })
                except (ValueError, IndexError):
                    continue
                if len(rows) >= target_rows:
                    break
            if rows:
                break

    # Method 2: SPARQL / Linked Data (backup)
    if not rows:
        print("  Trying Land Registry SPARQL endpoint...")
        sparql_url = "https://landregistry.data.gov.uk/app/ppd/ppd_data.csv?et%5B%5D=lrcommon%3Afreehold&limit=500&min_date=2024-01-01&header=true"
        r2 = get(sparql_url, timeout=30)
        if r2 and r2.status_code == 200 and len(r2.content) > 200:
            reader = csv.DictReader(io.StringIO(r2.text))
            for rec in reader:
                try:
                    price = int(rec.get("pricepaid", rec.get("price_paid", "0")))
                    if price <= 0:
                        continue
                    date = rec.get("date", rec.get("transaction_date", ""))
                    rows.append({
                        "price_local": price,
                        "currency": "GBP",
                        "transaction_date": date,
                        "yr": int(date[:4]) if date and len(date) >= 4 else None,
                        "postcode": rec.get("postcode", ""),
                        "property_type": rec.get("propertytype", rec.get("property_type", "")),
                        "city": rec.get("town", rec.get("district", "")),
                        "country": "United Kingdom",
                        "country_iso": "GB",
                        "source": "uk_ppd_sparql",
                    })
                except:
                    continue
                if len(rows) >= target_rows:
                    break
            print(f"    SPARQL: {len(rows)} transactions")

    print(f"  TOTAL UK: {len(rows)} transactions")
    return rows


# ═══════════════════════════════════════════════════════════════════
# 4. IRELAND — Property Price Register (CSV download)
# ═══════════════════════════════════════════════════════════════════
def get_ireland_ppr(target_rows=200):
    """Ireland Property Price Register - real transactions."""
    print("\n" + "="*70)
    print("[4] IRELAND — Property Price Register")
    print("="*70)

    # PPR is available as CSV from propertypriceregister.ie
    url = "https://www.propertypriceregister.ie/website/npsra/pprweb.nsf/PPRDownloads?OpenForm"
    # Direct CSV download
    csv_url = "https://www.propertypriceregister.ie/website/npsra/ppr/npsra-ppr.nsf/Downloads/PPR-ALL.zip/$FILE/PPR-ALL.zip"

    rows = []
    print(f"  Trying PPR CSV download...")
    r = get(csv_url, timeout=30)

    if r and r.status_code == 200 and len(r.content) > 1000:
        print(f"    Got {len(r.content)/1024:.0f} KB")
        try:
            import zipfile
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
                                    price_str = price_str.replace(",", "").replace("€", "").replace("\x80", "").strip()
                                    price = float(price_str)
                                    county = rec.get("County", "")
                                    address = rec.get("Address", "")
                                    desc = rec.get("Description of Property", "")
                                    yr = int(date.split("/")[-1]) if "/" in date else None

                                    if price <= 0 or not county:
                                        continue
                                    # Only recent years
                                    if yr and yr < 2020:
                                        continue

                                    rows.append({
                                        "price_local": price,
                                        "currency": "EUR",
                                        "transaction_date": date,
                                        "yr": yr,
                                        "city": county,
                                        "property_type": desc,
                                        "country": "Ireland",
                                        "country_iso": "IE",
                                        "source": "ireland_ppr",
                                        "address": address[:80],
                                    })
                                except:
                                    continue
                                if len(rows) >= target_rows:
                                    break
                        break
        except Exception as e:
            print(f"    Parse error: {e}")
    else:
        print(f"    HTTP {r.status_code if r else 'timeout'}")

    print(f"  TOTAL IRELAND: {len(rows)} transactions")
    return rows


# ═══════════════════════════════════════════════════════════════════
# 5. BIS HPI — Merge country-level price index onto rows
# ═══════════════════════════════════════════════════════════════════
def get_bis_hpi():
    """Fetch latest BIS HPI for enrichment."""
    print("\n" + "="*70)
    print("[5] BIS HPI — Country-Level Enrichment")
    print("="*70)

    hpi = {}
    countries = {
        "GB": "United Kingdom", "FR": "France", "JP": "Japan",
        "IE": "Ireland", "DE": "Germany", "AU": "Australia",
        "BR": "Brazil", "IN": "India", "US": "United States",
        "NL": "Netherlands", "SG": "Singapore", "ES": "Spain",
    }
    for iso, name in countries.items():
        sid = f"Q{iso}N628BIS"
        r = get(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}&cosd=2020-01-01", timeout=8)
        if r and r.status_code == 200 and len(r.content) > 50:
            lines = r.text.strip().split("\n")
            if len(lines) > 1:
                parts = lines[-1].split(",")
                if len(parts) == 2 and parts[1] != ".":
                    hpi[iso] = float(parts[1])
                    print(f"    {name}: {parts[1]}")
    return hpi


# ═══════════════════════════════════════════════════════════════════
# 6. RASTER TILES — ESA WorldCover + Copernicus DEM
# ═══════════════════════════════════════════════════════════════════
def enrich_rasters(df, n=50):
    """Enrich a sample of rows with STAC tile info."""
    print(f"\n  Enriching {n} points with raster tile info...")
    df["esa_tile"] = None
    df["dem_tile"] = None

    has_ll = df["lat"].notna() & df["lon"].notna()
    if has_ll.sum() == 0:
        return df

    sample = df[has_ll].sample(min(n, has_ll.sum()), random_state=42).index
    done = 0
    for idx in sample:
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
                    df.loc[idx, "esa_tile"] = feats[0].get("id", "")
        except:
            pass
        try:
            r2 = requests.post(
                "https://planetarycomputer.microsoft.com/api/stac/v1/search",
                json={"collections": ["cop-dem-glo-30"],
                      "intersects": {"type": "Point", "coordinates": [lon, lat]},
                      "limit": 1}, timeout=8)
            if r2 and r2.status_code == 200:
                feats2 = r2.json().get("features", [])
                if feats2:
                    df.loc[idx, "dem_tile"] = feats2[0].get("id", "")
        except:
            pass
        done += 1
        if done % 10 == 0:
            print(f"    {done}/{n}...")
    print(f"    Done: {done} points enriched")
    return df


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════
def main():
    t0 = time.time()
    print("="*70)
    print("BUILDING GLOBAL TRAINABLE PANEL")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

    all_rows = []

    # Pull transaction data from each source
    france = get_france_dvf(target_rows=800)
    all_rows.extend(france)

    japan = get_japan_mlit(target_rows=400)
    all_rows.extend(japan)

    uk = get_uk_ppd(target_rows=400)
    all_rows.extend(uk)

    ireland = get_ireland_ppr(target_rows=200)
    all_rows.extend(ireland)

    print(f"\n  Total raw transaction rows: {len(all_rows)}")

    if len(all_rows) < 100:
        print("\n  WARNING: Very few transactions pulled. APIs may be down.")
        print("  The panel will be small but structurally complete.")

    # Convert to DataFrame
    df = pd.DataFrame(all_rows)

    # Ensure columns
    for col in ["lat","lon","price_local","currency","area_m2","property_type",
                "transaction_date","yr","city","country","country_iso","source",
                "postcode","structure","use","n_rooms","terrain_m2",
                "build_year","floor_plan","district","address",
                "bis_hpi","esa_tile","dem_tile"]:
        if col not in df.columns:
            df[col] = None

    # Enrich with BIS HPI
    hpi = get_bis_hpi()
    for iso, val in hpi.items():
        mask = df["country_iso"] == iso
        df.loc[mask, "bis_hpi"] = val

    # Enrich with raster tiles (sample)
    df = enrich_rasters(df, n=40)

    # Save
    df.to_csv(OUT_CSV, index=False, encoding="utf-8")

    # Summary
    print("\n" + "="*70)
    print("TRAINABLE PANEL SUMMARY")
    print("="*70)
    n = len(df)
    print(f"  Total rows: {n}")
    print(f"  Countries: {df['country'].nunique()} ({', '.join(sorted(df['country'].dropna().unique()))})")
    print(f"  Sources: {', '.join(sorted(df['source'].dropna().unique()))}")
    print(f"  Rows with price: {df['price_local'].notna().sum()}")
    print(f"  Rows with lat/lon: {(df['lat'].notna() & df['lon'].notna()).sum()}")
    print(f"  Rows with area: {df['area_m2'].notna().sum()}")
    print(f"  Rows with BIS HPI: {df['bis_hpi'].notna().sum()}")
    if df["yr"].notna().sum() > 0:
        print(f"  Year range: {int(df['yr'].dropna().min())}-{int(df['yr'].dropna().max())}")

    # Per-country breakdown
    print("\n  PER-COUNTRY:")
    for c, grp in df.groupby("country"):
        hp = grp['price_local'].notna().sum()
        ha = grp['area_m2'].notna().sum()
        hl = (grp['lat'].notna() & grp['lon'].notna()).sum()
        print(f"    {c}: {len(grp)} rows, {hp} with price, {ha} with area, {hl} with coords")

    # Model readiness check
    trainable = df.dropna(subset=["price_local"])
    has_features = trainable[["area_m2", "yr"]].notna().all(axis=1).sum()
    print(f"\n  MODEL READINESS:")
    print(f"    Rows with price label: {len(trainable)}")
    print(f"    Rows with price + area + year: {has_features}")
    print(f"    Sufficient for tree model: {'YES' if has_features >= 100 else 'NO (need more data)'}")

    # Save summary JSON
    summary = {
        "total_rows": n,
        "countries": sorted(df["country"].dropna().unique().tolist()),
        "sources": sorted(df["source"].dropna().unique().tolist()),
        "rows_with_price": int(df["price_local"].notna().sum()),
        "rows_with_coords": int((df["lat"].notna() & df["lon"].notna()).sum()),
        "rows_with_area": int(df["area_m2"].notna().sum()),
        "model_ready_rows": int(has_features),
        "elapsed": round(time.time()-t0, 1),
    }
    json_path = OUT_CSV.replace(".csv", "_summary.json")
    with open(json_path, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\n  CSV: {OUT_CSV}")
    print(f"  Summary: {json_path}")
    print(f"  Completed in {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
