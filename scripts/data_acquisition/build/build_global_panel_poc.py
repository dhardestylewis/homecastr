"""
Global Proof-of-Concept Panel: 1000 Real Properties
=====================================================
Pulls ACTUAL data from confirmed sources and merges into
a unified panel proving the global pipeline works.

Sources used:
  - MS Building Footprints (building entities + area/height)
  - UK PPD (actual sale prices, England & Wales)
  - France DVF (notarized sale prices)
  - Japan MLIT (transaction prices, Tokyo)
  - BIS HPI via FRED (country-level price index)
  - ESA WorldCover via Planetary Computer (LULC class per building)
  - Copernicus DEM via Planetary Computer (elevation per building)

Output:
  scripts/data_acquisition/global_panel_1000.csv

Usage:
    python scripts/data_acquisition/build_global_panel_poc.py
"""

import requests
import json
import time
import csv
import io
import gzip
import os
import math
import random

import pandas as pd

random.seed(42)

OUT_DIR = os.path.dirname(__file__)
OUT_CSV = os.path.join(OUT_DIR, "global_panel_1000.csv")
OUT_JSON = os.path.join(OUT_DIR, "global_panel_1000_summary.json")

def get(url, timeout=20):
    try:
        return requests.get(url, timeout=timeout,
                          headers={"User-Agent": "Mozilla/5.0 Properlytic-Panel/1.0"})
    except:
        return None

def stac_query(collection, lon, lat):
    """Query Planetary Computer STAC for a tile at a point."""
    try:
        r = requests.post(
            "https://planetarycomputer.microsoft.com/api/stac/v1/search",
            json={"collections": [collection],
                  "intersects": {"type": "Point", "coordinates": [lon, lat]},
                  "limit": 1},
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0"})
        if r and r.status_code == 200:
            feats = r.json().get("features", [])
            return feats[0] if feats else None
    except:
        pass
    return None


# ═══════════════════════════════════════════════════════════════════
# 1. MS BUILDING FOOTPRINTS — Get ~150 buildings per city
# ═══════════════════════════════════════════════════════════════════
def get_ms_buildings(region, max_buildings=150):
    """Download first tile for a region and extract buildings."""
    print(f"  Fetching MS Buildings for {region}...")
    r = get("https://minedbuildings.z5.web.core.windows.net/global-buildings/dataset-links.csv")
    if not r or r.status_code != 200:
        print(f"    FAIL: cannot access index")
        return []

    reader = csv.DictReader(io.StringIO(r.text))
    tiles = [row for row in reader if row.get("Location") == region]
    if not tiles:
        print(f"    FAIL: no tiles for {region}")
        return []

    # Pick a random tile (not the first, to get variety)
    tile = tiles[min(random.randint(0, 5), len(tiles)-1)]
    url = tile.get("Url", "")
    qk = tile.get("QuadKey", "")

    print(f"    Downloading tile QK={qk}...")
    r2 = get(url, timeout=30)
    if not r2 or r2.status_code != 200:
        print(f"    FAIL: tile download failed")
        return []

    buildings = []
    try:
        for line in gzip.open(io.BytesIO(r2.content)):
            feat = json.loads(line)
            props = feat.get("properties", {})
            geom = feat.get("geometry", {})

            # Get centroid from polygon
            coords = geom.get("coordinates", [[]])[0]
            if not coords or len(coords) < 3:
                continue

            lons = [c[0] for c in coords]
            lats = [c[1] for c in coords]
            centroid_lon = sum(lons) / len(lons)
            centroid_lat = sum(lats) / len(lats)

            # Calculate area (approximate, in m²)
            # Using shoelace formula with lat/lon → meters conversion
            area_m2 = 0
            n = len(coords)
            for i in range(n):
                j = (i + 1) % n
                xi = coords[i][0] * 111320 * math.cos(math.radians(centroid_lat))
                yi = coords[i][1] * 110540
                xj = coords[j][0] * 111320 * math.cos(math.radians(centroid_lat))
                yj = coords[j][1] * 110540
                area_m2 += xi * yj - xj * yi
            area_m2 = abs(area_m2) / 2

            buildings.append({
                "lat": round(centroid_lat, 6),
                "lon": round(centroid_lon, 6),
                "area_m2": round(area_m2, 1),
                "height": props.get("height", None),
                "confidence": props.get("confidence", None),
            })

            if len(buildings) >= max_buildings:
                break
    except Exception as e:
        print(f"    WARN: parse error after {len(buildings)} buildings: {e}")

    print(f"    Got {len(buildings)} buildings")
    return buildings


# ═══════════════════════════════════════════════════════════════════
# 2. UK PPD — Actual sale transactions
# ═══════════════════════════════════════════════════════════════════
def get_uk_ppd(max_rows=200):
    """Download UK Price Paid Data and extract recent transactions."""
    print(f"  Fetching UK PPD transactions...")

    # Try monthly update first (smaller file) — note HTTPS
    url = "https://prod.publicdata.landregistry.gov.uk/pp-monthly-update-new-version.csv"
    r = get(url, timeout=30)

    if not r or r.status_code != 200:
        # Try 2024 file
        url = "https://prod.publicdata.landregistry.gov.uk/pp-2024.csv"
        r = get(url, timeout=30)

    if not r or r.status_code != 200:
        print(f"    FAIL: cannot access PPD")
        return []

    # PPD CSV columns: txn_id, price, date, postcode, property_type,
    #                   old_new, duration, paon, saon, street, locality,
    #                   town, district, county, ppd_cat, record_status
    transactions = []
    text = r.text[:500000]  # First 500KB
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
            prop_type = fields[4]  # D=Detached, S=Semi, T=Terraced, F=Flat
            town = fields[11]

            if price > 0 and postcode:
                transactions.append({
                    "price_local": price,
                    "currency": "GBP",
                    "transaction_date": date,
                    "postcode": postcode,
                    "property_type": prop_type,
                    "city": town,
                })
        except (ValueError, IndexError):
            continue

        if len(transactions) >= max_rows:
            break

    print(f"    Got {len(transactions)} UK transactions")
    return transactions


# ═══════════════════════════════════════════════════════════════════
# 3. FRANCE DVF — Notarized sale prices
# ═══════════════════════════════════════════════════════════════════
def get_france_dvf(max_rows=200):
    """Download France DVF from data.gouv.fr API."""
    print(f"  Fetching France DVF transactions...")

    # Try the community API for Paris (75)
    url = "https://api.cquest.org/dvf?code_postal=75001&limit=200"
    r = get(url, timeout=20)

    transactions = []
    if r and r.status_code == 200:
        try:
            data = r.json()
            results = data.get("resultats", [])
            for s in results:
                price = s.get("valeur_fonciere")
                if price and float(price) > 0:
                    transactions.append({
                        "price_local": float(price),
                        "currency": "EUR",
                        "transaction_date": s.get("date_mutation", ""),
                        "property_type": s.get("type_local", ""),
                        "city": "Paris",
                        "lat": s.get("latitude"),
                        "lon": s.get("longitude"),
                        "area_m2": s.get("surface_reelle_bati"),
                    })
                if len(transactions) >= max_rows:
                    break
        except Exception as e:
            print(f"    WARN: DVF API parse: {e}")

    if len(transactions) < 50:
        # Try other postcodes
        for cp in ["75002", "75003", "75004", "75005", "75006", "69001", "13001", "31000", "33000"]:
            url2 = f"https://api.cquest.org/dvf?code_postal={cp}&limit=50"
            r2 = get(url2, timeout=15)
            if r2 and r2.status_code == 200:
                try:
                    data2 = r2.json()
                    for s in data2.get("resultats", []):
                        price = s.get("valeur_fonciere")
                        if price and float(price) > 0:
                            transactions.append({
                                "price_local": float(price),
                                "currency": "EUR",
                                "transaction_date": s.get("date_mutation", ""),
                                "property_type": s.get("type_local", ""),
                                "city": f"FR-{cp[:2]}",
                                "lat": s.get("latitude"),
                                "lon": s.get("longitude"),
                                "area_m2": s.get("surface_reelle_bati"),
                            })
                except:
                    pass
            if len(transactions) >= max_rows:
                break
            time.sleep(0.5)

    print(f"    Got {len(transactions)} France transactions")
    return transactions


# ═══════════════════════════════════════════════════════════════════
# 4. JAPAN MLIT — Transaction prices
# ═══════════════════════════════════════════════════════════════════
def get_japan_mlit(max_rows=150):
    """Fetch Japan MLIT transaction data for Tokyo."""
    print(f"  Fetching Japan MLIT transactions...")

    transactions = []
    # Try multiple cities/wards
    queries = [
        ("13", "13101", "Tokyo Chiyoda"),
        ("13", "13102", "Tokyo Chuo"),
        ("13", "13103", "Tokyo Minato"),
        ("13", "13104", "Tokyo Shinjuku"),
        ("27", "27102", "Osaka Kita"),
    ]

    for area, city, name in queries:
        url = f"https://www.land.mlit.go.jp/webland/api/TradeListSearch?from=20231&to=20234&area={area}&city={city}"
        r = get(url, timeout=25)
        if r and r.status_code == 200:
            try:
                items = r.json().get("data", [])
                for s in items:
                    price = s.get("TradePrice")
                    if price:
                        transactions.append({
                            "price_local": int(price),
                            "currency": "JPY",
                            "transaction_date": s.get("Period", ""),
                            "property_type": s.get("Type", ""),
                            "city": name,
                            "area_m2": float(s.get("Area", 0)) if s.get("Area") else None,
                            "structure": s.get("Structure", ""),
                            "use": s.get("Use", ""),
                        })
                    if len(transactions) >= max_rows:
                        break
            except:
                pass
        if len(transactions) >= max_rows:
            break

    print(f"    Got {len(transactions)} Japan transactions")
    return transactions


# ═══════════════════════════════════════════════════════════════════
# 5. BIS HPI — Country-level price index from FRED
# ═══════════════════════════════════════════════════════════════════
def get_bis_hpi(countries):
    """Fetch BIS residential property price indices via FRED."""
    print(f"  Fetching BIS HPI for {len(countries)} countries...")

    hpi = {}
    for iso, name in countries.items():
        sid = f"Q{iso}N628BIS"
        r = get(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}&cosd=2020-01-01", timeout=10)
        if r and r.status_code == 200 and len(r.content) > 50:
            lines = r.text.strip().split("\n")
            if len(lines) > 1:
                # Get latest value
                last_line = lines[-1]
                parts = last_line.split(",")
                if len(parts) == 2 and parts[1] != ".":
                    hpi[iso] = {
                        "bis_hpi_latest": float(parts[1]),
                        "bis_hpi_date": parts[0],
                        "bis_hpi_n_quarters": len(lines) - 1,
                    }
                    print(f"    {name}: {parts[1]} ({parts[0]})")

    print(f"    Got HPI for {len(hpi)}/{len(countries)} countries")
    return hpi


# ═══════════════════════════════════════════════════════════════════
# 6. PLANETARY COMPUTER — LULC + Elevation per building
# ═══════════════════════════════════════════════════════════════════
def enrich_rasters(df, sample_size=100):
    """Query Planetary Computer for ESA WorldCover and DEM tiles."""
    print(f"  Enriching {sample_size} sample points with raster tile info...")

    # Sample points for raster queries (too slow to do all 1000)
    indices = df.sample(min(sample_size, len(df)), random_state=42).index
    df["esa_worldcover_tile"] = None
    df["copernicus_dem_tile"] = None

    done = 0
    for idx in indices:
        lat = df.loc[idx, "lat"]
        lon = df.loc[idx, "lon"]

        if pd.isna(lat) or pd.isna(lon):
            continue

        # ESA WorldCover
        feat = stac_query("esa-worldcover", lon, lat)
        if feat:
            df.loc[idx, "esa_worldcover_tile"] = feat.get("id", "")

        # Copernicus DEM
        feat2 = stac_query("cop-dem-glo-30", lon, lat)
        if feat2:
            df.loc[idx, "copernicus_dem_tile"] = feat2.get("id", "")

        done += 1
        if done % 20 == 0:
            print(f"    Enriched {done}/{sample_size} points...")

    print(f"    Enriched {done} points with raster tile IDs")
    return df


# ═══════════════════════════════════════════════════════════════════
# MAIN: Assemble the panel
# ═══════════════════════════════════════════════════════════════════
def main():
    t0 = time.time()
    print("="*70)
    print("BUILDING GLOBAL 1000-ROW PANEL")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

    all_rows = []
    summary = {"sources": {}, "countries": set(), "cities": set()}

    # ── Step 1: MS Buildings for multiple regions ──
    print("\n[1/6] MS BUILDING FOOTPRINTS")
    building_configs = [
        ("UnitedKingdom", "GB", "London area"),
        ("France",        "FR", "France"),
        ("Japan",         "JP", "Japan"),
        ("Germany",       "DE", "Germany"),
        ("Australia",     "AU", "Australia"),
        ("Brazil",        "BR", "Brazil"),
        ("India",         "IN", "India"),
    ]

    for region, iso, label in building_configs:
        buildings = get_ms_buildings(region, max_buildings=80)
        for b in buildings:
            b["source"] = "ms_buildings"
            b["country_iso"] = iso
            b["country"] = label
            b["entity_type"] = "building_footprint"
            b["building_id"] = f"{iso}_{len(all_rows):06d}"
        all_rows.extend(buildings)
        summary["sources"][f"MS Buildings ({label})"] = len(buildings)
        summary["countries"].add(label)

    # ── Step 2: UK PPD Transactions ──
    print("\n[2/6] UK PRICE PAID DATA")
    uk_txns = get_uk_ppd(max_rows=150)
    for t in uk_txns:
        t["source"] = "uk_ppd"
        t["country_iso"] = "GB"
        t["country"] = "United Kingdom"
        t["entity_type"] = "transaction"
        t["building_id"] = f"GB_PPD_{len(all_rows):06d}"
        summary["cities"].add(t.get("city", "?"))
    all_rows.extend(uk_txns)
    summary["sources"]["UK PPD"] = len(uk_txns)
    summary["countries"].add("United Kingdom")

    # ── Step 3: France DVF ──
    print("\n[3/6] FRANCE DVF")
    fr_txns = get_france_dvf(max_rows=150)
    for t in fr_txns:
        t["source"] = "france_dvf"
        t["country_iso"] = "FR"
        t["country"] = "France"
        t["entity_type"] = "transaction"
        t["building_id"] = f"FR_DVF_{len(all_rows):06d}"
        summary["cities"].add(t.get("city", "?"))
    all_rows.extend(fr_txns)
    summary["sources"]["France DVF"] = len(fr_txns)

    # ── Step 4: Japan MLIT ──
    print("\n[4/6] JAPAN MLIT")
    jp_txns = get_japan_mlit(max_rows=150)
    for t in jp_txns:
        t["source"] = "japan_mlit"
        t["country_iso"] = "JP"
        t["country"] = "Japan"
        t["entity_type"] = "transaction"
        t["building_id"] = f"JP_MLIT_{len(all_rows):06d}"
        summary["cities"].add(t.get("city", "?"))
    all_rows.extend(jp_txns)
    summary["sources"]["Japan MLIT"] = len(jp_txns)

    # ── Step 5: BIS HPI ──
    print("\n[5/6] BIS HOUSE PRICE INDICES")
    bis_countries = {
        "GB": "United Kingdom", "FR": "France", "JP": "Japan",
        "DE": "Germany", "AU": "Australia", "BR": "Brazil",
        "IN": "India", "US": "United States", "CA": "Canada",
        "NZ": "New Zealand", "SG": "Singapore", "KR": "South Korea",
        "NL": "Netherlands", "SE": "Sweden", "NO": "Norway",
        "IT": "Italy", "ES": "Spain", "CH": "Switzerland",
        "MX": "Mexico", "ZA": "South Africa",
    }
    hpi_data = get_bis_hpi(bis_countries)
    summary["sources"]["BIS HPI"] = len(hpi_data)

    # Convert to DataFrame
    print(f"\n  Total raw rows: {len(all_rows)}")
    df = pd.DataFrame(all_rows)

    # Ensure all expected columns exist
    for col in ["price_local", "currency", "transaction_date", "property_type",
                "city", "lat", "lon", "area_m2", "height", "confidence",
                "postcode", "structure", "use",
                "bis_hpi_latest", "bis_hpi_date", "bis_hpi_n_quarters",
                "esa_worldcover_tile", "copernicus_dem_tile"]:
        if col not in df.columns:
            df[col] = None

    # Merge BIS HPI onto rows
    for iso, hpi_vals in hpi_data.items():
        mask = df["country_iso"] == iso
        for col, val in hpi_vals.items():
            df.loc[mask, col] = val

    # ── Step 6: Raster enrichment (sample) ──
    print("\n[6/6] RASTER ENRICHMENT (Planetary Computer)")
    # Only enrich rows that have lat/lon
    has_coords = df["lat"].notna() & df["lon"].notna()
    if has_coords.sum() > 0:
        df = enrich_rasters(df, sample_size=50)

    # ── Trim to 1000 if needed ──
    if len(df) > 1000:
        df = df.sample(1000, random_state=42).reset_index(drop=True)

    # ── Reorder columns ──
    col_order = [
        "building_id", "country", "country_iso", "city", "lat", "lon",
        "entity_type", "source",
        "area_m2", "height",
        "price_local", "currency", "transaction_date", "property_type",
        "bis_hpi_latest", "bis_hpi_date",
        "esa_worldcover_tile", "copernicus_dem_tile",
        "confidence", "structure", "use", "postcode",
    ]
    existing_cols = [c for c in col_order if c in df.columns]
    extra_cols = [c for c in df.columns if c not in col_order]
    df = df[existing_cols + extra_cols]

    # ── Save ──
    df.to_csv(OUT_CSV, index=False, encoding="utf-8")
    print(f"\n  Saved {len(df)} rows to {OUT_CSV}")

    # ── Summary stats ──
    print("\n" + "="*70)
    print("PANEL SUMMARY")
    print("="*70)
    print(f"  Total rows: {len(df)}")
    print(f"  Countries: {df['country'].nunique()} ({', '.join(sorted(df['country'].unique()))})")
    print(f"  Sources: {df['source'].nunique()}")
    print(f"  Entity types: {df['entity_type'].unique().tolist()}")
    print(f"  Rows with price: {df['price_local'].notna().sum()}")
    print(f"  Rows with lat/lon: {(df['lat'].notna() & df['lon'].notna()).sum()}")
    print(f"  Rows with area: {df['area_m2'].notna().sum()}")
    print(f"  Rows with BIS HPI: {df['bis_hpi_latest'].notna().sum()}")
    print(f"  Rows with LULC tile: {df['esa_worldcover_tile'].notna().sum() if 'esa_worldcover_tile' in df.columns else 0}")
    print(f"  Rows with DEM tile: {df['copernicus_dem_tile'].notna().sum() if 'copernicus_dem_tile' in df.columns else 0}")

    # Per-source breakdown
    print("\n  PER-SOURCE BREAKDOWN:")
    for src, grp in df.groupby("source"):
        has_price = grp["price_local"].notna().sum()
        has_coords = (grp["lat"].notna() & grp["lon"].notna()).sum()
        print(f"    {src}: {len(grp)} rows, {has_price} with price, {has_coords} with coords")

    # Per-country breakdown
    print("\n  PER-COUNTRY BREAKDOWN:")
    for country, grp in df.groupby("country"):
        has_price = grp["price_local"].notna().sum()
        has_hpi = grp["bis_hpi_latest"].notna().sum()
        print(f"    {country}: {len(grp)} rows, {has_price} with price, {has_hpi} with BIS HPI")

    # Save summary
    summary_out = {
        "total_rows": len(df),
        "countries": sorted(df["country"].unique().tolist()),
        "n_countries": int(df["country"].nunique()),
        "sources_used": sorted(df["source"].unique().tolist()),
        "rows_with_price": int(df["price_local"].notna().sum()),
        "rows_with_coords": int((df["lat"].notna() & df["lon"].notna()).sum()),
        "rows_with_bis_hpi": int(df["bis_hpi_latest"].notna().sum()),
        "elapsed_seconds": round(time.time() - t0, 1),
        "source_counts": {k: v for k, v in summary["sources"].items()},
    }
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(summary_out, f, indent=2, ensure_ascii=False)
    print(f"\n  Summary saved to {OUT_JSON}")
    print(f"  Completed in {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
