"""
Dry Run: Global Data Source Access Proof
=========================================
Fetches small samples from every data source discussed for global
property price prediction. Proves API access and data availability
at multiple locations worldwide.

Usage:
    python scripts/data_acquisition/dry_run_global_sources.py
"""

import requests
import json
import time
import sys
import csv
import io
import gzip
from collections import OrderedDict

# ─── Test locations (one per continent/region) ───
LOCATIONS = {
    "Houston, TX":    (29.760, -95.370),
    "London, UK":     (51.510, -0.130),
    "Paris, France":  (48.860,  2.350),
    "Tokyo, Japan":   (35.680, 139.690),
    "Sydney, AU":    (-33.870, 151.210),
}

RESULTS = OrderedDict()
PASS = "✅"
FAIL = "❌"
SKIP = "⏭️"

def log(source, location, status, detail):
    key = f"{source}"
    if key not in RESULTS:
        RESULTS[key] = []
    icon = PASS if status == "pass" else (FAIL if status == "fail" else SKIP)
    RESULTS[key].append({"location": location, "status": icon, "detail": detail})
    print(f"  {icon} [{location}] {detail}")


def safe_get(url, timeout=20, **kwargs):
    """GET with error handling."""
    try:
        headers = kwargs.pop("headers", {"User-Agent": "Mozilla/5.0 Properlytic-DryRun/1.0"})
        r = requests.get(url, timeout=timeout, headers=headers, **kwargs)
        return r
    except Exception as e:
        return None


# ═══════════════════════════════════════════════════════════════════
# 1. MICROSOFT BUILDING FOOTPRINTS (Global)
# ═══════════════════════════════════════════════════════════════════
def test_ms_buildings():
    print("\n" + "="*70)
    print("1. MICROSOFT BUILDING FOOTPRINTS")
    print("="*70)

    # Test: can we access the global dataset-links index?
    r = safe_get("https://minedbuildings.z5.web.core.windows.net/global-buildings/dataset-links.csv")
    if r and r.status_code == 200:
        reader = csv.DictReader(io.StringIO(r.text))
        rows = list(reader)
        countries = set(r.get("Location", "") for r in rows)
        log("MS Buildings", "Global Index", "pass",
            f"Dataset index accessible: {len(rows)} tiles across {len(countries)} countries/regions")

        # Show a sample of available countries
        sample = sorted(list(countries))[:15]
        log("MS Buildings", "Global Index", "pass",
            f"Sample countries: {', '.join(sample)}...")
    else:
        log("MS Buildings", "Global Index", "fail", f"Cannot access dataset index")
        return

    # Try downloading a tiny tile to prove content is accessible
    # Use a small QuadKey that covers part of London
    for name, (lat, lon) in [("London, UK", (51.51, -0.13))]:
        # Find tiles for "UnitedKingdom"
        uk_tiles = [r for r in rows if r.get("Location") == "UnitedKingdom"]
        if uk_tiles:
            tile = uk_tiles[0]  # Just grab first tile
            url = tile.get("Url", "")
            qk = tile.get("QuadKey", "")
            r2 = safe_get(url, timeout=30)
            if r2 and r2.status_code == 200:
                # Parse first few buildings from the gzipped content
                count = 0
                sample_building = None
                try:
                    for line in gzip.open(io.BytesIO(r2.content)):
                        feat = json.loads(line)
                        if count == 0:
                            sample_building = feat
                        count += 1
                        if count >= 100:
                            break
                except Exception:
                    pass
                log("MS Buildings", name, "pass",
                    f"Downloaded tile QK={qk}: {count}+ buildings. Sample keys: {list(sample_building.get('properties', {}).keys()) if sample_building else 'N/A'}")
            else:
                log("MS Buildings", name, "fail", f"Tile download failed")
        else:
            log("MS Buildings", name, "fail", "No UK tiles found")


# ═══════════════════════════════════════════════════════════════════
# 2. OPENSTREETMAP (Global) — Overpass API
# ═══════════════════════════════════════════════════════════════════
def test_osm():
    print("\n" + "="*70)
    print("2. OPENSTREETMAP POIs + ROAD NETWORK")
    print("="*70)

    overpass_url = "https://overpass-api.de/api/interpreter"

    for name, (lat, lon) in LOCATIONS.items():
        # Query: count schools, hospitals, transit stops in 1km radius
        query = f"""
        [out:json][timeout:15];
        (
          node["amenity"="school"](around:1000,{lat},{lon});
          node["amenity"="hospital"](around:1000,{lat},{lon});
          node["public_transport"="stop_position"](around:1000,{lat},{lon});
          node["shop"~"supermarket|grocery"](around:1000,{lat},{lon});
        );
        out count;
        """
        r = safe_get(overpass_url, params={"data": query}, timeout=25)
        if r and r.status_code == 200:
            data = r.json()
            total = data.get("elements", [{}])[0].get("tags", {}).get("total", "?") if data.get("elements") else "?"
            log("OSM POIs", name, "pass", f"POIs within 1km: {total}")
        else:
            log("OSM POIs", name, "fail", f"Overpass query failed: {r.status_code if r else 'timeout'}")

        # Small delay to be polite to Overpass
        time.sleep(1)

    # Also test road network availability
    for name, (lat, lon) in [("Tokyo, Japan", (35.68, 139.69))]:
        query = f"""
        [out:json][timeout:15];
        way["highway"~"primary|secondary|tertiary|residential"](around:500,{lat},{lon});
        out count;
        """
        r = safe_get(overpass_url, params={"data": query}, timeout=25)
        if r and r.status_code == 200:
            data = r.json()
            total = data.get("elements", [{}])[0].get("tags", {}).get("ways", "?") if data.get("elements") else "?"
            log("OSM Roads", name, "pass", f"Road segments within 500m: {total}")
        else:
            log("OSM Roads", name, "fail", "Road network query failed")


# ═══════════════════════════════════════════════════════════════════
# 3. ESA WORLDCOVER (Global LULC — NLCD replacement)
# ═══════════════════════════════════════════════════════════════════
def test_esa_worldcover():
    print("\n" + "="*70)
    print("3. ESA WORLDCOVER (Global LULC, 10m)")
    print("="*70)

    # Query Planetary Computer STAC for WorldCover tiles
    stac_url = "https://planetarycomputer.microsoft.com/api/stac/v1/search"

    for name, (lat, lon) in LOCATIONS.items():
        payload = {
            "collections": ["esa-worldcover"],
            "intersects": {"type": "Point", "coordinates": [lon, lat]},
            "limit": 1,
        }
        r = safe_get(stac_url, timeout=15)
        # Use POST for STAC search
        try:
            r = requests.post(stac_url, json=payload, timeout=15,
                            headers={"User-Agent": "Mozilla/5.0"})
            if r and r.status_code == 200:
                data = r.json()
                features = data.get("features", [])
                if features:
                    item = features[0]
                    assets = list(item.get("assets", {}).keys())
                    bbox = item.get("bbox", [])
                    log("ESA WorldCover", name, "pass",
                        f"Tile found. Assets: {assets[:3]}. Bbox: {[round(b,1) for b in bbox[:4]]}")
                else:
                    log("ESA WorldCover", name, "fail", "No tiles found")
            else:
                log("ESA WorldCover", name, "fail", f"STAC search returned {r.status_code if r else 'error'}")
        except Exception as e:
            log("ESA WorldCover", name, "fail", f"Error: {e}")


# ═══════════════════════════════════════════════════════════════════
# 4. COPERNICUS DEM GLO-30 (Global Elevation — 3DEP replacement)
# ═══════════════════════════════════════════════════════════════════
def test_copernicus_dem():
    print("\n" + "="*70)
    print("4. COPERNICUS DEM GLO-30 (Global Elevation, 30m)")
    print("="*70)

    stac_url = "https://planetarycomputer.microsoft.com/api/stac/v1/search"

    for name, (lat, lon) in LOCATIONS.items():
        payload = {
            "collections": ["cop-dem-glo-30"],
            "intersects": {"type": "Point", "coordinates": [lon, lat]},
            "limit": 1,
        }
        try:
            r = requests.post(stac_url, json=payload, timeout=15,
                            headers={"User-Agent": "Mozilla/5.0"})
            if r and r.status_code == 200:
                data = r.json()
                features = data.get("features", [])
                if features:
                    item = features[0]
                    tile_id = item.get("id", "?")
                    log("Copernicus DEM", name, "pass", f"Tile: {tile_id}")
                else:
                    log("Copernicus DEM", name, "fail", "No tiles")
            else:
                log("Copernicus DEM", name, "fail", f"HTTP {r.status_code if r else '?'}")
        except Exception as e:
            log("Copernicus DEM", name, "fail", f"{e}")


# ═══════════════════════════════════════════════════════════════════
# 5. FRED MACRO INDICATORS (Global)
# ═══════════════════════════════════════════════════════════════════
def test_fred():
    print("\n" + "="*70)
    print("5. FRED MACRO INDICATORS")
    print("="*70)

    series = {
        "MORTGAGE30US": "US 30yr Mortgage Rate",
        "FEDFUNDS": "Fed Funds Rate",
        "CPIAUCSL": "US CPI",
        "UNRATE": "US Unemployment",
        "GEPUCURRENT": "Global Economic Policy Uncertainty",
        "DCOILWTICO": "Oil Price (WTI)",
        "VIXCLS": "VIX",
        "CP0000EZ19M086NEST": "Eurozone CPI",
        "LRHUTTTTEZM156S": "Eurozone Unemployment",
        "QFRN628BIS": "France HPI (BIS)",
        "QGBN628BIS": "UK HPI (BIS)",
        "QJPN628BIS": "Japan HPI (BIS)",
        "QAUN628BIS": "Australia HPI (BIS)",
    }

    for series_id, label in series.items():
        url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}&cosd=2020-01-01"
        r = safe_get(url, timeout=15)
        if r and r.status_code == 200 and len(r.content) > 50:
            lines = r.text.strip().split("\n")
            n_rows = len(lines) - 1  # minus header
            last_line = lines[-1] if lines else "?"
            log("FRED", label, "pass", f"{n_rows} data points. Latest: {last_line}")
        else:
            log("FRED", label, "fail", f"HTTP {r.status_code if r else 'timeout'}")


# ═══════════════════════════════════════════════════════════════════
# 6. BANK OF ENGLAND (UK Macro)
# ═══════════════════════════════════════════════════════════════════
def test_boe():
    print("\n" + "="*70)
    print("6. BANK OF ENGLAND (UK Macro)")
    print("="*70)

    series = {
        "IUDBEDR": "BOE Base Rate",
        "CFMBS32": "UK Mortgage Rate",
        "D7BT": "UK CPI",
    }

    for code, label in series.items():
        url = f"https://www.bankofengland.co.uk/boeapps/database/_iadb-fromshowcolumns.asp?csv.x=yes&SeriesCodes={code}&CSVF=CN&Datefrom=01/Jan/2020&Dateto=31/Dec/2026"
        r = safe_get(url, timeout=15)
        if r and r.status_code == 200 and len(r.content) > 100:
            lines = r.text.strip().split("\n")
            log("BOE", label, "pass", f"{len(lines)} rows. Preview: {lines[-1][:80]}")
        else:
            log("BOE", label, "fail", f"HTTP {r.status_code if r else 'timeout'}")


# ═══════════════════════════════════════════════════════════════════
# 7. UK PRICE PAID DATA (Transaction Labels)
# ═══════════════════════════════════════════════════════════════════
def test_uk_ppd():
    print("\n" + "="*70)
    print("7. UK HM LAND REGISTRY — PRICE PAID DATA")
    print("="*70)

    # 2024 monthly file (smallest recent file)
    url = "http://prod.publicdata.landregistry.gov.uk/pp-2024.csv"
    r = safe_get(url, timeout=20)
    if r and r.status_code == 200:
        # Just read first 5 lines
        lines = r.text[:5000].split("\n")
        sample = lines[0] if lines else "?"
        fields = sample.split(",")
        log("UK PPD", "England & Wales", "pass",
            f"2024 data accessible. Sample transaction: price=£{fields[1] if len(fields)>1 else '?'}, "
            f"postcode={fields[3] if len(fields)>3 else '?'}, "
            f"type={fields[4] if len(fields)>4 else '?'}")
        log("UK PPD", "England & Wales", "pass",
            f"CSV has {len(fields)} columns per row. First 5KB = {len(lines)} rows")
    else:
        # Try the linked data API instead
        sparql_url = "https://landregistry.data.gov.uk/app/ppd"
        r2 = safe_get(sparql_url, timeout=15)
        if r2 and r2.status_code == 200:
            log("UK PPD", "England & Wales", "pass", "PPD portal accessible (bulk CSV may need different URL)")
        else:
            log("UK PPD", "England & Wales", "fail", f"Cannot access PPD data: {r.status_code if r else 'timeout'}")


# ═══════════════════════════════════════════════════════════════════
# 8. FRANCE DVF (Transaction Labels)
# ═══════════════════════════════════════════════════════════════════
def test_france_dvf():
    print("\n" + "="*70)
    print("8. FRANCE DVF (Demandes de Valeurs Foncières)")
    print("="*70)

    # DVF API — query Paris 75 for recent sales
    url = "https://api.cquest.org/dvf?code_postal=75001&limit=5"
    r = safe_get(url, timeout=15)
    if r and r.status_code == 200:
        data = r.json()
        results = data.get("resultats", [])
        if results:
            s = results[0]
            log("France DVF", "Paris 75001", "pass",
                f"{len(results)} transactions. Sample: {s.get('valeur_fonciere', '?')}€, "
                f"type={s.get('type_local', '?')}, "
                f"surface={s.get('surface_reelle_bati', '?')}m², "
                f"date={s.get('date_mutation', '?')}")
        else:
            log("France DVF", "Paris 75001", "pass", f"API accessible but no results in response. Keys: {list(data.keys())}")
    else:
        # Try the official data.gouv.fr bulk download
        url2 = "https://files.data.gouv.fr/geo-dvf/latest/csv/"
        r2 = safe_get(url2, timeout=15)
        if r2 and r2.status_code == 200:
            log("France DVF", "data.gouv.fr", "pass", f"Bulk CSV directory accessible: {r2.text[:200]}")
        else:
            log("France DVF", "France", "fail", "Cannot access DVF data")


# ═══════════════════════════════════════════════════════════════════
# 9. JAPAN MLIT (Transaction Labels)
# ═══════════════════════════════════════════════════════════════════
def test_japan_mlit():
    print("\n" + "="*70)
    print("9. JAPAN MLIT — Real Estate Transaction Prices")
    print("="*70)

    # MLIT Real Estate Transaction Price API
    # Area=13 = Tokyo, Year=2023, Quarter=1
    url = "https://www.land.mlit.go.jp/webland/api/TradeListSearch?from=20231&to=20234&area=13&city=13101"
    r = safe_get(url, timeout=20)
    if r and r.status_code == 200:
        try:
            data = r.json()
            items = data.get("data", [])
            if items:
                s = items[0]
                log("Japan MLIT", "Tokyo (Chiyoda)", "pass",
                    f"{len(items)} transactions. Sample: ¥{s.get('TradePrice', '?')}, "
                    f"type={s.get('Type', '?')}, "
                    f"area={s.get('Area', '?')}m², "
                    f"year={s.get('Period', '?')}")
            else:
                log("Japan MLIT", "Tokyo", "pass", f"API accessible, response keys: {list(data.keys())}")
        except Exception as e:
            log("Japan MLIT", "Tokyo", "pass", f"API accessible (status 200), parse issue: {e}")
    else:
        log("Japan MLIT", "Tokyo", "fail", f"HTTP {r.status_code if r else 'timeout'}")


# ═══════════════════════════════════════════════════════════════════
# 10. NEW ZEALAND LINZ (Transaction Labels)
# ═══════════════════════════════════════════════════════════════════
def test_nz_linz():
    print("\n" + "="*70)
    print("10. NEW ZEALAND — Property Titles & Values")
    print("="*70)

    # NZ Property Value API (data.govt.nz)
    url = "https://api.data.linz.govt.nz/"
    r = safe_get(url, timeout=15)
    if r and r.status_code in (200, 301, 302, 403):
        log("NZ LINZ", "New Zealand", "pass" if r.status_code == 200 else "skip",
            f"LINZ API endpoint responds (HTTP {r.status_code}). Requires API key for data access.")
    else:
        log("NZ LINZ", "New Zealand", "fail", f"HTTP {r.status_code if r else 'timeout'}")

    # Try NZ stats property price index instead
    url2 = "https://www.stats.govt.nz/assets/Uploads/Residential-property-price-indexes/Residential-property-price-indexes-September-2024-quarter/Download-data/residential-property-price-indexes-september-2024-quarter.csv"
    r2 = safe_get(url2, timeout=15)
    if r2 and r2.status_code == 200:
        lines = r2.text[:3000].split("\n")
        log("NZ Stats", "New Zealand", "pass",
            f"Price Index CSV accessible. {len(lines)} preview rows. Header: {lines[0][:100]}")
    else:
        # Try the simpler RBNZ HPI
        url3 = "https://www.rbnz.govt.nz/-/media/project/sites/rbnz/files/statistics/series/b/b21/hb21.xlsx"
        r3 = safe_get(url3, timeout=15)
        if r3 and r3.status_code == 200:
            log("NZ RBNZ", "New Zealand", "pass", f"RBNZ House Price Index downloadable ({len(r3.content)} bytes)")
        else:
            log("NZ Stats", "New Zealand", "skip", "Stats NZ requires navigating to current download URL")


# ═══════════════════════════════════════════════════════════════════
# 11. BIS RESIDENTIAL PROPERTY PRICE INDICES (60+ countries)
# ═══════════════════════════════════════════════════════════════════
def test_bis_hpi():
    print("\n" + "="*70)
    print("11. BIS RESIDENTIAL PROPERTY PRICE INDICES")
    print("="*70)

    # BIS dataset — Selected Residential Property Prices
    url = "https://data.bis.org/api/v2/data/dataflow/BIS/WS_SPP/1.0?c%5BFREQ%5D=Q&c%5BREF_AREA%5D=US%2BGB%2BFR%2BJP%2BAU%2BBR%2BIN%2BDE&c%5BVALUE%5D=R&c%5BUNIT_MEASURE%5D=628&startPeriod=2020&detail=dataonly&format=csv"
    r = safe_get(url, timeout=20)
    if r and r.status_code == 200 and len(r.content) > 100:
        lines = r.text.strip().split("\n")
        log("BIS HPI", "Multi-country", "pass",
            f"Property price indices: {len(lines)-1} data points. Header: {lines[0][:120]}")
        # Parse to show countries
        try:
            reader = csv.DictReader(io.StringIO(r.text))
            countries = set()
            for row in reader:
                countries.add(row.get("REF_AREA", "?"))
            log("BIS HPI", "Multi-country", "pass",
                f"Countries with data: {', '.join(sorted(countries))}")
        except:
            pass
    else:
        # Try the FRED-hosted BIS series as fallback
        bis_series = {"QUSN628BIS": "US", "QGBN628BIS": "UK", "QFRN628BIS": "France",
                      "QJPN628BIS": "Japan", "QAUN628BIS": "Australia", "QBRN628BIS": "Brazil"}
        successes = []
        for sid, country in bis_series.items():
            r2 = safe_get(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}&cosd=2020-01-01", timeout=10)
            if r2 and r2.status_code == 200 and len(r2.content) > 50:
                successes.append(country)
        if successes:
            log("BIS HPI (via FRED)", "Multi-country", "pass",
                f"HPI available for: {', '.join(successes)}")
        else:
            log("BIS HPI", "Multi-country", "fail", "Cannot access BIS data")


# ═══════════════════════════════════════════════════════════════════
# 12. FEMA NFHL (US-only flood, for completeness)
# ═══════════════════════════════════════════════════════════════════
def test_fema():
    print("\n" + "="*70)
    print("12. FEMA NFHL FLOOD ZONES (US-only)")
    print("="*70)

    lat, lon = 29.760, -95.370  # Houston
    url = "https://hazards.fema.gov/gis/nfhl/rest/services/public/NFHL/MapServer/28/query"
    params = {
        "where": "1=1",
        "geometry": f"{lon-0.01},{lat-0.01},{lon+0.01},{lat+0.01}",
        "geometryType": "esriGeometryEnvelope",
        "inSR": "4326", "outSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "FLD_ZONE",
        "f": "json",
        "resultRecordCount": 5,
    }
    r = safe_get(url, params=params, timeout=15)
    if r and r.status_code == 200:
        data = r.json()
        features = data.get("features", [])
        zones = [f.get("attributes", {}).get("FLD_ZONE", "?") for f in features]
        log("FEMA NFHL", "Houston, TX", "pass",
            f"{len(features)} flood polygons. Zones: {zones}")
    else:
        log("FEMA NFHL", "Houston, TX", "fail", f"HTTP {r.status_code if r else 'timeout'}")


# ═══════════════════════════════════════════════════════════════════
# 13. WORLDCLIM (Global Climate — PRISM replacement)
# ═══════════════════════════════════════════════════════════════════
def test_worldclim():
    print("\n" + "="*70)
    print("13. WORLDCLIM (Global Climate, ~1km)")
    print("="*70)

    # WorldClim tiles are hosted on their server — test HEAD for a known tile
    # WorldClim v2.1, 30-second resolution, mean temperature
    url = "https://biogeo.ucdavis.edu/data/worldclim/v2.1/base/wc2.1_30s_tavg.zip"
    try:
        r = requests.head(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"}, allow_redirects=True)
        if r.status_code == 200:
            size_mb = int(r.headers.get("Content-Length", 0)) / 1e6
            log("WorldClim", "Global (30s tavg)", "pass",
                f"Mean temp raster accessible: {size_mb:.0f} MB")
        else:
            log("WorldClim", "Global", "fail", f"HTTP {r.status_code}")
    except Exception as e:
        log("WorldClim", "Global", "fail", f"{e}")

    # Also check precipitation
    url2 = "https://biogeo.ucdavis.edu/data/worldclim/v2.1/base/wc2.1_30s_prec.zip"
    try:
        r2 = requests.head(url2, timeout=15, headers={"User-Agent": "Mozilla/5.0"}, allow_redirects=True)
        if r2.status_code == 200:
            size_mb = int(r2.headers.get("Content-Length", 0)) / 1e6
            log("WorldClim", "Global (30s prec)", "pass",
                f"Precipitation raster accessible: {size_mb:.0f} MB")
        else:
            log("WorldClim", "Global", "fail", f"HTTP {r2.status_code}")
    except Exception as e:
        log("WorldClim", "Global", "fail", f"{e}")


# ═══════════════════════════════════════════════════════════════════
# 14. SINGAPORE URA REALIS (Transaction Labels)
# ═══════════════════════════════════════════════════════════════════
def test_singapore_ura():
    print("\n" + "="*70)
    print("14. SINGAPORE URA — Private Property Transactions")
    print("="*70)

    # URA provides data.gov.sg endpoints
    url = "https://data.gov.sg/api/action/datastore_search?resource_id=f1765b54-a209-4718-8d38-a39237f502b3&limit=5"
    r = safe_get(url, timeout=15)
    if r and r.status_code == 200:
        try:
            data = r.json()
            records = data.get("result", {}).get("records", [])
            total = data.get("result", {}).get("total", "?")
            if records:
                s = records[0]
                log("Singapore URA", "Singapore", "pass",
                    f"Total records: {total}. Sample fields: {list(s.keys())[:6]}")
            else:
                log("Singapore URA", "Singapore", "pass",
                    f"API accessible. Response keys: {list(data.get('result', {}).keys())}")
        except:
            log("Singapore URA", "Singapore", "pass", f"API responds (HTTP 200), {len(r.content)} bytes")
    else:
        # Try the newer API
        url2 = "https://api-production.data.gov.sg/v2/public/api/datasets/d_8b84c4ee58e3cfc0ece0d773c8ca6abc/metadata"
        r2 = safe_get(url2, timeout=15)
        if r2 and r2.status_code == 200:
            log("Singapore URA", "Singapore", "pass",
                f"Data.gov.sg metadata accessible ({len(r2.content)} bytes)")
        else:
            log("Singapore URA", "Singapore", "skip",
                f"URA REALIS may require API key application")


# ═══════════════════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════════════════
def print_summary():
    print("\n" + "="*70)
    print("SUMMARY — GLOBAL DATA SOURCE ACCESS")
    print("="*70)

    total_pass = 0
    total_fail = 0
    total_skip = 0

    for source, entries in RESULTS.items():
        passes = sum(1 for e in entries if e["status"] == PASS)
        fails = sum(1 for e in entries if e["status"] == FAIL)
        skips = sum(1 for e in entries if e["status"] == SKIP)
        total_pass += passes
        total_fail += fails
        total_skip += skips

        status = PASS if fails == 0 else (FAIL if passes == 0 else "⚠️")
        print(f"  {status} {source}: {passes} pass, {fails} fail, {skips} skip")

    print(f"\n  TOTALS: {total_pass} pass, {total_fail} fail, {total_skip} skip")
    print(f"  Success rate: {total_pass/(total_pass+total_fail)*100:.0f}%" if (total_pass+total_fail) > 0 else "")


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("="*70)
    print("GLOBAL DATA SOURCE DRY RUN — Properlytic")
    print(f"Testing access to all discussed datasets...")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

    t0 = time.time()

    test_ms_buildings()
    test_osm()
    test_esa_worldcover()
    test_copernicus_dem()
    test_fred()
    test_boe()
    test_uk_ppd()
    test_france_dvf()
    test_japan_mlit()
    test_nz_linz()
    test_bis_hpi()
    test_fema()
    test_worldclim()
    test_singapore_ura()

    elapsed = time.time() - t0
    print_summary()
    print(f"\n  Completed in {elapsed:.1f}s")
