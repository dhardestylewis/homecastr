"""
Dry Run V2: Scaled Global Data Source Access
=============================================
Expanded from V1: 15+ cities, 20+ data sources, covering all
sources discussed for global property price prediction.

Usage:
    python scripts/data_acquisition/dry_run_global_sources_v2.py
"""

import requests
import json
import time
import csv
import io
import gzip
from collections import OrderedDict

# ─── 15 test locations across 6 continents ───
LOCATIONS = OrderedDict([
    # North America
    ("Houston TX",       (29.760, -95.370)),
    ("New York NY",      (40.712, -74.006)),
    ("Toronto CA",       (43.653, -79.383)),
    ("Mexico City MX",   (19.433, -99.133)),
    # Europe
    ("London UK",        (51.510, -0.130)),
    ("Paris FR",         (48.860,  2.350)),
    ("Berlin DE",        (52.520, 13.405)),
    ("Amsterdam NL",     (52.370,  4.895)),
    ("Stockholm SE",     (59.330, 18.069)),
    # Asia
    ("Tokyo JP",         (35.680, 139.690)),
    ("Hong Kong HK",     (22.302, 114.177)),
    ("Mumbai IN",        (19.076,  72.878)),
    ("Singapore SG",     ( 1.352, 103.820)),
    # Oceania
    ("Sydney AU",       (-33.870, 151.210)),
    # South America
    ("São Paulo BR",    (-23.551, -46.634)),
])

RESULTS = OrderedDict()
PASS = "✅"
FAIL = "❌"
SKIP = "⏭️"

def log(source, location, status, detail):
    key = source
    if key not in RESULTS:
        RESULTS[key] = []
    icon = PASS if status == "pass" else (FAIL if status == "fail" else SKIP)
    RESULTS[key].append({"location": location, "status": icon, "detail": detail})
    print(f"  {icon} [{location}] {detail}")

def safe_get(url, timeout=15, **kwargs):
    try:
        headers = kwargs.pop("headers", {"User-Agent": "Mozilla/5.0 Properlytic-DryRun/2.0"})
        r = requests.get(url, timeout=timeout, headers=headers, **kwargs)
        return r
    except Exception as e:
        return None

def safe_post(url, timeout=15, **kwargs):
    try:
        headers = kwargs.pop("headers", {"User-Agent": "Mozilla/5.0 Properlytic-DryRun/2.0"})
        r = requests.post(url, timeout=timeout, headers=headers, **kwargs)
        return r
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════
# 1. MS BUILDING FOOTPRINTS (Global — all 15 cities)
# ═══════════════════════════════════════════════════════════════════
def test_ms_buildings():
    print("\n" + "="*70)
    print("1. MS BUILDING FOOTPRINTS — Global Coverage Test")
    print("="*70)

    r = safe_get("https://minedbuildings.z5.web.core.windows.net/global-buildings/dataset-links.csv", timeout=20)
    if not r or r.status_code != 200:
        log("MS Buildings", "Index", "fail", "Cannot access dataset index")
        return

    reader = csv.DictReader(io.StringIO(r.text))
    rows = list(reader)
    countries = sorted(set(row.get("Location", "") for row in rows))
    log("MS Buildings", "Index", "pass", f"{len(rows)} tiles, {len(countries)} countries/regions")

    # Test a small tile from multiple regions
    test_regions = {
        "London UK": "UnitedKingdom",
        "Tokyo JP": "Japan",
        "São Paulo BR": "Brazil",
        "Mumbai IN": "India",
        "Sydney AU": "Australia",
        "Berlin DE": "Germany",
        "Mexico City MX": "Mexico",
        "Hong Kong HK": "HongKong",  # May be under China
    }

    for city, region in test_regions.items():
        tiles = [r for r in rows if r.get("Location") == region]
        if not tiles:
            # Try alternative names
            alts = [r for r in rows if region.lower() in r.get("Location", "").lower()]
            if alts:
                tiles = alts
                region = alts[0].get("Location", region)

        if tiles:
            log("MS Buildings", city, "pass", f"{len(tiles)} tiles for '{region}'")
        else:
            log("MS Buildings", city, "fail", f"No tiles for '{region}'. Checking alternatives...")
            # Check if it's under a continental grouping
            possible = [r.get("Location") for r in rows
                       if any(kw in r.get("Location", "").lower()
                             for kw in region.lower().split())]
            if possible:
                log("MS Buildings", city, "pass", f"Found under: {set(possible)}")


# ═══════════════════════════════════════════════════════════════════
# 2. OSM POIs + RESIDENTIAL BUILDING TAGS (Global)
# ═══════════════════════════════════════════════════════════════════
def test_osm():
    print("\n" + "="*70)
    print("2. OSM POIs + RESIDENTIAL BUILDINGS")
    print("="*70)

    overpass_url = "https://overpass-api.de/api/interpreter"

    # Test a subset to avoid rate limits — pick diverse cities
    test_cities = ["Houston TX", "London UK", "Berlin DE", "Tokyo JP", "Mumbai IN", "São Paulo BR"]

    for name in test_cities:
        lat, lon = LOCATIONS[name]
        # Combined query: POIs + residential building count in 500m
        query = f"""
        [out:json][timeout:10];
        (
          node["amenity"~"school|hospital"](around:500,{lat},{lon});
          node["public_transport"="stop_position"](around:500,{lat},{lon});
        );
        out count;
        """
        r = safe_get(overpass_url, params={"data": query}, timeout=15)
        if r and r.status_code == 200:
            data = r.json()
            total = data.get("elements", [{}])[0].get("tags", {}).get("total", "?") if data.get("elements") else "?"
            log("OSM POIs", name, "pass", f"Amenities within 500m: {total}")
        else:
            log("OSM POIs", name, "fail", f"Timeout/rate-limited")
        time.sleep(2)  # Respect rate limits

    # Separate test: OSM residential building tags
    print("\n  --- OSM Residential Building Tags ---")
    for name in ["London UK", "Berlin DE", "Tokyo JP"]:
        lat, lon = LOCATIONS[name]
        query = f"""
        [out:json][timeout:10];
        way["building"~"residential|apartments|house|detached"](around:300,{lat},{lon});
        out count;
        """
        r = safe_get(overpass_url, params={"data": query}, timeout=15)
        if r and r.status_code == 200:
            data = r.json()
            total = data.get("elements", [{}])[0].get("tags", {}).get("total", "?") if data.get("elements") else "?"
            log("OSM Residential Tags", name, "pass", f"Tagged residential buildings within 300m: {total}")
        else:
            log("OSM Residential Tags", name, "fail", "Timeout")
        time.sleep(2)


# ═══════════════════════════════════════════════════════════════════
# 3. ESA WORLDCOVER — All 15 cities
# ═══════════════════════════════════════════════════════════════════
def test_esa_worldcover():
    print("\n" + "="*70)
    print("3. ESA WORLDCOVER (10m LULC) — All 15 cities")
    print("="*70)

    stac_url = "https://planetarycomputer.microsoft.com/api/stac/v1/search"

    for name, (lat, lon) in LOCATIONS.items():
        payload = {
            "collections": ["esa-worldcover"],
            "intersects": {"type": "Point", "coordinates": [lon, lat]},
            "limit": 1,
        }
        try:
            r = requests.post(stac_url, json=payload, timeout=10,
                            headers={"User-Agent": "Mozilla/5.0"})
            if r and r.status_code == 200:
                data = r.json()
                features = data.get("features", [])
                if features:
                    bbox = features[0].get("bbox", [])
                    log("ESA WorldCover", name, "pass",
                        f"Tile bbox: {[round(b,1) for b in bbox[:4]]}")
                else:
                    log("ESA WorldCover", name, "fail", "No tile")
            else:
                log("ESA WorldCover", name, "fail", f"HTTP {r.status_code if r else '?'}")
        except Exception as e:
            log("ESA WorldCover", name, "fail", f"{e}")


# ═══════════════════════════════════════════════════════════════════
# 4. COPERNICUS DEM GLO-30 — All 15 cities
# ═══════════════════════════════════════════════════════════════════
def test_copernicus_dem():
    print("\n" + "="*70)
    print("4. COPERNICUS DEM GLO-30 (30m Elevation) — All 15 cities")
    print("="*70)

    stac_url = "https://planetarycomputer.microsoft.com/api/stac/v1/search"

    for name, (lat, lon) in LOCATIONS.items():
        payload = {
            "collections": ["cop-dem-glo-30"],
            "intersects": {"type": "Point", "coordinates": [lon, lat]},
            "limit": 1,
        }
        try:
            r = requests.post(stac_url, json=payload, timeout=10,
                            headers={"User-Agent": "Mozilla/5.0"})
            if r and r.status_code == 200:
                data = r.json()
                features = data.get("features", [])
                if features:
                    tile_id = features[0].get("id", "?")
                    log("Copernicus DEM", name, "pass", f"Tile: {tile_id}")
                else:
                    log("Copernicus DEM", name, "fail", "No tile")
            else:
                log("Copernicus DEM", name, "fail", f"HTTP {r.status_code if r else '?'}")
        except Exception as e:
            log("Copernicus DEM", name, "fail", f"{e}")


# ═══════════════════════════════════════════════════════════════════
# 5. VIIRS NIGHTTIME LIGHTS (Global wealth proxy)
# ═══════════════════════════════════════════════════════════════════
def test_viirs_nightlights():
    print("\n" + "="*70)
    print("5. VIIRS NIGHTTIME LIGHTS (500m, Global)")
    print("="*70)

    # EOG / Colorado School of Mines hosts VIIRS annual composites
    # Check the STAC on Planetary Computer first
    stac_url = "https://planetarycomputer.microsoft.com/api/stac/v1/search"

    test_cities = ["Houston TX", "London UK", "Tokyo JP", "Mumbai IN", "São Paulo BR"]
    for name in test_cities:
        lat, lon = LOCATIONS[name]
        # Try the NASA Black Marble (VNP46A4) annual composites
        payload = {
            "collections": ["viirs-nighttime"],  # May not exist on PC
            "intersects": {"type": "Point", "coordinates": [lon, lat]},
            "limit": 1,
        }
        try:
            r = requests.post(stac_url, json=payload, timeout=10,
                            headers={"User-Agent": "Mozilla/5.0"})
            if r and r.status_code == 200:
                data = r.json()
                if data.get("features"):
                    log("VIIRS Nightlights", name, "pass", f"Tile found on Planetary Computer")
                    continue
        except:
            pass

        # Fallback: check EOG direct data access
        # Annual VNL V2 composites
        eog_url = "https://eogdata.mines.edu/nighttime_light/annual/v22/2023/"
        r = safe_get(eog_url + "?list", timeout=10)
        if r and r.status_code == 200:
            log("VIIRS Nightlights", name, "pass",
                f"EOG annual composites accessible (need tile download for {name})")
        else:
            # Try NASA LAADS DAAC / Earthdata
            log("VIIRS Nightlights", name, "skip",
                "EOG/Planetary Computer not directly reachable; available via NASA Earthdata")


# ═══════════════════════════════════════════════════════════════════
# 6. JRC GLOBAL SURFACE WATER (Free flood proxy)
# ═══════════════════════════════════════════════════════════════════
def test_jrc_water():
    print("\n" + "="*70)
    print("6. JRC GLOBAL SURFACE WATER (30m, Global flood proxy)")
    print("="*70)

    stac_url = "https://planetarycomputer.microsoft.com/api/stac/v1/search"

    test_cities = ["Houston TX", "London UK", "Tokyo JP", "Amsterdam NL", "Mumbai IN"]
    for name in test_cities:
        lat, lon = LOCATIONS[name]
        payload = {
            "collections": ["jrc-gsw"],
            "intersects": {"type": "Point", "coordinates": [lon, lat]},
            "limit": 1,
        }
        try:
            r = requests.post(stac_url, json=payload, timeout=10,
                            headers={"User-Agent": "Mozilla/5.0"})
            if r and r.status_code == 200:
                data = r.json()
                if data.get("features"):
                    assets = list(data["features"][0].get("assets", {}).keys())
                    log("JRC Surface Water", name, "pass",
                        f"Tile found. Assets: {assets[:4]}")
                else:
                    log("JRC Surface Water", name, "fail", "No tile on PC")
            else:
                log("JRC Surface Water", name, "fail", f"HTTP {r.status_code}")
        except Exception as e:
            log("JRC Surface Water", name, "fail", f"{e}")


# ═══════════════════════════════════════════════════════════════════
# 7. FRED MACRO — Expanded to more countries via BIS
# ═══════════════════════════════════════════════════════════════════
def test_fred_expanded():
    print("\n" + "="*70)
    print("7. FRED + BIS MACRO — Price Indices for 15+ Countries")
    print("="*70)

    # BIS Residential Property Price via FRED
    bis_series = {
        "QUSN628BIS":  "US",       "QGBN628BIS": "UK",
        "QFRN628BIS":  "France",   "QDEN628BIS": "Germany",
        "QJPN628BIS":  "Japan",    "QAUN628BIS": "Australia",
        "QBRN628BIS":  "Brazil",   "QINN628BIS": "India",
        "QCAN628BIS":  "Canada",   "QMXN628BIS": "Mexico",
        "QNLN628BIS":  "Netherlands", "QSEN628BIS": "Sweden",
        "QSGN628BIS":  "Singapore", "QHKN628BIS": "Hong Kong",
        "QNZN628BIS":  "New Zealand", "QCNN628BIS": "China",
        "QIDN628BIS":  "Indonesia", "QZAN628BIS": "South Africa",
        "QKRN628BIS":  "South Korea", "QNON628BIS": "Norway",
    }

    passed = []
    failed = []
    for sid, country in bis_series.items():
        url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}&cosd=2015-01-01"
        r = safe_get(url, timeout=10)
        if r and r.status_code == 200 and len(r.content) > 50:
            lines = r.text.strip().split("\n")
            last = lines[-1] if lines else "?"
            passed.append(country)
            log("BIS HPI (FRED)", country, "pass", f"{len(lines)-1} quarters. Latest: {last}")
        else:
            failed.append(country)
            log("BIS HPI (FRED)", country, "fail", f"No data (series {sid})")

    print(f"\n  Summary: {len(passed)}/{len(bis_series)} countries with BIS HPI on FRED")
    if failed:
        print(f"  Missing: {', '.join(failed)}")


# ═══════════════════════════════════════════════════════════════════
# 8. OECD HOUSE PRICE INDICES (40+ countries)
# ═══════════════════════════════════════════════════════════════════
def test_oecd_hpi():
    print("\n" + "="*70)
    print("8. OECD HOUSE PRICE INDICES")
    print("="*70)

    # OECD SDMX API — House Price Indices dataset
    url = "https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_AN_HOUSE_PRICES@DF_HOUSE_PRICES,1.0/Q...N.INDEX.2015..?startPeriod=2023&dimensionAtObservation=AllDimensions&format=csvfilewithlabels"
    r = safe_get(url, timeout=20)
    if r and r.status_code == 200 and len(r.content) > 200:
        lines = r.text.strip().split("\n")
        # Parse countries
        try:
            reader = csv.DictReader(io.StringIO(r.text))
            countries = set()
            for row in reader:
                ref = row.get("REF_AREA", row.get("Reference area", "?"))
                countries.add(ref)
            log("OECD HPI", "Multi-country", "pass",
                f"{len(lines)-1} data points across {len(countries)} countries")
            log("OECD HPI", "Countries", "pass",
                f"Available: {', '.join(sorted(list(countries))[:20])}...")
        except:
            log("OECD HPI", "Multi-country", "pass",
                f"Data accessible: {len(lines)} rows. Header: {lines[0][:120]}")
    else:
        # Try the older API endpoint
        url2 = "https://stats.oecd.org/SDMX-JSON/data/HOUSE_PRICES/AUS+BRA+CAN+CHN+FRA+DEU+IND+JPN+MEX+NLD+NZL+NOR+SGP+KOR+SWE+GBR+USA.REAL.Q/all?startTime=2023"
        r2 = safe_get(url2, timeout=15)
        if r2 and r2.status_code == 200:
            data = r2.json()
            log("OECD HPI", "Multi-country", "pass", f"SDMX-JSON accessible ({len(r2.content)} bytes)")
        else:
            log("OECD HPI", "Multi-country", "fail",
                f"HTTP {r.status_code if r else 'timeout'} / {r2.status_code if r2 else 'timeout'}")


# ═══════════════════════════════════════════════════════════════════
# 9. EUROSTAT NUTS-2/3 HOUSE PRICES (EU27)
# ═══════════════════════════════════════════════════════════════════
def test_eurostat_hpi():
    print("\n" + "="*70)
    print("9. EUROSTAT HOUSE PRICE INDEX (NUTS-0, quarterly)")
    print("="*70)

    # Eurostat JSON API — prc_hpi_q (House Price Index)
    url = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/prc_hpi_q/?format=JSON&geo=DE&geo=FR&geo=NL&geo=SE&geo=IT&geo=ES&startPeriod=2023-Q1&sinceTimePeriod=2023-Q1"
    r = safe_get(url, timeout=15)
    if r and r.status_code == 200:
        try:
            data = r.json()
            dims = data.get("dimension", {}).get("geo", {}).get("category", {}).get("label", {})
            vals = data.get("value", {})
            log("Eurostat HPI", "EU", "pass",
                f"Countries: {list(dims.values()) if dims else '?'}. {len(vals)} data points")
        except:
            log("Eurostat HPI", "EU", "pass", f"API accessible ({len(r.content)} bytes)")
    else:
        log("Eurostat HPI", "EU", "fail", f"HTTP {r.status_code if r else 'timeout'}")


# ═══════════════════════════════════════════════════════════════════
# 10. TRANSACTION DATA — Country-specific sources
# ═══════════════════════════════════════════════════════════════════
def test_transaction_sources():
    print("\n" + "="*70)
    print("10. TRANSACTION DATA — Country-Specific Sources")
    print("="*70)

    # ── UK PPD ──
    print("\n  --- UK Price Paid Data ---")
    url = "http://prod.publicdata.landregistry.gov.uk/pp-monthly-update-new-version.csv"
    r = safe_get(url, timeout=15)
    if r and r.status_code == 200 and len(r.content) > 100:
        lines = r.text[:3000].split("\n")
        log("UK PPD", "England & Wales", "pass",
            f"Monthly update accessible. {len(lines)} preview rows")
    else:
        # Try the complete file
        r2 = safe_get("http://prod.publicdata.landregistry.gov.uk/pp-complete.csv", timeout=15)
        if r2 and r2.status_code in (200, 206):
            log("UK PPD", "England & Wales", "pass", f"Complete dataset accessible")
        else:
            log("UK PPD", "England & Wales", "skip", "Bulk CSV requires direct download")

    # ── France DVF ──
    print("\n  --- France DVF ---")
    url = "https://files.data.gouv.fr/geo-dvf/latest/csv/2024/full.csv.gz"
    r = safe_get(url, timeout=15)
    if r and r.status_code in (200, 206):
        log("France DVF", "France", "pass",
            f"2024 bulk CSV accessible ({len(r.content)/1e6:.1f} MB partial)")
    else:
        r2 = safe_get("https://files.data.gouv.fr/geo-dvf/latest/csv/", timeout=10)
        if r2 and r2.status_code == 200:
            log("France DVF", "France", "pass", "Bulk directory listing accessible")
        else:
            log("France DVF", "France", "fail", "Cannot access DVF")

    # ── Japan MLIT ──
    print("\n  --- Japan MLIT ---")
    url = "https://www.land.mlit.go.jp/webland/api/TradeListSearch?from=20234&to=20234&area=13&city=13101"
    r = safe_get(url, timeout=20)
    if r and r.status_code == 200:
        try:
            data = r.json()
            items = data.get("data", [])
            if items:
                s = items[0]
                log("Japan MLIT", "Tokyo Chiyoda", "pass",
                    f"{len(items)} transactions. Fields: {list(s.keys())[:6]}")
            else:
                log("Japan MLIT", "Tokyo", "pass", f"API accessible, keys: {list(data.keys())}")
        except:
            log("Japan MLIT", "Tokyo", "pass", f"API responds ({len(r.content)} bytes)")
    else:
        log("Japan MLIT", "Tokyo", "fail", f"HTTP {r.status_code if r else 'timeout'}")

    # ── Hong Kong EPRC ──
    print("\n  --- Hong Kong EPRC ---")
    url = "https://www1.censtatd.gov.hk/eindex.jsp"
    r = safe_get(url, timeout=15)
    if r and r.status_code == 200:
        log("HK EPRC", "Hong Kong", "pass", "Census & Statistics site accessible")
    else:
        log("HK EPRC", "Hong Kong", "skip", "Government site may require specific endpoints")

    # HK Rating & Valuation Dept property review
    url2 = "https://www.rvd.gov.hk/en/property_market_statistics/index.html"
    r2 = safe_get(url2, timeout=15)
    if r2 and r2.status_code == 200:
        log("HK RVD", "Hong Kong", "pass", "Rating & Valuation Dept property stats accessible")
    else:
        log("HK RVD", "Hong Kong", "fail", f"HTTP {r2.status_code if r2 else 'timeout'}")

    # ── Netherlands CBS ──
    print("\n  --- Netherlands CBS StatLine ---")
    # CBS OData API — residential property transactions
    url = "https://opendata.cbs.nl/ODataApi/OData/83913NED?$top=5&$format=json"
    r = safe_get(url, timeout=15)
    if r and r.status_code == 200:
        try:
            data = r.json()
            vals = data.get("value", [])
            if vals:
                log("NL CBS", "Netherlands", "pass",
                    f"Property transaction data. {len(vals)} records. Sample keys: {list(vals[0].keys())[:5]}")
            else:
                log("NL CBS", "Netherlands", "pass", f"API accessible. Keys: {list(data.keys())}")
        except:
            log("NL CBS", "Netherlands", "pass", f"API responds ({len(r.content)} bytes)")
    else:
        # Try alternate CBS endpoint
        url2 = "https://opendata.cbs.nl/ODataApi/OData/83913NED"
        r2 = safe_get(url2, timeout=15)
        if r2 and r2.status_code == 200:
            log("NL CBS", "Netherlands", "pass", "CBS OData catalogue accessible")
        else:
            log("NL CBS", "Netherlands", "fail", f"HTTP {r.status_code if r else 'timeout'}")

    # ── Sweden SCB ──
    print("\n  --- Sweden SCB (Statistics Sweden) ---")
    # SCB API — property prices by region (BO0501)
    url = "https://api.scb.se/OV0104/v1/doris/en/ssd/BO/BO0501/BO0501B/FastprisSHRegAr"
    r = safe_get(url, timeout=15)
    if r and r.status_code == 200:
        try:
            data = r.json()
            variables = [v.get("text", "?") for v in data.get("variables", [])]
            log("SE SCB", "Sweden", "pass",
                f"Property price index API. Variables: {variables}")
        except:
            log("SE SCB", "Sweden", "pass", f"API responds ({len(r.content)} bytes)")
    else:
        log("SE SCB", "Sweden", "fail", f"HTTP {r.status_code if r else 'timeout'}")

    # ── Germany BORIS ──
    print("\n  --- Germany BORIS (Property Values) ---")
    url = "https://www.borisportal.de/"
    r = safe_get(url, timeout=15)
    if r and r.status_code == 200:
        log("DE BORIS", "Germany", "pass", "BORIS property portal accessible")
    else:
        log("DE BORIS", "Germany", "skip", "BORIS portal may vary by Bundesland")

    # Destatis house price index
    url2 = "https://www-genesis.destatis.de/genesis/online?operation=find&suchanweisung=61262"
    r2 = safe_get(url2, timeout=15)
    if r2 and r2.status_code == 200:
        log("DE Destatis", "Germany", "pass", "Destatis property price index accessible")
    else:
        log("DE Destatis", "Germany", "skip", "Requires GENESIS API registration")

    # ── Singapore URA ──
    print("\n  --- Singapore URA ---")
    url = "https://api-production.data.gov.sg/v2/public/api/datasets/d_8b84c4ee58e3cfc0ece0d773c8ca6abc/metadata"
    r = safe_get(url, timeout=15)
    if r and r.status_code == 200:
        log("SG URA", "Singapore", "pass", f"Data.gov.sg property metadata ({len(r.content)} bytes)")
    else:
        log("SG URA", "Singapore", "fail", f"HTTP {r.status_code if r else 'timeout'}")

    # ── Australia NSW Valuer General ──
    print("\n  --- Australia (NSW Valuer General) ---")
    url = "https://www.valuergeneral.nsw.gov.au/land_value_summaries/lv.php"
    r = safe_get(url, timeout=15)
    if r and r.status_code == 200:
        log("AU NSW VG", "Sydney", "pass", "NSW Valuer General site accessible")
    else:
        log("AU NSW VG", "Sydney", "skip", f"HTTP {r.status_code if r else 'timeout'}")

    # ABS (Australian Bureau of Stats) house prices
    url2 = "https://api.data.abs.gov.au/data/ABS,RPP,1.0/Q.1+1GSYD+1GMEL+1GPER.10.Q?startPeriod=2023&detail=dataonly&format=csv"
    r2 = safe_get(url2, timeout=15)
    if r2 and r2.status_code == 200:
        lines = r2.text.strip().split("\n")
        log("AU ABS", "Australia", "pass", f"ABS Residential Property Prices: {len(lines)-1} data points")
    else:
        log("AU ABS", "Australia", "fail", f"HTTP {r2.status_code if r2 else 'timeout'}")

    # ── Canada ──
    print("\n  --- Canada ---")
    # Stats Canada — New Housing Price Index (table 18-10-0205-01)
    url = "https://www150.statcan.gc.ca/t1/tbl1/en/dtl!downloadTbl/en/TV/74047?mime=text/csv"
    r = safe_get(url, timeout=15)
    if r and r.status_code == 200 and len(r.content) > 100:
        log("CA StatsCan", "Canada", "pass", f"NHPI data accessible ({len(r.content)} bytes)")
    else:
        # Try CANSIM API
        url2 = "https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1810020501"
        r2 = safe_get(url2, timeout=15)
        if r2 and r2.status_code == 200:
            log("CA StatsCan", "Canada", "pass", "NHPI table page accessible")
        else:
            log("CA StatsCan", "Canada", "skip", "Requires navigating StatsCan download flow")

    # ── Brazil FipeZap ──
    print("\n  --- Brazil FipeZap ---")
    url = "https://www.fipe.org.br/api/fipezap/indicadores/precos/resumo"
    r = safe_get(url, timeout=15)
    if r and r.status_code == 200:
        try:
            data = r.json()
            log("BR FipeZap", "Brazil", "pass", f"Price index API: {list(data.keys()) if isinstance(data, dict) else f'{len(data)} items'}")
        except:
            log("BR FipeZap", "Brazil", "pass", f"API responds ({len(r.content)} bytes)")
    else:
        # IBGE SIDRA (national statistics) as fallback
        url2 = "https://apisidra.ibge.gov.br/values/t/6588/n1/all/p/last%201"
        r2 = safe_get(url2, timeout=15)
        if r2 and r2.status_code == 200:
            log("BR IBGE", "Brazil", "pass", f"IBGE real estate stats accessible")
        else:
            log("BR FipeZap", "Brazil", "fail", f"HTTP {r.status_code if r else 'timeout'}")

    # ── India NHB RESIDEX ──
    print("\n  --- India NHB RESIDEX ---")
    url = "https://residex.nhbonline.org.in/"
    r = safe_get(url, timeout=15)
    if r and r.status_code == 200:
        log("IN RESIDEX", "India", "pass", "NHB RESIDEX portal accessible")
    else:
        log("IN RESIDEX", "India", "fail", f"HTTP {r.status_code if r else 'timeout'}")

    # RBI housing data as fallback
    url2 = "https://rbi.org.in/Scripts/PublicationsView.aspx?id=22250"
    r2 = safe_get(url2, timeout=15)
    if r2 and r2.status_code == 200:
        log("IN RBI", "India", "pass", "RBI housing statistics page accessible")
    else:
        log("IN RBI", "India", "skip", "RBI may have different publication URLs")

    # ── China NBS ──
    print("\n  --- China NBS 70 Cities ---")
    url = "https://data.stats.gov.cn/english/easyquery.htm?m=QueryData&dbcode=hgyd&rowcode=zb&colcode=sj&wds=[]&dfwds=[{%22wdcode%22:%22zb%22,%22valuecode%22:%22A010801%22}]&k1=1&h=1"
    r = safe_get(url, timeout=15)
    if r and r.status_code == 200:
        log("CN NBS", "China", "pass", f"NBS API accessible ({len(r.content)} bytes)")
    else:
        log("CN NBS", "China", "fail", f"HTTP {r.status_code if r else 'timeout'}")


# ═══════════════════════════════════════════════════════════════════
# 11. US CENSUS TIGER (County/Block boundaries)
# ═══════════════════════════════════════════════════════════════════
def test_census_tiger():
    print("\n" + "="*70)
    print("11. US CENSUS TIGER (County/Block boundaries)")
    print("="*70)

    # Query Harris County boundary
    url = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Current/MapServer/82/query"
    params = {
        "where": "GEOID='48201'",
        "outFields": "GEOID,BASENAME",
        "returnGeometry": "false",
        "f": "json",
        "resultRecordCount": 1,
    }
    r = safe_get(url, params=params, timeout=15)
    if r and r.status_code == 200:
        data = r.json()
        features = data.get("features", [])
        if features:
            attrs = features[0].get("attributes", {})
            log("Census TIGER", "Harris County TX", "pass",
                f"GEOID={attrs.get('GEOID')}, Name={attrs.get('BASENAME')}")
        else:
            log("Census TIGER", "Harris County TX", "pass", f"API responds, {len(r.content)} bytes")
    else:
        log("Census TIGER", "Harris County TX", "fail", f"HTTP {r.status_code if r else 'timeout'}")


# ═══════════════════════════════════════════════════════════════════
# 12. WORLDCLIM (retry with alternate mirror)
# ═══════════════════════════════════════════════════════════════════
def test_worldclim():
    print("\n" + "="*70)
    print("12. WORLDCLIM (Global Climate, 1km)")
    print("="*70)

    # Try multiple download endpoints
    urls = [
        ("biogeo.ucdavis.edu", "https://biogeo.ucdavis.edu/data/worldclim/v2.1/base/wc2.1_30s_tavg.zip"),
        ("Geodata (alt mirror)", "https://geodata.ucdavis.edu/climate/worldclim/2_1/base/wc2.1_30s_tavg.zip"),
        ("WorldClim tiles (2.5m)", "https://biogeo.ucdavis.edu/data/worldclim/v2.1/base/wc2.1_2.5m_tavg.zip"),
    ]

    for label, url in urls:
        try:
            r = requests.head(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"}, allow_redirects=True)
            if r.status_code == 200:
                size = int(r.headers.get("Content-Length", 0)) / 1e6
                log("WorldClim", label, "pass", f"Accessible ({size:.0f} MB)")
                break
            else:
                log("WorldClim", label, "fail", f"HTTP {r.status_code}")
        except Exception as e:
            log("WorldClim", label, "fail", f"{str(e)[:80]}")

    # Also check Planetary Computer for climate data as alternative
    stac_url = "https://planetarycomputer.microsoft.com/api/stac/v1/search"
    payload = {
        "collections": ["terraclimate"],
        "intersects": {"type": "Point", "coordinates": [2.35, 48.86]},
        "limit": 1,
    }
    try:
        r = requests.post(stac_url, json=payload, timeout=10,
                        headers={"User-Agent": "Mozilla/5.0"})
        if r and r.status_code == 200:
            data = r.json()
            if data.get("features"):
                log("TerraClimate (alt)", "Paris FR", "pass",
                    "TerraClimate available on Planetary Computer as WorldClim alternative")
    except:
        pass


# ═══════════════════════════════════════════════════════════════════
# 13. FEMA NFHL (retry)
# ═══════════════════════════════════════════════════════════════════
def test_fema():
    print("\n" + "="*70)
    print("13. FEMA NFHL FLOOD ZONES (US-only)")
    print("="*70)

    for name, (lat, lon) in [("Houston TX", (29.760, -95.370)), ("New York NY", (40.712, -74.006))]:
        url = "https://hazards.fema.gov/gis/nfhl/rest/services/public/NFHL/MapServer/28/query"
        params = {
            "where": "1=1",
            "geometry": f"{lon-0.005},{lat-0.005},{lon+0.005},{lat+0.005}",
            "geometryType": "esriGeometryEnvelope",
            "inSR": "4326", "outSR": "4326",
            "spatialRel": "esriSpatialRelIntersects",
            "outFields": "FLD_ZONE",
            "f": "json",
            "resultRecordCount": 3,
        }
        r = safe_get(url, params=params, timeout=15)
        if r and r.status_code == 200:
            data = r.json()
            features = data.get("features", [])
            zones = [f.get("attributes", {}).get("FLD_ZONE", "?") for f in features]
            log("FEMA NFHL", name, "pass", f"{len(features)} polygons. Zones: {zones}")
        else:
            log("FEMA NFHL", name, "fail", f"HTTP {r.status_code if r else 'timeout'}")


# ═══════════════════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════════════════
def print_summary():
    print("\n" + "="*70)
    print("SUMMARY — GLOBAL DATA SOURCE ACCESS (V2)")
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
    pct = total_pass / (total_pass + total_fail) * 100 if (total_pass + total_fail) > 0 else 0
    print(f"  Success rate: {pct:.0f}%")

    # Categorized summary
    print("\n" + "-"*70)
    print("  CATEGORY BREAKDOWN")
    print("-"*70)

    categories = {
        "Geospatial (building entities)": ["MS Buildings"],
        "OSM (POIs + network)": ["OSM POIs", "OSM Residential Tags", "OSM Roads"],
        "Rasters (LULC/DEM/climate)": ["ESA WorldCover", "Copernicus DEM", "WorldClim", "TerraClimate (alt)", "JRC Surface Water"],
        "Macro indicators": ["BIS HPI (FRED)", "OECD HPI", "Eurostat HPI"],
        "Transaction data": ["UK PPD", "France DVF", "Japan MLIT", "HK EPRC", "HK RVD",
                           "NL CBS", "SE SCB", "DE BORIS", "DE Destatis", "SG URA",
                           "AU NSW VG", "AU ABS", "CA StatsCan", "BR FipeZap", "BR IBGE",
                           "IN RESIDEX", "IN RBI", "CN NBS"],
        "US-specific": ["FEMA NFHL", "Census TIGER"],
        "Remote sensing proxies": ["VIIRS Nightlights"],
    }

    for cat, sources in categories.items():
        cat_pass = sum(sum(1 for e in RESULTS.get(s, []) if e["status"] == PASS) for s in sources)
        cat_fail = sum(sum(1 for e in RESULTS.get(s, []) if e["status"] == FAIL) for s in sources)
        cat_skip = sum(sum(1 for e in RESULTS.get(s, []) if e["status"] == SKIP) for s in sources)
        cat_total = cat_pass + cat_fail + cat_skip
        if cat_total > 0:
            icon = PASS if cat_fail == 0 else ("⚠️" if cat_pass > 0 else FAIL)
            print(f"  {icon} {cat}: {cat_pass}/{cat_total} ({cat_pass/(cat_pass+cat_fail)*100:.0f}%)" if (cat_pass+cat_fail) > 0 else f"  {icon} {cat}: {cat_skip} skipped")


# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("="*70)
    print("GLOBAL DATA SOURCE DRY RUN V2 — Properlytic")
    print(f"Testing {len(LOCATIONS)} cities across 6 continents")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

    t0 = time.time()

    test_ms_buildings()
    test_osm()
    test_esa_worldcover()
    test_copernicus_dem()
    test_viirs_nightlights()
    test_jrc_water()
    test_fred_expanded()
    test_oecd_hpi()
    test_eurostat_hpi()
    test_transaction_sources()
    test_census_tiger()
    test_worldclim()
    test_fema()

    elapsed = time.time() - t0
    print_summary()
    print(f"\n  Completed in {elapsed:.1f}s")
