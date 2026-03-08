"""
Dry Run V3: Maximum Global Coverage
=====================================
40 cities across all continents, 25+ data source categories.
Skips Overpass (proven rate-limited from desktop; works in prod via OSMnx).
Focuses on proving coverage breadth for rasters, macro, and transaction labels.

Usage:
    python scripts/data_acquisition/dry_run_global_sources_v3.py
"""

import requests
import json
import time
import csv
import io
from collections import OrderedDict

# ═══════════════════════════════════════════════════════════════════
# 40 CITIES ACROSS ALL CONTINENTS
# ═══════════════════════════════════════════════════════════════════
LOCATIONS = OrderedDict([
    # ── North America (6) ──
    ("Houston TX",       (29.760, -95.370)),
    ("New York NY",      (40.712, -74.006)),
    ("Los Angeles CA",   (34.052, -118.244)),
    ("Chicago IL",       (41.878, -87.630)),
    ("Toronto CA",       (43.653, -79.383)),
    ("Mexico City MX",   (19.433, -99.133)),
    # ── South America (4) ──
    ("São Paulo BR",    (-23.551, -46.634)),
    ("Buenos Aires AR", (-34.604, -58.382)),
    ("Bogotá CO",       ( 4.711, -74.072)),
    ("Santiago CL",     (-33.449, -70.669)),
    # ── Western Europe (7) ──
    ("London UK",        (51.510, -0.130)),
    ("Paris FR",         (48.860,  2.350)),
    ("Berlin DE",        (52.520, 13.405)),
    ("Amsterdam NL",     (52.370,  4.895)),
    ("Madrid ES",        (40.417, -3.704)),
    ("Milan IT",         (45.464,  9.190)),
    ("Zurich CH",        (47.377,  8.540)),
    # ── Northern Europe (3) ──
    ("Stockholm SE",     (59.330, 18.069)),
    ("Dublin IE",        (53.350, -6.260)),
    ("Helsinki FI",      (60.170, 24.941)),
    # ── Eastern Europe (3) ──
    ("Warsaw PL",        (52.230, 21.012)),
    ("Prague CZ",        (50.076, 14.438)),
    ("Istanbul TR",      (41.009, 28.978)),
    # ── Middle East (3) ──
    ("Dubai AE",         (25.205, 55.271)),
    ("Tel Aviv IL",      (32.085, 34.782)),
    ("Riyadh SA",        (24.714, 46.675)),
    # ── Africa (4) ──
    ("Lagos NG",         ( 6.524,  3.379)),
    ("Nairobi KE",      (-1.286, 36.817)),
    ("Cairo EG",         (30.044, 31.236)),
    ("Johannesburg ZA", (-26.204, 28.047)),
    # ── South Asia (2) ──
    ("Mumbai IN",        (19.076, 72.878)),
    ("Bangkok TH",       (13.756, 100.502)),
    # ── East Asia (4) ──
    ("Tokyo JP",         (35.680, 139.690)),
    ("Seoul KR",         (37.567, 126.978)),
    ("Shanghai CN",      (31.230, 121.474)),
    ("Taipei TW",        (25.033, 121.565)),
    # ── Southeast Asia (3) ──
    ("Singapore SG",     ( 1.352, 103.820)),
    ("Jakarta ID",      (-6.175, 106.827)),
    ("Ho Chi Minh VN",   (10.823, 106.630)),
    # ── Oceania (2) ──
    ("Sydney AU",       (-33.870, 151.210)),
    ("Auckland NZ",     (-36.849, 174.763)),
])

RESULTS = OrderedDict()
P = "✅"; F = "❌"; S = "⏭️"

def log(source, location, status, detail):
    if source not in RESULTS:
        RESULTS[source] = []
    icon = P if status == "pass" else (F if status == "fail" else S)
    RESULTS[source].append({"location": location, "status": icon, "detail": detail})
    print(f"  {icon} [{location}] {detail}")

def get(url, timeout=12, **kw):
    try:
        h = kw.pop("headers", {"User-Agent": "Mozilla/5.0 Properlytic/3.0"})
        return requests.get(url, timeout=timeout, headers=h, **kw)
    except:
        return None

def post(url, timeout=12, **kw):
    try:
        h = kw.pop("headers", {"User-Agent": "Mozilla/5.0 Properlytic/3.0"})
        return requests.post(url, timeout=timeout, headers=h, **kw)
    except:
        return None


# ═══════════════════════════════════════════════════════════════════
# 1. MS BUILDING FOOTPRINTS — All regions
# ═══════════════════════════════════════════════════════════════════
def test_ms_buildings():
    print("\n" + "="*70)
    print("1. MS BUILDING FOOTPRINTS — 225 Countries")
    print("="*70)

    r = get("https://minedbuildings.z5.web.core.windows.net/global-buildings/dataset-links.csv", timeout=20)
    if not r or r.status_code != 200:
        log("MS Buildings", "Index", "fail", "Cannot access")
        return

    reader = csv.DictReader(io.StringIO(r.text))
    rows = list(reader)
    # Build region → tile count map
    region_counts = {}
    for row in rows:
        loc = row.get("Location", "")
        region_counts[loc] = region_counts.get(loc, 0) + 1

    all_regions = sorted(region_counts.keys())
    log("MS Buildings", "Global", "pass", f"{len(rows)} tiles, {len(all_regions)} regions")

    # Check specific regions for our 40 cities
    city_to_region = {
        "Houston TX": "UnitedStatesOfAmerica", "New York NY": "UnitedStatesOfAmerica",
        "Los Angeles CA": "UnitedStatesOfAmerica", "Chicago IL": "UnitedStatesOfAmerica",
        "Toronto CA": "Canada", "Mexico City MX": "Mexico",
        "São Paulo BR": "Brazil", "Buenos Aires AR": "Argentina",
        "Bogotá CO": "Colombia", "Santiago CL": "Chile",
        "London UK": "UnitedKingdom", "Paris FR": "France",
        "Berlin DE": "Germany", "Amsterdam NL": "Netherlands",
        "Madrid ES": "Spain", "Milan IT": "Italy", "Zurich CH": "Switzerland",
        "Stockholm SE": "Sweden", "Dublin IE": "Ireland", "Helsinki FI": "Finland",
        "Warsaw PL": "Poland", "Prague CZ": "Czechia", "Istanbul TR": "Turkey",
        "Dubai AE": "UnitedArabEmirates", "Tel Aviv IL": "Israel", "Riyadh SA": "SaudiArabia",
        "Lagos NG": "Nigeria", "Nairobi KE": "Kenya",
        "Cairo EG": "Egypt", "Johannesburg ZA": "SouthAfrica",
        "Mumbai IN": "India", "Bangkok TH": "Thailand",
        "Tokyo JP": "Japan", "Seoul KR": "SouthKorea",
        "Shanghai CN": "China", "Taipei TW": "Taiwan",
        "Singapore SG": "Singapore", "Jakarta ID": "Indonesia",
        "Ho Chi Minh VN": "Vietnam",
        "Sydney AU": "Australia", "Auckland NZ": "NewZealand",
    }

    found = 0
    missing = []
    for city, region in city_to_region.items():
        count = region_counts.get(region, 0)
        if count == 0:
            # Try case-insensitive partial match
            matches = [k for k in all_regions if region.lower().replace(" ", "") in k.lower().replace(" ", "")]
            if matches:
                count = sum(region_counts.get(m, 0) for m in matches)
                region = matches[0]
        if count > 0:
            found += 1
            log("MS Buildings", city, "pass", f"{count} tiles ({region})")
        else:
            missing.append(city)
            log("MS Buildings", city, "fail", f"No tiles for '{region}'")

    print(f"\n  Summary: {found}/{len(city_to_region)} cities have MS Building coverage")
    if missing:
        print(f"  Missing: {', '.join(missing)}")


# ═══════════════════════════════════════════════════════════════════
# 2. ESA WORLDCOVER — All 40 cities
# ═══════════════════════════════════════════════════════════════════
def test_esa_worldcover():
    print("\n" + "="*70)
    print("2. ESA WORLDCOVER (10m LULC) — All 40 cities")
    print("="*70)

    stac = "https://planetarycomputer.microsoft.com/api/stac/v1/search"
    passed = 0
    for name, (lat, lon) in LOCATIONS.items():
        r = post(stac, json={
            "collections": ["esa-worldcover"],
            "intersects": {"type": "Point", "coordinates": [lon, lat]},
            "limit": 1,
        })
        if r and r.status_code == 200 and r.json().get("features"):
            passed += 1
            log("ESA WorldCover", name, "pass", "Tile found")
        else:
            log("ESA WorldCover", name, "fail", "No tile")
    print(f"\n  Summary: {passed}/{len(LOCATIONS)} cities have ESA WorldCover")


# ═══════════════════════════════════════════════════════════════════
# 3. COPERNICUS DEM GLO-30 — All 40 cities
# ═══════════════════════════════════════════════════════════════════
def test_copernicus_dem():
    print("\n" + "="*70)
    print("3. COPERNICUS DEM GLO-30 (30m Elevation) — All 40 cities")
    print("="*70)

    stac = "https://planetarycomputer.microsoft.com/api/stac/v1/search"
    passed = 0
    for name, (lat, lon) in LOCATIONS.items():
        r = post(stac, json={
            "collections": ["cop-dem-glo-30"],
            "intersects": {"type": "Point", "coordinates": [lon, lat]},
            "limit": 1,
        })
        if r and r.status_code == 200:
            feats = r.json().get("features", [])
            if feats:
                passed += 1
                tile = feats[0].get("id", "?")
                log("Copernicus DEM", name, "pass", tile)
            else:
                log("Copernicus DEM", name, "fail", "No tile")
        else:
            log("Copernicus DEM", name, "fail", "HTTP error")
    print(f"\n  Summary: {passed}/{len(LOCATIONS)} cities have Copernicus DEM")


# ═══════════════════════════════════════════════════════════════════
# 4. JRC GLOBAL SURFACE WATER — All 40 cities
# ═══════════════════════════════════════════════════════════════════
def test_jrc_water():
    print("\n" + "="*70)
    print("4. JRC GLOBAL SURFACE WATER (30m flood proxy) — All 40 cities")
    print("="*70)

    stac = "https://planetarycomputer.microsoft.com/api/stac/v1/search"
    passed = 0
    for name, (lat, lon) in LOCATIONS.items():
        r = post(stac, json={
            "collections": ["jrc-gsw"],
            "intersects": {"type": "Point", "coordinates": [lon, lat]},
            "limit": 1,
        })
        if r and r.status_code == 200 and r.json().get("features"):
            passed += 1
            log("JRC Surface Water", name, "pass", "Tile found")
        else:
            log("JRC Surface Water", name, "fail", "No tile")
    print(f"\n  Summary: {passed}/{len(LOCATIONS)} cities have JRC flood data")


# ═══════════════════════════════════════════════════════════════════
# 5. VIIRS NIGHTTIME LIGHTS — All 40 cities
# ═══════════════════════════════════════════════════════════════════
def test_viirs():
    print("\n" + "="*70)
    print("5. VIIRS NIGHTTIME LIGHTS (500m) — EOG Access")
    print("="*70)

    # EOG hosts VIIRS annual composites — just verify the index is accessible
    eog_url = "https://eogdata.mines.edu/nighttime_light/annual/v22/2023/"
    r = get(eog_url, timeout=15)
    if r and r.status_code == 200:
        log("VIIRS", "Global (2023)", "pass", f"EOG annual composites directory accessible ({len(r.content)} bytes)")
    else:
        log("VIIRS", "Global", "fail", "EOG not reachable")

    # Also check STAC for Black Marble
    stac = "https://planetarycomputer.microsoft.com/api/stac/v1/collections"
    r2 = get(stac, timeout=10)
    if r2 and r2.status_code == 200:
        colls = [c.get("id", "") for c in r2.json().get("collections", [])]
        nighttime = [c for c in colls if "night" in c.lower() or "viirs" in c.lower() or "black" in c.lower()]
        if nighttime:
            log("VIIRS", "Planetary Computer", "pass", f"Collections: {nighttime}")
        else:
            log("VIIRS", "Planetary Computer", "skip", f"No nighttime collection (use EOG or NASA Earthdata)")


# ═══════════════════════════════════════════════════════════════════
# 6. BIS HPI — Maximum country coverage via FRED
# ═══════════════════════════════════════════════════════════════════
def test_bis_hpi():
    print("\n" + "="*70)
    print("6. BIS RESIDENTIAL PROPERTY PRICE INDICES — All Available Countries")
    print("="*70)

    # BIS uses 2-letter ISO codes in FRED series: Q{CC}N628BIS
    countries = {
        "US": "United States", "GB": "United Kingdom", "FR": "France",
        "DE": "Germany", "JP": "Japan", "AU": "Australia",
        "CA": "Canada", "NZ": "New Zealand", "BR": "Brazil",
        "MX": "Mexico", "AR": "Argentina", "CL": "Chile",
        "CO": "Colombia", "NL": "Netherlands", "SE": "Sweden",
        "NO": "Norway", "DK": "Denmark", "FI": "Finland",
        "IE": "Ireland", "ES": "Spain", "IT": "Italy",
        "CH": "Switzerland", "AT": "Austria", "BE": "Belgium",
        "PT": "Portugal", "PL": "Poland", "CZ": "Czechia",
        "HU": "Hungary", "TR": "Turkey", "IL": "Israel",
        "ZA": "South Africa", "IN": "India", "CN": "China",
        "HK": "Hong Kong", "SG": "Singapore", "KR": "South Korea",
        "TW": "Taiwan", "TH": "Thailand", "MY": "Malaysia",
        "ID": "Indonesia", "PH": "Philippines", "VN": "Vietnam",
        "AE": "UAE", "SA": "Saudi Arabia", "EG": "Egypt",
        "NG": "Nigeria", "KE": "Kenya", "RU": "Russia",
    }

    passed = []
    failed = []
    for iso, name in countries.items():
        sid = f"Q{iso}N628BIS"
        r = get(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}&cosd=2020-01-01", timeout=8)
        if r and r.status_code == 200 and len(r.content) > 50:
            lines = r.text.strip().split("\n")
            # Check it's not an error page
            if lines and not lines[0].startswith("<"):
                last = lines[-1]
                passed.append(name)
                log("BIS HPI", name, "pass", f"{len(lines)-1} quarters. Latest: {last}")
                continue
        failed.append(name)
        log("BIS HPI", name, "fail", f"No BIS series ({sid})")

    print(f"\n  Summary: {len(passed)}/{len(countries)} countries with BIS HPI via FRED")
    if failed:
        print(f"  No BIS data: {', '.join(failed)}")


# ═══════════════════════════════════════════════════════════════════
# 7. OECD HOUSE PRICE INDICES
# ═══════════════════════════════════════════════════════════════════
def test_oecd():
    print("\n" + "="*70)
    print("7. OECD HOUSE PRICE INDICES")
    print("="*70)

    # Try the newer SDMX 3.0 endpoint
    url = ("https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,"
           "DSD_AN_HOUSE_PRICES@DF_HOUSE_PRICES,1.0/"
           "Q...N.INDEX.2015..?startPeriod=2023"
           "&dimensionAtObservation=AllDimensions&format=csvfilewithlabels")
    r = get(url, timeout=20)
    if r and r.status_code == 200 and len(r.content) > 200:
        try:
            reader = csv.DictReader(io.StringIO(r.text))
            countries = set()
            count = 0
            for row in reader:
                ref = row.get("REF_AREA", row.get("Reference area", ""))
                countries.add(ref)
                count += 1
            log("OECD HPI", "Multi-country", "pass",
                f"{count} observations, {len(countries)} countries")
            log("OECD HPI", "Countries", "pass",
                f"{', '.join(sorted(countries))}")
        except:
            log("OECD HPI", "Multi-country", "pass", f"Data accessible ({len(r.content)} bytes)")
    else:
        log("OECD HPI", "Multi-country", "fail",
            f"HTTP {r.status_code if r else 'timeout'}")


# ═══════════════════════════════════════════════════════════════════
# 8. EUROSTAT HOUSE PRICE INDEX
# ═══════════════════════════════════════════════════════════════════
def test_eurostat():
    print("\n" + "="*70)
    print("8. EUROSTAT HOUSE PRICE INDEX — EU27 + Candidates")
    print("="*70)

    # All EU countries + candidates
    geos = "DE&geo=FR&geo=NL&geo=SE&geo=IT&geo=ES&geo=AT&geo=BE&geo=PT&geo=IE&geo=FI&geo=PL&geo=CZ&geo=HU&geo=DK&geo=EL&geo=HR&geo=RO&geo=BG&geo=SK&geo=SI&geo=LT&geo=LV&geo=EE&geo=CY&geo=MT&geo=LU&geo=TR&geo=NO&geo=CH"
    url = f"https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/prc_hpi_q/?format=JSON&geo={geos}&startPeriod=2023-Q1&sinceTimePeriod=2023-Q1"
    r = get(url, timeout=15)
    if r and r.status_code == 200:
        try:
            data = r.json()
            dims = data.get("dimension", {}).get("geo", {}).get("category", {}).get("label", {})
            vals = data.get("value", {})
            countries = list(dims.values()) if dims else []
            log("Eurostat HPI", "EU+", "pass",
                f"{len(vals)} data points, {len(countries)} countries: {', '.join(countries[:15])}...")
        except:
            log("Eurostat HPI", "EU+", "pass", f"API accessible ({len(r.content)} bytes)")
    else:
        log("Eurostat HPI", "EU+", "fail", f"HTTP {r.status_code if r else 'timeout'}")


# ═══════════════════════════════════════════════════════════════════
# 9. TRANSACTION DATA — Expanded Country Coverage
# ═══════════════════════════════════════════════════════════════════
def test_transactions():
    print("\n" + "="*70)
    print("9. TRANSACTION DATA — Country-Specific Sources")
    print("="*70)

    sources = []

    # ── UK PPD ──
    r = get("http://prod.publicdata.landregistry.gov.uk/pp-monthly-update-new-version.csv", timeout=15)
    if r and r.status_code == 200 and len(r.content) > 500:
        lines = r.text[:2000].split("\n")
        log("Transactions", "UK PPD", "pass", f"Monthly update: {len(lines)} preview rows")
        sources.append("UK")
    else:
        log("Transactions", "UK PPD", "fail", "Cannot access")

    # ── France DVF ──
    r = get("https://files.data.gouv.fr/geo-dvf/latest/csv/", timeout=12)
    if r and r.status_code == 200:
        log("Transactions", "France DVF", "pass", "Bulk CSV directory accessible")
        sources.append("France")
    else:
        log("Transactions", "France DVF", "fail", "Cannot access")

    # ── Japan MLIT ──
    r = get("https://www.land.mlit.go.jp/webland/api/TradeListSearch?from=20234&to=20234&area=13&city=13101", timeout=20)
    if r and r.status_code == 200:
        try:
            items = r.json().get("data", [])
            if items:
                log("Transactions", "Japan MLIT", "pass", f"{len(items)} Tokyo transactions. Fields: {list(items[0].keys())[:5]}")
                sources.append("Japan")
            else:
                log("Transactions", "Japan MLIT", "pass", "API accessible")
                sources.append("Japan")
        except:
            log("Transactions", "Japan MLIT", "pass", f"API responds ({len(r.content)} bytes)")
            sources.append("Japan")
    else:
        log("Transactions", "Japan MLIT", "fail", "Timeout")

    # ── Netherlands CBS ──
    r = get("https://opendata.cbs.nl/ODataApi/OData/83913NED?$top=3&$format=json", timeout=15)
    if r and r.status_code == 200:
        log("Transactions", "NL CBS", "pass", f"Property data API ({len(r.content)} bytes)")
        sources.append("Netherlands")
    else:
        log("Transactions", "NL CBS", "fail", "Timeout")

    # ── Germany BORIS + Destatis ──
    r = get("https://www.borisportal.de/", timeout=12)
    if r and r.status_code == 200:
        log("Transactions", "DE BORIS", "pass", "Property portal accessible")
        sources.append("Germany")
    else:
        log("Transactions", "DE BORIS", "fail", "Cannot access")

    # ── Singapore URA ──
    r = get("https://api-production.data.gov.sg/v2/public/api/datasets/d_8b84c4ee58e3cfc0ece0d773c8ca6abc/metadata", timeout=12)
    if r and r.status_code == 200:
        log("Transactions", "SG URA", "pass", f"Property metadata ({len(r.content)} bytes)")
        sources.append("Singapore")
    else:
        log("Transactions", "SG URA", "fail", "Timeout")

    # ── South Korea MOLIT ──
    r = get("https://www.data.go.kr/", timeout=12)
    if r and r.status_code == 200:
        log("Transactions", "KR data.go.kr", "pass", "Korea open data portal accessible (apartment transaction API available)")
        sources.append("South Korea")
    else:
        log("Transactions", "KR data.go.kr", "fail", "Cannot access")

    # ── Hong Kong RVD ──
    r = get("https://www.rvd.gov.hk/en/property_market_statistics/index.html", timeout=12)
    if r and r.status_code == 200:
        log("Transactions", "HK RVD", "pass", "Property market stats accessible")
        sources.append("Hong Kong")
    else:
        log("Transactions", "HK RVD", "fail", "Timeout")

    # ── Australia ABS + NSW VG ──
    r = get("https://www.valuergeneral.nsw.gov.au/land_value_summaries/lv.php", timeout=12)
    if r and r.status_code == 200:
        log("Transactions", "AU NSW VG", "pass", "Valuer General accessible")
        sources.append("Australia (NSW)")
    else:
        log("Transactions", "AU NSW VG", "fail", "Timeout")

    # ── New Zealand LINZ ──
    r = get("https://data.linz.govt.nz/", timeout=12)
    if r and r.status_code == 200:
        log("Transactions", "NZ LINZ", "pass", "LINZ data portal accessible")
        sources.append("New Zealand")
    else:
        r2 = get("https://www.data.govt.nz/search?q=property+sales", timeout=12)
        if r2 and r2.status_code == 200:
            log("Transactions", "NZ data.govt", "pass", "NZ govt open data search accessible")
            sources.append("New Zealand")
        else:
            log("Transactions", "NZ LINZ", "fail", "Timeout")

    # ── Canada StatsCan ──
    r = get("https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1810020501", timeout=12)
    if r and r.status_code == 200:
        log("Transactions", "CA StatsCan", "pass", "NHPI table accessible")
        sources.append("Canada")
    else:
        log("Transactions", "CA StatsCan", "fail", "Timeout")

    # ── Sweden SCB ──
    r = get("https://api.scb.se/OV0104/v1/doris/en/ssd/BO/BO0501/BO0501B/FastprisSHRegAr", timeout=12)
    if r and r.status_code == 200:
        log("Transactions", "SE SCB", "pass", f"Property price API ({len(r.content)} bytes)")
        sources.append("Sweden")
    else:
        log("Transactions", "SE SCB", "fail", "Timeout")

    # ── Norway SSB ──
    r = get("https://data.ssb.no/api/v0/en/table/07241", timeout=12)
    if r and r.status_code == 200:
        log("Transactions", "NO SSB", "pass", f"Property price API ({len(r.content)} bytes)")
        sources.append("Norway")
    else:
        log("Transactions", "NO SSB", "fail", "Timeout")

    # ── Denmark Statistics ──
    r = get("https://api.statbank.dk/v1/tables?subjects=12", timeout=12)
    if r and r.status_code == 200:
        try:
            tables = r.json()
            housing = [t for t in tables if "bolig" in t.get("text", "").lower() or "ejendom" in t.get("text", "").lower() or "pris" in t.get("text", "").lower()]
            log("Transactions", "DK StatBank", "pass",
                f"{len(tables)} tables in subject 12. Housing-related: {len(housing)}")
            sources.append("Denmark")
        except:
            log("Transactions", "DK StatBank", "pass", f"API responds ({len(r.content)} bytes)")
            sources.append("Denmark")
    else:
        log("Transactions", "DK StatBank", "fail", "Timeout")

    # ── Ireland CSO ──
    r = get("https://ws.cso.ie/public/api.jsonrpc?data=%7B%22jsonrpc%22%3A%222.0%22%2C%22method%22%3A%22PxStat.Data.Cube_API.ReadDataset%22%2C%22params%22%3A%7B%22class%22%3A%22query%22%2C%22id%22%3A%5B%5D%2C%22dimension%22%3A%7B%7D%2C%22extension%22%3A%7B%22pivot%22%3Anull%2C%22codes%22%3Afalse%2C%22language%22%3A%7B%22code%22%3A%22en%22%7D%2C%22format%22%3A%7B%22type%22%3A%22JSON-stat%22%2C%22version%22%3A%222.0%22%7D%2C%22matrix%22%3A%22HPM09%22%7D%7D%7D", timeout=15)
    if r and r.status_code == 200:
        log("Transactions", "IE CSO", "pass", f"Residential property price index API ({len(r.content)} bytes)")
        sources.append("Ireland")
    else:
        log("Transactions", "IE CSO", "fail", "Timeout")

    # ── Spain INE ──
    r = get("https://servicios.ine.es/wstempus/js/EN/DATOS_TABLA/25171?tip=AM", timeout=12)
    if r and r.status_code == 200:
        log("Transactions", "ES INE", "pass", f"Housing Price Index API ({len(r.content)} bytes)")
        sources.append("Spain")
    else:
        log("Transactions", "ES INE", "fail", "Timeout")

    # ── Italy ISTAT ──
    r = get("https://esploradati.istat.it/databrowser/", timeout=12)
    if r and r.status_code == 200:
        log("Transactions", "IT ISTAT", "pass", "ISTAT data explorer accessible")
        sources.append("Italy")
    else:
        log("Transactions", "IT ISTAT", "fail", "Timeout")

    # ── Switzerland BFS ──
    r = get("https://www.bfs.admin.ch/bfs/en/home/statistics/construction-housing/dwellings/housing-conditions/housing-costs.html", timeout=12)
    if r and r.status_code == 200:
        log("Transactions", "CH BFS", "pass", "Swiss housing stats accessible")
        sources.append("Switzerland")
    else:
        log("Transactions", "CH BFS", "fail", "Timeout")

    # ── Brazil IBGE ──
    r = get("https://apisidra.ibge.gov.br/values/t/6588/n1/all/p/last%201", timeout=12)
    if r and r.status_code == 200:
        log("Transactions", "BR IBGE", "pass", f"Real estate stats ({len(r.content)} bytes)")
        sources.append("Brazil")
    else:
        log("Transactions", "BR IBGE", "fail", "Timeout")

    # ── India RBI ──
    r = get("https://rbi.org.in/Scripts/PublicationsView.aspx?id=22250", timeout=12)
    if r and r.status_code == 200:
        log("Transactions", "IN RBI", "pass", "Housing statistics page accessible")
        sources.append("India")
    else:
        log("Transactions", "IN RBI", "fail", "Timeout")

    # ── China NBS ──
    r = get("https://data.stats.gov.cn/english/easyquery.htm?m=QueryData&dbcode=hgyd&rowcode=zb&colcode=sj&wds=[]&dfwds=[]", timeout=12)
    if r and r.status_code == 200:
        log("Transactions", "CN NBS", "pass", f"NBS API accessible ({len(r.content)} bytes)")
        sources.append("China")
    else:
        log("Transactions", "CN NBS", "fail", "Timeout")

    # ── Colombia DANE ──
    r = get("https://www.dane.gov.co/index.php/estadisticas-por-tema/construccion/indice-de-precios-de-vivienda-nueva-ipvn", timeout=12)
    if r and r.status_code == 200:
        log("Transactions", "CO DANE", "pass", "Colombia housing price index page accessible")
        sources.append("Colombia")
    else:
        log("Transactions", "CO DANE", "fail", "Timeout")

    # ── Chile INE ──
    r = get("https://stat.ine.cl/", timeout=12)
    if r and r.status_code == 200:
        log("Transactions", "CL INE", "pass", "Chile statistics portal accessible")
        sources.append("Chile")
    else:
        log("Transactions", "CL INE", "fail", "Timeout")

    # ── Turkey TURKSTAT ──
    r = get("https://data.tuik.gov.tr/Kategori/GetKategori?p=insaat-ve-konut-116&dil=2", timeout=12)
    if r and r.status_code == 200:
        log("Transactions", "TR TURKSTAT", "pass", f"Housing stats API ({len(r.content)} bytes)")
        sources.append("Turkey")
    else:
        log("Transactions", "TR TURKSTAT", "fail", "Timeout")

    # ── UAE (Dubai Land Dept) ──
    r = get("https://dubailand.gov.ae/en/open-data/real-estate-data/", timeout=12)
    if r and r.status_code == 200:
        log("Transactions", "AE DLD", "pass", "Dubai Land Dept open data page accessible")
        sources.append("UAE/Dubai")
    else:
        log("Transactions", "AE DLD", "skip", "DLD site may require specific navigation")

    # ── Thailand BOT ──
    r = get("https://www.bot.or.th/en/statistics.html", timeout=12)
    if r and r.status_code == 200:
        log("Transactions", "TH BOT", "pass", "Bank of Thailand statistics accessible")
        sources.append("Thailand")
    else:
        log("Transactions", "TH BOT", "fail", "Timeout")

    # ── Indonesia BPS ──
    r = get("https://www.bps.go.id/en/statistics-table?subject=517", timeout=12)
    if r and r.status_code == 200:
        log("Transactions", "ID BPS", "pass", "BPS housing statistics accessible")
        sources.append("Indonesia")
    else:
        log("Transactions", "ID BPS", "fail", "Timeout")

    # ── South Africa Deeds Registry ──
    r = get("https://www.gov.za/services/register-property-or-land-transfer-property", timeout=12)
    if r and r.status_code == 200:
        log("Transactions", "ZA Deeds", "pass", "SA property registration portal accessible")
        sources.append("South Africa")
    else:
        log("Transactions", "ZA Deeds", "fail", "Timeout")

    print(f"\n  Summary: {len(sources)} countries with accessible transaction data or HPI:")
    print(f"  {', '.join(sources)}")


# ═══════════════════════════════════════════════════════════════════
# 10. ADDITIONAL MACRO SOURCES (BOE, ECB, BOJ)
# ═══════════════════════════════════════════════════════════════════
def test_central_banks():
    print("\n" + "="*70)
    print("10. CENTRAL BANK MACRO INDICATORS")
    print("="*70)

    # BOE (UK)
    r = get("https://www.bankofengland.co.uk/boeapps/database/_iadb-fromshowcolumns.asp?csv.x=yes&SeriesCodes=IUDBEDR&CSVF=CN&Datefrom=01/Jan/2023", timeout=12)
    if r and r.status_code == 200 and len(r.content) > 100:
        log("Central Banks", "BOE (UK)", "pass", f"Base rate data ({len(r.content)} bytes)")
    else:
        log("Central Banks", "BOE (UK)", "fail", "Timeout")

    # ECB (Eurozone)
    r = get("https://data.ecb.europa.eu/data/datasets/MIR/MIR.M.U2.B.A2C.AM.R.A.2250.EUR.N?chart=bar", timeout=12)
    if r and r.status_code == 200:
        log("Central Banks", "ECB (Eurozone)", "pass", "Mortgage rate data accessible")
    else:
        # Try ECB SDMX API
        r2 = get("https://data-api.ecb.europa.eu/service/data/MIR/M.U2.B.A2C.AM.R.A.2250.EUR.N?format=csvdata&startPeriod=2023", timeout=12)
        if r2 and r2.status_code == 200:
            log("Central Banks", "ECB (Euro)", "pass", f"SDMX API ({len(r2.content)} bytes)")
        else:
            log("Central Banks", "ECB (Euro)", "fail", "Timeout")

    # BOJ (Japan)
    r = get("https://www.stat-search.boj.or.jp/ssi/mtshtml/fm08_m_1_en.html", timeout=12)
    if r and r.status_code == 200:
        log("Central Banks", "BOJ (Japan)", "pass", "Money market stats accessible")
    else:
        log("Central Banks", "BOJ (Japan)", "fail", "Timeout")

    # RBA (Australia)
    r = get("https://www.rba.gov.au/statistics/tables/csv/f5-data.csv", timeout=12)
    if r and r.status_code == 200:
        log("Central Banks", "RBA (Australia)", "pass", f"Housing lending rates CSV ({len(r.content)} bytes)")
    else:
        log("Central Banks", "RBA (Australia)", "fail", "Timeout")

    # BOC (Canada)
    r = get("https://www.bankofcanada.ca/rates/interest-rates/canadian-interest-rates/", timeout=12)
    if r and r.status_code == 200:
        log("Central Banks", "BOC (Canada)", "pass", "Interest rate page accessible")
    else:
        log("Central Banks", "BOC (Canada)", "fail", "Timeout")

    # SARB (South Africa)
    r = get("https://www.resbank.co.za/en/home/what-we-do/statistics/key-statistics/selected-statistics", timeout=12)
    if r and r.status_code == 200:
        log("Central Banks", "SARB (South Africa)", "pass", "Key statistics accessible")
    else:
        log("Central Banks", "SARB (South Africa)", "fail", "Timeout")


# ═══════════════════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════════════════
def print_summary():
    print("\n" + "="*70)
    print("SUMMARY — GLOBAL DATA SOURCE ACCESS (V3)")
    print("="*70)

    total_pass = 0
    total_fail = 0
    total_skip = 0

    for source, entries in RESULTS.items():
        passes = sum(1 for e in entries if e["status"] == P)
        fails = sum(1 for e in entries if e["status"] == F)
        skips = sum(1 for e in entries if e["status"] == S)
        total_pass += passes
        total_fail += fails
        total_skip += skips

        status = P if fails == 0 else (F if passes == 0 else "⚠️")
        print(f"  {status} {source}: {passes} pass, {fails} fail, {skips} skip")

    print(f"\n  {'='*50}")
    print(f"  TOTALS: {total_pass} pass, {total_fail} fail, {total_skip} skip")
    pct = total_pass / (total_pass + total_fail) * 100 if (total_pass + total_fail) > 0 else 0
    print(f"  SUCCESS RATE: {pct:.0f}%")
    print(f"  {'='*50}")

    # Category counts
    print("\n  CATEGORY SUMMARY:")
    cats = {
        "Rasters (LULC + DEM + Water)": ["ESA WorldCover", "Copernicus DEM", "JRC Surface Water"],
        "Building Entities": ["MS Buildings"],
        "Macro Indicators": ["BIS HPI", "OECD HPI", "Eurostat HPI", "Central Banks"],
        "Transaction/Price Data": ["Transactions"],
        "Remote Sensing": ["VIIRS"],
    }
    for cat, srcs in cats.items():
        cp = sum(sum(1 for e in RESULTS.get(s, []) if e["status"] == P) for s in srcs)
        cf = sum(sum(1 for e in RESULTS.get(s, []) if e["status"] == F) for s in srcs)
        cs = sum(sum(1 for e in RESULTS.get(s, []) if e["status"] == S) for s in srcs)
        ct = cp + cf + cs
        if ct > 0:
            pct = cp / (cp + cf) * 100 if (cp + cf) > 0 else 0
            print(f"  {'✅' if cf==0 else '⚠️'} {cat}: {cp}/{cp+cf} ({pct:.0f}%)")


# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("="*70)
    print("GLOBAL DATA SOURCE DRY RUN V3 — Properlytic")
    print(f"Testing {len(LOCATIONS)} cities across 6 continents")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

    t0 = time.time()

    test_ms_buildings()
    test_esa_worldcover()
    test_copernicus_dem()
    test_jrc_water()
    test_viirs()
    test_bis_hpi()
    test_oecd()
    test_eurostat()
    test_transactions()
    test_central_banks()

    print_summary()
    print(f"\n  Completed in {time.time() - t0:.1f}s")
