"""Find and pull NYC DOF Property Valuation and Assessment Data from NYC Open Data."""
import requests, json, os

OUT = os.path.dirname(__file__)

# Search NYC Open Data for the dataset
print("=== Searching NYC Open Data ===")
r = requests.get('https://data.cityofnewyork.us/api/catalog/v1', params={
    'q': 'Property Valuation Assessment Data Tax Classes',
    'limit': 5,
    'only': 'datasets',
}, timeout=15)
results = r.json().get('results', [])
for res in results:
    rsrc = res.get('resource', {})
    name = rsrc.get('name', '')
    uid = rsrc.get('id', '')
    rows = rsrc.get('rows_count', 0)
    cols = rsrc.get('columns_count', 0)
    desc = res.get('resource', {}).get('description', '')[:100]
    print(f"  {uid}: {name}")
    print(f"    Rows: {rows}, Cols: {cols}")
    print(f"    Desc: {desc}")
    print()

# Also try searching by column name
print("=== Searching by column name PARID ===")
r2 = requests.get('https://data.cityofnewyork.us/api/catalog/v1', params={
    'q': 'PARID CURMKTTOT property assessment',
    'limit': 3,
    'only': 'datasets',
}, timeout=15)
for res in r2.json().get('results', []):
    rsrc = res.get('resource', {})
    uid = rsrc.get('id', '')
    name = rsrc.get('name', '')
    rows = rsrc.get('rows_count', 0)
    print(f"  {uid}: {name} ({rows} rows)")

# Try known IDs for NYC DOF assessment data
print("\n=== Testing known dataset IDs ===")
# Common IDs found in NYC Open Data for DOF data:
candidates = [
    'yjxr-fw8i',  # Property Valuation and Assessment Data
    '8y4t-faws',  # DOF Cooperative Comparable Rental Income
    'j2iz-mwzu',  # DOF Condominium Comparable Rental Income
    'usep-8jbt',  # RPAD
    'svzw-fczf',  # Property Assessment Roll
    'nqwf-w8eh',  # Annualized Sales
    'erm2-nwe9',  # Rolling Sales
    '64uk-42ks',  # Property Valuation
]

found_id = None
for did in candidates:
    try:
        r = requests.get(f'https://data.cityofnewyork.us/api/views/{did}.json', timeout=10)
        if r.status_code == 200:
            d = r.json()
            name = d.get('name', '?')
            ncols = len(d.get('columns', []))
            print(f"  {did}: {name} ({ncols} cols)")
            if 'valuation' in name.lower() and 'assessment' in name.lower():
                found_id = did
                print(f"    *** MATCH ***")
        else:
            print(f"  {did}: HTTP {r.status_code}")
    except Exception as e:
        print(f"  {did}: {e}")

# If we found it, pull a sample
if found_id:
    print(f"\n=== Pulling sample from {found_id} ===")
    r = requests.get(f'https://data.cityofnewyork.us/resource/{found_id}.json', params={
        '$limit': 5,
        '$select': 'parid,boro,block,lot,subident,year,curmkttot,curmktland,bldg_class,zip_code,yrbuilt,units,land_area,bld_story,street_name,housenum_lo',
    }, timeout=15)
    if r.status_code == 200:
        rows = r.json()
        print(f"  Got {len(rows)} rows")
        for row in rows:
            print(f"  {json.dumps(row, indent=2)}")
    else:
        print(f"  Error: {r.status_code} {r.text[:200]}")
else:
    print("\nDataset not found via known IDs. Trying search-based approach...")
    # Try a general SODA query
    # The dataset page says it's from Department of Finance
    # URL pattern: data.cityofnewyork.us/City-Government/Property-Valuation-and-Assessment-Data-Tax-Classes/XXXX-XXXX
    r = requests.get('https://data.cityofnewyork.us/api/catalog/v1', params={
        'q': 'property valuation assessment tax classes department finance',
        'limit': 10,
        'only': 'datasets',
    }, timeout=15)
    for res in r.json().get('results', []):
        rsrc = res.get('resource', {})
        uid = rsrc.get('id', '')
        name = rsrc.get('name', '')
        rows = rsrc.get('rows_count', 0)
        agency = res.get('classification', {}).get('domain_metadata', [])
        print(f"  {uid}: {name} ({rows} rows)")
        
        # Try pulling from this ID
        if rows > 1000000:
            print(f"    Large dataset — trying sample...")
            r3 = requests.get(f'https://data.cityofnewyork.us/resource/{uid}.json', params={
                '$limit': 2,
            }, timeout=10)
            if r3.status_code == 200:
                sample = r3.json()
                if sample:
                    print(f"    Columns: {list(sample[0].keys())}")
                    found_id = uid
