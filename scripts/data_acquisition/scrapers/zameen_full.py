"""Fetch full Zameen data from CKAN with all columns, then retrain."""
import requests, json, csv, io, os, warnings
import pandas as pd, numpy as np
warnings.filterwarnings('ignore')
import lightgbm as lgb
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import cross_val_score, KFold

OUT = os.path.dirname(__file__)

PK_CITIES = {
    'Islamabad': (33.684, 73.048), 'Lahore': (31.520, 74.359),
    'Karachi': (24.861, 67.001), 'Rawalpindi': (33.565, 73.017),
    'Faisalabad': (31.450, 73.135),
}

def fetch_zameen():
    """Fetch Zameen property data from CKAN with all columns."""
    print(">>> Fetching Zameen data via CKAN...")
    
    # First: search for the package
    r = requests.get('https://opendata.com.pk/api/3/action/package_search',
                     params={'q': 'zameen', 'rows': '5'}, timeout=20)
    pkgs = r.json()['result']['results']
    
    for pkg in pkgs:
        print(f"  Package: {pkg.get('name', '')}")
        for res in pkg.get('resources', []):
            rid = res.get('id', '')
            fmt = (res.get('format', '') or '').lower()
            ds = res.get('datastore_active', False)
            print(f"    Resource: {res.get('name', '')} [{fmt}] datastore={ds}")
            
            if ds:
                # Use datastore API — get all fields
                print(f"    Trying datastore API with resource_id={rid}...")
                r2 = requests.get(
                    'https://opendata.com.pk/api/3/action/datastore_search',
                    params={'resource_id': rid, 'limit': 500},
                    timeout=30)
                data = r2.json()
                if data.get('success'):
                    fields = [f['id'] for f in data['result'].get('fields', [])]
                    total = data['result'].get('total', 0)
                    records = data['result'].get('records', [])
                    print(f"    FIELDS: {fields}")
                    print(f"    TOTAL: {total}, fetched: {len(records)}")
                    if records:
                        print(f"    SAMPLE: {json.dumps(records[0], indent=2)}")
                    return pd.DataFrame(records), fields
    
    # Fallback: try direct CSV with redirect-following
    print("  No datastore. Trying direct CSV...")
    urls = [
        'https://opendata.com.pk/dataset/9e959916-1cfc-4e28-85c8-f10ff63e5df2/resource/2c0d06e0-5c9d-4803-a4ba-d75ca23b0e15/download/zameen-property-data.csv',
    ]
    for url in urls:
        try:
            r = requests.get(url, timeout=30, allow_redirects=True, 
                           headers={'Accept': 'text/csv,application/csv,*/*'})
            if r.status_code == 200 and not r.text.startswith('<!'):
                reader = csv.DictReader(io.StringIO(r.text))
                rows = list(reader)[:500]
                cols = reader.fieldnames
                print(f"  Downloaded {len(rows)} rows, columns: {cols}")
                return pd.DataFrame(rows), cols
        except Exception as e:
            print(f"  Error: {e}")
    
    # Last resort: use sweep7 existing data
    print("  Falling back to sweep7 data")
    return None, None


def scrape_zameen_api():
    """Try Zameen.com search API directly."""
    print("\n>>> Trying Zameen.com search API...")
    
    urls_to_try = [
        # API search
        ('search API', 'https://www.zameen.com/api/search/?purpose=1&location_ids=1&page_size=50&sort=newest', {}),
        # GraphQL endpoint used by zameen.com
        ('graphql', 'https://www.zameen.com/api/graphql', {
            'operationName': 'SearchPage',
            'variables': {'purpose': 'FOR_SALE', 'location': 'islamabad-1', 'page': 1, 'sort': 'NEWEST_FIRST'},
            'query': 'query SearchPage { search { listings { id price area bedrooms } } }'
        }),
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.5',
    }
    
    for name, url, body in urls_to_try:
        try:
            if body:
                r = requests.post(url, json=body, headers=headers, timeout=15)
            else:
                r = requests.get(url, headers=headers, timeout=15)
            print(f"  {name}: HTTP {r.status_code}, {len(r.content)} bytes")
            ct = r.headers.get('Content-Type', '')
            if 'json' in ct.lower() and r.status_code == 200:
                data = r.json()
                print(f"  Keys: {list(data.keys()) if isinstance(data, dict) else type(data)}")
                # Print first listing if found
                if isinstance(data, dict):
                    for key in ['results', 'listings', 'data', 'items', 'hits']:
                        if key in data:
                            items = data[key]
                            if isinstance(items, list) and items:
                                print(f"  First {key} item: {json.dumps(items[0], indent=2)[:500]}")
                                return data
                            elif isinstance(items, dict):
                                print(f"  {key}: {json.dumps(items, indent=2)[:500]}")
                                return data
            else:
                print(f"  Response: {r.text[:300]}")
        except Exception as e:
            print(f"  {name}: {e}")
    
    return None


def main():
    print("=" * 60)
    print("ZAMEEN FULL DATA EXTRACTION + RETRAIN")
    print("=" * 60)
    
    # Try CKAN first
    df_ckan, fields = fetch_zameen()
    
    # Try Zameen API
    api_data = scrape_zameen_api()
    
    # Determine best data source
    if df_ckan is not None and len(df_ckan) > 0:
        print(f"\n  Using CKAN data: {len(df_ckan)} records")
        print(f"  Columns: {list(df_ckan.columns)}")
        print(f"\n  Sample record:")
        print(df_ckan.iloc[0].to_dict())
        
        # Check what columns we actually have
        possible_area = [c for c in df_ckan.columns if any(k in c.lower() for k in ['area', 'size', 'marla', 'kanal', 'sqft'])]
        possible_bed = [c for c in df_ckan.columns if any(k in c.lower() for k in ['bed', 'room'])]
        possible_bath = [c for c in df_ckan.columns if any(k in c.lower() for k in ['bath'])]
        possible_loc = [c for c in df_ckan.columns if any(k in c.lower() for k in ['location', 'address', 'lat', 'lon', 'coord'])]
        possible_price = [c for c in df_ckan.columns if any(k in c.lower() for k in ['price', 'amount', 'value'])]
        
        print(f"\n  Area columns: {possible_area}")
        print(f"  Bedroom columns: {possible_bed}")
        print(f"  Bathroom columns: {possible_bath}")
        print(f"  Location columns: {possible_loc}")
        print(f"  Price columns: {possible_price}")
        
        # Extract features
        df = df_ckan.copy()
        
        # Parse price
        for col in possible_price:
            df['price_raw'] = pd.to_numeric(df[col], errors='coerce')
            break
        
        # Parse area
        for col in possible_area:
            df['area_raw'] = pd.to_numeric(df[col], errors='coerce')
            break
        
        # Parse bedrooms
        for col in possible_bed:
            df['bedrooms'] = pd.to_numeric(df[col], errors='coerce')
            break
        
        # Parse bathrooms
        for col in possible_bath:
            df['bathrooms'] = pd.to_numeric(df[col], errors='coerce')
            break
        
        # Filter valid price
        df = df[df.get('price_raw', pd.Series(dtype=float)).notna()]
        df = df[df['price_raw'] > 100000]
        
        if len(df) > 0:
            print(f"\n  After filtering: {len(df)} rows")
            
            # Add features
            df['log_price'] = np.log1p(df['price_raw'])
            
            # City from location
            if possible_loc:
                loc_col = possible_loc[0]
                df['city_raw'] = df[loc_col].astype(str)
                # Try to extract city from location string
                for city_name in ['Islamabad', 'Lahore', 'Karachi', 'Rawalpindi', 'Faisalabad']:
                    df.loc[df['city_raw'].str.contains(city_name, case=False, na=False), 'city'] = city_name
                df['city'] = df.get('city', pd.Series('Unknown', index=df.index)).fillna('Unknown')
            
            # Geocode
            df['lat'] = df.get('city', pd.Series('', index=df.index)).map(lambda c: PK_CITIES.get(c, (30, 70))[0])
            df['lon'] = df.get('city', pd.Series('', index=df.index)).map(lambda c: PK_CITIES.get(c, (30, 70))[1])
            
            # Encode categoricals
            le_type = LabelEncoder()
            type_col = [c for c in df.columns if 'type' in c.lower() or 'property_type' in c.lower()]
            if type_col:
                df['type_enc'] = le_type.fit_transform(df[type_col[0]].fillna('unknown'))
            else:
                df['type_enc'] = 0
            
            le_city = LabelEncoder()
            df['city_enc'] = le_city.fit_transform(df.get('city', pd.Series('Unknown', index=df.index)).fillna('Unknown'))
            
            # Fill area
            df['area_fill'] = df.get('area_raw', pd.Series(dtype=float))
            if 'area_fill' in df.columns:
                df['area_fill'] = df['area_fill'].fillna(df['area_fill'].median() if df['area_fill'].notna().any() else 50)
            else:
                df['area_fill'] = 50
            
            df['bedrooms_fill'] = df.get('bedrooms', pd.Series(dtype=float))
            if 'bedrooms_fill' in df.columns:
                df['bedrooms_fill'] = df['bedrooms_fill'].fillna(3)
            else:
                df['bedrooms_fill'] = 3
            
            df['bathrooms_fill'] = df.get('bathrooms', pd.Series(dtype=float))
            if 'bathrooms_fill' in df.columns:
                df['bathrooms_fill'] = df['bathrooms_fill'].fillna(2)
            else:
                df['bathrooms_fill'] = 2
            
            # Build feature matrix
            feats_minimal = ['city_enc', 'type_enc']
            feats_geo = ['lat', 'lon', 'type_enc']
            feats_full = ['lat', 'lon', 'type_enc', 'area_fill', 'bedrooms_fill', 'bathrooms_fill']
            
            y = df['log_price'].values
            
            print(f"\n{'='*50}")
            print("PAKISTAN ENRICHED MODEL RESULTS")
            print(f"{'='*50}")
            
            feature_sets = {
                'minimal (city+type)': feats_minimal,
                'geocoded (lat/lon+type)': feats_geo,
            }
            
            # Only add full if we have area
            has_area = df.get('area_raw', pd.Series(dtype=float)).notna().sum() > 10
            has_beds = df.get('bedrooms', pd.Series(dtype=float)).notna().sum() > 10
            if has_area or has_beds:
                feature_sets['full (geo+area+beds+baths)'] = feats_full
            
            for fname, fcols in feature_sets.items():
                available = [c for c in fcols if c in df.columns]
                if not available:
                    continue
                X = df[available].values
                m = lgb.LGBMRegressor(n_estimators=200, max_depth=6, verbose=-1, min_child_samples=5)
                s = cross_val_score(m, X, y, cv=5, scoring='r2')
                print(f"  {fname:35s}: R²={s.mean():.3f} ± {s.std():.3f}")
            
            # Leave-one-city-out
            print(f"\n  Leave-One-City-Out (full features):")
            best_feats = feats_full if has_area else feats_geo
            available_feats = [c for c in best_feats if c in df.columns]
            cities = df.get('city', pd.Series(index=df.index)).value_counts()
            for test_city in cities[cities >= 5].index:
                test = df['city'] == test_city
                train = ~test
                if train.sum() < 5:
                    continue
                m = lgb.LGBMRegressor(n_estimators=200, max_depth=6, verbose=-1, min_child_samples=3)
                m.fit(df.loc[train, available_feats].values, y[train])
                p = m.predict(df.loc[test, available_feats].values)
                yt = y[test]
                r2 = 1 - np.sum((yt - p)**2) / np.sum((yt - yt.mean())**2)
                actual = np.expm1(yt)
                predicted = np.expm1(p)
                mape = np.median(np.abs(actual - predicted) / actual) * 100
                print(f"    {test_city:15s} (n={test.sum():4d}): R²={r2:.3f}  MedAPE={mape:.1f}%")
            
            # Save enriched data
            df.to_csv(os.path.join(OUT, 'zameen_enriched.csv'), index=False)
            print(f"\n  Saved zameen_enriched.csv: {len(df)} rows")
        else:
            print("  No valid records after filtering")
    else:
        print("  No CKAN data available, using sweep7 fallback")

if __name__ == '__main__':
    main()
