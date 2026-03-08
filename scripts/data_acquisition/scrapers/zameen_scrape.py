"""Scrape Zameen.com listing data from __NEXT_DATA__ embedded JSON — no browser/DOM."""
import requests, json, re, os, warnings
import pandas as pd, numpy as np
warnings.filterwarnings('ignore')
import lightgbm as lgb
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import cross_val_score

OUT = os.path.dirname(__file__)
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
}

PK_CITIES_COORDS = {
    'islamabad': (33.684, 73.048), 'lahore': (31.520, 74.359),
    'karachi': (24.861, 67.001), 'rawalpindi': (33.565, 73.017),
    'faisalabad': (31.450, 73.135), 'peshawar': (34.012, 71.578),
    'multan': (30.196, 71.475), 'hyderabad': (25.396, 68.377),
    'quetta': (30.183, 66.997),
}

def scrape_zameen_page(city_slug, city_id, page=1):
    """Fetch a Zameen listing page and extract data from __NEXT_DATA__."""
    url = f"https://www.zameen.com/Homes/{city_slug}-{city_id}-{page}.html"
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            return [], f"HTTP {r.status_code}"
        
        html = r.text
        
        # Extract __NEXT_DATA__
        match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
        if not match:
            return [], "No __NEXT_DATA__ found"
        
        data = json.loads(match.group(1))
        props = data.get('props', {}).get('pageProps', {})
        
        # Navigate to listings
        listings = []
        
        # Try different paths where listings might be
        search_data = props
        for path in [
            ['searchResult', 'listings'],
            ['data', 'listings'],
            ['listings'],
            ['searchData', 'listings'],
            ['initialState', 'search', 'listings'],
        ]:
            obj = search_data
            for key in path:
                if isinstance(obj, dict) and key in obj:
                    obj = obj[key]
                else:
                    obj = None
                    break
            if obj and isinstance(obj, list):
                listings = obj
                break
        
        if not listings:
            # Dump top-level keys for debugging
            keys_debug = list(props.keys()) if isinstance(props, dict) else str(type(props))
            return [], f"No listings found. pageProps keys: {keys_debug}"
        
        rows = []
        for item in listings:
            row = {}
            
            # Price
            for pkey in ['price', 'priceFormatted', 'amount', 'price_value']:
                if pkey in item:
                    val = item[pkey]
                    if isinstance(val, dict):
                        row['price'] = val.get('value') or val.get('amount')
                    else:
                        row['price'] = val
                    break
            
            # Area
            for akey in ['area', 'areaFormatted', 'size', 'area_value']:
                if akey in item:
                    val = item[akey]
                    if isinstance(val, dict):
                        row['area'] = val.get('value') or val.get('size')
                        row['area_unit'] = val.get('unit', '')
                    else:
                        row['area'] = val
                    break
            
            # Bedrooms / Bathrooms
            for bkey in ['bedrooms', 'beds', 'bedroom_count']:
                if bkey in item:
                    row['bedrooms'] = item[bkey]
                    break
            for bkey in ['bathrooms', 'baths', 'bathroom_count']:
                if bkey in item:
                    row['bathrooms'] = item[bkey]
                    break
            
            # Location
            for lkey in ['location', 'address', 'location_name']:
                if lkey in item:
                    val = item[lkey]
                    if isinstance(val, dict):
                        row['location'] = val.get('name') or val.get('address') or str(val)
                        row['lat'] = val.get('lat') or val.get('latitude')
                        row['lon'] = val.get('lng') or val.get('longitude')
                    else:
                        row['location'] = val
                    break
            
            # Property type
            for tkey in ['type', 'property_type', 'category', 'propertyType']:
                if tkey in item:
                    val = item[tkey]
                    row['property_type'] = val.get('name') if isinstance(val, dict) else val
                    break
            
            # Purpose (sale/rent)
            row['purpose'] = item.get('purpose', '')
            row['title'] = item.get('title', '')
            row['id'] = item.get('id', '')
            row['city'] = city_slug
            
            # Dump all keys for first item to see what's available
            if not rows:
                row['_all_keys'] = list(item.keys())
            
            rows.append(row)
        
        return rows, f"OK: {len(rows)} listings"
    except Exception as e:
        return [], str(e)


def main():
    print("=" * 60)
    print("ZAMEEN.COM SCRAPE — __NEXT_DATA__ EXTRACTION")
    print("=" * 60)
    
    cities = [
        ('Islamabad', 3),
        ('Lahore', 2),
        ('Karachi', 1),
        ('Rawalpindi', 60),
        ('Faisalabad', 43),
    ]
    
    all_rows = []
    for city, cid in cities:
        for page in range(1, 6):  # 5 pages per city
            rows, status = scrape_zameen_page(city, cid, page)
            print(f"  {city} p{page}: {status}")
            if rows:
                # Print first row's keys for debugging
                if page == 1 and '_all_keys' in rows[0]:
                    print(f"    Available keys: {rows[0]['_all_keys']}")
                all_rows.extend(rows)
            if not rows:
                break  # No more pages
    
    if not all_rows:
        print("\nNo data extracted. Zameen may be blocking.")
        return
    
    df = pd.DataFrame(all_rows)
    print(f"\nTotal: {len(df)} listings")
    print(f"Columns: {list(df.columns)}")
    
    # Print sample
    sample_cols = [c for c in ['price','area','area_unit','bedrooms','bathrooms','property_type','city','location'] if c in df.columns]
    print(f"\nSample (first 5):")
    print(df[sample_cols].head().to_string())
    
    # Clean price
    df['price_num'] = pd.to_numeric(df['price'], errors='coerce')
    
    # Clean area
    df['area_num'] = pd.to_numeric(df.get('area', pd.Series(dtype=float)), errors='coerce')
    
    # Clean beds/baths
    df['beds_num'] = pd.to_numeric(df.get('bedrooms', pd.Series(dtype=float)), errors='coerce')
    df['baths_num'] = pd.to_numeric(df.get('bathrooms', pd.Series(dtype=float)), errors='coerce')
    
    # Geocode
    df['lat_geo'] = df['city'].str.lower().map(lambda c: PK_CITIES_COORDS.get(c, (30, 70))[0])
    df['lon_geo'] = df['city'].str.lower().map(lambda c: PK_CITIES_COORDS.get(c, (30, 70))[1])
    
    # Use per-property lat/lon if available
    df['lat_final'] = pd.to_numeric(df.get('lat', pd.Series(dtype=float)), errors='coerce').fillna(df['lat_geo'])
    df['lon_final'] = pd.to_numeric(df.get('lon', pd.Series(dtype=float)), errors='coerce').fillna(df['lon_geo'])
    
    # Filter valid
    valid = df[df['price_num'].notna() & (df['price_num'] > 100000)].copy()
    print(f"\nAfter price filter: {len(valid)} listings")
    
    has_area = valid['area_num'].notna().sum()
    has_beds = valid['beds_num'].notna().sum()
    has_baths = valid['baths_num'].notna().sum()
    has_latlon = (valid['lat'].notna() if 'lat' in valid.columns else pd.Series(False, index=valid.index)).sum()
    print(f"  Has area: {has_area}/{len(valid)}")
    print(f"  Has beds: {has_beds}/{len(valid)}")
    print(f"  Has baths: {has_baths}/{len(valid)}")
    print(f"  Has per-property lat/lon: {has_latlon}/{len(valid)}")
    
    if len(valid) < 20:
        print("Not enough data for modeling")
        valid.to_csv(os.path.join(OUT, 'zameen_scraped.csv'), index=False)
        return
    
    # ═══ MODEL TRAINING ═══
    valid['log_price'] = np.log1p(valid['price_num'])
    valid['area_fill'] = valid['area_num'].fillna(valid['area_num'].median() if has_area > 5 else 10)
    valid['beds_fill'] = valid['beds_num'].fillna(3)
    valid['baths_fill'] = valid['baths_num'].fillna(2)
    
    le_type = LabelEncoder()
    valid['type_enc'] = le_type.fit_transform(valid.get('property_type', pd.Series('unknown', index=valid.index)).fillna('unknown'))
    le_city = LabelEncoder()
    valid['city_enc'] = le_city.fit_transform(valid['city'].fillna('unknown'))
    
    y = valid['log_price'].values
    
    print(f"\n{'='*50}")
    print("PAKISTAN MODEL — ENRICHED ZAMEEN DATA")
    print(f"{'='*50}")
    
    feature_configs = {
        'minimal (city+type)': ['city_enc', 'type_enc'],
        'geo (lat/lon+type)': ['lat_final', 'lon_final', 'type_enc'],
    }
    if has_area > 10:
        feature_configs['+ area'] = ['lat_final', 'lon_final', 'type_enc', 'area_fill']
    if has_beds > 10:
        feature_configs['+ beds/baths'] = ['lat_final', 'lon_final', 'type_enc', 'area_fill', 'beds_fill', 'baths_fill']
    if has_latlon > 10:
        feature_configs['full (property lat/lon)'] = ['lat_final', 'lon_final', 'type_enc', 'area_fill', 'beds_fill', 'baths_fill']
    
    results = {}
    for fname, fcols in feature_configs.items():
        avail = [c for c in fcols if c in valid.columns]
        X = valid[avail].values
        m = lgb.LGBMRegressor(n_estimators=200, max_depth=6, verbose=-1, min_child_samples=3)
        s = cross_val_score(m, X, y, cv=min(5, len(valid)//10), scoring='r2')
        r2 = s.mean()
        results[fname] = round(r2, 3)
        print(f"  {fname:35s}: R2={r2:.3f} +/- {s.std():.3f}")
    
    # Leave-one-city-out
    print(f"\n  Leave-One-City-Out (best features):")
    best_feats = list(feature_configs.values())[-1]
    avail = [c for c in best_feats if c in valid.columns]
    cities_vc = valid['city'].value_counts()
    loco = {}
    for tc in cities_vc[cities_vc >= 5].index:
        test = valid['city'] == tc
        train = ~test
        if train.sum() < 5: continue
        m = lgb.LGBMRegressor(n_estimators=200, max_depth=6, verbose=-1, min_child_samples=3)
        m.fit(valid.loc[train, avail].values, y[train])
        p = m.predict(valid.loc[test, avail].values)
        yt = y[test]
        r2 = 1 - np.sum((yt-p)**2)/np.sum((yt-yt.mean())**2)
        act = np.expm1(yt); pred = np.expm1(p)
        mape = np.median(np.abs(act-pred)/act)*100
        loco[tc] = {'r2': round(r2,3), 'mape': round(mape,1), 'n': int(test.sum())}
        print(f"    {tc:15s} (n={test.sum():4d}): R2={r2:.3f}  MedAPE={mape:.1f}%")
    
    results['loco'] = loco
    
    # Save
    valid.to_csv(os.path.join(OUT, 'zameen_scraped.csv'), index=False)
    with open(os.path.join(OUT, 'zameen_model_results.json'), 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\nSaved: zameen_scraped.csv ({len(valid)} rows), zameen_model_results.json")


if __name__ == '__main__':
    main()
