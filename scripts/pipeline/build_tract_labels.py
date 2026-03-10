import os
import json
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

BASE_DIR = r"C:\tmp\geo_layers"
OUT_FILE = r"C:\tmp\geo_layers\tract_labels.json"
STATES_TO_PROCESS = [
    "01", "02", "04", "05", "06", "08", "09", "10", "11", "12", "13", "15", "16", "17", 
    "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", 
    "32", "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "44", "45", "46", 
    "47", "48", "49", "50", "51", "53", "54", "55", "56"
]

def get_tiger_path(layer, fips):
    dir_name = layer + "s"
    return os.path.join(BASE_DIR, dir_name, fips, f"tl_2022_{fips}_{layer}.shp")

def load_gnis():
    gnis_txt = os.path.join(BASE_DIR, "gnis", "DomesticNames_National.txt")
    if not os.path.exists(gnis_txt):
        return None
    print("Loading GNIS...")
    # GNIS uses | delimiter
    df = pd.read_csv(gnis_txt, sep="|", dtype=str, low_memory=False)
    # Filter to Populated Places
    df = df[df['FEATURE_CLASS'] == 'Populated Place']
    # Filter out missing coords
    df = df.dropna(subset=['PRIMARY_LATITUDE', 'PRIMARY_LONGITUDE'])
    # Convert to numeric
    df['lat'] = pd.to_numeric(df['PRIMARY_LATITUDE'], errors='coerce')
    df['lon'] = pd.to_numeric(df['PRIMARY_LONGITUDE'], errors='coerce')
    df = df.dropna(subset=['lat', 'lon'])
    
    geometry = [Point(xy) for xy in zip(df['lon'], df['lat'])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4269")
    return gdf

def load_zip_hints():
    zip_crosswalk_path = "lib/publishing/tract-zcta-crosswalk.json"
    zip_names_path = "lib/publishing/zip-city-names.json"
    if os.path.exists(zip_crosswalk_path) and os.path.exists(zip_names_path):
        with open(zip_crosswalk_path, 'r') as f:
            xwalk = json.load(f)
        with open(zip_names_path, 'r') as f:
            names = json.load(f)
        return {tract: names.get(zcta) for tract, zcta in xwalk.items() if zcta in names}
    return {}

def process_state(fips, gnis_gdf, zip_hints):
    print(f"Processing state {fips}...")
    tract_path = get_tiger_path("tract", fips)
    place_path = get_tiger_path("place", fips)
    cousub_path = get_tiger_path("cousub", fips)
    
    if not os.path.exists(tract_path):
        print(f"Missing tracts for {fips}")
        return []

    tracts = gpd.read_file(tract_path)
    
    # Reproject to a projected CRS for accurate area/distance calculations (e.g. Albers Equal Area EPSG:5070)
    tracts = tracts.to_crs(epsg=5070)
    tracts['tract_area'] = tracts.geometry.area
    tracts['centroid'] = tracts.geometry.centroid
    
    places = gpd.read_file(place_path).to_crs(epsg=5070) if os.path.exists(place_path) else None
    cousubs = gpd.read_file(cousub_path).to_crs(epsg=5070) if os.path.exists(cousub_path) else None
    
    if gnis_gdf is not None:
        state_gnis = gnis_gdf[gnis_gdf['STATE_NUMERIC'] == fips].to_crs(epsg=5070)
    else:
        state_gnis = None

    results = []

    for idx, tract in tracts.iterrows():
        geoid = tract['GEOID']
        county_name = tract.get('NAMELSADCO', f"County {tract['COUNTYFP']}") # Placeholder if Namelsadco not present, but TIGER tract rarely has county name directly. Let's use NAMELSAD or rely on a county lookup later. Or we can just use "County" for now. Actually, let's just use empty string or lookup. 
        # Wait, TIGER tract 'NAMELSAD' is like 'Census Tract 101'.
        
        candidates = []
        
        # 1. Place Intersection
        if places is not None:
            # Get places that intersect this tract
            possible_places = places[places.geometry.intersects(tract.geometry)]
            for _, place in possible_places.iterrows():
                intersection = place.geometry.intersection(tract.geometry)
                overlap_share = intersection.area / tract['tract_area']
                centroid_in = place.geometry.contains(tract['centroid'])
                
                if overlap_share > 0.1 or centroid_in:
                    score = overlap_share + (0.5 if centroid_in else 0)
                    candidates.append({
                        'name': place['NAME'],
                        'type': 'place',
                        'score': score,
                        'overlap': overlap_share,
                        'centroid_in': centroid_in
                    })
        
        # 2. Cousub Intersection
        if cousubs is not None:
            possible_cousubs = cousubs[cousubs.geometry.intersects(tract.geometry)]
            for _, cousub in possible_cousubs.iterrows():
                # Avoid non-functioning cousubs (e.g., "County subdivisions not defined")
                name = cousub['NAME']
                if 'not defined' in name.lower() or 'unorganized' in name.lower():
                    continue
                
                intersection = cousub.geometry.intersection(tract.geometry)
                overlap_share = intersection.area / tract['tract_area']
                centroid_in = cousub.geometry.contains(tract['centroid'])
                
                if overlap_share > 0.1 or centroid_in:
                    score = overlap_share + (0.5 if centroid_in else 0)
                    # Cousubs are weighted slightly lower than places if there's a tie
                    candidates.append({
                        'name': name,
                        'type': 'cousub',
                        'score': score * 0.9, 
                        'overlap': overlap_share,
                        'centroid_in': centroid_in
                    })

        # 3. GNIS Proximity
        if state_gnis is not None and not state_gnis.empty:
            # Find GNIS points within tract
            pts_in_tract = state_gnis[state_gnis.geometry.intersects(tract.geometry)]
            if not pts_in_tract.empty:
                # Can pick the first one or largest one (if population is available, but it's often not in GNIS easily).
                # We'll just take the first one that intersects.
                gnis_name = pts_in_tract.iloc[0]['FEATURE_NAME']
                candidates.append({
                    'name': gnis_name,
                    'type': 'gnis',
                    'score': 0.8, # Fixed score for GNIS inside tract
                    'overlap': 1.0,
                    'centroid_in': True
                })
            else:
                # Find nearest GNIS point within ~5km (5000 meters)
                buffer = tract.geometry.buffer(5000)
                pts_near = state_gnis[state_gnis.geometry.intersects(buffer)]
                if not pts_near.empty:
                    # Sort by distance to centroid
                    pts_near = pts_near.copy()
                    pts_near['dist'] = pts_near.geometry.distance(tract['centroid'])
                    closest = pts_near.loc[pts_near['dist'].idxmin()]
                    
                    dist_penalty = max(0, 1 - (closest['dist'] / 5000))
                    candidates.append({
                        'name': closest['FEATURE_NAME'],
                        'type': 'gnis_near',
                        'score': 0.5 * dist_penalty,
                        'overlap': 0.0,
                        'centroid_in': False
                    })
        
        # 4. ZIP Hint
        zip_hint = zip_hints.get(geoid)
        if zip_hint:
            candidates.append({
                'name': zip_hint,
                'type': 'zip_hint',
                'score': 0.4,
                'overlap': 0,
                'centroid_in': False
            })
            
        # Decision Logic
        if candidates:
            # Sort by score descending
            candidates.sort(key=lambda x: x['score'], reverse=True)
            best = candidates[0]
            anchor_name = best['name']
            anchor_type = best['type']
            confidence_val = best['score']
            
            if confidence_val >= 1.0:
                confidence = "high"
                label_short = anchor_name
            elif confidence_val >= 0.5:
                confidence = "medium"
                label_short = f"{anchor_name} area"
            else:
                confidence = "low"
                label_short = f"Near {anchor_name}"
        else:
            anchor_name = "Unknown"
            anchor_type = "fallback"
            confidence = "low"
            # Attempt to pull a county name nicely, but we only have county FIPS here.
            # We'll default to a generic fallback.
            label_short = f"Tract {tract['TRACTCE']}"
            
        results.append({
            "tract_geoid": geoid,
            "anchor_name": anchor_name,
            "anchor_type": anchor_type,
            "anchor_overlap_share": best.get('overlap', 0) if candidates else 0,
            "confidence": confidence,
            "label_short": label_short,
            "label_medium": f"{label_short}", # Add county info if available later
            "label_long": f"Area designated as {label_short} (Tract {geoid})" # Expanded later
        })

    return results

def main():
    gnis_gdf = load_gnis()
    zip_hints = load_zip_hints()
    
    all_results = []
    for state in STATES_TO_PROCESS:
        state_results = process_state(state, gnis_gdf, zip_hints)
        all_results.extend(state_results)
        
    print(f"Total tracts processed: {len(all_results)}")
    
    # Save output
    df = pd.DataFrame(all_results)
    df.to_csv(OUT_FILE.replace(".json", ".csv"), index=False)
    
    # Save as JSON lookup mapping geoid -> { labels }
    json_out = {row['tract_geoid']: row for row in all_results}
    with open(OUT_FILE, 'w') as f:
        json.dump(json_out, f, indent=2)
        
    print(f"Results saved to {OUT_FILE}")

if __name__ == "__main__":
    main()
