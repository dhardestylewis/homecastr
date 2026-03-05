"""Download King County parcel polygons via direct FGDB download from KC GIS Open Data."""
import os, tempfile, requests, zipfile, io
import geopandas as gpd
from sqlalchemy import create_engine, text

DB_URL = "postgresql://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"

# King County parcels - ArcGIS Hub GeoJSON download
KC_GEOJSON_URL = "https://gisdata.kingcounty.gov/arcgis/rest/services/OpenDataPortal/property__parcel_area/MapServer/2370/query"

def main():
    print("=" * 60)
    print("Download King County Parcel Polygons")
    print("=" * 60)

    engine = create_engine(DB_URL, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
        print("Connected to Supabase")

    # Try multiple ArcGIS layer IDs (the parcel layer ID varies)
    layer_ids = [2370, 290, 0, 1, 2, 3]
    base_url = "https://gisdata.kingcounty.gov/arcgis/rest/services/OpenDataPortal/property__parcel_area/MapServer"
    
    # First, discover available layers
    print("\nDiscovering available layers...")
    try:
        r = requests.get(f"{base_url}?f=json", timeout=30)
        if r.status_code == 200:
            info = r.json()
            layers = info.get("layers", [])
            for l in layers:
                print(f"  Layer {l['id']}: {l['name']}")
            if layers:
                layer_ids = [l["id"] for l in layers]
    except Exception as e:
        print(f"  Could not discover layers: {e}")

    # Try each layer until we find parcels
    for layer_id in layer_ids:
        url = f"{base_url}/{layer_id}/query"
        print(f"\nTrying layer {layer_id}...")
        
        # First check what fields are available
        try:
            r = requests.get(url, params={
                "where": "1=1",
                "returnCountOnly": "true",
                "f": "json"
            }, timeout=30)
            if r.status_code != 200:
                continue
            count_data = r.json()
            count = count_data.get("count", 0)
            print(f"  Total features: {count}")
            if count == 0:
                continue
        except Exception as e:
            print(f"  Error: {e}")
            continue

        # Download parcels in Seattle metro area
        all_features = []
        offset = 0
        batch_size = 2000
        max_parcels = 50000

        while offset < max_parcels:
            params = {
                "where": "1=1",
                "outFields": "*",
                "geometry": "-122.45,47.45,-122.2,47.75",
                "geometryType": "esriGeometryEnvelope",
                "spatialRel": "esriSpatialRelIntersects",
                "inSR": "4326",
                "outSR": "4326",
                "f": "geojson",
                "resultOffset": offset,
                "resultRecordCount": batch_size,
            }
            r = requests.get(url, params=params, timeout=120)
            if r.status_code != 200:
                print(f"  Error {r.status_code}")
                break

            data = r.json()
            features = data.get("features", [])
            if not features:
                break

            all_features.extend(features)
            offset += len(features)
            print(f"  Downloaded {offset} parcels...")

            if len(features) < batch_size:
                break

        if all_features:
            print(f"\n  Got {len(all_features)} parcels from layer {layer_id}")
            break

    if not all_features:
        # Fallback: try the Socrata API
        print("\nTrying Socrata API fallback...")
        socrata_url = "https://data.kingcounty.gov/resource/jvxe-y7is.geojson"
        r = requests.get(socrata_url, params={"$limit": 50000}, timeout=120)
        if r.status_code == 200:
            data = r.json()
            all_features = data.get("features", [])
            print(f"  Got {len(all_features)} features from Socrata")

    if not all_features:
        print("No parcels downloaded from any source!")
        return

    # Convert to GeoDataFrame
    from shapely.geometry import shape
    rows = []
    for f in all_features:
        geom = shape(f["geometry"]) if f.get("geometry") else None
        props = f.get("properties", {})
        # Try multiple possible ID fields
        pin = props.get("PIN", props.get("pin", props.get("PARCEL_ID", props.get("parcel_id", props.get("MAJOR", "")))))
        if geom and pin:
            rows.append({"acct": str(pin).strip(), "geom": geom})

    gdf = gpd.GeoDataFrame(rows, geometry="geom", crs="EPSG:4326")
    print(f"\nParsed {len(gdf)} parcels with geometry")

    # Upload to geo_parcel_poly
    print(f"Uploading to geo_parcel_poly...")
    n = 0
    with engine.begin() as conn:
        for i, (_, row) in enumerate(gdf.iterrows()):
            try:
                conn.execute(text("""
                    INSERT INTO public.geo_parcel_poly (acct, geom)
                    VALUES (:acct, ST_GeomFromText(:wkt, 4326))
                    ON CONFLICT (acct) DO UPDATE SET geom = EXCLUDED.geom
                """), {"acct": row["acct"], "wkt": row["geom"].wkt})
                n += 1
            except Exception as e:
                if n < 3:
                    print(f"  Error: {e}")
            if (i + 1) % 1000 == 0:
                print(f"  {i+1}/{len(gdf)} ...")

    print(f"Uploaded {n} Seattle parcel polygons")


if __name__ == "__main__":
    main()
