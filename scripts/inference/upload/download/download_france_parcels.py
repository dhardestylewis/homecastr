"""Download France cadastre parcel polygons and upload to Supabase.
Uses cadastre.data.gouv.fr Etalab GeoJSON exports by département.
Downloads the 10 most populated départements to start."""
import os, requests, gzip, json, time
import geopandas as gpd
from sqlalchemy import create_engine, text

DB_URL = "postgresql://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"

# Top 10 most populated départements (covers Paris, Lyon, Marseille, etc.)
DEPTS = ["75", "13", "69", "59", "33", "31", "44", "34", "06", "67"]

CADASTRE_URL = "https://cadastre.data.gouv.fr/data/etalab-cadastre/latest/geojson/departements/{dept}/cadastre-{dept}-parcelles.json.gz"

def main():
    print("=" * 60)
    print("Download France Cadastre Parcel Polygons")
    print("=" * 60)

    engine = create_engine(DB_URL, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
        print("Connected to Supabase")

    total_uploaded = 0

    for dept in DEPTS:
        url = CADASTRE_URL.format(dept=dept)
        print(f"\n--- Département {dept} ---")
        print(f"  Downloading {url}...")

        try:
            r = requests.get(url, timeout=300, stream=True)
            if r.status_code != 200:
                print(f"  Error {r.status_code}")
                continue

            # Decompress and parse GeoJSON
            raw = gzip.decompress(r.content)
            data = json.loads(raw)
            features = data.get("features", [])
            print(f"  {len(features)} parcels")

            if not features:
                continue

            # Upload in batches
            n = 0
            with engine.begin() as conn:
                for i, f in enumerate(features):
                    geom = f.get("geometry")
                    props = f.get("properties", {})
                    parcel_id = props.get("id", "")  # e.g., "750101000A0001"

                    if not geom or not parcel_id:
                        continue

                    from shapely.geometry import shape
                    try:
                        g = shape(geom)
                        conn.execute(text("""
                            INSERT INTO public.geo_parcel_poly (acct, geom)
                            VALUES (:acct, ST_GeomFromText(:wkt, 4326))
                            ON CONFLICT (acct) DO UPDATE SET geom = EXCLUDED.geom
                        """), {"acct": parcel_id, "wkt": g.wkt})
                        n += 1
                    except Exception as e:
                        if n < 3:
                            print(f"  Error on {parcel_id}: {e}")

                    if (i + 1) % 5000 == 0:
                        print(f"  {i+1}/{len(features)} ...")

            print(f"  Uploaded {n} parcels for dept {dept}")
            total_uploaded += n

        except Exception as e:
            print(f"  Failed: {e}")

    print(f"\nTotal France parcels uploaded: {total_uploaded}")


if __name__ == "__main__":
    main()
