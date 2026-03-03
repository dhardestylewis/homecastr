import modal

app = modal.App("upload-zcta-geometries")
image = modal.Image.debian_slim(python_version="3.11").apt_install(["libgdal-dev", "gdal-bin"]).pip_install(["geopandas", "sqlalchemy", "geoalchemy2", "psycopg2-binary"])

@app.function(
    image=image,
    timeout=3600,
    secrets=[modal.Secret.from_name("supabase-creds", required_keys=["SUPABASE_DB_URL"])]
)
def upload_zcta():
    import os
    import urllib.request
    import zipfile
    import geopandas as gpd
    from sqlalchemy import create_engine
    
    url = 'https://www2.census.gov/geo/tiger/TIGER2020/ZCTA520/tl_2020_us_zcta520.zip'
    local_zip = '/tmp/tl_2020_us_zcta520.zip'
    extract_dir = '/tmp/zcta'
    
    print("Downloading ZCTA shapefile from Census Bureau...")
    urllib.request.urlretrieve(url, local_zip)
    
    print("Extracting...")
    with zipfile.ZipFile(local_zip, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
        
    shp_path = os.path.join(extract_dir, 'tl_2020_us_zcta520.shp')
    
    print("Loading into GeoPandas...")
    # EPSG:4269 is NAD83, typical for Census. We project to WGS84 (EPSG:4326) for Mapbox PostGIS
    gdf = gpd.read_file(shp_path)
    gdf = gdf.to_crs(epsg=4326)
    
    # Map to public.geo_zcta20_us columns
    gdf = gdf[['GEOID20', 'geometry']].rename(columns={'GEOID20': 'zcta5', 'geometry': 'geom'})
    gdf.set_geometry('geom', inplace=True)
    
    print(f"Loaded {len(gdf)} ZCTA geometries.")
    
    db_url = os.environ["SUPABASE_DB_URL"].replace("postgres://", "postgresql://")
    engine = create_engine(db_url)
    
    print("Uploading to Supabase `public.geo_zcta20_us` via PostGIS...")
    # Since current data might just be Harris county, we can drop and replace with the true nationwide Census file
    # This ensures mapping is seamless.
    gdf.to_postgis(
        name="geo_zcta20_us", 
        schema="public", 
        con=engine, 
        if_exists="replace", 
        index=False # Let Supabase create indices, or we'll just insert
    )
    print("Complete!")
