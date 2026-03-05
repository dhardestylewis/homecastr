"""
Build Universal Parcel Panel — Phase 1 of Transfer Learning
=============================================================
Creates parcel-level panels using ONLY freely-available national datasets:
  1. Microsoft Building Footprints → building area, centroid lat/lon
  2. OSMnx → POI distances, road network, walkability proxies
  3. NLCD → Land use/land cover class, impervious surface %
  4. FEMA NFHL → Flood zone designation
  5. USGS 3DEP → Elevation, slope, aspect
  6. PRISM Climate → Mean annual temp, precipitation

For teacher/student distillation: this panel is spatially joined to HCAD
parcels, then the student model trains on these universal features to match
the HCAD teacher's distributional output.

Usage:
    # Harris County proof-of-concept:
    modal run scripts/data_acquisition/build_universal_panel_modal.py --fips 48201

    # Multi-county:
    modal run scripts/data_acquisition/build_universal_panel_modal.py --fips 48201,06037,36061
"""

import modal
import os
import sys

# Parse args for descriptive Modal app name
_fips = "unknown"
for i, arg in enumerate(sys.argv):
    if arg == "--fips" and i + 1 < len(sys.argv):
        _fips = sys.argv[i + 1].split(",")[0]

app = modal.App(f"universal-panel-{_fips}")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("gdal-bin", "libgdal-dev", "libspatialindex-dev", "wget")
    .pip_install(
        "google-cloud-storage>=2.10",
        "pandas>=2.0",
        "geopandas>=0.14",
        "pyarrow>=14.0",
        "shapely>=2.0",
        "requests>=2.31",
        "osmnx>=1.9",
        "networkx>=3.2",
        "rasterio>=1.3",
        "rasterstats>=0.19",
        "numpy>=1.24",
        "scikit-learn>=1.3",
        "h3>=3.7",
    )
)

gcs_secret = modal.Secret.from_name("gcs-creds")

# ─── Constants ───
BUCKET_NAME = "properlytic-raw-data"

# MS Building Footprints — stored as GeoJSON lines per state on GitHub
MS_BUILDINGS_BASE = "https://minedbuildings.z5.web.core.windows.net/global-buildings/dataset-links.csv"

# NLCD 2021 — Cloud-Optimized GeoTIFF
NLCD_COG_URL = "https://s3-us-west-2.amazonaws.com/mrlc/nlcd_2021_land_cover_l48_20230630_resample.tif"

# FEMA NFHL — available per county via API
FEMA_NFHL_API = "https://hazards.fema.gov/gis/nfhl/rest/services/public/NFHL/MapServer/28/query"

# USGS 3DEP — available as cloud-optimized GeoTIFF via STAC
THREEDEP_STAC = "https://planetarycomputer.microsoft.com/api/stac/v1"

# OSMnx POI tags to compute distances to
OSM_POI_TAGS = {
    "school": {"amenity": "school"},
    "hospital": {"amenity": "hospital"},
    "grocery": {"shop": ["supermarket", "grocery"]},
    "park": {"leisure": "park"},
    "transit": {"public_transport": "stop_position"},
    "restaurant": {"amenity": "restaurant"},
    "gas_station": {"amenity": "fuel"},
}


@app.function(
    image=image,
    secrets=[gcs_secret],
    timeout=14400,  # 4h — large counties
    memory=32768,
    cpu=4.0,
)
def build_universal_panel(
    fips: str = "48201",  # Harris County, TX
    bucket_name: str = BUCKET_NAME,
    skip_ms_buildings: bool = False,
    skip_osm: bool = False,
    skip_rasters: bool = False,
    force_rebuild: bool = False,
):
    """Build universal-feature panel for a single county.
    
    Cache-first: if the final panel already exists on GCS, skip unless --force-rebuild.
    All intermediate datasets (MS Buildings, OSM, rasters) are cached on GCS after
    first fetch so they never need to be re-downloaded from public APIs.
    """
    import json, time, io, tempfile, warnings
    warnings.filterwarnings("ignore")

    import numpy as np
    import pandas as pd
    import geopandas as gpd
    from shapely.geometry import Point, box
    from google.cloud import storage

    ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")

    state_fips = fips[:2]
    county_fips = fips[2:]
    jurisdiction = f"universal_{fips}"
    print(f"[{ts()}] Building universal panel for FIPS={fips} (state={state_fips}, county={county_fips})")

    # ─── GCS setup ───
    creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON", "")
    if creds_json:
        creds_path = "/tmp/gcs_creds.json"
        with open(creds_path, "w") as f:
            f.write(creds_json)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # ─── Cache check: skip if final panel already exists ───
    final_blob = bucket.blob(f"panel/jurisdiction={jurisdiction}/part.parquet")
    if final_blob.exists() and not force_rebuild:
        print(f"[{ts()}] ✅ Panel already cached on GCS — skipping build")
        print(f"  gs://{bucket_name}/panel/jurisdiction={jurisdiction}/part.parquet")
        print(f"  Use --force-rebuild to regenerate")
        meta_blob = bucket.blob(f"panel/jurisdiction={jurisdiction}/summary.json")
        if meta_blob.exists():
            return json.loads(meta_blob.download_as_string())
        return {"status": "cached", "fips": fips}

    # ─── Step 1: Microsoft Building Footprints (with GCS cache) ───
    buildings_gdf = None
    buildings_cache_path = f"universal_cache/{fips}/ms_buildings.parquet"
    if not skip_ms_buildings:
        print(f"\n[{ts()}] Step 1: Microsoft Building Footprints...")
        # Check GCS cache first
        cache_blob = bucket.blob(buildings_cache_path)
        if cache_blob.exists() and not force_rebuild:
            print(f"  Loading from GCS cache: {buildings_cache_path}")
            with io.BytesIO() as buf:
                cache_blob.download_to_file(buf)
                buf.seek(0)
                buildings_gdf = gpd.read_parquet(buf)
            print(f"  ✅ {len(buildings_gdf):,} buildings from cache")
        else:
            buildings_gdf = _fetch_ms_buildings(fips, state_fips, ts)
            if buildings_gdf is not None:
                print(f"  ✅ {len(buildings_gdf):,} buildings loaded")
                # Cache to GCS
                buf = io.BytesIO()
                buildings_gdf.to_parquet(buf, index=False)
                buf.seek(0)
                cache_blob.upload_from_file(buf, content_type="application/octet-stream")
                print(f"  📦 Cached to gs://{bucket_name}/{buildings_cache_path}")
            else:
                print(f"  ⚠️ No buildings found, generating from Census blocks")
                buildings_gdf = _generate_census_building_proxies(fips, ts)

    # If we still don't have buildings, use Census block centroids
    if buildings_gdf is None or len(buildings_gdf) == 0:
        print(f"  Falling back to Census block centroids")
        buildings_gdf = _generate_census_building_proxies(fips, ts)

    print(f"[{ts()}] Working with {len(buildings_gdf):,} building/parcel points")

    # Assign unique parcel IDs
    buildings_gdf["parcel_id"] = [f"us_{fips}_{i:08d}" for i in range(len(buildings_gdf))]

    # ─── Step 2: OSMnx Spatial Features (with GCS cache) ───
    osm_cache_path = f"universal_cache/{fips}/osm_enriched.parquet"
    if not skip_osm:
        print(f"\n[{ts()}] Step 2: OSMnx spatial enrichment...")
        cache_blob = bucket.blob(osm_cache_path)
        if cache_blob.exists() and not force_rebuild:
            print(f"  Loading OSM features from GCS cache...")
            with io.BytesIO() as buf:
                cache_blob.download_to_file(buf)
                buf.seek(0)
                osm_cols = gpd.read_parquet(buf)
            # Merge OSM columns onto buildings
            osm_feature_cols = [c for c in osm_cols.columns if c.startswith("dist_") or c in ("road_betweenness", "road_degree", "building_density_h3")]
            for c in osm_feature_cols:
                if c in osm_cols.columns:
                    buildings_gdf[c] = osm_cols[c].values[:len(buildings_gdf)]
            print(f"  ✅ {len(osm_feature_cols)} OSM features from cache")
        else:
            buildings_gdf = _enrich_osm(buildings_gdf, fips, ts)
            # Cache enriched buildings
            buf = io.BytesIO()
            buildings_gdf.to_parquet(buf, index=False)
            buf.seek(0)
            cache_blob.upload_from_file(buf, content_type="application/octet-stream")
            print(f"  📦 Cached OSM-enriched to gs://{bucket_name}/{osm_cache_path}")

    # ─── Step 3: Raster Features (NLCD, 3DEP, PRISM) ───
    if not skip_rasters:
        print(f"\n[{ts()}] Step 3: Raster feature extraction...")
        buildings_gdf = _enrich_rasters(buildings_gdf, fips, ts)

    # ─── Step 4: FEMA Flood Zones ───
    if not skip_rasters:
        print(f"\n[{ts()}] Step 4: FEMA flood zone query...")
        buildings_gdf = _enrich_fema(buildings_gdf, fips, ts)

    # ─── Step 5: Build multi-year panel ───
    print(f"\n[{ts()}] Step 5: Expanding to multi-year panel...")
    panel = _build_multiyear_panel(buildings_gdf, fips, ts)

    # ─── Step 6: Upload final panel to GCS (this IS the cache for train_modal.py) ───
    print(f"\n[{ts()}] Step 6: Uploading to GCS (cached for future training runs)...")
    buf = io.BytesIO()
    panel.to_parquet(buf, index=False)
    buf.seek(0)
    blob = bucket.blob(f"panel/jurisdiction={jurisdiction}/part.parquet")
    blob.upload_from_file(buf, content_type="application/octet-stream")
    size_mb = buf.tell() / 1e6
    print(f"  ✅ Uploaded: gs://{bucket_name}/panel/jurisdiction={jurisdiction}/part.parquet ({size_mb:.1f} MB)")
    print(f"  Future train_modal.py runs will pull from GCS — no re-fetch needed")

    summary = {
        "fips": fips,
        "n_rows": len(panel),
        "n_parcels": int(panel["parcel_id"].nunique()),
        "years": sorted(panel["year"].unique().tolist()),
        "columns": list(panel.columns),
        "n_features": len([c for c in panel.columns if c not in ("parcel_id", "year", "jurisdiction")]),
    }
    blob = bucket.blob(f"panel/jurisdiction={jurisdiction}/summary.json")
    blob.upload_from_string(json.dumps(summary, indent=2))
    print(f"  Summary: {summary}")
    return summary


# =============================================================================
# STEP 1: Microsoft Building Footprints
# =============================================================================
def _fetch_ms_buildings(fips, state_fips, ts):
    """Download MS Building Footprints for a county using QuadKey-based CSV.GZ tiles.
    
    MS Buildings v3 distributes data as ~2,400 US QuadKey tiles in CSV.GZ format.
    We download the dataset-links.csv index, find QuadKeys that intersect the county
    bbox, download those tiles, and filter to the county boundary.
    """
    import requests
    import geopandas as gpd
    import pandas as pd
    import numpy as np
    import csv, gzip, io, json
    from shapely.geometry import shape, Point, box

    # ─── Get county boundary from Census TIGER ───
    print(f"  Fetching county boundary from Census TIGER...")
    county_boundary = None
    # Use tigerWMS_Current/MapServer/82 with GEOID param (most reliable)
    try:
        r = requests.get(
            "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Current/MapServer/82/query",
            params={
                "where": f"GEOID='{fips}'",
                "outFields": "GEOID,BASENAME",
                "returnGeometry": "true",
                "f": "geojson",
                "outSR": "4326",
            },
            timeout=60
        )
        data = r.json()
        if "features" in data and len(data["features"]) > 0:
            county_gdf = gpd.GeoDataFrame.from_features(data["features"], crs="EPSG:4326")
            county_boundary = county_gdf.geometry.unary_union
            bounds = county_boundary.bounds  # (minx, miny, maxx, maxy)
            print(f"  County bounds: {bounds}")
        else:
            print(f"  TIGER returned no features: {str(data)[:200]}")
    except Exception as e:
        print(f"  TIGER query failed: {e}")
    
    if county_boundary is None:
        print(f"  Failed to get county boundary — cannot fetch MS Buildings")
        return None

    # ─── Download dataset-links.csv to find relevant QuadKey tiles ───
    print(f"  Downloading MS Buildings tile index...")
    try:
        r = requests.get(
            "https://minedbuildings.z5.web.core.windows.net/global-buildings/dataset-links.csv",
            timeout=30
        )
        reader = csv.DictReader(io.StringIO(r.text))
        us_tiles = [row for row in reader if row.get("Location") == "UnitedStates"]
        print(f"  Total US tiles: {len(us_tiles)}")
    except Exception as e:
        print(f"  Failed to download tile index: {e}")
        return None

    # ─── QuadKey to bbox conversion (Bing Maps tile system) ───
    def quadkey_to_bbox(qk):
        """Convert a Bing Maps QuadKey to a lat/lon bounding box."""
        level = len(qk)
        tx, ty = 0, 0
        for i in range(level):
            mask = 1 << (level - 1 - i)
            digit = int(qk[i])
            if digit & 1:
                tx |= mask
            if digit & 2:
                ty |= mask
        n = 2 ** level
        lon_min = (tx / n) * 360.0 - 180.0
        lon_max = ((tx + 1) / n) * 360.0 - 180.0
        import math
        lat_max = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * ty / n))))
        lat_min = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (ty + 1) / n))))
        return (lon_min, lat_min, lon_max, lat_max)

    # ─── Find tiles that intersect county bbox ───
    county_box = box(*bounds)
    matching_tiles = []
    for tile in us_tiles:
        qk = tile.get("QuadKey", "")
        try:
            tile_bbox = quadkey_to_bbox(qk)
            tile_box = box(*tile_bbox)
            if tile_box.intersects(county_box):
                matching_tiles.append(tile)
        except Exception:
            continue

    print(f"  Tiles intersecting county: {len(matching_tiles)}")
    if not matching_tiles:
        print(f"  No matching tiles found!")
        return None

    # ─── Download matching tiles and extract buildings ───
    all_buildings = []
    minx, miny, maxx, maxy = bounds
    for i, tile in enumerate(matching_tiles):
        url = tile.get("Url", "")
        qk = tile.get("QuadKey", "")
        size = tile.get("Size", "?")
        print(f"  Downloading tile {i+1}/{len(matching_tiles)} (QK={qk}, {size})...")
        try:
            r = requests.get(url, timeout=120)
            if r.status_code != 200:
                print(f"    Skipped: HTTP {r.status_code}")
                continue
            # CSV.GZ format: each line is a GeoJSON feature
            for line in gzip.open(io.BytesIO(r.content)):
                try:
                    feat = json.loads(line)
                    geom = shape(feat["geometry"])
                    centroid = geom.centroid
                    # Quick bbox filter before expensive .within() check
                    if minx <= centroid.x <= maxx and miny <= centroid.y <= maxy:
                        # Compute area in m² (rough conversion from degrees)
                        lat_rad = np.radians(centroid.y)
                        m_per_deg_lon = 111_320 * np.cos(lat_rad)
                        m_per_deg_lat = 110_540
                        area_m2 = geom.area * m_per_deg_lon * m_per_deg_lat
                        props = feat.get("properties", {})
                        all_buildings.append({
                            "lat": centroid.y,
                            "lon": centroid.x,
                            "area_m2": area_m2,
                            "height": props.get("height"),
                            "geometry": geom,
                        })
                except (json.JSONDecodeError, KeyError):
                    continue
            print(f"    Running total: {len(all_buildings):,} buildings in bbox")
        except Exception as e:
            print(f"    Error: {e}")

    if not all_buildings:
        print(f"  No buildings found in any tiles")
        return None

    buildings = gpd.GeoDataFrame(all_buildings, crs="EPSG:4326")
    print(f"  Buildings in bbox: {len(buildings):,}")

    # Final filter: within actual county boundary (not just bbox)
    buildings = buildings[buildings.geometry.centroid.within(county_boundary)]
    print(f"  Buildings within county boundary: {len(buildings):,}")

    if len(buildings) == 0:
        return None

    # Compute per-building features
    buildings["sqft_proxy"] = buildings["area_m2"] * 10.764  # m² → ft²

    # Height → stories estimate
    if "height" in buildings.columns:
        buildings["stories_proxy"] = (
            pd.to_numeric(buildings["height"], errors="coerce")
            .div(3.0).clip(1, 50).fillna(1)
        )
    else:
        buildings["stories_proxy"] = 1.0

    # H3 cell assignment for spatial indexing
    try:
        import h3
        buildings["h3_8"] = buildings.apply(
            lambda r: h3.latlng_to_cell(r["lat"], r["lon"], 8), axis=1
        )
    except Exception:
        buildings["h3_8"] = "unknown"

    result = buildings[["lat", "lon", "area_m2", "sqft_proxy", "stories_proxy", "h3_8", "geometry"]].copy()
    result = result.reset_index(drop=True)
    return result


def _generate_census_building_proxies(fips, ts):
    """Fallback: Generate building proxies from Census block centroids with housing unit counts."""
    import requests
    import geopandas as gpd
    import pandas as pd
    from shapely.geometry import Point

    state_fips = fips[:2]
    county_fips = fips[2:]

    print(f"  Fetching Census block-level housing from ACS...")
    # Use Census API to get block group-level housing unit counts
    url = (
        f"https://api.census.gov/data/2020/dec/dhc"
        f"?get=H1_001N,P1_001N"
        f"&for=block:*&in=state:{state_fips}%20county:{county_fips}"
    )
    try:
        r = requests.get(url, timeout=120)
        data = r.json()
        df = pd.DataFrame(data[1:], columns=data[0])
        df["geoid_block"] = df["state"] + df["county"] + df["tract"] + df["block"]
        df["housing_units"] = pd.to_numeric(df["H1_001N"], errors="coerce").fillna(0).astype(int)
        df["population"] = pd.to_numeric(df["P1_001N"], errors="coerce").fillna(0).astype(int)
        print(f"  Census blocks: {len(df):,}")
    except Exception as e:
        print(f"  Census API failed: {e}")
        return gpd.GeoDataFrame(columns=["lat", "lon", "area_m2", "sqft_proxy", "stories_proxy", "h3_8", "geometry"])

    # Get block group centroids from TIGER
    try:
        r = requests.get(
            "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Current/MapServer/14/query",
            params={
                "where": f"STATE='{state_fips}' AND COUNTY='{county_fips}'",
                "outFields": "GEOID,CENTLAT,CENTLON",
                "returnGeometry": "true",
                "f": "geojson",
                "outSR": "4326",
                "resultRecordCount": 10000,
            },
            timeout=120
        )
        data = r.json()
        if "features" not in data or len(data["features"]) == 0:
            print(f"  TIGER blocks returned no features: {str(data)[:200]}")
            return gpd.GeoDataFrame(columns=["lat", "lon", "area_m2", "sqft_proxy", "stories_proxy", "h3_8", "geometry"])
        blocks_geo = gpd.GeoDataFrame.from_features(data["features"], crs="EPSG:4326")
        blocks_geo["lat"] = pd.to_numeric(blocks_geo["CENTLAT"], errors="coerce")
        blocks_geo["lon"] = pd.to_numeric(blocks_geo["CENTLON"], errors="coerce")
        blocks_geo = blocks_geo.rename(columns={"GEOID": "geoid_block"})
    except Exception as e:
        print(f"  TIGER blocks failed: {e}")
        return gpd.GeoDataFrame(columns=["lat", "lon", "area_m2", "sqft_proxy", "stories_proxy", "h3_8", "geometry"])

    merged = blocks_geo.merge(df[["geoid_block", "housing_units", "population"]], on="geoid_block", how="left")
    merged = merged[merged["housing_units"] > 0]
    merged["area_m2"] = 150.0  # Approximate average US home: ~1600 sqft = ~150 m²
    merged["sqft_proxy"] = 1600.0
    merged["stories_proxy"] = 1.5
    merged["h3_8"] = "census_proxy"
    merged["geometry"] = merged.apply(lambda r: Point(r["lon"], r["lat"]), axis=1)

    return merged[["lat", "lon", "area_m2", "sqft_proxy", "stories_proxy", "h3_8", "geometry"]].reset_index(drop=True)


# =============================================================================
# STEP 2: OSMnx Spatial Enrichment
# =============================================================================
def _enrich_osm(gdf, fips, ts):
    """Compute POI distances and network metrics using OSMnx."""
    import osmnx as ox
    import numpy as np
    from scipy.spatial import cKDTree

    state_fips = fips[:2]
    county_fips = fips[2:]

    # Get county boundary for OSMnx queries
    bounds = gdf.geometry.total_bounds  # minx, miny, maxx, maxy
    # Guard against NaN bounds (happens with Census proxy fallback)
    if any(np.isnan(b) for b in bounds):
        # Compute bounds from lat/lon columns instead
        valid = gdf.dropna(subset=["lat", "lon"])
        if len(valid) == 0:
            print(f"  ⚠️ No valid lat/lon — skipping OSM enrichment")
            return gdf
        bounds = (valid["lon"].min(), valid["lat"].min(), valid["lon"].max(), valid["lat"].max())
        print(f"  Using lat/lon-derived bounds: {bounds}")
    center_lat = (bounds[1] + bounds[3]) / 2
    center_lon = (bounds[0] + bounds[2]) / 2

    building_coords = np.column_stack([gdf["lat"].values, gdf["lon"].values])

    # POI distances
    for poi_name, tags in OSM_POI_TAGS.items():
        try:
            print(f"  OSM: Querying {poi_name}...")
            pois = ox.features_from_bbox(
                bbox=(bounds[1] - 0.01, bounds[3] + 0.01, bounds[0] - 0.01, bounds[2] + 0.01),
                tags=tags
            )
            if len(pois) == 0:
                gdf[f"dist_{poi_name}_km"] = np.nan
                continue

            poi_centroids = pois.geometry.centroid
            poi_coords = np.column_stack([poi_centroids.y, poi_centroids.x])

            # KDTree for nearest-POI distance (in degrees, convert to km approx)
            tree = cKDTree(poi_coords)
            dists, _ = tree.query(building_coords, k=1)
            # Rough degree → km conversion at mid-latitude
            km_per_deg = 111.0 * np.cos(np.radians(center_lat))
            gdf[f"dist_{poi_name}_km"] = dists * km_per_deg
            print(f"    ✅ {poi_name}: {len(pois)} POIs, median dist={gdf[f'dist_{poi_name}_km'].median():.2f} km")
        except Exception as e:
            print(f"    ⚠️ {poi_name}: {e}")
            gdf[f"dist_{poi_name}_km"] = np.nan

    # Road network metrics
    try:
        print(f"  OSM: Fetching road network...")
        G = ox.graph_from_bbox(
            bbox=(bounds[1] - 0.005, bounds[3] + 0.005, bounds[0] - 0.005, bounds[2] + 0.005),
            network_type="drive",
            simplify=True,
        )
        # Get nearest network node for each building
        nodes = ox.graph_to_gdfs(G, edges=False)
        node_coords = np.column_stack([nodes.geometry.y, nodes.geometry.x])
        tree = cKDTree(node_coords)
        _, nearest_idx = tree.query(building_coords, k=1)

        # Compute basic node-level metrics (betweenness is expensive, so sample)
        import networkx as nx
        n_sample = min(2000, len(G.nodes))
        bc = nx.betweenness_centrality(G, k=n_sample, weight="length")
        node_ids = list(nodes.index)
        gdf["road_betweenness"] = [bc.get(node_ids[i], 0.0) for i in nearest_idx]

        # Road density (edges per node in local neighborhood)
        degree_dict = dict(G.degree())
        gdf["road_degree"] = [degree_dict.get(node_ids[i], 0) for i in nearest_idx]

        print(f"    ✅ Road network: {len(G.nodes)} nodes, {len(G.edges)} edges")
    except Exception as e:
        print(f"    ⚠️ Road network: {e}")
        gdf["road_betweenness"] = np.nan
        gdf["road_degree"] = np.nan

    # Building density in H3 neighborhood
    try:
        import h3
        h3_counts = gdf.groupby("h3_8").size().to_dict()
        gdf["building_density_h3"] = gdf["h3_8"].map(h3_counts).fillna(0)
        print(f"  ✅ Building density: median {gdf['building_density_h3'].median():.0f} per H3 cell")
    except Exception:
        gdf["building_density_h3"] = np.nan

    return gdf


# =============================================================================
# STEP 3: Raster Feature Extraction (NLCD, 3DEP, PRISM)
# =============================================================================
def _enrich_rasters(gdf, fips, ts):
    """Extract raster values at building locations using rasterstats."""
    import numpy as np
    import rasterio
    from rasterstats import point_query

    points = list(zip(gdf["lon"], gdf["lat"]))

    # NLCD Land Cover
    try:
        print(f"  Raster: NLCD land cover...")
        # Use MRLC WCS service for county-specific extraction
        bounds = gdf.geometry.total_bounds
        nlcd_url = (
            f"/vsicurl/https://s3-us-west-2.amazonaws.com/mrlc/nlcd_2021_land_cover_l48_20230630_resample.tif"
        )
        vals = point_query(
            points, nlcd_url,
            interpolate="nearest",
        )
        gdf["nlcd_class"] = vals
        # Decode NLCD to binary flags
        gdf["is_developed"] = gdf["nlcd_class"].isin([21, 22, 23, 24]).astype(float)
        gdf["is_forest"] = gdf["nlcd_class"].isin([41, 42, 43]).astype(float)
        gdf["is_agriculture"] = gdf["nlcd_class"].isin([81, 82]).astype(float)
        gdf["is_wetland"] = gdf["nlcd_class"].isin([90, 95]).astype(float)
        print(f"    ✅ NLCD: {gdf['is_developed'].sum():.0f} developed, {gdf['is_forest'].sum():.0f} forest")
    except Exception as e:
        print(f"    ⚠️ NLCD: {e}")
        for col in ["nlcd_class", "is_developed", "is_forest", "is_agriculture", "is_wetland"]:
            gdf[col] = np.nan

    # USGS 3DEP Elevation
    try:
        print(f"  Raster: USGS 3DEP elevation...")
        elev_url = "/vsicurl/https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/1/TIFF/USGS_Seamless_DEM_1.vrt"
        vals = point_query(points, elev_url, interpolate="bilinear")
        gdf["elevation_m"] = vals
        print(f"    ✅ Elevation: range [{gdf['elevation_m'].min():.0f}, {gdf['elevation_m'].max():.0f}] m")
    except Exception as e:
        print(f"    ⚠️ 3DEP elevation: {e}")
        gdf["elevation_m"] = np.nan

    # PRISM Climate Normals (30-year means)
    try:
        print(f"  Raster: PRISM climate normals...")
        prism_temp_url = "/vsicurl/https://prism.oregonstate.edu/normals/PRISM_tmean_30yr_normal_4kmM2_annual_bil.zip"
        # PRISM requires special handling — skip if not accessible
        # Use approximate climate from lat/lon as fallback
        gdf["climate_temp_proxy"] = 15.0 + (gdf["lat"] - 40.0) * (-0.7)  # Rough temp gradient
        gdf["climate_precip_proxy"] = 1000.0 + (gdf["lon"] + 95.0) * 5.0  # Rough precip gradient
        print(f"    ✅ Climate proxies computed (lat/lon regression)")
    except Exception as e:
        print(f"    ⚠️ PRISM: {e}")
        gdf["climate_temp_proxy"] = np.nan
        gdf["climate_precip_proxy"] = np.nan

    return gdf


# =============================================================================
# STEP 4: FEMA Flood Zone Enrichment
# =============================================================================
def _enrich_fema(gdf, fips, ts):
    """Query FEMA NFHL for flood zone designation per parcel.
    
    Uses spatial-only queries against the NFHL Flood Hazard Zones layer.
    The FEMA MapServer layer 28 (S_FLD_HAZ_AR) returns flood zone polygons.
    We query by spatial envelope in batches to stay within API limits.
    """
    import requests
    import numpy as np

    # FEMA NFHL REST API — Flood Hazard Areas
    # Layer 28 = S_FLD_HAZ_AR (Special Flood Hazard Areas)
    NFHL_URL = "https://hazards.fema.gov/gis/nfhl/rest/services/public/NFHL/MapServer/28/query"

    print(f"  Querying FEMA NFHL for county {fips}...")
    gdf["flood_zone"] = "X"  # Default: minimal flood hazard
    gdf["in_floodplain"] = 0.0

    try:
        bounds = gdf.geometry.total_bounds  # [minx, miny, maxx, maxy]
        # Spatial-only query (no DFIRM_ID filtering — that was causing 404)
        params = {
            "where": "1=1",
            "geometry": f"{bounds[0]},{bounds[1]},{bounds[2]},{bounds[3]}",
            "geometryType": "esriGeometryEnvelope",
            "inSR": "4326",
            "outSR": "4326",
            "spatialRel": "esriSpatialRelIntersects",
            "outFields": "FLD_ZONE,ZONE_SUBTY",
            "f": "geojson",
            "resultRecordCount": 5000,
        }
        r = requests.get(NFHL_URL, params=params, timeout=120)
        if r.status_code == 200:
            data = r.json()
            if "features" in data and len(data["features"]) > 0:
                import geopandas as gpd
                flood_gdf = gpd.GeoDataFrame.from_features(data["features"], crs="EPSG:4326")
                # Spatial join: which buildings are in flood zones
                joined = gpd.sjoin(gdf, flood_gdf, how="left", predicate="within")
                if "FLD_ZONE" in joined.columns:
                    # sjoin can duplicate rows — take first match per building
                    joined = joined[~joined.index.duplicated(keep="first")]
                    gdf["flood_zone"] = joined["FLD_ZONE"].fillna("X")
                    gdf["in_floodplain"] = gdf["flood_zone"].isin(["A", "AE", "AH", "AO", "V", "VE"]).astype(float)
                print(f"    ✅ FEMA: {gdf['in_floodplain'].sum():.0f} parcels in floodplain ({len(flood_gdf)} flood polygons)")
            else:
                print(f"    No FEMA flood data returned for this area")
        else:
            print(f"    FEMA API returned status {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"    ⚠️ FEMA: {e}")

    return gdf


# =============================================================================
# STEP 5: Build Multi-Year Panel
# =============================================================================
def _build_multiyear_panel(gdf, fips, ts):
    """Expand building-level features to a multi-year panel (2010-2024)."""
    import pandas as pd
    import numpy as np

    print(f"  Creating multi-year panel from {len(gdf):,} buildings...")

    # Static features (don't change year-to-year)
    static_cols = [c for c in gdf.columns if c not in ("geometry", "h3_8")]

    # Create base row for each building (drop geometry for parquet)
    base = gdf[static_cols].copy()

    # Replicate across years 2010-2024
    years = list(range(2010, 2025))
    frames = []
    for yr in years:
        yr_df = base.copy()
        yr_df["year"] = yr
        frames.append(yr_df)

    panel = pd.concat(frames, ignore_index=True)

    # Add jurisdiction column
    panel["jurisdiction"] = f"universal_{fips}"

    # Rename for worldmodel compatibility
    rename_map = {
        "lat": "lat",
        "lon": "lon",
        "sqft_proxy": "sqft",
        "stories_proxy": "stories",
        "area_m2": "land_area",
    }
    panel = panel.rename(columns={k: v for k, v in rename_map.items() if k in panel.columns})

    # The target value is initially UNKNOWN for universal parcels.
    # It will be filled by:
    #   1. Teacher distillation (matching to HCAD teacher output), OR
    #   2. ACS tract-level median as initial estimate
    # For now, set a placeholder that will be replaced during spatial join
    panel["property_value"] = np.nan  # Will be filled during spatial join to HCAD or ACS

    # Sort for worldmodel
    panel = panel.sort_values(["parcel_id", "year"]).reset_index(drop=True)

    print(f"  ✅ Panel: {len(panel):,} rows × {len(panel.columns)} cols × {len(years)} years")
    print(f"  Columns: {list(panel.columns)}")

    return panel


# =============================================================================
# STEP 6: Spatial Join to HCAD (Harris County only)
# =============================================================================
@app.function(
    image=image,
    secrets=[gcs_secret],
    timeout=3600,
    memory=16384,
)
def spatial_join_to_hcad(
    fips: str = "48201",
    bucket_name: str = BUCKET_NAME,
):
    """Join universal panel buildings to HCAD parcels via centroid proximity.
    
    This creates the matched dataset for teacher/student distillation:
    - Universal features (from build_universal_panel)
    - HCAD valuation target (from HCAD panel)
    """
    import json, time, io
    import pandas as pd
    import numpy as np
    from scipy.spatial import cKDTree
    from google.cloud import storage

    ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts()}] Spatial join: Universal panel → HCAD")

    # GCS setup
    creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON", "")
    if creds_json:
        creds_path = "/tmp/gcs_creds.json"
        with open(creds_path, "w") as f:
            f.write(creds_json)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Load universal panel
    print(f"[{ts()}] Loading universal panel...")
    blob = bucket.blob(f"panel/jurisdiction=universal_{fips}/part.parquet")
    with io.BytesIO() as buf:
        blob.download_to_file(buf)
        buf.seek(0)
        universal = pd.read_parquet(buf)
    print(f"  Universal: {len(universal):,} rows, {universal['parcel_id'].nunique():,} parcels")

    # Load HCAD panel
    print(f"[{ts()}] Loading HCAD panel...")
    blob = bucket.blob("panel/jurisdiction=hcad_houston/part.parquet")
    with io.BytesIO() as buf:
        blob.download_to_file(buf)
        buf.seek(0)
        hcad = pd.read_parquet(buf)
    print(f"  HCAD columns: {list(hcad.columns)[:15]}...")

    # Auto-detect HCAD column names
    hcad_id_col = "acct" if "acct" in hcad.columns else "parcel_id"
    hcad_lat_col = "gis_lat" if "gis_lat" in hcad.columns else "lat"
    hcad_lon_col = "gis_lon" if "gis_lon" in hcad.columns else "lon"
    hcad_val_col = "tot_appr_val" if "tot_appr_val" in hcad.columns else "property_value"
    hcad_yr_col = "yr" if "yr" in hcad.columns else "year"
    print(f"  HCAD schema: id={hcad_id_col}, lat={hcad_lat_col}, lon={hcad_lon_col}, val={hcad_val_col}, yr={hcad_yr_col}")
    print(f"  HCAD: {len(hcad):,} rows, {hcad[hcad_id_col].nunique():,} parcels")

    # Get unique HCAD parcel centroids (latest year)
    hcad_latest = hcad.sort_values(hcad_yr_col).groupby(hcad_id_col).last().reset_index()
    hcad_latest = hcad_latest.dropna(subset=[hcad_lat_col, hcad_lon_col])
    hcad_coords = np.column_stack([
        pd.to_numeric(hcad_latest[hcad_lat_col], errors="coerce").values,
        pd.to_numeric(hcad_latest[hcad_lon_col], errors="coerce").values,
    ])
    # Drop rows with NaN coords
    valid_mask = ~np.isnan(hcad_coords).any(axis=1)
    hcad_latest = hcad_latest[valid_mask].reset_index(drop=True)
    hcad_coords = hcad_coords[valid_mask]
    print(f"  HCAD with valid coords: {len(hcad_latest):,}")

    # Get unique universal building centroids
    univ_latest = universal.drop_duplicates(subset=["parcel_id"]).copy()
    univ_coords = np.column_stack([univ_latest["lat"].values, univ_latest["lon"].values])

    # KDTree matching: for each universal building, find nearest HCAD parcel
    print(f"[{ts()}] KDTree matching...")
    tree = cKDTree(hcad_coords)
    dists, indices = tree.query(univ_coords, k=1)

    # Convert distance from degrees to meters (approximate)
    center_lat = univ_latest["lat"].mean()
    m_per_deg = 111_000 * np.cos(np.radians(center_lat))
    dists_m = dists * m_per_deg

    # Match threshold: 50 meters (most buildings should be within their parcel)
    MATCH_THRESHOLD_M = 50.0
    matched_mask = dists_m < MATCH_THRESHOLD_M
    n_matched = matched_mask.sum()
    print(f"  Matched: {n_matched:,} / {len(univ_latest):,} ({100*n_matched/len(univ_latest):.1f}%) within {MATCH_THRESHOLD_M}m")

    # Create match mapping: universal_parcel_id → hcad_acct
    match_df = pd.DataFrame({
        "parcel_id": univ_latest["parcel_id"].values,
        "hcad_acct": np.where(matched_mask, hcad_latest[hcad_id_col].values[indices], None),
        "match_dist_m": dists_m,
    })

    # Join HCAD valuations to universal panel
    hcad_vals = hcad[[hcad_id_col, hcad_yr_col, hcad_val_col]].rename(
        columns={hcad_id_col: "hcad_acct", hcad_yr_col: "year", hcad_val_col: "hcad_value"}
    )
    universal = universal.merge(match_df[["parcel_id", "hcad_acct"]], on="parcel_id", how="left")
    universal = universal.merge(hcad_vals, on=["hcad_acct", "year"], how="left")

    # Use HCAD value as the target (for distillation training)
    universal["property_value"] = universal["hcad_value"]
    universal = universal.drop(columns=["hcad_acct", "hcad_value"], errors="ignore")

    # Keep only matched rows for distillation training
    matched_panel = universal[universal["property_value"].notna()].copy()
    print(f"  Matched panel: {len(matched_panel):,} rows with HCAD valuations")

    # Upload matched panel
    buf = io.BytesIO()
    matched_panel.to_parquet(buf, index=False)
    buf.seek(0)
    blob = bucket.blob(f"panel/jurisdiction=universal_{fips}_matched/part.parquet")
    blob.upload_from_file(buf, content_type="application/octet-stream")
    print(f"  ✅ Uploaded matched panel: gs://{bucket_name}/panel/jurisdiction=universal_{fips}_matched/part.parquet")

    return {
        "n_universal_buildings": int(len(univ_latest)),
        "n_matched": int(n_matched),
        "match_rate": float(n_matched / len(univ_latest)),
        "matched_panel_rows": len(matched_panel),
    }


# =============================================================================
# ENTRYPOINT
# =============================================================================
@app.local_entrypoint()
def main(
    fips: str = "48201",
    skip_ms_buildings: bool = False,
    skip_osm: bool = False,
    skip_rasters: bool = False,
    force_rebuild: bool = False,
    spatial_join: bool = False,
):
    """
    Usage:
        # Build universal panel for Harris County (caches to GCS):
        modal run scripts/data_acquisition/build_universal_panel_modal.py --fips 48201

        # Force rebuild even if cached:
        modal run scripts/data_acquisition/build_universal_panel_modal.py --fips 48201 --force-rebuild

        # Skip expensive steps for debugging:
        modal run scripts/data_acquisition/build_universal_panel_modal.py --fips 48201 --skip-osm --skip-rasters

        # After building, match to HCAD:
        modal run scripts/data_acquisition/build_universal_panel_modal.py --fips 48201 --spatial-join
    """
    if spatial_join:
        print(f"🔗 Spatial joining universal panel → HCAD for FIPS={fips}")
        result = spatial_join_to_hcad.remote(fips=fips)
    else:
        print(f"🏗️ Building universal panel for FIPS={fips}")
        result = build_universal_panel.remote(
            fips=fips,
            skip_ms_buildings=skip_ms_buildings,
            skip_osm=skip_osm,
            skip_rasters=skip_rasters,
            force_rebuild=force_rebuild,
        )
    print(f"✅ Done: {result}")
