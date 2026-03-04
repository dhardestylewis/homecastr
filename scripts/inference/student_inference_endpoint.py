"""
Student Model Viewport Inference — On-Demand Endpoint
======================================================
Modal web endpoint that accepts a bounding box and returns building-level
student model predictions for buildings visible in the viewport.

Pipeline:
  1. Query MS Building Footprints for the bbox (QuadKey-based GeoJSON-Lines)
  2. Enrich with universal features (lat/lon proxies for speed)
  3. Load student checkpoint (cached on volume)
  4. Run v11 diffusion inference → p10/p50/p90 per building
  5. Return GeoJSON FeatureCollection with POLYGON geometries

Deploy:
    modal deploy scripts/inference/student_inference_endpoint.py

Test:
    curl -X POST https://<modal-url>/predict \
      -H "Content-Type: application/json" \
      -d '{"bbox": [30.25, -97.78, 30.30, -97.73], "year": 2026}'
"""
import modal
import os
import sys

# Ensure local imports work reliably both locally and in Modal container
if "/root" not in sys.path:
    sys.path.append("/root")
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

try:
    from scripts.inference import worldmodel_inference as wm
except ImportError:
    pass # Will be imported inside the function if top-level fails

app = modal.App("student-viewport-inference")

image = (
    modal.Image.from_registry(
        "pytorch/pytorch:2.3.0-cuda12.1-cudnn8-runtime",
        add_python="3.11",
    )
    .apt_install("libgdal-dev", "libspatialindex-dev")
    .pip_install(
        "google-cloud-storage>=2.10",
        "pandas>=2.0",
        "geopandas>=0.14",
        "numpy>=1.24",
        "pyarrow>=12.0",
        "requests>=2.28",
        "shapely>=2.0",
        "h3>=3.7",
        "fastapi[standard]",
        "wandb",
        "polars",
        "torch",
    )
    .add_local_dir(
        local_path=os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "scripts"),
        remote_path="/root/scripts"
    )
)

gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
ckpt_vol = modal.Volume.from_name("properlytic-checkpoints", create_if_missing=True)

BUCKET_NAME = "properlytic-raw-data"
CKPT_GCS_PATH = "checkpoints/student_universal/ckpt_v11_origin_2024_SF500K.pt"
CKPT_LOCAL = "/checkpoints/student_ckpt.pt"

# MS Buildings dataset links (GeoJSON-Lines format)
MS_BUILDINGS_INDEX = "https://minedbuildings.z5.web.core.windows.net/global-buildings/dataset-links.csv"

# Maximum buildings per request (prevent OOM + keep response fast)
MAX_BUILDINGS = 3000


# =============================================================================
# HELPERS
# =============================================================================

def _setup_gcs():
    """Setup GCS credentials and return client + bucket."""
    from google.cloud import storage
    creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON", "")
    if creds_json:
        with open("/tmp/gcs_creds.json", "w") as f:
            f.write(creds_json)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/gcs_creds.json"
    client = storage.Client()
    return client, client.bucket(BUCKET_NAME)


def _bbox_to_quadkeys(min_lat, min_lng, max_lat, max_lng, zoom=9):
    """Convert a lat/lon bbox to Bing Maps QuadKeys at given zoom."""
    import math
    
    def latlon_to_tile(lat, lon, z):
        lat_rad = math.radians(lat)
        n = 2 ** z
        x = int((lon + 180) / 360 * n)
        y = int((1 - math.log(math.tan(lat_rad) + 1 / math.cos(lat_rad)) / math.pi) / 2 * n)
        return x, y
    
    def tile_to_quadkey(x, y, z):
        qk = ""
        for i in range(z, 0, -1):
            digit = 0
            mask = 1 << (i - 1)
            if x & mask:
                digit += 1
            if y & mask:
                digit += 2
            qk += str(digit)
        return qk
    
    x1, y1 = latlon_to_tile(max_lat, min_lng, zoom)
    x2, y2 = latlon_to_tile(min_lat, max_lng, zoom)
    
    quadkeys = []
    for x in range(min(x1, x2), max(x1, x2) + 1):
        for y in range(min(y1, y2), max(y1, y2) + 1):
            quadkeys.append(tile_to_quadkey(x, y, zoom))
    
    return quadkeys


def _fetch_buildings_for_bbox(min_lat, min_lng, max_lat, max_lng):
    """
    Fetch building footprints with POLYGON geometries within bbox using OSM Overpass API.
    Replaces deprecated MS Buildings dataset blob storage.
    """
    import json
    import requests
    from shapely.geometry import Polygon

    print(f"  Querying OSM buildings for bbox: [{min_lat:.4f}, {min_lng:.4f}] → [{max_lat:.4f}, {max_lng:.4f}]")
    
    # Bounding box for Overpass is (s, w, n, e) -> (min_lat, min_lng, max_lat, max_lng)
    query = f"""[out:json][timeout:25];
(
  way["building"]({min_lat},{min_lng},{max_lat},{max_lng});
);
out geom;
"""
    
    import time
    
    all_buildings = []
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            resp = requests.post("https://overpass-api.de/api/interpreter", data=query, timeout=30)
            
            # If we hit a 504 or 429 and can retry, back off and retry
            if resp.status_code in (504, 429) and attempt < max_retries - 1:
                sleep_s = 2 ** (attempt + 1)
                print(f"  ⚠️ Overpass API {resp.status_code} error. Retrying {attempt+1}/{max_retries} in {sleep_s}s...")
                time.sleep(sleep_s)
                continue
                
            resp.raise_for_status()
            data = resp.json()
            
            for el in data.get("elements", []):
                if el["type"] == "way":
                    coords = [(pt["lon"], pt["lat"]) for pt in el.get("geometry", [])]
                    if len(coords) < 3:
                        continue
                    
                    # Ensure closed polygon
                    if coords[0] != coords[-1]:
                        coords.append(coords[0])
                        
                    tags = el.get("tags", {})
                    levels = tags.get("building:levels", "1")
                    try:
                        stories = max(1, float(levels))
                    except:
                        stories = 1.0
                    
                    try:
                        poly = Polygon(coords)
                        centroid = poly.centroid
                        # Area in sq degrees → approximate sq meters → sq feet
                        area_sqm = poly.area * (111000 ** 2)  # crude deg² to m²
                        sqft = area_sqm * 10.764
                        
                        centroid_lat = centroid.y
                        centroid_lng = centroid.x
                    except:
                        continue
                    
                    height_m = stories * 3.0
                    
                    # We need proper GeoJSON format for the geometry
                    geojson_geom = {
                        "type": "Polygon",
                        "coordinates": [coords]
                    }
                    
                    all_buildings.append({
                        "geometry": geojson_geom,
                        "lat": centroid_lat,
                        "lon": centroid_lng,
                        "sqft": sqft,
                        "stories": float(stories),
                        "height_m": height_m,
                    })
                    
                    if len(all_buildings) >= MAX_BUILDINGS:
                        break
            
            # If we arrive here, parsing worked completely, so break out of the retry loop
            break
                        
        except requests.exceptions.RequestException as e:
            # Catch network/timeout errors specifically to allow retries
            if attempt < max_retries - 1:
                sleep_s = 2 ** (attempt + 1)
                print(f"  ⚠️ Overpass connection error '{e}'. Retrying {attempt+1}/{max_retries} in {sleep_s}s...")
                time.sleep(sleep_s)
                continue
            import traceback
            print(f"  ⚠️ Failed to fetch OSM buildings after {max_retries} attempts: {e}\n{traceback.format_exc()}")
            return [], []
            
        except Exception as e:
            # Non-network error (e.g., json parsing error from a bad response)
            import traceback
            print(f"  ⚠️ Failed to process OSM buildings: {e}\n{traceback.format_exc()}")
            return [], []

    if not all_buildings:
        print(f"  No OSM Buildings found in bbox")
        return [], []

    print(f"  ✅ {len(all_buildings):,} OSM buildings loaded with polygon geometries")
    return all_buildings[:MAX_BUILDINGS], [b["geometry"] for b in all_buildings[:MAX_BUILDINGS]]


def _enrich_features_fast(buildings):
    """
    Fast feature enrichment using lat/lon proxies.
    Returns a pandas DataFrame with all features needed for inference.
    """
    import numpy as np
    import pandas as pd
    import h3
    
    df = pd.DataFrame(buildings)
    n = len(df)
    lats = df["lat"].values
    lons = df["lon"].values
    
    # Land area from building sqft (proxy)
    df["land_area"] = df["sqft"] * 2.5
    
    # OSM distance proxies — add spatial variation based on building density
    h3_ids = [h3.latlng_to_cell(lat, lon, 8) for lat, lon in zip(lats, lons)]
    h3_counts = {}
    for h in h3_ids:
        h3_counts[h] = h3_counts.get(h, 0) + 1
    df["building_density_h3"] = [h3_counts.get(h, 1) for h in h3_ids]
    
    # Scale OSM distances inversely with density (denser = closer amenities)
    density = df["building_density_h3"].values.astype(float)
    density_factor = np.clip(50.0 / np.maximum(density, 1), 0.3, 3.0)
    
    rng = np.random.default_rng(int(abs(lats.mean() * 1e5)) % (2**31))
    osm_defaults = {
        "dist_school_km": 1.2,
        "dist_hospital_km": 5.0,
        "dist_grocery_km": 2.5,
        "dist_park_km": 1.5,
        "dist_transit_km": 3.0,
        "dist_restaurant_km": 1.8,
        "dist_gas_station_km": 2.0,
    }
    for col, default in osm_defaults.items():
        noise = rng.normal(0, default * 0.15, n)
        df[col] = np.maximum(default * density_factor + noise, 0.1)
    
    # Road network proxies — vary with density
    df["road_betweenness"] = 0.001 + rng.exponential(0.001, n) * density_factor
    df["road_degree"] = 3.0 + np.clip(np.log(density + 1), 0, 3) + rng.normal(0, 0.3, n)
    
    # NLCD / land cover
    df["nlcd_class"] = 22
    df["is_developed"] = 1
    df["is_forest"] = 0
    df["is_agriculture"] = 0
    df["is_wetland"] = 0
    
    # Elevation proxy
    df["elevation_m"] = 200 + (lats - 30) * 15 + rng.normal(0, 10, n)
    
    # Climate proxies
    df["climate_temp_proxy"] = 25.0 - (lats - 30) * 0.8
    df["climate_precip_proxy"] = 1200 - (lats - 30) * 20 + lons * 2
    
    # Flood
    df["flood_zone"] = "X"
    df["in_floodplain"] = 0
    
    return df


def _run_student_inference(df, ckpt_path, origin_year=2024, year=2026):
    """
    Run student v11 diffusion inference on enriched buildings using the ACTUAL PyTorch model.
    Returns DataFrame with p10/p50/p90 columns representing the true diffusion density.
    """
    import numpy as np
    import torch
    # We need to make sure the local worldmodel_inference.py is resolvable
    # (Imported at module level as wm)
    
    print(f"  Loading checkpoint: {ckpt_path}")
    device = "cuda" if torch.cuda.is_available() else "cpu"
    ckpt = torch.load(ckpt_path, map_location=device, weights_only=False)
    
    cfg = ckpt.get("cfg", {})
    num_use = ckpt.get("num_use", [])
    cat_use = ckpt.get("cat_use", [])
    
    # Hydrate model components
    H_dim = ckpt.get("target_dim", 5)  # typically H=5
    hist_len = cfg.get("FULL_HIST_LEN", 15)
    
    # Checkpoint specifies exact NUM_DIM and N_CAT at the time of trace
    model_num_dim = len(num_use)
    model_cat_dim = len(cat_use) if "n_cat" not in cfg else cfg["n_cat"]
    
    print(f"  Features: {len(num_use)} numeric, {model_cat_dim} categorical")
    print(f"  Device: {device}. Instantiating PyTorch modules...")
    
    denoiser = wm.create_denoiser_v11(target_dim=H_dim, hist_len=hist_len, num_dim=model_num_dim, n_cat=model_cat_dim).to(device)
    denoiser.load_state_dict(ckpt["model_state_dict"])
    denoiser.eval()
    
    gating = wm.create_gating_network(hist_len=hist_len, num_dim=model_num_dim, n_cat=model_cat_dim).to(device)
    gating.load_state_dict(ckpt["gating_net_state_dict"])
    gating.eval()
    
    token_persistence = wm.create_token_persistence().to(device)
    if "token_persistence_state_dict" in ckpt:
        token_persistence.load_state_dict(ckpt["token_persistence_state_dict"])
    token_persistence.eval()
    
    coh_scale = wm.create_coherence_scale().to(device)
    if "coh_scale_state_dict" in ckpt:
        coh_scale.load_state_dict(ckpt["coh_scale_state_dict"])
    coh_scale.eval()
    
    n = len(df)
    
    # ─── Data Prep ─────────────────────────────────────────────
    # The diffusion pipeline expects arrays:
    # hist_y_b (N, hist_len) — we fill with anchors since we have no history for MS Buildings
    # cur_num_b (N, num_dim)
    # cur_cat_b (N, n_cat)
    # region_id_b (N,)
    
    base_log_val = 11.5  # Approx $100k
    
    cur_num = np.zeros((n, model_num_dim), dtype=np.float32)
    for i, col in enumerate(num_use):
        if col in df.columns:
            cur_num[:, i] = df[col].fillna(0).values.astype(np.float32)
            
    # Hash categoricals 
    cur_cat = np.zeros((n, model_cat_dim), dtype=np.int64)
    if "jurisdiction" in df.columns:
        # e.g., universal hash logic from worldmodel
        for i, val in enumerate(df["jurisdiction"]):
            cur_cat[i, 0] = hash(str(val)) % 10000
    
    region_id = np.zeros(n, dtype=np.int64)
    for i, lat in enumerate(df["lat"]):
        region_id[i] = hash(f"r_{int(lat)}") % 5000
        
    hist_y = np.full((n, hist_len), base_log_val, dtype=np.float32)
    
    # ─── Inference ─────────────────────────────────────────────
    # Step 1: Pre-sample token paths [S, K, H] for spatial coherence
    S = 256  # Scenarios
    K = 8    # Tokens
    Z_tokens = wm.sample_token_paths(K, H_dim, token_persistence, S, device)
    
    # Step 2: Initialize DDIM Scheduler
    sched = wm.Scheduler(steps=20, device=device)
    
    # Override globals temporarily for the sampler
    wm.SAMPLER_X_CLIP = 15.0
    wm.SAMPLER_Z_CLIP = 50.0
    wm.S_BLOCK = S
    wm.DIFF_STEPS_SAMPLE = 20
    wm.SAMPLER_REPORT_BAD_STEP = False
    
    # Step 3: Run the forward diffusion pass!
    print(f"  Running v11 diffusion on {n} buildings X {S} scenarios...")
    
    # deltas: [N, S, H]
    deltas = wm.sample_ddim_v11(
        model=denoiser,
        gating_net=gating,
        sched=sched,
        hist_y_b=hist_y,
        cur_num_b=cur_num,
        cur_cat_b=cur_cat,
        region_id_b=region_id,
        Z_tokens=Z_tokens,
        device=device,
        coh_scale=coh_scale
    )
    
    # Output is predicted log-growth per timestep. Reconstruct price levels:
    y_anchor = np.full((n,), base_log_val, dtype=np.float32)
    y_levels = y_anchor[:, None, None] + np.cumsum(deltas, axis=2)  # [N, S, H]
    price_levels = np.expm1(y_levels)  # Dollar values
    
    # Calculate quantiles across all horizons
    p10_all = np.percentile(price_levels, 10, axis=1)  # [N, H]
    p50_all = np.percentile(price_levels, 50, axis=1)  # [N, H]
    p90_all = np.percentile(price_levels, 90, axis=1)  # [N, H]
    
    # Still keep the single point prediction for the map fill color
    horizon_idx = min(year - origin_year - 1, H_dim - 1)
    if horizon_idx < 0: horizon_idx = 0
    
    prices_at_h = price_levels[:, :, horizon_idx]  # [N, S]
    p10 = np.percentile(prices_at_h, 10, axis=1)
    p50 = np.percentile(prices_at_h, 50, axis=1)
    p90 = np.percentile(prices_at_h, 90, axis=1)
    
    df["p10"] = np.maximum(p10, 10000).astype(int)
    df["p50"] = np.maximum(p50, 15000).astype(int)
    df["p90"] = np.maximum(p90, 20000).astype(int)
    df["horizon_months"] = (horizon_idx + 1) * 12
    
    # Store the arrays directly in the DataFrame as lists
    df["p10_arr"] = [np.maximum(arr, 10000).astype(int).tolist() for arr in p10_all]
    df["p50_arr"] = [np.maximum(arr, 15000).astype(int).tolist() for arr in p50_all]
    df["p90_arr"] = [np.maximum(arr, 20000).astype(int).tolist() for arr in p90_all]
    
    print(f"  ✅ Inference complete: p50 range ${df['p50'].min():,} — ${df['p50'].max():,}")
    
    return df


def _to_geojson(df, geometries):
    """Convert enriched DataFrame to GeoJSON with POLYGON geometries."""
    features = []
    for i, (_, row) in enumerate(df.iterrows()):
        geom = geometries[i] if i < len(geometries) else {
            "type": "Point",
            "coordinates": [float(row["lon"]), float(row["lat"])]
        }
        features.append({
            "type": "Feature",
            "geometry": geom,
            "properties": {
                "id": f"bldg-{i}",
                "p10": int(row.get("p10", 0)),
                "p50": int(row.get("p50", 0)),
                "p90": int(row.get("p90", 0)),
                "p10_arr": row.get("p10_arr", []),
                "p50_arr": row.get("p50_arr", []),
                "p90_arr": row.get("p90_arr", []),
                "sqft": round(float(row.get("sqft", 0))),
                "stories": int(row.get("stories", 1)),
                "h": int(row.get("horizon_months", 12)),
            }
        })
    
    return {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "n_buildings": len(features),
            "source": "student-v11-universal",
        }
    }


# =============================================================================
# WEB ENDPOINT
# =============================================================================

@app.function(
    image=image,
    gpu="T4",
    secrets=[gcs_secret],
    volumes={"/checkpoints": ckpt_vol},
    timeout=120,
    memory=8192,
    allow_concurrent_inputs=10,
    container_idle_timeout=300,
)
@modal.web_endpoint(method="POST")
def predict(data: dict):
    """
    On-demand viewport inference endpoint.
    
    Request body:
        {
            "bbox": [min_lat, min_lng, max_lat, max_lng],
            "year": 2026  // optional, default 2026
        }
    
    Response: GeoJSON FeatureCollection with polygon geometries + p10/p50/p90.
    """
    import time
    ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")
    
    bbox = data.get("bbox", [])
    if len(bbox) != 4:
        return {"error": "bbox must be [min_lat, min_lng, max_lat, max_lng]"}
    
    min_lat, min_lng, max_lat, max_lng = bbox
    year = data.get("year", 2026)
    
    print(f"[{ts()}] Viewport inference request: bbox={bbox}, year={year}")
    t0 = time.time()
    
    try:
        # Import locally inside the function to avoid top-level import errors in Modal
        from scripts.inference import worldmodel_inference as wm
        
        # Setup GCS
        client, bucket = _setup_gcs()
    
        # Step 1: Fetch buildings with polygon geometries
        print(f"[{ts()}] Step 1: Fetching buildings...")
        buildings, geometries = _fetch_buildings_for_bbox(min_lat, min_lng, max_lat, max_lng)
        
        if not buildings:
            return {
                "type": "FeatureCollection", 
                "features": [],
                "metadata": {"n_buildings": 0, "source": "student-v11-universal", "latency_s": round(time.time() - t0, 2)}
            }
        
        # Step 2: Enrich features
        print(f"[{ts()}] Step 2: Enriching features...")
        df = _enrich_features_fast(buildings)
        
        # Step 3: Run inference
        print(f"[{ts()}] Step 3: Running student inference...")
        
        # Ensure checkpoint is available
        if not os.path.exists(CKPT_LOCAL):
            print(f"  Downloading checkpoint from GCS...")
            ckpt_blob = bucket.blob(CKPT_GCS_PATH)
            os.makedirs(os.path.dirname(CKPT_LOCAL), exist_ok=True)
            ckpt_blob.download_to_filename(CKPT_LOCAL)
        
        df = _run_student_inference(df, CKPT_LOCAL, origin_year=2024, year=year)
        
        # Step 4: Return GeoJSON with polygon geometries
        result = _to_geojson(df, geometries)
        
        dt = time.time() - t0
        result["metadata"]["latency_s"] = round(dt, 2)
        print(f"[{ts()}] ✅ Complete: {len(result['features'])} buildings in {dt:.1f}s")
        
        return result

    except Exception as e:
        import traceback
        err_msg = str(e)
        trace = traceback.format_exc()
        print(f"[{ts()}] ❌ ERROR: {err_msg}\n{trace}")
        return {"error": err_msg, "traceback": trace}


# =============================================================================
# HEALTH CHECK
# =============================================================================

@app.function(image=image)
@modal.web_endpoint(method="GET")
def health():
    return {"status": "ok", "model": "student-v11-universal", "version": "2.0"}


# =============================================================================
# LOCAL ENTRYPOINT (for testing)
# =============================================================================

@app.local_entrypoint()
def main():
    """Test the endpoint locally."""
    result = predict.remote({
        "bbox": [30.25, -97.78, 30.30, -97.73],  # Austin, TX
        "year": 2026,
    })
    print(f"Result: {result.get('metadata', {})}")
    if result.get("features"):
        f = result["features"][0]
        print(f"Geometry type: {f['geometry']['type']}")
        print(f"Properties: {f['properties']}")
