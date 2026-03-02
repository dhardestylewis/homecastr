"""Check HCAD master panel columns and King County parcel data availability."""
import requests

# 1. HCAD panel columns via GCS streaming
print("=== HCAD Panel Columns ===")
from google.cloud import storage
import pyarrow.parquet as pq
client = storage.Client()
bucket = client.bucket("properlytic-raw-data")

# Download just enough for schema (need the footer → download last 100KB)
blob = bucket.blob("hcad/hcad_master_panel_2005_2025_leakage_strict_FIXEDYR_WITHGIS.parquet")
size = blob.size
print(f"  File size: {size/1e9:.2f} GB")

# Read schema by downloading parquet metadata only
import tempfile, os
with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
    tmppath = f.name
    blob.download_to_filename(tmppath, start=max(0, size - 500000))  # last 500KB for footer

# Try reading schema from the end
try:
    pf = pq.ParquetFile(tmppath)
    print(f"  Columns: {pf.schema.names}")
    print(f"  Rows: {pf.metadata.num_rows}")
except Exception as e:
    print(f"  Direct read failed: {e}")
    # Fallback: just list what we see
    print("  Falling back to full metadata download...")

os.unlink(tmppath)

# 2. King County Parcels — try OpenData Hub GeoJSON API
print("\n=== King County Parcel Data ===")
# Try the hub API
urls = [
    "https://gisdata.kingcounty.gov/arcgis/rest/services/OpenDataPortal/property__parcel_area/MapServer/2397/query?where=1%3D1&outFields=PIN,MAJOR,MINOR&f=json&resultRecordCount=2&outSR=4326&returnGeometry=true",
    "https://services.arcgis.com/L0BNmVJBGoGg4QKn/arcgis/rest/services/Parcels_for_King_County___parcel_area/FeatureServer/0/query?where=1%3D1&outFields=PIN&f=json&resultRecordCount=3&outSR=4326&returnCentroid=true",
]
for url in urls:
    try:
        r = requests.get(url, timeout=15)
        d = r.json()
        if "features" in d and len(d["features"]) > 0:
            print(f"  ✅ Working URL: {url[:80]}...")
            print(f"  Fields: {[f['name'] for f in d.get('fields', [])]}")
            print(f"  Features: {len(d['features'])}")
            for feat in d["features"][:2]:
                print(f"    {feat['attributes']}")
                if "geometry" in feat:
                    print(f"    Geometry: {feat['geometry']}")
            break
        else:
            print(f"  ❌ {url[:60]}... → {list(d.keys())}")
    except Exception as e:
        print(f"  ❌ {url[:60]}... → {e}")
