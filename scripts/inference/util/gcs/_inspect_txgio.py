"""Inspect TxGIO SHP schema by downloading one small county."""
import urllib.request
import tempfile
import os

# Download Andrews County (48003) - small county, ~21MB dbf
base = "https://tnris-data-warehouse.s3.us-east-1.amazonaws.com"
prefix = "LCD/collection/stratmap-2024-land-parcels/shp"
county = "stratmap24-landparcels_48003_andrews_202404"

tmpdir = tempfile.mkdtemp()
for ext in ["shp", "dbf", "shx", "prj"]:
    url = f"{base}/{prefix}/{county}.{ext}"
    dest = os.path.join(tmpdir, f"{county}.{ext}")
    print(f"Downloading .{ext} ...")
    urllib.request.urlretrieve(url, dest)

import geopandas as gpd
gdf = gpd.read_file(os.path.join(tmpdir, f"{county}.shp"))
print(f"\nRows: {len(gdf)}")
print(f"Columns ({len(gdf.columns)}):")
for col in gdf.columns:
    dtype = gdf[col].dtype
    sample = gdf[col].dropna().iloc[0] if not gdf[col].dropna().empty else "N/A"
    if col != "geometry":
        print(f"  {col:30s} {str(dtype):10s} sample: {sample}")

# Show value-related columns specifically
print("\n=== VALUE COLUMNS ===")
val_keywords = ["val", "mkt", "appr", "land", "impr", "tot", "assess", "price", "worth"]
for col in gdf.columns:
    if any(kw in col.lower() for kw in val_keywords):
        try:
            nums = gdf[col].dropna().astype(float)
            if len(nums) > 0:
                print(f"  {col}: min={nums.min():.0f}, max={nums.max():.0f}, mean={nums.mean():.0f}")
        except (ValueError, TypeError):
            print(f"  {col}: (non-numeric) sample={gdf[col].dropna().iloc[0] if len(gdf[col].dropna()) > 0 else 'N/A'}")
