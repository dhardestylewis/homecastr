"""
Build Florida DOR Panel via Modal.
==================================
Fans out across the 67 Florida counties to build longitudinal (2002-2025) property panels.
Reads NAL and SDF CSVs directly from Zip archives in GCS using Polars.

Usage:
  python -m modal run --detach scripts/data_acquisition/build_florida_dor_panel_modal.py
"""

import modal
import os
import io

app = modal.App("florida-dor-panel-build")

# Polars is extremely memory efficient and fast for parsing CSVs
image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install("google-cloud-storage", "polars", "pyarrow")
)

GCS_BUCKET = "properlytic-raw-data"
DOR_PREFIX = "florida_dor"
OUT_PREFIX = "panels/florida_dor/counties"

# DOR uses county numbers 11 through 77
COUNTIES = list(range(11, 78))
YEARS = list(range(2002, 2026))

# Known NAL/SDF suffixes. We'll search for the latest available (S, then P, then F)
SUFFIXES = ["S", "P", "F"]

gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])


def get_latest_blob(bucket, dataset: str, year: int) -> str:
    """Find the latest available zip file for a given year (prefers S > P > F)."""
    for sfx in SUFFIXES:
        blob_name = f"{DOR_PREFIX}/{dataset}/{year}{sfx}.zip"
        if bucket.blob(blob_name).exists():
            return blob_name
    return None


@app.function(
    image=image,
    secrets=[gcs_secret],
    timeout=1800,  # 30 mins per county (handling 24 years of data)
    max_containers=67,  # One per county
    memory=4096,  # 4GB RAM should be plenty for streaming one county
)
def build_county_panel(county_id: int) -> dict:
    import json
    import tempfile
    import zipfile
    import polars as pl
    from google.cloud import storage

    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket(GCS_BUCKET)

    county_str = f"{county_id:02d}"
    print(f"[{county_str}] Starting panel build...")

    out_blob_name = f"{OUT_PREFIX}/county_{county_str}.parquet"
    if bucket.blob(out_blob_name).exists():
        return {"status": "skipped", "county": county_str}

    all_years_dfs = []

    for year in YEARS:
        # NAL (Assessments)
        nal_blob_name = get_latest_blob(bucket, "NAL", year)
        if not nal_blob_name:
            continue

        try:
            # Download Zip to memory/temp
            tmp_zip = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
            bucket.blob(nal_blob_name).download_to_filename(tmp_zip.name)

            with zipfile.ZipFile(tmp_zip.name) as z:
                # Find the CSV for this county
                csv_names = [n for n in z.namelist() if n.endswith('.csv') and f"_{county_str}" in n]
                if not csv_names:
                    os.unlink(tmp_zip.name)
                    continue
                
                # Use Polars to read just the columns we need
                target_csv = csv_names[0]
                with z.open(target_csv) as f:
                    # Read into Polars
                    df = pl.read_csv(f.read(), infer_schema_length=0) # All strings

            os.unlink(tmp_zip.name)
            
            # Map NAL Schema
            # Some older years might have slightly different names, but these are the standard:
            col_map = {
                "CO_NO": "county_id", "PARCEL_ID": "parcel_id", "TAX_YR": "year",
                "JV": "assessed_value", "LND_VAL": "land_value", "IMP_VAL": "improvement_value",
                "TOT_LVG_AREA": "building_area_sqft", "ACT_YR_BLT": "year_built",
                "LND_SQFOOT": "land_area_sqft", "USE_CD": "property_use_code",
                "LAT_DD": "latitude", "LON_DD": "longitude"
            }
            
            # Filter to existing columns and rename
            avail_cols = [c for c in col_map.keys() if c in df.columns]
            rename_map = {c: col_map[c] for c in avail_cols}
            
            df = df.select(avail_cols).rename(rename_map)
            
            # Ensure required types
            if "year" not in df.columns:
                df = df.with_columns(pl.lit(str(year)).alias("year"))
                
            # Create a true parcel_id (county + parcel)
            if "county_id" in df.columns and "parcel_id" in df.columns:
                 df = df.with_columns(
                     pl.concat_str([pl.col("county_id"), pl.col("parcel_id")]).alias("global_parcel_id")
                 )

            # SDF (Sales) - Join if available for this year
            sdf_blob_name = get_latest_blob(bucket, "SDF", year)
            if sdf_blob_name and "parcel_id" in df.columns:
                tmp_sdf = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
                bucket.blob(sdf_blob_name).download_to_filename(tmp_sdf.name)
                
                with zipfile.ZipFile(tmp_sdf.name) as sz:
                    sdf_csvs = [n for n in sz.namelist() if n.endswith('.csv') and f"_{county_str}" in n]
                    if sdf_csvs:
                        with sz.open(sdf_csvs[0]) as sf:
                            sdf_df = pl.read_csv(sf.read(), infer_schema_length=0)
                            if "SALE_PRC1" in sdf_df.columns and "PARCEL_ID" in sdf_df.columns:
                                # We only want the primary sale amount and date
                                sdf_df = sdf_df.select(["PARCEL_ID", "SALE_PRC1", "SALE_MO1", "SALE_YR1"])
                                sdf_df = sdf_df.rename({"PARCEL_ID": "parcel_id", "SALE_PRC1": "sale_price"})
                                # Join
                                df = df.join(sdf_df, on="parcel_id", how="left")
                os.unlink(tmp_sdf.name)

            all_years_dfs.append(df)
            print(f"[{county_str}] Processed {year} ({len(df)} rows)")

        except Exception as e:
            print(f"[{county_str}] Error on year {year}: {e}")
            if os.path.exists(tmp_zip.name):
                os.unlink(tmp_zip.name)

    if not all_years_dfs:
        return {"status": "error", "county": county_str, "reason": "No data extracted"}

    # Concatenate all years
    print(f"[{county_str}] Concatenating {len(all_years_dfs)} years...")
    try:
        # Some columns might be missing in older years, use diagonal concat
        final_df = pl.concat(all_years_dfs, how="diagonal_relaxed")
        
        # Cast to proper numeric types (infer_schema_length=0 reads everything as Utf8)
        INT_COLS = {"year", "year_built", "county_id"}
        FLOAT_COLS = {"assessed_value", "land_value", "improvement_value",
                      "building_area_sqft", "land_area_sqft", "latitude", "longitude",
                      "sale_price"}
        cast_exprs = []
        for col in final_df.columns:
            if col in INT_COLS and final_df[col].dtype == pl.Utf8:
                cast_exprs.append(pl.col(col).cast(pl.Int32, strict=False))
            elif col in FLOAT_COLS and final_df[col].dtype == pl.Utf8:
                cast_exprs.append(pl.col(col).cast(pl.Float64, strict=False))
        if cast_exprs:
            final_df = final_df.with_columns(cast_exprs)

        # Write to Parquet in memory and upload
        print(f"[{county_str}] Uploading {len(final_df)} total rows to GCS...")
        parquet_buffer = io.BytesIO()
        final_df.write_parquet(parquet_buffer)
        parquet_buffer.seek(0)
        
        bucket.blob(out_blob_name).upload_from_file(parquet_buffer, content_type="application/octet-stream")
        
        return {"status": "ok", "county": county_str, "rows": len(final_df)}

    except Exception as e:
        return {"status": "error", "county": county_str, "reason": str(e)}


@app.local_entrypoint()
def main():
    print(f"Fanning out to {len(COUNTIES)} counties...")
    
    stats = {"ok": 0, "skipped": 0, "error": 0, "total_rows": 0}
    
    for result in build_county_panel.map(COUNTIES):
        st = result["status"]
        stats[st] = stats.get(st, 0) + 1
        
        if st == "ok":
            rows = result["rows"]
            stats["total_rows"] += rows
            print(f"✅ County {result['county']} built: {rows:,} rows")
        elif st == "skipped":
            print(f"⏭️  County {result['county']} already exists")
        else:
            print(f"❌ County {result['county']} failed: {result.get('reason')}")
            
    print("\n" + "="*40)
    print(f"Florida Panel Build Complete")
    print(f"Total Rows Processed: {stats.get('total_rows', 0):,}")
    print(f"Success: {stats['ok']} | Skipped: {stats['skipped']} | Error: {stats['error']}")
    print("="*40)
