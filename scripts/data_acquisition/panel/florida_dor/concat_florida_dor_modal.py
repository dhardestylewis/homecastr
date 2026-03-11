"""
Concatenate Florida DOR county parquets into a single statewide panel.
"""
import modal

app = modal.App("florida-dor-panel-concat")

gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])

image = modal.Image.debian_slim(python_version="3.11").pip_install(
    "google-cloud-storage", "polars", "pyarrow"
)


@app.function(
    image=image,
    secrets=[gcs_secret],
    timeout=1800,
    memory=16384,  # 16GB RAM for concatenating 67 county files
)
def concat_counties():
    import json
    import os
    import io
    import polars as pl
    from google.cloud import storage

    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")

    print("Finding county parquets...")
    blobs = list(bucket.list_blobs(prefix="panels/florida_dor/counties/county_"))
    print(f"Found {len(blobs)} county parquets.")

    dfs = []
    total_size = 0
    for b in blobs:
        print(f"  Downloading {b.name} ({(b.size or 0)/1e6:.1f}MB)")
        data = b.download_as_bytes()
        df = pl.read_parquet(data)
        dfs.append(df)
        total_size += len(df)

    print(f"Concatenating {len(dfs)} dataframes ({total_size:,} total rows)...")
    final_df = pl.concat(dfs, how="diagonal_relaxed")

    print(f"Writing to GCS as florida_dor_panel.parquet...")
    out_buf = io.BytesIO()
    final_df.write_parquet(out_buf)
    out_buf.seek(0)

    bucket.blob("panels/florida_dor_panel.parquet").upload_from_file(
        out_buf, content_type="application/octet-stream"
    )
    size_mb = out_buf.tell() / 1e6
    print(f"Done! {total_size:,} rows, {size_mb:.0f} MB")
    return total_size


@app.local_entrypoint()
def main():
    rows = concat_counties.remote()
    print(f"Successfully concatenated Florida DOR Panel: {rows:,} rows")
