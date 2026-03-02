"""Read and sample the cached forecast lookup files"""
import modal

app = modal.App("gcs-read-forecasts")
image = modal.Image.debian_slim(python_version="3.11").pip_install("google-cloud-storage", "pandas", "pyarrow")
gcs_secret = modal.Secret.from_name("gcs-creds")

@app.function(image=image, secrets=[gcs_secret], timeout=120)
def check():
    import json, os, io
    import pandas as pd
    from google.cloud import storage

    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")

    # List all files in entity_backtest_counterfactual recursively
    print("=== ALL FILES in entity_backtest_counterfactual/ ===")
    for b in bucket.list_blobs(prefix="entity_backtest_counterfactual/"):
        print(f"  {b.name}  [{b.size/1e6:.2f}MB]")

    # Try to read the forecast_lookup cache files
    for origin in [2021, 2022, 2023]:
        for path_attempt in [
            f"entity_backtest_counterfactual/cache/forecast_lookup_o{origin}.parquet",
            f"entity_backtest_counterfactual/forecast_lookup_o{origin}.parquet",
        ]:
            blob = bucket.blob(path_attempt)
            if blob.exists():
                buf = io.BytesIO()
                blob.download_to_file(buf)
                buf.seek(0)
                df = pd.read_parquet(buf)
                print(f"\n=== {path_attempt} ===")
                print(f"  Shape: {df.shape}")
                print(f"  Columns: {list(df.columns)}")
                if 'p50' in df.columns:
                    print(f"  p50: min={df['p50'].min():.0f} max={df['p50'].max():.0f} nulls={df['p50'].isna().sum()}")
                if 'forecast_year' in df.columns:
                    print(f"  forecast_years: {sorted(df['forecast_year'].unique())}")
                if 'origin_year' in df.columns:
                    print(f"  origin_years: {sorted(df['origin_year'].unique())}")
                print(f"  Sample:\n{df.head(3).to_string()}")
                break

@app.local_entrypoint()
def main():
    check.remote()
