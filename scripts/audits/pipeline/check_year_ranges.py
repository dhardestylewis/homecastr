"""Check year ranges for SF and HCAD panels on GCS."""
import modal, os

app = modal.App("check-year-ranges")
image = modal.Image.debian_slim(python_version="3.11").pip_install("google-cloud-storage", "polars", "pyarrow")
gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])

@app.function(image=image, secrets=[gcs_secret], timeout=120)
def check() -> str:
    import json, polars as pl
    from google.cloud import storage
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")
    
    results = {}
    for jur in ["sf_ca", "hcad_houston"]:
        blob = bucket.blob(f"panel/jurisdiction={jur}/part.parquet")
        if not blob.exists():
            results[jur] = "NOT FOUND"
            continue
        blob.download_to_filename(f"/tmp/{jur}.parquet")
        df = pl.read_parquet(f"/tmp/{jur}.parquet")
        yr_col = "year" if "year" in df.columns else "yr"
        years = sorted(df[yr_col].drop_nulls().unique().to_list())
        n_parcels = df["parcel_id" if "parcel_id" in df.columns else "acct"].n_unique()
        results[jur] = {
            "rows": len(df),
            "unique_parcels": n_parcels,
            "year_range": f"{min(years)}-{max(years)}",
            "all_years": years,
            "rows_per_year": {int(y): int(df.filter(pl.col(yr_col) == y).height) for y in years[-5:]},
        }
    return json.dumps(results, indent=2)

@app.local_entrypoint()
def main():
    result = check.remote()
    with open("scripts/logs/panel_year_ranges.json", "w") as f:
        f.write(result)
    print(result)
