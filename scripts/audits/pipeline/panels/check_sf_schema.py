"""Check SF raw data columns for any market value field."""
import modal, os

app = modal.App("check-sf-schema")
image = modal.Image.debian_slim(python_version="3.11").pip_install("google-cloud-storage", "pandas", "pyarrow")
gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])

@app.function(image=image, secrets=[gcs_secret], timeout=120)
def check() -> str:
    import json, io, pandas as pd
    from google.cloud import storage
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")

    results = {}
    
    # Check SF raw data columns (first chunk)
    blob = bucket.blob("sf/secured_roll/chunk_0001.csv")
    content = blob.download_as_bytes()
    df = pd.read_csv(io.BytesIO(content), nrows=100, low_memory=False)
    
    results["sf_raw_columns"] = list(df.columns)
    results["sf_raw_sample"] = df.head(3).to_dict(orient="records")
    
    # Look for any column that might be market value
    value_cols = [c for c in df.columns if any(kw in c.lower() for kw in 
        ["value", "market", "price", "sale", "apprais", "assess", "worth", "fmv", "fair"])]
    results["sf_value_columns"] = value_cols
    
    # Show sample values for value columns
    if value_cols:
        results["sf_value_samples"] = df[value_cols].head(10).to_dict(orient="records")
    
    # Also check the SF panel columns
    blob2 = bucket.blob("panel/jurisdiction=sf_ca/part.parquet")
    content2 = blob2.download_as_bytes()
    df2 = pd.read_parquet(io.BytesIO(content2), columns=None)
    results["sf_panel_columns"] = list(df2.columns)
    results["sf_panel_rows"] = len(df2)
    
    # Check for value columns in panel
    panel_value_cols = [c for c in df2.columns if any(kw in c.lower() for kw in 
        ["value", "market", "price", "sale", "apprais", "assess", "worth"])]
    results["sf_panel_value_columns"] = panel_value_cols
    if panel_value_cols:
        results["sf_panel_value_stats"] = {c: {
            "non_null": int(df2[c].notna().sum()),
            "pct_non_null": round(float(df2[c].notna().mean() * 100), 1),
            "mean": round(float(df2[c].mean()), 2) if df2[c].dtype in ['float64', 'int64', 'float32', 'int32'] else None,
            "sample": str(df2[c].dropna().head(3).tolist()),
        } for c in panel_value_cols}
    
    return json.dumps(results, indent=2, default=str)

@app.local_entrypoint()
def main():
    result = check.remote()
    with open("scripts/logs/sf_schema_audit.json", "w") as f:
        f.write(result)
    print(result)
