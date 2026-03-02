"""Check acct format in forecast cache vs HCAD panel."""
import modal, os, json

app = modal.App("backtest-acct-check")
image = modal.Image.debian_slim(python_version="3.11").pip_install(
    "google-cloud-storage", "pandas", "pyarrow"
)
gcs_secret = modal.Secret.from_name("gcs-creds")

@app.function(image=image, secrets=[gcs_secret], timeout=180, memory=8192)
def check_acct_format():
    import io, pandas as pd
    from google.cloud import storage
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")

    # Load forecast cache for 2021
    path = "entity_backtest_counterfactual/cache/forecast_lookup_o2021.parquet"
    buf = io.BytesIO(); bucket.blob(path).download_to_file(buf); buf.seek(0)
    fc = pd.read_parquet(buf)
    print("=== Forecast cache (origin=2021) ===")
    print(f"  Rows: {len(fc):,}")
    print(f"  Columns: {list(fc.columns)}")
    print(f"  Sample accts: {fc['acct'].head(10).tolist()}")
    print(f"  Acct dtype: {fc['acct'].dtype}")
    print(f"  Acct lengths: {fc['acct'].str.len().value_counts().head(5).to_dict()}")
    print(f"  Origin years: {sorted(fc['origin_year'].astype(int).unique())}")
    print(f"  Forecast years: {sorted(fc['forecast_year'].astype(int).unique())}")

    # Load HCAD panel
    buf2 = io.BytesIO()
    bucket.blob("panel/jurisdiction=hcad_houston/part.parquet").download_to_file(buf2)
    buf2.seek(0)
    panel = pd.read_parquet(buf2, columns=['acct', 'yr'])
    panel = panel[panel['yr'] == 2021]
    print("\n=== HCAD panel (year=2021) ===")
    print(f"  Rows: {len(panel):,}")
    print(f"  Sample accts: {panel['acct'].head(10).tolist()}")
    print(f"  Acct dtype: {panel['acct'].dtype}")
    print(f"  Acct lengths: {panel['acct'].astype(str).str.len().value_counts().head(5).to_dict()}")

    # Overlap check
    panel_accts = set(panel['acct'].astype(str).unique())
    fc_accts = set(fc['acct'].astype(str).unique())
    overlap = panel_accts & fc_accts
    print(f"\n=== Overlap ===")
    print(f"  Panel accts (2021): {len(panel_accts):,}")
    print(f"  Forecast accts (2021): {len(fc_accts):,}")
    print(f"  Overlap: {len(overlap):,}")

@app.local_entrypoint()
def main():
    check_acct_format.remote()
