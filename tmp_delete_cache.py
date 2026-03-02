"""Delete stale GCS forecast cache parquets (again)."""
from google.cloud import storage
import json, os

# Use GOOGLE_APPLICATION_CREDENTIALS_JSON env var if set, otherwise try ADC
creds_json_str = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
if creds_json_str:
    client = storage.Client.from_service_account_info(json.loads(creds_json_str))
elif os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
    client = storage.Client()
else:
    # Try loading from a common local path
    for p in [
        os.path.expanduser("~/.config/gcloud/application_default_credentials.json"),
        "properlytic-raw-data-key.json",
    ]:
        if os.path.exists(p):
            with open(p) as f:
                info = json.load(f)
            if info.get("type") == "service_account":
                client = storage.Client.from_service_account_info(info)
            else:
                client = storage.Client()
            break
    else:
        client = storage.Client()  # falls back to ADC

bucket = client.bucket("properlytic-raw-data")

to_delete = [
    "entity_backtest_counterfactual/cache/forecast_lookup_o2021.parquet",
    "entity_backtest_counterfactual/cache/forecast_lookup_o2022.parquet",
    "entity_backtest_counterfactual/cache/forecast_lookup_o2023.parquet",
]

for path in to_delete:
    blob = bucket.blob(path)
    if blob.exists():
        blob.delete()
        print(f"Deleted: {path}")
    else:
        print(f"Not found (OK): {path}")

print("Done.")
