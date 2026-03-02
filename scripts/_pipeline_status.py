"""Comprehensive pipeline status check across all jurisdictions."""
from google.cloud import storage
import json

creds = json.load(open("scripts/.gcs-key.json"))
c = storage.Client.from_service_account_info(creds)
b = c.bucket("properlytic-raw-data")

juris = ["nyc", "philly", "vancouver", "uk_ppd", "seattle_wa", "france_dvf",
         "cook_county", "sf", "hcad_houston"]

print("=" * 70)
print("PIPELINE STATUS — ALL JURISDICTIONS")
print("=" * 70)

for j in juris:
    panel = len(list(b.list_blobs(prefix=f"panel/jurisdiction={j}/"))) > 0
    ckpts = [x.name.split("/")[-1] for x in b.list_blobs(prefix=f"checkpoints/{j}/") if x.name.endswith(".pt")]
    config = any(x.name.endswith("run_config.json") for x in b.list_blobs(prefix=f"checkpoints/{j}/"))
    inf_out = len(list(b.list_blobs(prefix=f"inference_output/{j}/", max_results=3)))

    status = []
    if panel: status.append("PANEL ✅")
    else: status.append("panel ❌")
    if ckpts: status.append(f"CKPT ✅ ({len(ckpts)} .pt)")
    elif config: status.append("config-only ⚠️ (no .pt)")
    else: status.append("ckpt ❌")
    if inf_out: status.append("INFERENCE ✅")
    else: status.append("inference ❌")

    print(f"  {j:20s}  {' | '.join(status)}")

# Also check raw data availability
print("\n=== Raw Data Blobs ===")
for prefix in ["raw_data/nyc/", "raw_data/philly/", "raw_data/vancouver/",
               "raw_data/uk_ppd/", "raw_data/seattle/", "raw_data/france/",
               "raw_data/cook_county/"]:
    blobs = list(b.list_blobs(prefix=prefix, max_results=3))
    if blobs:
        total = sum(1 for _ in b.list_blobs(prefix=prefix))
        print(f"  {prefix}: {total} files")
    else:
        print(f"  {prefix}: EMPTY")

print("\nDone!")
