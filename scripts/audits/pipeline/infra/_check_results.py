"""Quick check of v3 backtest results."""
from google.cloud import storage
import json

c = storage.Client.from_service_account_info(json.load(open("scripts/.gcs-key.json")))
b = c.bucket("properlytic-raw-data")

# Check entity portfolios
blob = b.blob("entity_backtest/entity_portfolios.json")
d = json.loads(blob.download_as_text())
print(f"Entities: {d['n_entities']:,}")
print(f"Accts: {d['n_accts']:,}")
print(f"Sample: {list(d['entities'].keys())[:5]}")
lens = [len(v) for v in d["entities"].values()]
print(f"Portfolio sizes: min={min(lens)}, median={sorted(lens)[len(lens)//2]}, max={max(lens)}")
print(f"Entities with 10+: {sum(1 for l in lens if l>=10):,}")
print(f"Entities with 50+: {sum(1 for l in lens if l>=50):,}")

# Check all entity_backtest files
print("\n=== All entity_backtest files on GCS ===")
blobs = list(b.list_blobs(prefix="entity_backtest/"))
for x in blobs:
    print(f"  {x.name} ({x.size/1e3:.1f}KB)")
print(f"Total: {len(blobs)} files")

# Check TxGIO uploads
print("\n=== TxGIO files on GCS ===")
for x in b.list_blobs(prefix="txgio/"):
    print(f"  {x.name} ({x.size/1e6:.1f}MB)")
