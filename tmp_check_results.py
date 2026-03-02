"""Check forecast cache parquets via the GCS gsutil CLI."""
import subprocess, sys

paths = [
    "gs://properlytic-raw-data/entity_backtest_counterfactual/cache/forecast_lookup_o2021.parquet",
    "gs://properlytic-raw-data/entity_backtest_counterfactual/cache/forecast_lookup_o2022.parquet",
    "gs://properlytic-raw-data/entity_backtest_counterfactual/cache/forecast_lookup_o2023.parquet",
    "gs://properlytic-raw-data/entity_backtest_counterfactual/results_v2.parquet",
]

for p in paths:
    result = subprocess.run(["gsutil", "du", "-h", p], capture_output=True, text=True)
    if result.returncode == 0:
        print(result.stdout.strip())
    else:
        print(f"NOT FOUND: {p.split('/')[-1]}")
