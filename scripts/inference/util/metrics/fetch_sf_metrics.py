"""Fetch SF eval metrics from W&B for all origins and horizons."""
import modal, os

app = modal.App("fetch-sf-metrics")
image = modal.Image.debian_slim(python_version="3.11").pip_install("wandb")
wb_secret = modal.Secret.from_name("wandb-secret")

@app.function(image=image, secrets=[wb_secret], timeout=120)
def fetch() -> str:
    import wandb, json
    api = wandb.Api()
    
    results = {}
    # Check all SF eval runs
    runs = api.runs("dhardestylewis-columbia-university/homecastr",
                    filters={"display_name": {"$regex": "eval_v11_sf_ca_.*"}})
    
    for run in runs:
        name = run.name
        summary = dict(run.summary)
        # Extract eval metrics
        metrics = {}
        for k, v in summary.items():
            if k.startswith("eval/") and isinstance(v, (int, float)):
                metrics[k] = round(v, 4) if isinstance(v, float) else v
        if metrics:
            results[name] = metrics
    
    return json.dumps(results, indent=2)

@app.local_entrypoint()
def main():
    result = fetch.remote()
    with open("scripts/logs/sf_eval_all_metrics.json", "w") as f:
        f.write(result)
    print("Saved to scripts/logs/sf_eval_all_metrics.json")
    # Print summary
    import json
    data = json.loads(result)
    for run_name, metrics in sorted(data.items()):
        print(f"\n=== {run_name} ===")
        # Group by horizon
        bias_keys = sorted([k for k in metrics if "bias" in k.lower()])
        mdae_keys = sorted([k for k in metrics if "mdae" in k.lower() or "MdAE" in k])
        covg_keys = sorted([k for k in metrics if "covg" in k.lower() or "Covg" in k])
        rho_keys = sorted([k for k in metrics if k.startswith("eval/rho/") and "_ALL" in k])
        for k in rho_keys + bias_keys + mdae_keys + covg_keys:
            print(f"  {k}: {metrics[k]}")
