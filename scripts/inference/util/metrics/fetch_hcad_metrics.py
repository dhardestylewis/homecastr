"""Fetch HCAD eval metrics from W&B."""
import modal, os

app = modal.App("fetch-hcad-metrics")
image = modal.Image.debian_slim(python_version="3.11").pip_install("wandb")
wandb_secret = modal.Secret.from_name("wandb-creds", required_keys=["WANDB_API_KEY"])

@app.function(image=image, secrets=[wandb_secret], timeout=120)
def fetch() -> str:
    import json, wandb
    api = wandb.Api()

    # Get latest HCAD eval runs
    runs = api.runs("dhardestylewis-columbia-university/homecastr",
                    filters={"display_name": {"$regex": "eval_v11_hcad"}},
                    order="-created_at", per_page=10)

    results = {}
    for run in runs:
        name = run.name
        summary = dict(run.summary)
        config = dict(run.config) if run.config else {}
        # Filter to metric keys
        metrics = {k: v for k, v in summary.items()
                   if isinstance(v, (int, float)) and not k.startswith("_")}
        results[name] = {
            "state": run.state,
            "created": str(run.created_at),
            "metrics": metrics,
        }
        print(f"\n{name} ({run.state}):")
        for k, v in sorted(metrics.items()):
            print(f"  {k}: {v}")

    return json.dumps(results, indent=2, default=str)

@app.local_entrypoint()
def main():
    result = fetch.remote()
    with open("scripts/logs/hcad_eval_metrics.json", "w") as f:
        f.write(result)
    print(result)
