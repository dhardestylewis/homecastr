"""Re-pull W&B runs with full metrics and config inspection."""
import wandb, json

api = wandb.Api()
runs = list(api.runs("dhardestylewis-columbia-university/homecastr", per_page=200))
print(f"Total runs: {len(runs)}")

results = []
for r in runs:
    cfg = dict(r.config) if r.config else {}
    summ = {}
    if r.summary:
        for k, v in dict(r.summary).items():
            if not k.startswith("_") and not isinstance(v, (dict, list)):
                try:
                    json.dumps(v)  # ensure serializable
                    summ[k] = v
                except:
                    summ[k] = str(v)

    results.append({
        "name": r.name,
        "id": r.id,
        "state": r.state,
        "created": str(r.created_at),
        "config": cfg,
        "summary_metrics": summ,
    })

out = r"c:\Users\dhl\data\Projects\Properlytic_UI\v0-properlytic-8v\scripts\logs\wandb_runs.json"
with open(out, "w") as f:
    json.dump(results, f, indent=2, default=str)
print(f"Wrote {len(results)} runs to {out}")

# Print summary table
print("\n=== TRAINING RUNS ===")
for r in results:
    if r["name"].startswith("v11-"):
        sm = r["summary_metrics"]
        # Find best metric keys
        metric_keys = sorted(sm.keys())
        loss = None
        for k in metric_keys:
            if "loss" in k.lower():
                loss = sm[k]
                break
        print(f"  {r['state']:10s} {r['name']:45s} metrics={len(sm):>3} loss={loss}")

print("\n=== EVAL RUNS ===")
for r in results:
    if r["name"].startswith("eval_"):
        sm = r["summary_metrics"]
        print(f"  {r['state']:10s} {r['name']:45s} metrics={len(sm):>3}")

# Show metric keys from first run with metrics
for r in results:
    sm = r["summary_metrics"]
    if len(sm) > 3:
        print(f"\n=== Sample metric keys from '{r['name']}' ===")
        for k in sorted(sm.keys()):
            print(f"  {k}: {sm[k]}")
        break

# Show config keys from first training run
for r in results:
    if r["name"].startswith("v11-") and r["config"]:
        print(f"\n=== Sample config from '{r['name']}' ===")
        for k in sorted(r["config"].keys()):
            print(f"  {k}: {r['config'][k]}")
        break
