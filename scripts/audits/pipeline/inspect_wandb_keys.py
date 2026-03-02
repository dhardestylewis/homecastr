"""Inspect W&B metric keys — what do training + eval runs actually report?"""
import wandb, json

api = wandb.Api()
runs = list(api.runs("dhardestylewis-columbia-university/homecastr", per_page=200))

# Find richest training run and richest eval run
best_train = max((r for r in runs if r.name.startswith("v11-")), key=lambda r: len(dict(r.summary or {})), default=None)
best_eval = max((r for r in runs if r.name.startswith("eval_")), key=lambda r: len(dict(r.summary or {})), default=None)

for label, run in [("TRAINING", best_train), ("EVAL", best_eval)]:
    if not run:
        continue
    summ = {k: v for k, v in dict(run.summary).items() if not k.startswith("_") and not isinstance(v, (dict, list))}
    cfg = dict(run.config) if run.config else {}
    print(f"\n{'='*60}")
    print(f"{label}: {run.name} ({run.state}, {len(summ)} metrics)")
    print(f"Config: {json.dumps(cfg, indent=2, default=str)}")
    print(f"\nAll summary metrics ({len(summ)}):")
    for k in sorted(summ.keys()):
        v = summ[k]
        if isinstance(v, float):
            print(f"  {k}: {v:.6f}")
        else:
            print(f"  {k}: {v}")
