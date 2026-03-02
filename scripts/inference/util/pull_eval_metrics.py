"""Pull eval metrics from W&B and save to eval_results.txt."""
import wandb
import sys

api = wandb.Api()
runs = api.runs("dhardestylewis-columbia-university/homecastr",
                filters={"group": "eval_v11_sf_ca"},
                order="-created_at")

METRICS = ["rho", "mdPIT", "avgFW", "MdAE", "MAE", "MAPE",
           "coverage", "bias", "pct_neg", "med_growth", "rho_p"]

out = open("scripts/eval_results.txt", "w", encoding="utf-8")

for r in list(runs)[:7]:
    s = dict(r.summary)
    origin = r.config.get("origin", "?")
    state = r.state
    out.write(f"\n=== {r.name} (origin={origin}, state={state}) ===\n")
    found = False
    for h in range(1, 6):
        rho = s.get(f"eval/rho/o{origin}_h{h}_ALL")
        if rho is None:
            continue
        found = True
        line = f"  h{h}:"
        for k in METRICS:
            v = s.get(f"eval/{k}/o{origin}_h{h}_ALL")
            if v is not None:
                if k == "rho_p":
                    line += f"  {k}={v:.2e}"
                elif abs(v) > 100:
                    line += f"  {k}={v:.0f}"
                else:
                    line += f"  {k}={v:.3f}"
            else:
                line += f"  {k}=N/A"
        out.write(line + "\n")
    if not found:
        out.write("  (no metrics found)\n")

out.close()
print("Done - saved to scripts/eval_results.txt")
