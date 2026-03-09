import subprocess
import time
import sys

origins = [2020, 2021, 2022, 2023, 2024, 2025]
for o in origins:
    cmd = ["python", "-m", "modal", "run", "--detach", "scripts/inference/eval/eval_modal_sb.py::evaluate_checkpoints", "--jurisdiction", "nyc", "--origin", str(o)]
    print(f"Launching {o}...")
    subprocess.run(cmd)
    time.sleep(1)
print("Done.")
