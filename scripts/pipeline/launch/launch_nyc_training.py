import subprocess
import sys
import time
import os

JURISDICTIONS = ["nyc"]
ORIGINS = [2020, 2021, 2022, 2023, 2024, 2025, 2026]

def launch(jurisdiction, origin):
    cmd = [
        sys.executable, "-m", "modal", "run",
        "scripts/pipeline/training/train_modal.py",
        "--jurisdiction", jurisdiction,
        "--origin", str(origin),
    ]
    print(f"  Launching: {jurisdiction} origin={origin}")
    # Run detached so they queue up in Modal natively without holding PowerShell
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        encoding="utf-8", errors="replace"
    )
    return proc

if __name__ == "__main__":
    procs = []
    
    for jur in JURISDICTIONS:
        for origin in ORIGINS:
            p = launch(jur, origin)
            procs.append((jur, origin, p))
            time.sleep(2)  # stagger slightly to avoid API throttle
            
    print(f"\nLaunched {len(procs)} training runs for {JURISDICTIONS[0]}. Waiting for completion...")
    
    for jur, origin, p in procs:
        stdout, stderr = p.communicate()
        status = "OK" if p.returncode == 0 else f"FAIL (rc={p.returncode})"
        print(f"  [{status}] {jur} origin={origin}")
        if p.returncode != 0:
            err_tail = stderr[-500:] if stderr else "(no stderr)"
            print(f"    stderr: {err_tail}")

    print("\nAll NYC runs complete.")
