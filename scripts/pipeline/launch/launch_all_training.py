"""
Batch launcher: kicks off all jurisdiction x origin training on Modal.
Each run is independent (separate Modal function call).

Usage:
    python scripts/launch_all_training.py
"""
import subprocess
import sys
import time

JURISDICTIONS = [
    "hcad_houston",   # Houston / Harris County
    # "sf_ca",        # Already trained — skip
    "cook_county_il", # Chicago / Cook County
    "nyc",            # New York City
    "philly",         # Philadelphia
]

ORIGINS = [2019, 2020, 2021, 2022, 2023, 2024]

# Already running or completed — skip these
SKIP = {("hcad_houston", 2019)}

# Grand panel (all jurisdictions combined)
GRAND = "all"

def launch(jurisdiction, origin):
    cmd = [
        sys.executable, "-m", "modal", "run",
        "scripts/train_modal.py",
        "--jurisdiction", jurisdiction,
        "--origin", str(origin),
    ]
    print(f"  Launching: {jurisdiction} origin={origin}")
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        encoding="utf-8", errors="replace"
    )
    return proc

if __name__ == "__main__":
    procs = []

    # Launch per-jurisdiction training
    for jur in JURISDICTIONS:
        for origin in ORIGINS:
            if (jur, origin) in SKIP:
                print(f"  Skipping {jur} origin={origin} (already running)")
                continue
            p = launch(jur, origin)
            procs.append((jur, origin, p))
            time.sleep(1)  # stagger slightly to avoid API throttle

    # Launch grand panel training
    for origin in ORIGINS:
        p = launch(GRAND, origin)
        procs.append((GRAND, origin, p))
        time.sleep(1)

    print(f"\nLaunched {len(procs)} training runs. Waiting for completion...")

    # Monitor
    for jur, origin, p in procs:
        stdout, stderr = p.communicate()
        status = "OK" if p.returncode == 0 else f"FAIL (rc={p.returncode})"
        print(f"  [{status}] {jur} origin={origin}")
        if p.returncode != 0:
            # Print last 500 chars of stderr for diagnostics
            err_tail = stderr[-500:] if stderr else "(no stderr)"
            print(f"    stderr: {err_tail}")

    print("\nAll runs complete.")
