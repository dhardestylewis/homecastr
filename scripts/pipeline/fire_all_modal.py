"""
Batch launcher: fires ALL cancelled Modal jobs with --detach.
Each `modal run --detach` fires individually.
--detach keeps the app alive after local process exits.

Run: python scripts/pipeline/fire_all_modal.py
"""
import subprocess, sys, time

# Each job is a separate `modal run --detach` — they each get their own app lifecycle
JOBS = [
    # Maintenance — fire individual entrypoints (not orchestrator)
    ("maintenance-indexes", "python -m modal run --detach scripts/pipeline/maintenance_modal.py::indexes"),
    ("maintenance-history", "python -m modal run --detach scripts/pipeline/maintenance_modal.py::fix_history"),
    ("maintenance-downloads", "python -m modal run --detach scripts/pipeline/maintenance_modal.py::downloads"),

    # TxGIO parcel downloads
    ("txgio", "python -m modal run --detach scripts/inference/upload/download_txgio_modal.py"),

    # Training jobs
    ("train-nyc-o2023", "python -m modal run --detach scripts/pipeline/train_modal.py --jurisdiction nyc --origin 2023"),
    ("train-nyc-o2024", "python -m modal run --detach scripts/pipeline/train_modal.py --jurisdiction nyc --origin 2024"),
    ("train-uk_ppd-o2022", "python -m modal run --detach scripts/pipeline/train_modal.py --jurisdiction uk_ppd --origin 2022"),
    ("train-philly-o2022", "python -m modal run --detach scripts/pipeline/train_modal.py --jurisdiction philly --origin 2022"),

    # Inference jobs
    ("inference-france_dvf-o2023", "python -m modal run --detach scripts/inference/inference_modal.py --jurisdiction france_dvf --origin 2023"),
    ("inference-france_dvf-o2024", "python -m modal run --detach scripts/inference/inference_modal.py --jurisdiction france_dvf --origin 2024"),
    ("inference-seattle_wa-o2024", "python -m modal run --detach scripts/inference/inference_modal.py --jurisdiction seattle_wa --origin 2024"),

    # Data acquisition
    ("acquire-jurisdictions", "python -m modal run --detach scripts/data_acquisition/download/acquire_new_jurisdictions.py"),

    # SF sales download
    ("download-sf-sales", "python -m modal run --detach scripts/data_acquisition/download/download_sf_sales.py"),

    # Entity backtest v3
    ("backtest-v3", "python -m modal run --detach scripts/inference/backtest/entity_backtest_v3_modal.py"),
]

print(f"Firing {len(JOBS)} Modal jobs with --detach")
print(f"Each gets its own app — survives laptop close\n")

results = []
for name, cmd in JOBS:
    print(f"  [{name}] ...", end=" ", flush=True)
    try:
        proc = subprocess.run(
            cmd, shell=True, capture_output=True, text=True,
            timeout=120, cwd="."
        )
        if proc.returncode == 0:
            status = "OK"
        else:
            err = proc.stderr.strip()[-150:] if proc.stderr else proc.stdout.strip()[-150:]
            status = f"FAIL({proc.returncode}): {err}"
        results.append((name, proc.returncode == 0, status))
        print(status)
    except subprocess.TimeoutExpired:
        results.append((name, False, "TIMEOUT"))
        print("TIMEOUT")
    except Exception as e:
        results.append((name, False, str(e)[:100]))
        print(str(e)[:100])

ok = sum(1 for _, s, _ in results if s)
print(f"\n{'='*60}")
print(f"SUBMITTED: {ok}/{len(JOBS)}")
for name, success, msg in results:
    icon = "+" if success else "X"
    print(f"  [{icon}] {name}: {msg[:80]}")
print(f"\nMonitor all at: https://modal.com/apps")
