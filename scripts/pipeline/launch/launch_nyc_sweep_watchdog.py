import subprocess
import sys
import time
import datetime
from google.cloud import storage

JURISDICTION = "nyc"
ORIGINS = [2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]

def check_gcs_ckpt(origin):
    try:
        client = storage.Client()
        bucket = client.bucket("properlytic-raw-data")
        prefix = f"checkpoints/{JURISDICTION}/"
        blobs = list(bucket.list_blobs(prefix=prefix))
        for b in blobs:
            if f"origin_{origin}" in b.name and b.name.endswith(".pt"):
                age_hours = (datetime.datetime.now(datetime.timezone.utc) - b.updated).total_seconds() / 3600
                if age_hours < 24:
                    return True
        return False
    except Exception as e:
        print(f"\n  [GCS Error: {e}]")
        return False

def run_sweep(origin):
    cmd = [
        sys.executable, "-m", "modal", "run",
        "scripts/inference/eval/sweep_stage1_calibration.py::evaluate_checkpoints",
        "--jurisdiction", JURISDICTION,
        "--origin", str(origin),
    ]
    print(f"\n[{time.strftime('%H:%M:%S')}] 🚀 Launching Sweep for {JURISDICTION} origin={origin}")
    result = subprocess.run(cmd)
    return result.returncode == 0

if __name__ == "__main__":
    print(f"Starting synchronized watchdog sweep for {JURISDICTION} origins {ORIGINS}...")
    
    for origin in ORIGINS:
        print(f"\n⏳ Waiting for checkpoint for origin {origin}...")
        while not check_gcs_ckpt(origin):
            time.sleep(60)
            sys.stdout.write(".")
            sys.stdout.flush()
            
        print(f"\n✅ Checkpoint found for origin {origin}! Initiating sweep.")
        success = run_sweep(origin)
        if not success:
            print(f"❌ Sweep failed for origin {origin}. Halting chain to preserve calibration sequence.")
            break
            
    print("\n🎉 All sweeps complete!")
