"""
Structured launcher for Modal training/eval jobs.
Each job gets its own log file in scripts/logs/{job_type}/{jurisdiction}_{origin}.log

Usage:
    python scripts/launch_jobs.py train cook_county_il 2019
    python scripts/launch_jobs.py train cook_county_il 2019,2020,2021
    python scripts/launch_jobs.py eval sf_ca 2019,2020,2021,2022,2023
    python scripts/launch_jobs.py train all 2019      # trains grand panel
"""
import subprocess, sys, os, time
from pathlib import Path
from datetime import datetime

LOG_DIR = Path(__file__).parent / "logs"

def run_job(job_type: str, jurisdiction: str, origin: int):
    """Run a single Modal job and capture logs to a file."""
    subdir = LOG_DIR / job_type
    subdir.mkdir(parents=True, exist_ok=True)
    
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = subdir / f"{jurisdiction}_o{origin}_{ts}.log"
    
    if job_type == "train":
        cmd = ["python", "-m", "modal", "run", "scripts/train_modal.py",
               "--jurisdiction", jurisdiction, "--origin", str(origin)]
    elif job_type == "eval":
        cmd = ["python", "-m", "modal", "run", "scripts/eval_modal.py",
               "--jurisdiction", jurisdiction, "--origin", str(origin)]
    else:
        print(f"Unknown job type: {job_type}")
        return None
    
    print(f"  [{ts}] Launching {job_type} {jurisdiction} o{origin} → {log_path.name}")
    
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"# {job_type} {jurisdiction} origin={origin}\n")
        f.write(f"# Started: {datetime.now().isoformat()}\n")
        f.write(f"# Command: {' '.join(cmd)}\n\n")
        
        proc = subprocess.Popen(
            cmd, stdout=f, stderr=subprocess.STDOUT,
            cwd=str(Path(__file__).parent.parent),
            encoding="utf-8", errors="replace",
        )
    
    return proc, log_path


def main():
    if len(sys.argv) < 4:
        print("Usage: python launch_jobs.py {train|eval} <jurisdiction> <origin1,origin2,...>")
        sys.exit(1)
    
    job_type = sys.argv[1]
    jurisdiction = sys.argv[2]
    origins = [int(o) for o in sys.argv[3].split(",")]
    
    print(f"\n{'='*60}")
    print(f"  Launching {len(origins)} {job_type} jobs for {jurisdiction}")
    print(f"  Origins: {origins}")
    print(f"  Logs: {LOG_DIR / job_type}/")
    print(f"{'='*60}\n")
    
    procs = []
    for origin in origins:
        result = run_job(job_type, jurisdiction, origin)
        if result:
            procs.append(result)
        time.sleep(1)
    
    print(f"\n  All {len(procs)} jobs launched. Waiting for completion...\n")
    
    for proc, log_path in procs:
        proc.wait()
        # Read last 10 lines of log for summary
        try:
            with open(log_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
            tail = lines[-10:] if len(lines) > 10 else lines
            status = "✅" if proc.returncode == 0 else "❌"
            print(f"  {status} {log_path.name} (exit={proc.returncode})")
            for line in tail:
                line = line.rstrip()
                if line:
                    print(f"     {line}")
        except Exception as e:
            print(f"  ⚠️ {log_path.name}: {e}")
    
    print(f"\n  Done. All logs in {LOG_DIR / job_type}/")


if __name__ == "__main__":
    main()
