"""
Master launcher: Upload fixed worldmodel.py, build all panels, retrain all jurisdictions.
Runs everything sequentially to avoid Modal concurrency issues.

Usage:
    modal run scripts/launch_all.py
"""
import modal, os, sys, json, subprocess, time

def run_modal(cmd: str, label: str, timeout: int = 600) -> bool:
    """Run a modal command, return True if succeeded."""
    print(f"\n{'='*60}")
    print(f"[LAUNCH] {label}")
    print(f"  CMD: {cmd}")
    print(f"{'='*60}")
    
    start = time.time()
    result = subprocess.run(
        cmd, shell=True, capture_output=True, text=True, timeout=timeout,
        env={**os.environ, "PYTHONIOENCODING": "utf-8"}
    )
    elapsed = time.time() - start
    
    if result.returncode == 0:
        print(f"  ✅ {label} succeeded ({elapsed:.0f}s)")
        if result.stdout:
            # Print last 10 lines
            lines = result.stdout.strip().split("\n")
            for line in lines[-10:]:
                print(f"    {line}")
        return True
    else:
        print(f"  ❌ {label} FAILED ({elapsed:.0f}s)")
        if result.stderr:
            lines = result.stderr.strip().split("\n")
            for line in lines[-10:]:
                print(f"    {line}")
        return False


def main():
    os.makedirs("scripts/logs/train", exist_ok=True)
    os.makedirs("scripts/logs", exist_ok=True)
    
    results = {}
    
    # Phase 1: Upload fixed worldmodel.py
    ok = run_modal(
        "python -m modal run scripts/upload_worldmodel.py",
        "Upload fixed worldmodel.py to GCS"
    )
    results["upload_worldmodel"] = ok
    if not ok:
        print("\n⚠️ WorldModel upload failed - training will use old code")
    
    # Phase 2: Data acquisition (parallel-safe, different apps)
    for script, label in [
        ("scripts/download_txgio.py", "TX-wide data (TXGIO)"),
        ("scripts/download_dc.py", "DC data (Open Data DC)"),
    ]:
        if os.path.exists(script):
            ok = run_modal(f"python -m modal run {script}", label, timeout=1800)
            results[label] = ok
    
    # Phase 3: Build panels for jurisdictions with raw data
    if os.path.exists("scripts/build_panels_modal.py"):
        ok = run_modal(
            "python -m modal run scripts/build_panels_modal.py",
            "Build panels (Cook County, NY State, France DVF)",
            timeout=1800
        )
        results["build_panels"] = ok
    
    # Phase 4: Training runs (sequential to stay within GPU limits)
    training_runs = [
        ("hcad_houston", "2025"),
        ("hcad_houston", "2024"),
        ("sf_ca", "2024"),
        ("nyc", "2024"),
        ("philly", "2024"),
    ]
    
    for jur, origin in training_runs:
        label = f"train-{jur}-o{origin}"
        ok = run_modal(
            f"python -m modal run scripts/train_modal.py --jurisdiction {jur} --origin {origin}",
            label,
            timeout=7200  # 2 hours per training run
        )
        results[label] = ok
    
    # Summary
    print(f"\n{'='*60}")
    print("LAUNCH SUMMARY")
    print(f"{'='*60}")
    for k, v in results.items():
        status = "✅" if v else "❌"
        print(f"  {status} {k}")
    
    with open("scripts/logs/launch_all_results.json", "w") as f:
        json.dump(results, f, indent=2)


if __name__ == "__main__":
    main()
