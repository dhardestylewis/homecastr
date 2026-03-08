"""
Batch launcher for training and eval — serializes Modal runs
to stay within Starter plan GPU concurrency limit (10).

Usage:
    python scripts/launch_batch.py --mode train --jurisdiction nyc --origins 2019,2020,2021,2022,2023,2024,2025
    python scripts/launch_batch.py --mode eval --jurisdiction hcad_houston
    python scripts/launch_batch.py --mode all --jurisdictions nyc,philly,hcad_houston
"""
import subprocess, sys, time, argparse


def run_modal(args_list: list[str], label: str) -> bool:
    """Run a single modal command, wait for completion, return success."""
    cmd = [sys.executable, "-m", "modal", "run"] + args_list
    print(f"\n{'='*60}")
    print(f"🚀 [{label}] {' '.join(cmd)}")
    print(f"{'='*60}")
    t0 = time.time()
    result = subprocess.run(cmd, cwd=".")
    dt = time.time() - t0
    ok = result.returncode == 0
    status = "✅" if ok else "❌"
    print(f"{status} [{label}] {'Completed' if ok else 'FAILED'} in {dt/60:.1f} min")
    return ok


def train_jurisdiction(jurisdiction: str, origins: list[int], epochs: int = 60, sample: int = 500_000, model: str = "v12_sb"):
    """Train one jurisdiction across multiple origins SEQUENTIALLY."""
    script = ("scripts/pipeline/training/train_modal_sb.py" if model == "v12_sb"
              else "scripts/pipeline/training/train_modal.py")
    results = {}
    for origin in origins:
        label = f"train-{model}-{jurisdiction}-o{origin}"
        ok = run_modal(
            [script,
             "--jurisdiction", jurisdiction,
             "--origin", str(origin),
             "--epochs", str(epochs),
             "--sample-size", str(sample)],
            label
        )
        results[f"o{origin}"] = "✅" if ok else "❌"
        # Brief pause between runs to let Modal clean up containers
        time.sleep(5)

    print(f"\n📋 Training summary for {jurisdiction} ({model}):")
    for k, v in results.items():
        print(f"  {v} {k}")
    return results


def eval_jurisdiction(jurisdiction: str, origins: list[int] = None, model: str = "v12_sb"):
    """Eval one jurisdiction (launches origins in parallel inside Modal)."""
    script = ("scripts/inference/eval/eval_modal_sb.py" if model == "v12_sb"
              else "scripts/inference/eval/eval_modal.py")
    label = f"eval-{model}-{jurisdiction}"
    args = [script, "--jurisdiction", jurisdiction]
    if origins:
        args.extend(["--origins", ",".join(str(o) for o in origins)])
    ok = run_modal(args, label)
    return ok


def main():
    parser = argparse.ArgumentParser(description="Batch launcher for Modal training/eval")
    parser.add_argument("--mode", choices=["train", "eval", "all"], required=True)
    parser.add_argument("--jurisdiction", type=str, help="Single jurisdiction")
    parser.add_argument("--jurisdictions", type=str, help="Comma-separated jurisdictions for 'all' mode")
    parser.add_argument("--origins", type=str, default="2019,2020,2021,2022,2023,2024,2025",
                        help="Comma-separated origin years")
    parser.add_argument("--epochs", type=int, default=60)
    parser.add_argument("--sample-size", type=int, default=500_000)
    parser.add_argument("--model", choices=["v11", "v12_sb"], default="v12_sb",
                        help="Model variant: v11 (original diffusion) or v12_sb (Schrödinger Bridge)")
    args = parser.parse_args()

    origins = [int(o) for o in args.origins.split(",")]

    if args.mode == "train":
        train_jurisdiction(args.jurisdiction, origins, args.epochs, args.sample_size, model=args.model)

    elif args.mode == "eval":
        eval_jurisdiction(args.jurisdiction, origins, model=args.model)

    elif args.mode == "all":
        jurisdictions = args.jurisdictions.split(",") if args.jurisdictions else [args.jurisdiction]
        for jur in jurisdictions:
            print(f"\n{'#'*60}")
            print(f"# JURISDICTION: {jur}")
            print(f"{'#'*60}")

            # Train all origins sequentially (1 GPU at a time)
            train_jurisdiction(jur, origins, args.epochs, args.sample_size, model=args.model)

            # Then eval (parallel inside Modal, but only after training is done)
            eval_jurisdiction(jur, origins, model=args.model)

            print(f"\n✅ {jur} complete!")


if __name__ == "__main__":
    main()
