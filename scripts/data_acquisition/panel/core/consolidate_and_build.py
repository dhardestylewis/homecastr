"""
Colab-only script: Consolidate ALL data into GCS + build panel
Run this in a Colab notebook connected to your Google Drive.

Steps:
  1. Mount Drive
  2. Copy HCAD + any multistate data from Drive → GCS
  3. Build panel partitions for all verified sources
  4. Report panel status

Usage (in Colab):
  !pip install -q google-cloud-storage pyarrow pyyaml
  %run consolidate_and_build.py
"""

import os
import glob
import subprocess
from pathlib import Path

# ─── 1. Mount Drive ───
try:
    from google.colab import drive
    drive.mount("/content/drive", force_remount=False)
    print("✅ Drive mounted")
except ImportError:
    print("⚠️ Not on Colab — skipping Drive mount")

BUCKET = "gs://properlytic-raw-data"
PROJECT = "properlytic-data"

def gsutil_cp(src, dst):
    """Copy file to GCS."""
    cmd = f"gcloud storage cp '{src}' '{dst}' --project={PROJECT}"
    print(f"  📤 {Path(src).name} → {dst}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  ⚠️ {result.stderr.strip()}")
    return result.returncode == 0


# ─── 2. Copy HCAD data from Drive → GCS ───
print("\n" + "="*60)
print("📦 STEP 1: Copy HCAD from Drive → GCS")

HCAD_PATHS = {
    "master_panel": "/content/drive/MyDrive/HCAD_Archive_Aggregates/hcad_master_panel_2005_2025_leakage_strict_FIXEDYR_WITHGIS.parquet",
}

# Also check for raw HCAD year archives
HCAD_RAW_BASE = "/content/drive/MyDrive/HCAD_Archive/property"
if os.path.isdir(HCAD_RAW_BASE):
    for year_dir in sorted(glob.glob(f"{HCAD_RAW_BASE}/2*")):
        year = os.path.basename(year_dir)
        for zf in glob.glob(f"{year_dir}/*.zip"):
            fname = os.path.basename(zf)
            HCAD_PATHS[f"raw_{year}_{fname}"] = zf

for key, src in HCAD_PATHS.items():
    if os.path.exists(src):
        fname = os.path.basename(src)
        gsutil_cp(src, f"{BUCKET}/hcad/{fname}")
    else:
        print(f"  ⏭️ {key}: {src} not found")


# ─── 3. Copy multistate data from Drive → GCS ───
print("\n" + "="*60)
print("📦 STEP 2: Copy multistate data from Drive → GCS")

MULTISTATE_BASE = "/content/drive/MyDrive/data_backups/multistate_raw"
if os.path.isdir(MULTISTATE_BASE):
    for subdir in sorted(os.listdir(MULTISTATE_BASE)):
        full = os.path.join(MULTISTATE_BASE, subdir)
        if os.path.isdir(full):
            print(f"\n  📁 {subdir}/")
            for f in sorted(os.listdir(full)):
                fp = os.path.join(full, f)
                if os.path.isfile(fp) and os.path.getsize(fp) > 0:
                    gsutil_cp(fp, f"{BUCKET}/{subdir}/{f}")
        elif os.path.isfile(full):
            gsutil_cp(full, f"{BUCKET}/multistate/{subdir}")
else:
    print(f"  ⏭️ {MULTISTATE_BASE} not found")


# ─── 4. Check for NYC data on Drive ───
print("\n" + "="*60)
print("📦 STEP 3: Check for NYC/Austin data on Drive")

NYC_CANDIDATES = [
    "/content/drive/MyDrive/data_backups/multistate_raw/nyc",
    "/content/drive/MyDrive/NYC_data",
    "/content/drive/MyDrive/data_backups/nyc",
]
for path in NYC_CANDIDATES:
    if os.path.isdir(path):
        print(f"  Found NYC data at: {path}")
        for f in os.listdir(path):
            fp = os.path.join(path, f)
            if os.path.isfile(fp):
                gsutil_cp(fp, f"{BUCKET}/nyc/{f}")

AUSTIN_CANDIDATES = [
    "/content/drive/MyDrive/data_backups/austin",
    "/content/drive/MyDrive/Austin_data",
]
for path in AUSTIN_CANDIDATES:
    if os.path.isdir(path):
        print(f"  Found Austin data at: {path}")
        for f in os.listdir(path):
            fp = os.path.join(path, f)
            if os.path.isfile(fp):
                gsutil_cp(fp, f"{BUCKET}/austin/{f}")


# ─── 5. Build panel partitions ───
print("\n" + "="*60)
print("📦 STEP 4: Build panel from GCS data")

# The HCAD master panel is ALREADY a processed parquet — just copy it as a partition
hcad_panel = f"{BUCKET}/hcad/hcad_master_panel_2005_2025_leakage_strict_FIXEDYR_WITHGIS.parquet"
result = subprocess.run(
    f"gcloud storage ls {hcad_panel} --project={PROJECT}",
    shell=True, capture_output=True, text=True
)
if result.returncode == 0:
    # Copy HCAD panel as a partition
    gsutil_cp(
        f"{hcad_panel}",
        f"{BUCKET}/panel/jurisdiction=hcad_houston/part.parquet"
    )
    print("  ✅ HCAD partition written")

# Now run build_panel.py for other sources
print("\n  Running build_panel.py --all ...")
script_dir = "/content"
# Try to find the repo
repo_candidates = [
    "/content/drive/MyDrive/Colab Notebooks/v0-properlytic-8v",
    "/content/v0-properlytic-8v",
]
for cand in repo_candidates:
    bp = os.path.join(cand, "scripts/data_acquisition/build_panel.py")
    if os.path.exists(bp):
        subprocess.run(["python", bp, "--all", "--max-chunks", "10"], cwd=os.path.dirname(bp))
        break
else:
    print("  ⚠️ build_panel.py not found — clone repo first:")
    print("  !git clone https://github.com/dhardestylewis/v0-properlytic-8v /content/v0-properlytic-8v")


# ─── 6. Final inventory ───
print("\n" + "="*60)
print("📊 FINAL GCS INVENTORY")
subprocess.run(
    f"gcloud storage ls -l --recursive {BUCKET}/ --project={PROJECT} | tail -5",
    shell=True
)

print("\n📊 PANEL PARTITIONS")
subprocess.run(
    f"gcloud storage ls {BUCKET}/panel/ --project={PROJECT}",
    shell=True
)

print("\n✅ DONE — ready for training!")
print(f"   Training reads: pd.read_parquet('{BUCKET}/panel/')")
