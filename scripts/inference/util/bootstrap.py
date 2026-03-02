# Colab bootstrap: mount Drive + resolve paths needed to run the LAST pasted cell (the seam-collapse diagnostic)
# ASCII-only. Safe to run standalone before the diagnostic.

import os, time, json

def ts():
    return time.strftime("%Y-%m-%d %H:%M:%S")

# 1) Mount Drive (only if needed)
try:
    from google.colab import drive
    if not os.path.exists("/content/drive"):
        drive.mount("/content/drive", force_remount=False)
    else:
        print(f"[{ts()}] Drive already present at /content/drive")
except Exception as e:
    print(f"[{ts()}] Drive mount skipped: {e}")

# 2) Ensure expected output dir exists (diagnostic itself does not require it, but matches your conventions)
OUT_DIR_DEFAULT = "/content/drive/MyDrive/data_backups/world_model_v10_2_fullpanel"
try:
    os.makedirs(OUT_DIR_DEFAULT, exist_ok=True)
    print(f"[{ts()}] OUT_DIR ok: {OUT_DIR_DEFAULT}")
except Exception as e:
    print(f"[{ts()}] OUT_DIR mkdir failed: {e}")

# 3) Sanity check that the panel parquet is visible where your v10.2 cell expects it
PANEL_PATH_DRIVE = "/content/drive/MyDrive/HCAD_Archive_Aggregates/hcad_master_panel_2005_2025_leakage_strict_FIXEDYR_WITHGIS.parquet"
if os.path.exists(PANEL_PATH_DRIVE):
    sz = os.path.getsize(PANEL_PATH_DRIVE)
    print(f"[{ts()}] Panel present: {PANEL_PATH_DRIVE} bytes={sz}")
else:
    print(f"[{ts()}] WARNING: panel missing at: {PANEL_PATH_DRIVE}")
    print(f"[{ts()}] If you used a local copy earlier, ensure PANEL_PATH_LOCAL exists or update PANEL_PATH_DRIVE.")

# 4) Minimal imports the diagnostic uses (avoid torch install here; diagnostic assumes your v10.2 model cell already did that)
try:
    import numpy as np
    import pandas as pd
    import polars as pl
    import torch
    print(f"[{ts()}] Imports ok: numpy/pandas/polars/torch")
except Exception as e:
    raise RuntimeError(
        "Missing required packages for the diagnostic cell. "
        "Run your v10.2 FULLPANEL setup cell (it installs polars/pyarrow/torch) first. "
        f"Import error: {e}"
    )

print(f"[{ts()}] Bootstrap complete. If your v10.2 FULLPANEL cell already ran, you can run the diagnostic cell next.")
