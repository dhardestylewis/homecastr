"""
Multi-State Property Data Acquisition
======================================
Run this in Colab to download free, parcel-level assessment data from:
  1. Cook County IL  (API — instant, 1999-present)
  2. NYC             (direct CSV — instant, 2009-present)
  3. NJ MOD-IV       (download after registration at modiv.rutgers.edu)
  4. Florida DOR     (email request — 1-2 week turnaround)

All data saves to Google Drive under:
  /content/drive/MyDrive/data_backups/multistate_raw/

Requires:  pip install sodapy requests pandas
"""

import os
import time
import requests
import pandas as pd

# ── CONFIG ──────────────────────────────────────────────────────────
DRIVE_BASE = "/content/drive/MyDrive/data_backups/multistate_raw"
os.makedirs(DRIVE_BASE, exist_ok=True)

# ════════════════════════════════════════════════════════════════════
# 1. COOK COUNTY IL — Socrata Open Data API (no auth needed)
# ════════════════════════════════════════════════════════════════════

def download_cook_county():
    """
    Dataset: Assessed Values (1999-present), ~1.8M parcels × 25+ years
    API: Socrata — free, no key needed (throttled to 1000/req without key)
    Source: https://datacatalog.cookcountyil.gov/Property-Taxation/
    """
    out_dir = os.path.join(DRIVE_BASE, "cook_county_il")
    os.makedirs(out_dir, exist_ok=True)
    
    # Socrata dataset ID for Cook County Assessed Values
    dataset_id = "uzyt-m557"
    base_url = f"https://datacatalog.cookcountyil.gov/resource/{dataset_id}.csv"
    
    page_size = 50000
    offset = 0
    all_chunks = []
    chunk_idx = 0
    
    print(f"[COOK COUNTY] Starting download from Socrata API...")
    print(f"[COOK COUNTY] Output dir: {out_dir}")
    
    while True:
        url = f"{base_url}?$limit={page_size}&$offset={offset}&$order=:id"
        t0 = time.time()
        
        try:
            resp = requests.get(url, timeout=120)
            resp.raise_for_status()
        except Exception as e:
            print(f"[COOK COUNTY] Error at offset={offset}: {e}")
            print(f"[COOK COUNTY] Retrying in 10s...")
            time.sleep(10)
            continue
        
        # Parse CSV response
        from io import StringIO
        chunk_df = pd.read_csv(StringIO(resp.text))
        
        if len(chunk_df) == 0:
            print(f"[COOK COUNTY] Done — no more rows at offset={offset}")
            break
        
        # Save chunk to Drive
        chunk_idx += 1
        chunk_fp = os.path.join(out_dir, f"assessed_values_chunk_{chunk_idx:04d}.parquet")
        chunk_df.to_parquet(chunk_fp, index=False)
        
        dt = time.time() - t0
        offset += page_size
        print(f"[COOK COUNTY] Chunk {chunk_idx}: {len(chunk_df)} rows | "
              f"total offset={offset:,} | {dt:.1f}s | saved {chunk_fp}")
        
        # Rate limit: be polite to Socrata
        time.sleep(1.0)
    
    print(f"[COOK COUNTY] Complete: {chunk_idx} chunks saved to {out_dir}")
    return out_dir


# ════════════════════════════════════════════════════════════════════
# 2. NYC — Open Data direct CSV downloads
# ════════════════════════════════════════════════════════════════════

def download_nyc():
    """
    NYC Dept of Finance — Final Assessment Roll + PLUTO
    Direct CSV download, no auth needed.
    Source: https://data.cityofnewyork.us/
    """
    out_dir = os.path.join(DRIVE_BASE, "nyc")
    os.makedirs(out_dir, exist_ok=True)
    
    datasets = {
        # NYC Property Assessment Roll (current, ~1M parcels)
        "assessment_roll": "https://data.cityofnewyork.us/api/views/yjxr-fw8i/rows.csv?accessType=DOWNLOAD",
        # PLUTO (land use + building characteristics for enrichment)
        "pluto": "https://data.cityofnewyork.us/api/views/64uk-42ks/rows.csv?accessType=DOWNLOAD",
        # DOF Rolling Sales (recent sales for validation)
        "rolling_sales": "https://data.cityofnewyork.us/api/views/usep-8jbt/rows.csv?accessType=DOWNLOAD",
    }
    
    for name, url in datasets.items():
        out_fp = os.path.join(out_dir, f"{name}.csv")
        if os.path.exists(out_fp):
            sz_mb = os.path.getsize(out_fp) / 1e6
            print(f"[NYC] {name} already exists ({sz_mb:.0f}MB), skipping")
            continue
        
        print(f"[NYC] Downloading {name}...")
        t0 = time.time()
        
        try:
            resp = requests.get(url, stream=True, timeout=300)
            resp.raise_for_status()
            
            with open(out_fp, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            dt = time.time() - t0
            sz_mb = os.path.getsize(out_fp) / 1e6
            print(f"[NYC] {name}: {sz_mb:.0f}MB downloaded in {dt:.0f}s → {out_fp}")
        except Exception as e:
            print(f"[NYC] Error downloading {name}: {e}")
    
    print(f"[NYC] Complete: files saved to {out_dir}")
    return out_dir


# ════════════════════════════════════════════════════════════════════
# 3. FLORIDA DOR — Email template (manual step)
# ════════════════════════════════════════════════════════════════════

def print_florida_email():
    """Print the email to send to Florida DOR for NAL files."""
    email = """
═══════════════════════════════════════════════════════════════
SEND THIS EMAIL TO: PTOTechnology@floridarevenue.com
═══════════════════════════════════════════════════════════════

Subject: Request for Historical NAL Assessment Roll Data (2002-2025)

Dear Property Tax Oversight Technology Team,

I am a researcher building property valuation models and would
like to request the following data under Chapter 119, F.S.:

1. NAL (Name-Address-Legal) files for ALL 67 counties,
   years 2002 through 2025 (preliminary or final, whichever
   is most complete).

2. SDF (Sale Data Files) for ALL 67 counties,
   years 2009 through 2025.

I understand that files larger than 10 MB will be provided
via a temporary download directory with an emailed link,
which is my preferred method.

Please let me know if you need any additional information
to process this request.

Thank you for your time.

Best regards,
[YOUR NAME]
[YOUR EMAIL]
═══════════════════════════════════════════════════════════════
"""
    print(email)


# ════════════════════════════════════════════════════════════════════
# 4. NJ MOD-IV — Registration instructions (manual step)
# ════════════════════════════════════════════════════════════════════

def print_nj_instructions():
    """Print instructions for NJ MOD-IV registration and download."""
    instr = """
═══════════════════════════════════════════════════════════════
NJ MOD-IV Historical Database (1989-present, FREE)
═══════════════════════════════════════════════════════════════

1. Go to: https://modiv.rutgers.edu
2. Click "Register" (free, instant approval)
3. After login, select:
   - State: "New Jersey" (statewide)
   - Years: select all available (1989-2025)
   - Format: CSV
4. Download files (may need to do by county if statewide
   downloads have size limits)
5. Upload to Google Drive:
   /content/drive/MyDrive/data_backups/multistate_raw/nj_modiv/

NOTES:
- Dataset contains 105M+ parcel records
- Includes: assessed value, property class, sale prices,
  land use, square footage, lot size
- Owner names are redacted (Daniel's Law)
═══════════════════════════════════════════════════════════════
"""
    print(instr)


# ════════════════════════════════════════════════════════════════════
# MAIN — Run everything
# ════════════════════════════════════════════════════════════════════

if __name__ == "__main__" or True:  # always run in Colab
    print("=" * 70)
    print("  MULTI-STATE DATA ACQUISITION")
    print("=" * 70)
    print()
    
    # ── Automated downloads ──
    print("▶ Step 1/4: Cook County IL (API pull, ~30-60 min)")
    cook_dir = download_cook_county()
    
    print()
    print("▶ Step 2/4: NYC Open Data (direct download, ~10 min)")
    nyc_dir = download_nyc()
    
    # ── Manual steps ──
    print()
    print("▶ Step 3/4: Florida DOR (send this email NOW)")
    print_florida_email()
    
    print()
    print("▶ Step 4/4: NJ MOD-IV (register + download)")
    print_nj_instructions()
    
    print()
    print("=" * 70)
    print("  SUMMARY")
    print("=" * 70)
    print(f"  Cook County IL: {cook_dir}")
    print(f"  NYC:            {nyc_dir}")
    print(f"  Florida:        Send email above, data arrives in 1-2 weeks")
    print(f"  NJ MOD-IV:      Register at modiv.rutgers.edu, download manually")
    print("=" * 70)
