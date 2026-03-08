"""
NYC Panel Builder: Parse DOF Assessment Roll files (dual-format: TSV + FWF).
- FY20-FY26: Tab-delimited (140 columns, from XLSX "Record Layout")
- FY09-FY19: Fixed-width (1535 chars/line, from tarfieldcodes.pdf)

Usage:
  python build_nyc_panel.py --inspect          # Preview FY26 + FY19 + FY09
  python build_nyc_panel.py --fy 26            # Build FY26 panel
  python build_nyc_panel.py --all              # Build all FYs (9-26)
"""
import csv, zipfile, io, os, sys, argparse
import pandas as pd
import numpy as np
from google.cloud import storage

GCS_BUCKET = "properlytic-raw-data"

# ── TSV field names (140 columns, FY20+) ─────────────────────────────────
TSV_FIELD_NAMES = [
    "PARID", "BORO", "BLOCK", "LOT", "EASE", "SUBIDENT_REUC",
    "RECTYPE", "TAXYR", "IDENT", "SUBIDENT", "ROLL_SECTION", "SECVOL",
    "PYMKTLAND", "PYMKTTOT", "PYACTLAND", "PYACTTOT", "PYACTEXTOT",
    "PYTRNLAND", "PYTRNTOT", "PYTRNEXTOT", "PYTXBTOT", "PYTXBEXTOT", "PYTAXCLASS",
    "TENMKTLAND", "TENMKTTOT", "TENACTLAND", "TENACTTOT", "TENACTEXTOT",
    "TENTRNLAND", "TENTRNTOT", "TENTRNEXTOT", "TENTXBTOT", "TENTXBEXTOT", "TENTAXCLASS",
    "CBNMKTLAND", "CBNMKTTOT", "CBNACTLAND", "CBNACTTOT", "CBNACTEXTOT",
    "CBNTRNLAND", "CBNTRNTOT", "CBNTRNEXTOT", "CBNTXBTOT", "CBNTXBEXTOT", "CBNTAXCLASS",
    "FINMKTLAND", "FINMKTTOT", "FINACTLAND", "FINACTTOT", "FINACTEXTOT",
    "FINTRNLAND", "FINTRNTOT", "FINTRNEXTOT", "FINTXBTOT", "FINTXBEXTOT", "FINTAXCLASS",
    "CURMKTLAND", "CURMKTTOT", "CURACTLAND", "CURACTTOT", "CURACTEXTOT",
    "CURTRNLAND", "CURTRNTOT", "CURTRNEXTOT", "CURTXBTOT", "CURTXBEXTOT", "CURTAXCLASS",
    "PERIOD", "NEWDROP", "NOAV", "VALREF",
    "BLDG_CLASS", "OWNER", "ZONING",
    "HOUSENUM_LO", "HOUSENUM_HI", "STREET_NAME",
    "ZIP_CODE", "GEOSUPPORT_RC", "STCODE",
    "LOT_FRT", "LOT_DEP", "LOT_IRREG",
    "BLD_FRT", "BLD_DEP", "BLD_EXT",
    "BLD_STORY", "CORNER", "LAND_AREA",
    "NUM_BLDGS", "YRBUILT", "YRBUILT_RANGE", "YRBUILT_FLAG",
    "YRALT1", "YRALT1_RANGE", "YRALT2", "YRALT2_RANGE",
    "COOP_APTS", "UNITS", "REUC_REF",
    "APTNO", "COOP_NUM",
    "CPB_BORO", "CPB_DIST",
    "APPT_DATE", "APPT_BORO", "APPT_BLOCK", "APPT_LOT", "APPT_EASE",
    "CONDO_NUMBER", "CONDO_SFX1", "CONDO_SFX2", "CONDO_SFX3",
    "UAF_LAND", "UAF_BLDG",
    "PROTEST_1", "PROTEST_2", "PROTEST_OLD",
    "ATTORNEY_GROUP1", "ATTORNEY_GROUP2", "ATTORNEY_GROUP_OLD",
    "GROSS_SQFT",
    "HOTEL_AREA_GROSS", "OFFICE_AREA_GROSS", "RESIDENTIAL_AREA_GROSS",
    "RETAIL_AREA_GROSS", "LOFT_AREA_GROSS", "FACTORY_AREA_GROSS",
    "WAREHOUSE_AREA_GROSS", "STORAGE_AREA_GROSS", "GARAGE_AREA",
    "OTHER_AREA_GROSS",
    "REUC_DESCRIPTION",
    "EXTRACTDT",
    "PYTAXFLAG", "TENTAXFLAG", "CBNTAXFLAG", "FINTAXFLAG", "CURTAXFLAG",
    "EXTRA_PAD",
]

# ── FWF field slicing (FY09-FY19, from tarfieldcodes.pdf) ────────────────
# Lines are 1535 chars. Numeric fields are 22 chars (right-justified decimal).
# Dates are 10 chars. Layout per tarfieldcodes.pdf field order:
#
#   [0:11]     AV_BBLE = boro(1) + block(5) + lot(4) + ease(1)
#   [11:124]   Header: BORO, BLOCK, LOT (as decimals), SECVOL, DISTRICT, YEAR4, etc.
#   [124:212]  Market values: CUR_FV_LAND(22), CUR_FV_TOTAL(22), NEW_FV_LAND(22), NEW_FV_TOTAL(22)
#   [212:222]  FV_CHGDT (date, 10 chars)
#   [222:398]  Current assessed: 8 × 22 = 176 (transitional + actual, non-exempt + exempt)
#   [398:408]  CHGDT (date)
#   [408:584]  Tentative assessed: 8 × 22 = 176
#   [584:594]  FCHGDT (date)
#   [594:770]  Final assessed: 8 × 22 = 176
#   [770:778]  CURTAXCLASS(2) + OLD_TAX_CLASS(2) + CBN_TAX_CLASS(2) + BLDG_CLASS(2)
#   [778:780]  EXMTCL (2)
#   [780:801]  OWNER (21)
#   [801:845]  HOUSENUM_LO(12) + HOUSENUM_HI(12) + STREET_NAME(20)
#   [845:850]  ZIP (5)
#   [850:...]  Remaining fields (units, dimensions, YRBUILT, etc.)

# Map FWF positions → standardized column names (matching TSV where possible)
FWF_SLICES = {
    # BBL components from BBLE
    "BORO":         (0, 1),
    "BLOCK":        (1, 6),
    "LOT":          (6, 10),
    # Market values (22-char fields)
    "CURMKTLAND":   (124, 146),   # AV_CUR_FV_LAND
    "CURMKTTOT":    (146, 168),   # AV_CUR_FV_TOTAL
    # Current assessed values (start at 222)
    "CURACTLAND":   (222, 244),   # AV_CURAVL (transitional assessed land)
    "CURACTTOT":    (244, 266),   # AV_CURAVT (transitional assessed total)
    # Current actual assessed
    "CURACTLAND_ACT": (310, 332), # AV_CURAVL_ACT
    "CURACTTOT_ACT":  (332, 354), # AV_CURAVT_ACT
    # Final assessed (start at 594)
    "FINACTLAND":   (594, 616),   # AV_FINAL_AVL
    "FINACTTOT":    (616, 638),   # AV_FINAL_AVT
    # Final actual assessed
    "FINACTLAND_ACT": (682, 704), # AV_FINAL_AVL_ACT
    "FINACTTOT_ACT":  (704, 726), # AV_FINAL_AVT_ACT
    # Property descriptive
    "CURTAXCLASS":  (770, 772),
    "BLDG_CLASS":   (776, 778),
    "OWNER":        (780, 801),
    "ZIP_CODE":     (845, 850),
}

# Panel columns to keep (unified across TSV and FWF)
PANEL_COLS = [
    "BBL", "FY", "BORO", "BLOCK", "LOT",
    "CURMKTLAND", "CURMKTTOT",
    "CURACTTOT",
    "FINACTTOT",
    "CURTAXCLASS", "BLDG_CLASS", "OWNER", "ZIP_CODE",
]

NUMERIC_COLS = [
    "BORO", "BLOCK", "LOT",
    "CURMKTLAND", "CURMKTTOT",
    "CURACTLAND", "CURACTTOT",
    "FINACTLAND", "FINACTTOT",
    "FINMKTLAND", "FINMKTTOT",
    "FINACTLAND_ACT", "FINACTTOT_ACT",
    "CURACTLAND_ACT", "CURACTTOT_ACT",
]


def detect_format(z):
    """Auto-detect TSV vs FWF by checking tab count in first line."""
    for name in z.namelist():
        if name.upper().endswith(".TXT"):
            with z.open(name) as f:
                first_line = f.readline().decode("utf-8", errors="replace")
                return "TSV" if first_line.count("\t") > 10 else "FWF"
    return "FWF"


def parse_tsv(z, fy_label):
    """Parse tab-delimited DOF file (FY20+)."""
    dfs = []
    for name in z.namelist():
        if not name.upper().endswith(".TXT"):
            continue
        with z.open(name) as f:
            df = pd.read_csv(
                f, sep="\t", header=None,
                names=TSV_FIELD_NAMES,
                dtype=str,
                encoding="utf-8",
                encoding_errors="replace",
                on_bad_lines="skip",
                quoting=csv.QUOTE_NONE,
            )
            print(f"    {name}: {len(df)} rows [TSV]")
            dfs.append(df)
    return pd.concat(dfs, ignore_index=True) if dfs else None


def parse_fwf(z, fy_label):
    """Parse fixed-width DOF file (FY09-FY19) line by line."""
    rows = []
    for name in z.namelist():
        if not name.upper().endswith(".TXT"):
            continue
        with z.open(name) as f:
            raw = f.read().decode("utf-8", errors="replace")
            for line in raw.split("\n"):
                if len(line) < 800:
                    continue
                row = {}
                for field, (start, end) in FWF_SLICES.items():
                    row[field] = line[start:end].strip()
                rows.append(row)
            print(f"    {name}: {len(rows)} rows [FWF]")
    return pd.DataFrame(rows) if rows else None


def clean_panel(df, fy_label):
    """Clean and standardize parsed data."""
    # Map FWF-specific column names to TSV equivalents
    rename_map = {
        "CURACTLAND_ACT": "CURACTLAND",
        "CURACTTOT_ACT": "CURACTTOT",
        "FINACTLAND_ACT": "FINACTLAND",
        "FINACTTOT_ACT": "FINACTTOT",
    }
    for old, new in rename_map.items():
        if old in df.columns and new not in df.columns:
            df = df.rename(columns={old: new})

    # Convert numeric columns
    for col in NUMERIC_COLS:
        if col not in df.columns:
            continue
        s = df[col].astype(str).str.strip().str.strip("'\"").str.lstrip("+")
        # Strip trailing .00 from FWF decimal format
        s = s.str.replace(r"\.0+$", "", regex=True)
        # Remove leading zeros but keep "0"
        s = s.str.replace(r"^0+(\d)", r"\1", regex=True)
        df[col] = pd.to_numeric(s, errors="coerce")

    # Build BBL
    valid = df[["BORO", "BLOCK", "LOT"]].notna().all(axis=1)
    valid &= (df["BORO"] > 0) & (df["BORO"] <= 5)
    if not valid.all():
        drop_count = (~valid).sum()
        print(f"  Dropping {drop_count} rows with invalid/missing BBL")
        df = df[valid].copy()

    if len(df) == 0:
        return df

    df["BBL"] = (
        df["BORO"].astype(int).astype(str)
        + df["BLOCK"].astype(int).astype(str).str.zfill(5)
        + df["LOT"].astype(int).astype(str).str.zfill(4)
    )
    df["FY"] = fy_label

    # Keep only panel columns that exist
    keep = [c for c in PANEL_COLS if c in df.columns]
    df = df[keep].copy()

    print(f"  Final: {len(df)} rows, {df['BBL'].nunique()} unique BBLs")
    return df


def parse_dof_zip(bucket, gcs_path, fy_label):
    """Download, auto-detect format, and parse a DOF ZIP."""
    blob = bucket.blob(gcs_path)
    if not blob.exists():
        print(f"  SKIP {gcs_path}: not in GCS")
        return None
    blob.reload()
    size_mb = blob.size / 1e6
    print(f"  Reading {gcs_path} ({size_mb:.0f}MB)...")
    data = blob.download_as_bytes()
    z = zipfile.ZipFile(io.BytesIO(data))

    fmt = detect_format(z)
    if fmt == "TSV":
        df = parse_tsv(z, fy_label)
    else:
        df = parse_fwf(z, fy_label)

    if df is None or len(df) == 0:
        return None
    return clean_panel(df, fy_label)


def main():
    parser = argparse.ArgumentParser(description="Build NYC panel from DOF")
    parser.add_argument("--fy", type=int, help="Single FY to process")
    parser.add_argument("--all", action="store_true", help="Process all FYs (9-26)")
    parser.add_argument("--inspect", action="store_true", help="Preview data")
    args = parser.parse_args()

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)

    if args.inspect:
        for label, path in [
            ("FY26 TC1 (TSV)", "nyc/dof_assessment/fy26_tc1.zip"),
            ("FY19 TC1 (FWF)", "nyc/dof_assessment/fy19_tc1.zip"),
            ("FY09 TC1 (FWF)", "nyc/dof_assessment/fy09_tc1.zip"),
        ]:
            print(f"\n=== {label} ===")
            df = parse_dof_zip(bucket, path, label.split()[0])
            if df is not None and len(df) > 0:
                show = [c for c in ["BBL", "CURMKTTOT", "FINACTTOT", "BLDG_CLASS", "ZIP_CODE", "OWNER"]
                        if c in df.columns]
                print(df[show].head(5).to_string())
                for c in ["CURMKTTOT", "FINACTTOT"]:
                    if c in df.columns:
                        vals = df[c].dropna()
                        vals = vals[vals > 0]
                        if len(vals):
                            print(f"  {c}: median=${vals.median():,.0f} mean=${vals.mean():,.0f} n={len(vals)}")
        return

    # Determine FYs to process
    if args.fy:
        fys = [args.fy]
    elif args.all:
        fys = list(range(9, 27))
    else:
        fys = [26]

    all_dfs = []
    for fy in fys:
        fy_str = f"fy{fy:02d}"
        fy_label = f"FY{fy:02d}"
        print(f"\n=== {fy_label} ===")
        for tc in ["tc1", "tc234"]:
            gcs_path = f"nyc/dof_assessment/{fy_str}_{tc}.zip"
            df = parse_dof_zip(bucket, gcs_path, fy_label)
            if df is not None and len(df) > 0:
                all_dfs.append(df)

    if not all_dfs:
        print("No data parsed!")
        return

    panel = pd.concat(all_dfs, ignore_index=True)
    print(f"\n{'='*60}")
    print(f"PANEL: {len(panel):,} rows, {panel['BBL'].nunique():,} unique BBLs")
    print(f"\nFY distribution:")
    print(panel["FY"].value_counts().sort_index())

    out_name = f"panel_{'_'.join(f'fy{fy:02d}' for fy in fys)}.parquet"
    out_path = f"nyc/dof_panel/{out_name}"
    blob = bucket.blob(out_path)
    buf = io.BytesIO()
    panel.to_parquet(buf, index=False)
    size = buf.tell()
    buf.seek(0)
    blob.upload_from_file(buf, content_type="application/octet-stream")
    print(f"\nUploaded: gs://{GCS_BUCKET}/{out_path} ({size/1e6:.1f}MB)")


if __name__ == "__main__":
    main()
