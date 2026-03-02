"""
ETL Pipeline: Raw GCS → Hive-Partitioned Canonical Panel → GCS
Each source writes its own partition independently. No interference.
Supports concurrent execution: run multiple instances for different sources.

Usage (Colab):
  !pip install pandas pyarrow pyyaml google-cloud-storage
  %run build_panel.py --source cook_county_il   # process one source
  %run build_panel.py --source sf_ca            # concurrent, different source
  %run build_panel.py --all                     # process all verified
  %run build_panel.py --dry-run                 # show status

Output structure:
  gs://properlytic-raw-data/panel/
    jurisdiction=cook_county_il/part.parquet
    jurisdiction=sf_ca/part.parquet
    jurisdiction=france_dvf/part.parquet
    ...
    _meta.json  (coverage report)

Training reads:
  pd.read_parquet("gs://properlytic-raw-data/panel/")  # all partitions
"""

import argparse
import io
import json
import yaml
import pandas as pd
import numpy as np
from pathlib import Path
from google.cloud import storage as gcs

# ─── Config ───
BUCKET_NAME = "properlytic-raw-data"
PROJECT_ID = "properlytic-data"
PANEL_PREFIX = "panel"
SCHEMA_PATH = Path(__file__).parent / "schema_registry.yaml"

CANONICAL_COLS = [
    "parcel_id", "jurisdiction", "year",
    "sale_price", "sale_date",
    "assessed_value", "land_value", "improvement_value",
    "dwelling_type", "sqft", "land_area",
    "year_built", "bedrooms", "bathrooms", "stories",
    "address", "lat", "lon",
]


def get_client():
    return gcs.Client(project=PROJECT_ID)


def load_schema():
    with open(SCHEMA_PATH) as f:
        return yaml.safe_load(f)


# ─── GCS Readers ───

def read_csv_chunks(client, prefix, max_chunks=None):
    """Read chunked CSVs from GCS → single DataFrame."""
    bucket = client.bucket(BUCKET_NAME)
    blobs = sorted(
        [b for b in bucket.list_blobs(prefix=prefix) if b.name.endswith(".csv")],
        key=lambda b: b.name
    )
    if max_chunks:
        blobs = blobs[:max_chunks]
    print(f"   Found {len(blobs)} CSV chunks")

    dfs = []
    for b in blobs:
        fname = b.name.split("/")[-1]
        print(f"   📄 {fname} ({b.size / 1e6:.1f} MB)")
        try:
            df = pd.read_csv(io.StringIO(b.download_as_text()), low_memory=False)
            dfs.append(df)
        except Exception as e:
            print(f"   ⚠️ {fname}: {e}")
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def read_single_csv(client, blob_path):
    """Read one CSV from GCS."""
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_path)
    if not blob.exists():
        print(f"   ❌ {blob_path} not found")
        return pd.DataFrame()
    return pd.read_csv(io.StringIO(blob.download_as_text()), low_memory=False)


def read_gz_csvs(client, prefix, max_files=None):
    """Read gzipped CSVs from GCS."""
    import gzip
    bucket = client.bucket(BUCKET_NAME)
    blobs = sorted(
        [b for b in bucket.list_blobs(prefix=prefix) if b.name.endswith(".csv.gz")],
        key=lambda b: b.name
    )
    if max_files:
        blobs = blobs[:max_files]
    print(f"   Found {len(blobs)} gzipped CSVs")

    dfs = []
    for b in blobs:
        fname = b.name.split("/")[-1]
        print(f"   📄 {fname} ({b.size / 1e6:.1f} MB)")
        try:
            raw = b.download_as_bytes()
            text = gzip.decompress(raw).decode("utf-8", errors="replace")
            df = pd.read_csv(io.StringIO(text), low_memory=False, sep=",")
            dfs.append(df)
        except Exception as e:
            print(f"   ⚠️ {fname}: {e}")
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


# ─── Column Mapping ───

def apply_mapping(df, config, jurisdiction):
    """Map source columns → canonical schema."""
    mapping = config.get("mapping", {})
    derived = config.get("derived", {})

    # Rename
    rename = {src: canon for canon, src in mapping.items()
              if src and src in df.columns}
    out = df.rename(columns=rename)

    # Derived fields
    for field, expr in derived.items():
        try:
            if "parse_point" in expr and "the_geom" in out.columns:
                coords = out["the_geom"].astype(str).str.extract(
                    r"POINT\s*\(\s*([-\d.]+)\s+([-\d.]+)\s*\)")
                out[field] = pd.to_numeric(
                    coords[0] if "[0]" in expr else coords[1], errors="coerce")
            elif "+" in expr:
                parts = [p.strip() for p in expr.split("+")]
                total = pd.Series(0.0, index=out.index)
                for part in parts:
                    if "*" in part:
                        coeff, col = part.split("*")
                        col = col.strip()
                        if col in out.columns:
                            total += float(coeff) * pd.to_numeric(out[col], errors="coerce")
                    elif part.strip("'\" ") in out.columns:
                        col = part.strip("'\" ")
                        total += pd.to_numeric(out[col], errors="coerce")
                out[field] = total
        except Exception as e:
            print(f"   ⚠️ derived '{field}': {e}")

    out["jurisdiction"] = jurisdiction
    keep = [c for c in CANONICAL_COLS if c in out.columns]
    return out[keep]


# ─── Per-Source Processing ───

def process_source(client, name, config, max_chunks=5):
    """Process one source → write partition to GCS."""
    print(f"\n{'='*60}")
    print(f"📦 {name}")

    if not config.get("verified"):
        print(f"   ⏭️ Not verified, skipping")
        return None

    fmt = config.get("format", "")
    prefix = config.get("gcs_prefix", "")

    # Read raw data
    if fmt == "csv_chunked":
        raw = read_csv_chunks(client, prefix, max_chunks)
    elif fmt == "csv_gz_yearly":
        raw = read_gz_csvs(client, prefix, max_chunks)
    elif fmt == "csv":
        raw = read_single_csv(client, prefix)
    elif fmt == "gdb_zip":
        print(f"   ⏭️ GDB format — requires geopandas (TODO)")
        return None
    else:
        print(f"   ⏭️ Unknown format: {fmt}")
        return None

    if raw.empty:
        print(f"   ❌ No data")
        return None

    print(f"   📊 Raw: {raw.shape[0]:,} × {raw.shape[1]}")

    # Map to canonical
    panel = apply_mapping(raw, config, name)
    print(f"   ✅ Panel: {panel.shape[0]:,} × {panel.shape[1]}")

    # Coverage
    for col in CANONICAL_COLS:
        if col in panel.columns:
            pct = panel[col].notna().mean() * 100
            print(f"      {col:24s} {pct:5.1f}%")
        else:
            print(f"      {col:24s}     —")

    # Write partition to GCS
    write_partition(client, panel, name)
    return panel


def write_partition(client, df, jurisdiction):
    """Write a single jurisdiction partition to GCS."""
    bucket = client.bucket(BUCKET_NAME)
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    size_mb = buf.tell() / 1e6

    blob_path = f"{PANEL_PREFIX}/jurisdiction={jurisdiction}/part.parquet"
    blob = bucket.blob(blob_path)
    buf.seek(0)
    blob.upload_from_file(buf, content_type="application/octet-stream")
    print(f"   💾 gs://{BUCKET_NAME}/{blob_path} ({size_mb:.1f} MB)")


def write_meta(client, results):
    """Write combined metadata."""
    bucket = client.bucket(BUCKET_NAME)
    meta = {
        "jurisdictions": list(results.keys()),
        "total_rows": sum(r["rows"] for r in results.values()),
        "per_source": results,
    }
    blob = bucket.blob(f"{PANEL_PREFIX}/_meta.json")
    blob.upload_from_string(json.dumps(meta, indent=2))
    print(f"\n📋 gs://{BUCKET_NAME}/{PANEL_PREFIX}/_meta.json")


# ─── Main ───

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=str, help="Single source to process")
    parser.add_argument("--all", action="store_true", help="Process all verified")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-chunks", type=int, default=5)
    args = parser.parse_args()

    schema = load_schema()
    sources = schema.get("sources", {})
    client = get_client()

    if args.dry_run:
        print("=== SOURCES ===")
        for name, cfg in sources.items():
            v = "✅" if cfg.get("verified") else "❌"
            cols = len(cfg.get("columns_verbatim", []))
            mapped = len(cfg.get("mapping", {}))
            print(f"  {v} {name:22s} fmt={cfg.get('format','?'):15s} "
                  f"cols={cols:2d} mapped={mapped:2d} prefix={cfg.get('gcs_prefix','?')}")
        # Check existing partitions
        print("\n=== EXISTING PARTITIONS ===")
        blobs = list(client.bucket(BUCKET_NAME).list_blobs(prefix=f"{PANEL_PREFIX}/"))
        for b in blobs:
            print(f"  📁 {b.name} ({b.size/1e6:.1f} MB)")
        return

    # Process
    results = {}
    if args.source:
        if args.source not in sources:
            print(f"Unknown: {args.source}. Available: {list(sources.keys())}")
            return
        p = process_source(client, args.source, sources[args.source], args.max_chunks)
        if p is not None:
            results[args.source] = {"rows": len(p), "cols": list(p.columns)}
    elif args.all:
        for name, cfg in sources.items():
            p = process_source(client, name, cfg, args.max_chunks)
            if p is not None:
                results[name] = {"rows": len(p), "cols": list(p.columns)}
    else:
        print("Specify --source <name> or --all")
        return

    if results:
        write_meta(client, results)
        total = sum(r["rows"] for r in results.values())
        print(f"\n🎯 DONE: {total:,} total rows across {len(results)} jurisdictions")


if __name__ == "__main__":
    main()
