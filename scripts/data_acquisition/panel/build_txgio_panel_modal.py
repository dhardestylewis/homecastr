"""
Build TxGIO Cross-Year Panel for World Model Training  (v2 — parallel + resumable)
===================================================================================
Architecture
------------
* One Modal container per year (modal.map fan-out → ~5–6× wall-time speedup).
* Per-year GCS checkpoints → re-run skips already-finished years automatically.
* ThreadPoolExecutor for concurrent per-county GDB/SHP reads within each year.
* Fixed index-alignment bug that caused 2019 to report 0 valid parcels.

Output: gs://properlytic-raw-data/panel/jurisdiction=txgio_texas/part.parquet

Checkpoints: gs://properlytic-raw-data/panel/jurisdiction=txgio_texas/checkpoints/year=<YYYY>.parquet
             Delete a checkpoint to force a specific year to reprocess.
"""

import modal
import os

app = modal.App("txgio-panel-builder")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("gdal-bin", "libgdal-dev")
    .pip_install(
        "google-cloud-storage",
        "pandas",
        "pyarrow",
        "geopandas",
        "fiona",
        "pyogrio",
    )
)

gcs_secret = modal.Secret.from_name("gcs-creds")

BUCKET = "properlytic-raw-data"
CHECKPOINT_PREFIX = "panel/jurisdiction=txgio_texas/checkpoints"
PANEL_PATH = "panel/jurisdiction=txgio_texas/part.parquet"
SUMMARY_PATH = "panel/jurisdiction=txgio_texas/summary.json"


# ─────────────────────────────────────────────────────────────────────────────
# Helper: runs inside each per-year container
# ─────────────────────────────────────────────────────────────────────────────

def _gcs_client():
    import json
    from google.cloud import storage
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    return client, client.bucket(BUCKET)


def _parse_year(blob_name: str) -> int | None:
    name = blob_name.split("/")[-1]
    try:
        yr_str = name.split("-")[0].replace("stratmap", "")
        yr = int(yr_str)
        return yr + 2000 if yr < 100 else yr
    except (ValueError, IndexError):
        return None


def _read_one_layer(args):
    """Worker: read a single GDB layer or SHP file (called from thread pool)."""
    path, layer, is_gdb = args
    import geopandas as gpd
    try:
        if is_gdb:
            gdf = gpd.read_file(
                path, layer=layer, engine="pyogrio",
                use_arrow=True, ignore_geometry=True,
            )
        else:
            gdf = gpd.read_file(
                path, engine="pyogrio",
                use_arrow=True, ignore_geometry=True,
            )
        # Normalize ALL column names to lowercase so pd.concat doesn't
        # create duplicate columns (e.g. LAND_VALUE vs land_value) when
        # GDB files across counties use inconsistent casing.
        gdf.columns = [c.lower() for c in gdf.columns]
        return gdf, None
    except Exception as e:
        return None, f"{path}[{layer}]: {e}"


def _build_value(df: "pd.DataFrame") -> "pd.Series":
    """
    Discover value columns and return a numeric float64 Series aligned to df.index.
    Bulletproof against Arrow-backed dtypes, nullable Int64/Float64, and missing cols.
    """
    import numpy as np
    import pandas as pd

    val_mapping = {}
    for col in df.columns:
        cl = col.lower()
        if "tot" in cl and ("val" in cl or "appr" in cl or "mkt" in cl):
            val_mapping["total_value"] = col
        elif "land" in cl and ("val" in cl or "appr" in cl):
            val_mapping["land_value"] = col
        elif ("impr" in cl or "imp_" in cl or "impv" in cl or "imprv" in cl) and (
            "val" in cl or "appr" in cl
        ):
            val_mapping["improvement_value"] = col
        elif "mkt" in cl and "val" in cl and "tot" not in cl:
            val_mapping["market_value"] = col

    print(f"  Value cols: {val_mapping}")

    n = len(df)

    def _parse(col_name: str) -> np.ndarray:
        """Parse a column to float64 numpy array, safe against all dtypes."""
        if col_name not in df.columns:
            return np.zeros(n, dtype=np.float64)
        s = df[col_name]
        # Convert Arrow-backed or categorical to plain Python objects first
        try:
            s = s.to_numpy(dtype=object, na_value=None)
        except Exception:
            s = s.values
        # Stringify numeric-looking objects and strip currency chars
        arr = []
        for v in s:
            if v is None:
                arr.append(np.nan)
            else:
                sv = str(v).replace("$", "").replace(",", "").strip()
                try:
                    arr.append(float(sv))
                except (ValueError, TypeError):
                    arr.append(np.nan)
        return np.array(arr, dtype=np.float64)

    parsed = {k: _parse(v) for k, v in val_mapping.items()}

    def _any_positive(arr: np.ndarray) -> bool:
        return bool(np.any(arr > 0))

    has_tot = "total_value" in parsed and _any_positive(parsed["total_value"])
    has_mkt = "market_value" in parsed and _any_positive(parsed["market_value"])

    if has_tot:
        result_arr = parsed["total_value"]
    elif has_mkt:
        result_arr = parsed["market_value"]
    else:
        land = parsed.get("land_value", np.zeros(n, dtype=np.float64))
        impr = parsed.get("improvement_value", np.zeros(n, dtype=np.float64))
        result_arr = np.nan_to_num(land, nan=0.0) + np.nan_to_num(impr, nan=0.0)

    return pd.Series(result_arr, index=df.index, dtype="float64")


# ─────────────────────────────────────────────────────────────────────────────
# Per-year worker function  (one Modal container per year)
# ─────────────────────────────────────────────────────────────────────────────

@app.function(
    image=image,
    secrets=[gcs_secret],
    timeout=7200,       # 2 h — generous per year
    memory=16384,       # 16 GB per container
    cpu=8,              # 8 vCPUs → feeds the thread pool
    # Note: Modal containers have ample /tmp scratch for zips up to ~15 GB
)
def process_one_year(blob_name: str) -> dict:
    """
    Download one TxGIO year zip, extract parcel values, save GCS checkpoint.
    Returns a summary dict (used by the orchestrator to detect failures).
    Skips if a checkpoint already exists.
    """
    import io, time, tempfile, zipfile, glob, shutil
    import pandas as pd
    from concurrent.futures import ThreadPoolExecutor, as_completed

    ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")

    client, bucket = _gcs_client()

    year = _parse_year(blob_name)
    if year is None:
        return {"error": f"Cannot parse year from {blob_name}"}

    # ── Checkpoint check ────────────────────────────────────────────────────
    ckpt_path = f"{CHECKPOINT_PREFIX}/year={year}.parquet"
    ckpt_blob = bucket.blob(ckpt_path)
    if ckpt_blob.exists():
        print(f"[{ts()}] ⏭️  Year {year}: checkpoint exists → skipping.")
        return {"year": year, "skipped": True}

    # ── Download ─────────────────────────────────────────────────────────────
    blob = bucket.blob(blob_name)
    tmpdir = tempfile.mkdtemp()
    zip_path = os.path.join(tmpdir, blob_name.split("/")[-1])

    print(f"[{ts()}] Year {year}: downloading {blob_name}...")
    blob.download_to_filename(zip_path)
    downloaded_gb = os.path.getsize(zip_path) / 1e9
    print(f"  Downloaded: {downloaded_gb:.1f} GB")

    # ── Extract ─────────────────────────────────────────────────────────────
    print(f"  Extracting...")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(tmpdir)
    os.remove(zip_path)  # free space immediately

    shp_files = glob.glob(os.path.join(tmpdir, "**/*.shp"), recursive=True)
    gdb_dirs  = [d for d in glob.glob(os.path.join(tmpdir, "**/*.gdb"), recursive=True)
                 if os.path.isdir(d)]
    print(f"  Found {len(shp_files)} shapefiles, {len(gdb_dirs)} GDBs")

    # ── Build task list for thread pool ─────────────────────────────────────
    import pyogrio
    tasks = [(p, None, False) for p in shp_files]
    for gdb_path in gdb_dirs:
        try:
            layers = pyogrio.list_layers(gdb_path)
            for layer in layers:
                layer_name = layer[0].lower()
                # Skip non-parcel layers (roads, boundaries, address pts, etc.)
                # Parcel layers contain 'parcel', 'landparcels', or county names
                skip_keywords = ('road', 'rd_', 'address', 'boundary', 'line', 'point',
                                 'centerline', 'annotation', 'trail', 'hydro', 'water')
                if any(kw in layer_name for kw in skip_keywords):
                    continue
                tasks.append((gdb_path, layer[0], True))
        except Exception as e:
            print(f"  ⚠️ Cannot list layers in {gdb_path}: {e}")

    # ── Concurrent reads (4 workers to avoid I/O saturation → heartbeat death) ─
    year_dfs = []
    errors   = []
    printed_cols = False
    print(f"  Reading {len(tasks)} layers with ThreadPoolExecutor(max_workers=4)...")
    with ThreadPoolExecutor(max_workers=4) as pool:
        futs = {pool.submit(_read_one_layer, t): t for t in tasks}
        for fut in as_completed(futs):
            gdf, err = fut.result()
            if err:
                errors.append(err)
            elif gdf is not None and not gdf.empty:
                if not printed_cols:
                    print(f"  Sample columns: {list(gdf.columns[:20])}{'...' if len(gdf.columns) > 20 else ''}")
                    printed_cols = True
                year_dfs.append(gdf)

    if errors:
        print(f"  ⚠️ {len(errors)} read errors (first 5):")
        for e in errors[:5]:
            print(f"     {e}")

    if not year_dfs:
        shutil.rmtree(tmpdir, ignore_errors=True)
        return {"year": year, "error": "No valid data read"}

    # ── Concat ───────────────────────────────────────────────────────────────
    df = pd.concat(year_dfs, ignore_index=True)
    del year_dfs  # release memory
    print(f"  {year}: {len(df):,} rows after concat")

    # ── Pre-filter: drop non-parcel rows (roads, boundaries, etc.) ───────────
    # Some zips include mixed layer types. Non-parcel layers have no value
    # columns, so any row where ALL value-like columns are null is excluded.
    val_like = [c for c in df.columns
                if any(k in c for k in ("land", "imp", "mkt", "tot_val", "total_v", "appr"))]
    if val_like:
        parcel_mask = df[val_like].notna().any(axis=1)
        n_before = len(df)
        df = df[parcel_mask].reset_index(drop=True)
        print(f"  After parcel-row pre-filter: {len(df):,} / {n_before:,} rows (dropped non-parcel layers)")
    print(f"  {year}: {len(df):,} parcels total")

    # ── Synthesize parcel ID ─────────────────────────────────────────────────
    fips_cols      = [c for c in df.columns if c.lower() in ("cnty_fips", "fips", "cntyfips", "cnty_cd", "state_cd")]
    if not fips_cols: fips_cols = [c for c in df.columns if "fips" in c.lower()]

    cnty_name_cols = [c for c in df.columns if c.lower() in ("cnty_nm", "county", "cnty_name")]

    # Pick the best-populated prop_id candidate (most non-null values wins)
    prop_candidates = [c for c in df.columns if c.lower() in ("prop_id", "pid", "prp_id", "parcel_id", "acct")]
    if not prop_candidates:
        prop_candidates = [c for c in df.columns if "id" in c.lower() and "geo" not in c.lower()]
    prop_cols = sorted(prop_candidates, key=lambda c: df[c].notna().sum(), reverse=True)

    geo_cols = [c for c in df.columns if c.lower() in ("geo_id", "geoid", "parcelfips")]

    if fips_cols and prop_cols:
        id_strategy = f"FIPS ({fips_cols[0]}) + PROP_ID ({prop_cols[0]})"
        acct_series = df[fips_cols[0]].astype(str).str.strip() + "_" + df[prop_cols[0]].astype(str).str.strip()
    elif cnty_name_cols and prop_cols:
        id_strategy = f"COUNTY ({cnty_name_cols[0]}) + PROP_ID ({prop_cols[0]})"
        acct_series = df[cnty_name_cols[0]].astype(str).str.strip() + "_" + df[prop_cols[0]].astype(str).str.strip()
    elif geo_cols:
        id_strategy = f"GEO_ID ({geo_cols[0]})"
        acct_series = df[geo_cols[0]].astype(str).str.strip()
    elif prop_cols:
        id_strategy = f"WARNING! Raw PROP_ID ({prop_cols[0]}) — collision risk"
        acct_series = df[prop_cols[0]].astype(str).str.strip()
    else:
        id_strategy = f"FATAL FALLBACK: {df.columns[0]}"
        acct_series = df[df.columns[0]].astype(str).str.strip()

    print(f"  ID Strategy: {id_strategy}")

    # ── Build result frame (keep df index intact so value slices stay aligned) ──
    bad_accts = {"none", "nan", "", "none_none", "nan_nan", "none_nan", "nan_none"}
    keep_mask = ~acct_series.str.lower().str.strip().isin(bad_accts) & acct_series.notna()
    df = df[keep_mask].copy()          # filter df so it stays aligned with acct_series
    acct_series = acct_series[keep_mask]
    print(f"  After dropping empty IDs: {len(df):,} rows")

    # Build value on the filtered df (indices are now consistent)
    value_series = _build_value(df)

    result = pd.DataFrame({
        "acct":  acct_series.values,
        "year":  year,
        "value": value_series.values,
    })

    result = result[result["value"] > 0].copy()
    print(f"  After value filter: {len(result):,} parcels with value > 0")
    print(f"  Value stats: mean=${result['value'].mean():,.0f}  median=${result['value'].median():,.0f}")

    shutil.rmtree(tmpdir, ignore_errors=True)

    if result.empty:
        return {"year": year, "error": "0 parcels with value > 0 after filtering"}

    # ── Save checkpoint to GCS ───────────────────────────────────────────────
    buf = io.BytesIO()
    result.to_parquet(buf, index=False)
    buf.seek(0)
    ckpt_blob.upload_from_file(buf, content_type="application/octet-stream")
    print(f"  ✅ Checkpoint saved: gs://{BUCKET}/{ckpt_path}")

    return {
        "year": year,
        "n_rows": len(result),
        "value_mean": float(result["value"].mean()),
        "value_median": float(result["value"].median()),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Orchestrator: fan-out + merge
# ─────────────────────────────────────────────────────────────────────────────

@app.function(
    image=image,
    secrets=[gcs_secret],
    timeout=14400,   # 4h — orchestrator blocks on modal.map() until all workers finish
    memory=8192,
    cpu=2,
)
def build_txgio_panel():
    """
    Orchestrator: fans out one container per year, waits for all to finish,
    reads checkpoints, merges, computes growth pct, uploads final panel.
    """
    import io, json, time
    import pandas as pd

    ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")

    client, bucket = _gcs_client()

    # Discover year zips
    txgio_blobs = [b for b in bucket.list_blobs(prefix="txgio/") if b.name.endswith(".zip")]
    print(f"[{ts()}] Found {len(txgio_blobs)} TxGIO zip files:")
    for b in txgio_blobs:
        size_str = f"{b.size / 1e9:.1f} GB" if b.size is not None else "? GB"
        print(f"    {size_str}  {b.name}")

    blob_names = [b.name for b in txgio_blobs]

    # ── Fan-out: return_exceptions=True so one bad year doesn't cancel others ──
    print(f"\n[{ts()}] Launching {len(blob_names)} parallel year containers...")
    year_results = list(process_one_year.map(blob_names, return_exceptions=True))

    for r in year_results:
        if isinstance(r, Exception):
            print(f"  ❌ Year container raised exception: {r}")
        elif isinstance(r, dict) and "error" in r:
            print(f"  ⚠️ Year {r.get('year', '?')} failed: {r['error']}")
        elif isinstance(r, dict) and r.get("skipped"):
            print(f"  ⏭️  Year {r['year']} was already checkpointed.")
        elif isinstance(r, dict):
            print(f"  ✅ Year {r['year']}: {r['n_rows']:,} rows  "
                  f"mean=${r['value_mean']:,.0f}  median=${r['value_median']:,.0f}")
        else:
            print(f"  ⚠️ Unexpected result type: {type(r)} — {r}")

    # ── Load all checkpoints ─────────────────────────────────────────────────
    print(f"\n[{ts()}] Loading checkpoints...")
    ckpt_blobs = list(bucket.list_blobs(prefix=CHECKPOINT_PREFIX + "/"))
    all_dfs = []
    for cb in ckpt_blobs:
        buf = io.BytesIO(cb.download_as_bytes())
        all_dfs.append(pd.read_parquet(buf))
        print(f"  Loaded {cb.name}")

    if not all_dfs:
        print("❌ No checkpoint data found — aborting.")
        return {"error": "no data"}

    # ── Merge + growth ───────────────────────────────────────────────────────
    panel = pd.concat(all_dfs, ignore_index=True)
    panel = panel.sort_values(["acct", "year"]).reset_index(drop=True)
    panel["prior_value"] = panel.groupby("acct")["value"].shift(1)
    panel["growth_pct"]  = (panel["value"] - panel["prior_value"]) / panel["prior_value"]

    print(f"\n[{ts()}] Final panel:")
    print(f"  Total rows:      {panel.shape[0]:,}")
    print(f"  Unique parcels:  {panel['acct'].nunique():,}")
    print(f"  Years:           {sorted(panel['year'].unique().tolist())}")
    print(f"\n{panel[['value', 'growth_pct']].describe().to_string()}")

    # ── Upload final panel ───────────────────────────────────────────────────
    print(f"\n[{ts()}] Uploading final panel to GCS...")
    buf = io.BytesIO()
    panel.to_parquet(buf, index=False)
    size_mb = buf.tell() / 1e6
    buf.seek(0)
    bucket.blob(PANEL_PATH).upload_from_file(buf, content_type="application/octet-stream")
    print(f"  ✅ gs://{BUCKET}/{PANEL_PATH}  ({size_mb:.0f} MB)")

    summary = {
        "n_rows":       int(len(panel)),
        "n_parcels":    int(panel["acct"].nunique()),
        "years":        sorted(panel["year"].unique().tolist()),
        "value_mean":   float(panel["value"].mean()),
        "value_median": float(panel["value"].median()),
    }
    bucket.blob(SUMMARY_PATH).upload_from_string(json.dumps(summary, indent=2))
    print(f"  ✅ Summary saved")

    return summary


# ─────────────────────────────────────────────────────────────────────────────
# Local entry-point
# ─────────────────────────────────────────────────────────────────────────────

@app.local_entrypoint()
def main():
    result = build_txgio_panel.remote()
    print(f"\n✅ TxGIO panel: {result}")
