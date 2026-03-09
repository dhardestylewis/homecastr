"""
stage_macro_lookup.py
=====================
One-shot utility to pre-stage the annual FRED macro lookup table in GCS.

Builds gs://properlytic-raw-data/macro/macro_annual_lookup.parquet
  Columns: yr (Int32), macro_mortgage30, macro_fedfunds, macro_10yr_treasury,
           macro_cpi_us, macro_cpi_eurozone, macro_oil_price,
           macro_unemployment_us, macro_unemployment_eurozone, macro_vix,
           macro_global_epu, macro_euribor_3m

Run once (or quarterly when FRED data is refreshed):
    modal run scripts/inference/utils/stage_macro_lookup.py

inference_modal_parallel.py checks for this blob at step 5b (macro enrichment)
and uses it as a fast path instead of downloading 11 CSVs per shard.
"""
import modal, os, json, io

app = modal.App("stage-macro-lookup")

image = (
    modal.Image.from_registry("pytorch/pytorch:2.3.0-cuda12.1-cudnn8-runtime", add_python="3.11")
    .pip_install("google-cloud-storage", "pandas", "polars", "pyarrow")
)

gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])

MACRO_SERIES = {
    "macro/fred/MORTGAGE30US.csv":            "macro_mortgage30",
    "macro/fred/FEDFUNDS.csv":                "macro_fedfunds",
    "macro/fred/DGS10.csv":                   "macro_10yr_treasury",
    "macro/fred/CPIAUCSL.csv":                "macro_cpi_us",
    "macro/fred/CP0000EZ19M086NEST.csv":      "macro_cpi_eurozone",
    "macro/fred/DCOILWTICO.csv":              "macro_oil_price",
    "macro/fred/UNRATE.csv":                  "macro_unemployment_us",
    "macro/fred/LRHUTTTTEZM156S.csv":         "macro_unemployment_eurozone",
    "macro/fred/VIXCLS.csv":                  "macro_vix",
    "macro/fred/GEPUCURRENT.csv":             "macro_global_epu",
    "macro/fred/IR3TIB01EZM156N.csv":         "macro_euribor_3m",
}

OUTPUT_BLOB = "macro/macro_annual_lookup.parquet"
GCS_BUCKET  = "properlytic-raw-data"


@app.function(image=image, secrets=[gcs_secret], timeout=600, memory=4096)
def build_and_upload():
    import pandas as pd
    import polars as pl
    from google.cloud import storage

    creds  = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket(GCS_BUCKET)

    macro_frames = {}
    errors = []

    for gcs_path, col_name in MACRO_SERIES.items():
        blob = bucket.blob(gcs_path)
        if not blob.exists():
            print(f"  ⚠️  Missing: {gcs_path} — skipping")
            errors.append(gcs_path)
            continue
        try:
            raw = pd.read_csv(io.BytesIO(blob.download_as_bytes()), on_bad_lines="skip")
            if len(raw.columns) < 2:
                print(f"  ⚠️  {gcs_path}: too few columns, skipping")
                continue
            date_col, val_col = raw.columns[0], raw.columns[1]
            raw[date_col] = pd.to_datetime(raw[date_col], errors="coerce")
            raw[val_col]  = pd.to_numeric(raw[val_col],  errors="coerce")
            raw = raw.dropna(subset=[date_col, val_col])
            raw["_year"] = raw[date_col].dt.year
            annual = raw.groupby("_year")[val_col].mean().reset_index()
            annual.columns = ["_year", col_name]
            macro_frames[col_name] = annual
            print(f"  ✅ {col_name}: {len(annual)} years [{int(annual['_year'].min())}–{int(annual['_year'].max())}]")
        except Exception as e:
            print(f"  ❌ {gcs_path}: {e}")
            errors.append(gcs_path)

    if not macro_frames:
        raise RuntimeError("No macro frames loaded — aborting upload.")

    # Outer-join all series on year
    merged = None
    for col_name, frame in macro_frames.items():
        merged = frame if merged is None else merged.merge(frame, on="_year", how="outer")
    merged = merged.sort_values("_year").reset_index(drop=True)

    # Convert to Polars and cast year to Int32
    macro_pl = pl.from_pandas(merged).rename({"_year": "yr"}).cast({"yr": pl.Int32})

    print(f"\n📊 Lookup table: {len(macro_pl)} rows × {len(macro_pl.columns)} cols")
    print(f"   Columns: {macro_pl.columns}")
    print(f"   Year range: {macro_pl['yr'].min()} – {macro_pl['yr'].max()}")

    # Serialize to parquet and upload
    buf = io.BytesIO()
    macro_pl.write_parquet(buf)
    buf.seek(0)

    bucket.blob(OUTPUT_BLOB).upload_from_file(buf, content_type="application/octet-stream")
    print(f"\n✅ Uploaded {OUTPUT_BLOB} to gs://{GCS_BUCKET}/")

    if errors:
        print(f"\n⚠️  {len(errors)} series had errors: {errors}")

    return {
        "rows": len(macro_pl),
        "cols": macro_pl.columns,
        "output": f"gs://{GCS_BUCKET}/{OUTPUT_BLOB}",
        "errors": errors,
    }


@app.local_entrypoint()
def main():
    result = build_and_upload.remote()
    print(f"\n🎉 Done: {result['rows']} years, {len(result['cols'])} cols → {result['output']}")
    if result["errors"]:
        print(f"   Errors: {result['errors']}")
