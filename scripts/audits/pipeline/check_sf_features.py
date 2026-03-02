"""Check SF features and write results to GCS as clean JSON."""
import modal, os

app = modal.App("check-sf-features")
image = modal.Image.debian_slim(python_version="3.11").pip_install(
    "google-cloud-storage", "polars", "pyarrow"
)
gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])


@app.function(image=image, secrets=[gcs_secret], timeout=120)
def check():
    import json
    import polars as pl
    from google.cloud import storage

    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")

    blob = bucket.blob("panel/jurisdiction=sf_ca/part.parquet")
    blob.download_to_filename("/tmp/sf_panel.parquet")
    df = pl.read_parquet("/tmp/sf_panel.parquet")

    result = {"rows": len(df), "raw_columns": df.columns}

    rename_map = {
        "parcel_id": "acct", "year": "yr",
        "sqft": "living_area", "land_area": "land_ar",
        "year_built": "yr_blt", "bedrooms": "bed_cnt",
        "bathrooms": "full_bath", "stories": "nbr_story",
        "lat": "gis_lat", "lon": "gis_lon",
    }
    actual_renames = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(actual_renames)

    val_cols = [c for c in ["sale_price", "property_value", "assessed_value"] if c in df.columns]
    if val_cols:
        df = df.with_columns(pl.coalesce([pl.col(c) for c in val_cols]).alias("tot_appr_val"))

    leaky = ["sale_price", "property_value", "assessed_value", "land_value", "improvement_value"]
    drop = [c for c in leaky if c in df.columns]
    df = df.drop(drop)
    result["dropped_leaky"] = drop

    null_counts = df.null_count()
    n_rows = len(df)
    all_null = [c for c in df.columns if null_counts[c][0] == n_rows and c not in ("acct", "yr", "tot_appr_val")]
    if all_null:
        df = df.drop(all_null)
    result["dropped_all_null"] = all_null

    schema = {c: str(df.schema[c]) for c in df.columns}
    cat_cols = [c for c in df.columns if c not in ["acct", "yr", "tot_appr_val"] and "str" in schema.get(c, "").lower()]
    num_cols = [c for c in df.columns if c not in ["acct", "yr", "tot_appr_val"] and c not in cat_cols]

    null_type = [c for c in num_cols if schema[c].lower() in ("null", "unknown")]
    num_cols = [c for c in num_cols if c not in set(null_type)]

    NUM_DROP = {
        "bld_mean_accrued_depr_pct", "bld_sum_cama_replacement_cost",
        "tot_mkt_val_lag1", "assessed_val_lag1", "land_val_lag1",
        "bld_val_lag1", "x_features_val_lag1", "ag_val_lag1",
        "new_construction_val_lag1", "tot_rcn_val_lag1", "gis_year_used",
    }
    dropped_num = [c for c in num_cols if c in NUM_DROP]
    num_cols = [c for c in num_cols if c not in NUM_DROP]

    promoted = [c for c in ["gis_lat", "gis_lon"] if c in num_cols]
    remaining = [c for c in num_cols if c not in set(promoted)]
    num_cols = promoted + remaining

    result["dropped_null_type"] = null_type
    result["dropped_NUM_DROP"] = dropped_num
    result["num_features"] = []
    for c in num_cols:
        sample = df[c].drop_nulls().head(5).to_list()
        null_pct = round(float(df[c].null_count()) / n_rows * 100, 1)
        result["num_features"].append({
            "name": c, "dtype": schema[c], "null_pct": null_pct,
            "sample": [str(x) for x in sample]
        })
    result["cat_features"] = []
    for c in cat_cols:
        sample = df[c].drop_nulls().head(3).to_list()
        result["cat_features"].append({"name": c, "sample": sample})

    # Write to GCS
    out = json.dumps(result, indent=2)
    blob = bucket.blob("diagnostics/sf_feature_audit.json")
    blob.upload_from_string(out, content_type="application/json")
    print("Wrote sf_feature_audit.json to GCS")
    print(out)
