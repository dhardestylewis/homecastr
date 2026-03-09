import modal
import os
import duckdb

app = modal.App("txgio-deepdive")
image = modal.Image.debian_slim(python_version="3.11").pip_install("duckdb", "google-cloud-storage", "polars")
gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])

@app.function(image=image, secrets=[gcs_secret])
def run_deepdive():
    import json
    # Set up credentials
    creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON", "")
    if creds_json:
        creds_path = "/tmp/gcs_creds.json"
        with open(creds_path, "w") as f:
            f.write(creds_json)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
        
    url = "gs://properlytic-raw-data/panel/jurisdiction=txgio_texas/part.parquet"
    
    # We can use duckdb's built-in duckdb.connect() with python aws extension or just read it via polars
    import polars as pl
    print("Downloading panel...")
    df = pl.read_parquet("gs://properlytic-raw-data/panel/jurisdiction=txgio_texas/part.parquet")
    print("Loaded panel. Rows:", len(df))
    
    # 1. Look at duplicates in 2021
    df_2021 = df.filter(pl.col("year") == 2021)
    print("\nTotal accounts in 2021:", len(df_2021))
    dup_counts = df_2021.group_by("acct").agg(pl.len().alias("count")).filter(pl.col("count") > 1).sort("count", descending=True)
    print("Number of duplicate accts in 2021:", len(dup_counts))
    if len(dup_counts) > 0:
        top_dup = dup_counts["acct"][0]
        print(f"Top duplicated acct in 2021: '{top_dup}' (occurs {dup_counts['count'][0]} times)")
        print(df_2021.filter(pl.col("acct") == top_dup).head(10))
    
    # 2. Look at cross-year overlap
    years = sorted(df["year"].unique().to_list())
    print("\nOverlap between adjacent years:")
    for i in range(1, len(years)):
        y1, y2 = years[i-1], years[i]
        accts_y1 = set(df.filter(pl.col("year") == y1)["acct"].to_list())
        accts_y2 = set(df.filter(pl.col("year") == y2)["acct"].to_list())
        intersect = accts_y1.intersection(accts_y2)
        print(f"  {y1} ∩ {y2}: {len(intersect):,} accounts overlap (out of {len(accts_y2):,} in {y2})")
    
    # 3. Look at sample of IDs from each year
    print("\nSample IDs by year:")
    for y in years:
        sample = df.filter(pl.col("year") == y)["acct"].head(5).to_list()
        print(f"  {y}: {sample}")

@app.local_entrypoint()
def main():
    run_deepdive.remote()
