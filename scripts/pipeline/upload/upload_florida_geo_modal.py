"""
Upload Florida DOR parcel point-geometries to Supabase via Modal.
================================================================
Reads the latest NAL ZIP from GCS, extracts PARCEL_ID + CO_NO + LAT_DD + LON_DD,
buffers points into small squares, and inserts into public.geo_parcel_poly.

Usage:
  python -m modal run scripts/pipeline/upload/upload_florida_geo_modal.py
"""

import modal
import os

app = modal.App("florida-geo-upload")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install("google-cloud-storage", "polars", "psycopg2-binary")
)

gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
sb_secret = modal.Secret.from_name("supabase-db", required_keys=["POSTGRES_URL_NON_POOLING"])

GCS_BUCKET = "properlytic-raw-data"
DOR_PREFIX = "florida_dor"
BUFFER_DEG = 0.00015  # ~15m radius


@app.function(
    image=image,
    secrets=[gcs_secret, sb_secret],
    timeout=3600,
    memory=8192,
)
def extract_and_upload():
    import json
    import zipfile
    import tempfile
    import polars as pl
    import psycopg2
    import psycopg2.extras
    from google.cloud import storage

    # ── GCS client ──
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket(GCS_BUCKET)

    # ── Find latest NAL ZIP (prefer S > P > F for 2025, then 2024, ...) ──
    nal_blob_name = None
    for year in range(2025, 2001, -1):
        for sfx in ["S", "P", "F"]:
            candidate = f"{DOR_PREFIX}/NAL/{year}{sfx}.zip"
            if bucket.blob(candidate).exists():
                nal_blob_name = candidate
                break
        if nal_blob_name:
            break

    if not nal_blob_name:
        return {"status": "error", "reason": "No NAL ZIP found"}

    print(f"Using NAL file: {nal_blob_name}")

    # ── Download ZIP ──
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
    bucket.blob(nal_blob_name).download_to_filename(tmp.name)
    print(f"Downloaded {os.path.getsize(tmp.name)/1e6:.1f} MB")

    # ── Extract all county CSVs and collect lat/lon ──
    all_rows = []
    with zipfile.ZipFile(tmp.name) as zf:
        csv_names = sorted([n for n in zf.namelist() if n.endswith('.csv')])
        print(f"Found {len(csv_names)} CSVs in ZIP")

        for csv_name in csv_names:
            try:
                with zf.open(csv_name) as f:
                    df = pl.read_csv(f.read(), infer_schema_length=0)

                # Check if lat/lon columns exist
                has_lat = "LAT_DD" in df.columns
                has_lon = "LON_DD" in df.columns
                has_parcel = "PARCEL_ID" in df.columns
                has_county = "CO_NO" in df.columns

                if not (has_parcel and has_county):
                    print(f"  {csv_name}: missing PARCEL_ID or CO_NO, skipping")
                    continue

                if not (has_lat and has_lon):
                    print(f"  {csv_name}: missing LAT_DD/LON_DD, skipping")
                    continue

                sub = df.select(["CO_NO", "PARCEL_ID", "LAT_DD", "LON_DD"])
                sub = sub.with_columns([
                    pl.col("LAT_DD").cast(pl.Float64, strict=False).alias("lat"),
                    pl.col("LON_DD").cast(pl.Float64, strict=False).alias("lon"),
                    pl.concat_str([pl.col("CO_NO"), pl.col("PARCEL_ID")]).alias("acct"),
                ])
                sub = sub.filter(
                    pl.col("lat").is_not_null()
                    & pl.col("lon").is_not_null()
                    & (pl.col("lat").abs() > 1)
                    & (pl.col("lon").abs() > 1)
                )
                sub = sub.unique(subset=["acct"])
                print(f"  {csv_name}: {len(sub):,} valid parcels with coords")
                all_rows.append(sub.select(["acct", "lat", "lon"]))

            except Exception as e:
                print(f"  {csv_name}: error: {e}")

    os.unlink(tmp.name)

    if not all_rows:
        return {"status": "error", "reason": "No lat/lon data found in any CSV"}

    combined = pl.concat(all_rows).unique(subset=["acct"])
    print(f"\nTotal unique parcels with coords: {len(combined):,}")

    # ── Upload to Supabase ──
    db_url = os.environ["POSTGRES_URL_NON_POOLING"]
    conn = psycopg2.connect(db_url)
    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '30min';")

    inserted = 0
    batch = []
    batch_size = 5000

    with conn.cursor() as cur:
        for row in combined.iter_rows(named=True):
            acct = str(row["acct"])
            lat = row["lat"]
            lon = row["lon"]
            batch.append((
                acct,
                lon - BUFFER_DEG,
                lat - BUFFER_DEG,
                lon + BUFFER_DEG,
                lat + BUFFER_DEG,
            ))

            if len(batch) >= batch_size:
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO public.geo_parcel_poly (acct, geom) VALUES %s ON CONFLICT (acct) DO NOTHING",
                    batch,
                    template="(%s, ST_Multi(ST_MakeEnvelope(%s, %s, %s, %s, 4326)))",
                    page_size=batch_size,
                )
                inserted += len(batch)
                if inserted % 100000 == 0:
                    print(f"Inserted: {inserted:,} / {len(combined):,}")
                batch = []

        if batch:
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO public.geo_parcel_poly (acct, geom) VALUES %s ON CONFLICT (acct) DO NOTHING",
                batch,
                template="(%s, ST_Multi(ST_MakeEnvelope(%s, %s, %s, %s, 4326)))",
                page_size=batch_size,
            )
            inserted += len(batch)

    conn.close()
    print(f"\n✅ Done. Sent {inserted:,} parcel geometries to Supabase.")
    return {"status": "ok", "total": inserted}


@app.local_entrypoint()
def main():
    result = extract_and_upload.remote()
    print(result)
