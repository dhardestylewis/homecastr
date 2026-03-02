"""
maintenance_modal.py
====================
Unified Modal script for all long-running maintenance tasks.
Run with --detach so your laptop can close/sleep.

Jobs (each runs as a separate Modal function):
  1. create-indexes   — Build btree indexes on all forecast/history tables
  2. fix-hcad-history — Rebuild metrics_zcta/tract/tabblock_history for HCAD from parcel history
  3. download-acs     — Download ACS B25077 (2018-2023) to GCS
  4. download-fhfa    — Download FHFA HPI (state/MSA/ZIP3) to GCS

Usage:
  modal run --detach scripts/pipeline/maintenance_modal.py            # all jobs
  modal run --detach scripts/pipeline/maintenance_modal.py::indexes   # just indexes
  modal run --detach scripts/pipeline/maintenance_modal.py::fix_history
  modal run --detach scripts/pipeline/maintenance_modal.py::downloads
"""

import modal

app = modal.App("properlytic-maintenance")

supabase_secret = modal.Secret.from_name("supabase-creds", required_keys=["SUPABASE_DB_URL"])
gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])

base_image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install("psycopg2-binary", "requests", "google-cloud-storage")
)

SCHEMA = "forecast_20260220_7f31c6e4"

# ─────────────────────────────────────────────────────────────────────────────
# Job 1: Create indexes on all forecast/history tables
# ─────────────────────────────────────────────────────────────────────────────
@app.function(
    image=base_image,
    secrets=[supabase_secret],
    timeout=7200,  # 2h — CONCURRENTLY indexes can take a while
)
def run_create_indexes():
    import os, psycopg2, time

    conn = psycopg2.connect(os.environ["SUPABASE_DB_URL"], connect_timeout=30)
    conn.autocommit = True

    s = SCHEMA
    specs = [
        # History tables — fast key-based lookups
        (f"{s}.metrics_zcta_history",     "idx_mzh_zcta5",        "zcta5"),
        (f"{s}.metrics_zcta_history",     "idx_mzh_year",         "year"),
        (f"{s}.metrics_tract_history",    "idx_mth_tract",        "tract_geoid20"),
        (f"{s}.metrics_tract_history",    "idx_mth_year",         "year"),
        (f"{s}.metrics_tabblock_history", "idx_mtbh_tabblock",    "tabblock_geoid20"),
        (f"{s}.metrics_tabblock_history", "idx_mtbh_year",        "year"),
        (f"{s}.metrics_parcel_history",   "idx_mph_acct",         "acct"),
        (f"{s}.metrics_parcel_history",   "idx_mph_year",         "year"),
        # Forecast tables
        (f"{s}.metrics_zcta_forecast",     "idx_mzf_zcta5",       "zcta5"),
        (f"{s}.metrics_zcta_forecast",     "idx_mzf_origin",      "origin_year"),
        (f"{s}.metrics_tract_forecast",    "idx_mtf_tract",       "tract_geoid20"),
        (f"{s}.metrics_tract_forecast",    "idx_mtf_origin",      "origin_year"),
        (f"{s}.metrics_tabblock_forecast", "idx_mtbf_tabblock",   "tabblock_geoid20"),
        (f"{s}.metrics_tabblock_forecast", "idx_mtbf_origin",     "origin_year"),
        (f"{s}.metrics_parcel_forecast",   "idx_mpf_acct",        "acct"),
        (f"{s}.metrics_parcel_forecast",   "idx_mpf_origin",      "origin_year"),
        # Public crosswalk
        ("public.parcel_ladder_v1",        "idx_pl1_acct",        "acct"),
        ("public.parcel_ladder_v1",        "idx_pl1_zcta5",       "zcta5"),
        ("public.parcel_ladder_v1",        "idx_pl1_tract",       "tract_geoid20"),
        ("public.parcel_ladder_v1",        "idx_pl1_tabblock",    "tabblock_geoid20"),
    ]

    created = skipped = errors = 0
    with conn.cursor() as cur:
        for table, idx_name, col in specs:
            # Fast catalog check — no table scan
            schema_name, tbl = (table.split(".", 1) if "." in table else ("public", table))
            cur.execute("SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname=%s AND tablename=%s)", (schema_name, tbl))
            if not cur.fetchone()[0]:
                print(f"  SKIP {table} — table does not exist")
                continue
            cur.execute("SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE indexname=%s)", (idx_name,))
            if cur.fetchone()[0]:
                print(f"  EXISTS {idx_name}")
                skipped += 1
                continue
            try:
                t0 = time.time()
                cur.execute(f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {idx_name} ON {table} ({col})")
                print(f"  ✅ {idx_name} on {table}({col}) [{time.time()-t0:.1f}s]")
                created += 1
            except Exception as e:
                print(f"  ⚠️  {idx_name}: {e}")
                errors += 1

        # ANALYZE all tables
        tables = set(t for t, _, _ in specs)
        for table in sorted(tables):
            try:
                cur.execute(f"ANALYZE {table}")
                print(f"  ANALYZE {table}")
            except Exception as e:
                print(f"  ANALYZE error {table}: {e}")

    conn.close()
    return {"created": created, "skipped": skipped, "errors": errors}


# ─────────────────────────────────────────────────────────────────────────────
# Job 2: Rebuild HCAD aggregate history (zcta + tract + tabblock)
# ─────────────────────────────────────────────────────────────────────────────
@app.function(
    image=base_image,
    secrets=[supabase_secret],
    timeout=7200,
)
def run_fix_hcad_history():
    import os, psycopg2, time

    conn = psycopg2.connect(os.environ["SUPABASE_DB_URL"], connect_timeout=30)
    conn.autocommit = True
    s = SCHEMA

    geo_levels = [
        ("zcta5",            "metrics_zcta_history",     "zcta5",            "zcta5"),
        ("tract_geoid20",    "metrics_tract_history",    "tract_geoid20",    "tract_geoid20"),
        ("tabblock_geoid20", "metrics_tabblock_history", "tabblock_geoid20", "tabblock_geoid20"),
    ]

    total = 0
    with conn.cursor() as cur:
        # Get available years from parcel history
        cur.execute(f"SELECT year FROM {s}.metrics_parcel_history WHERE jurisdiction='hcad' GROUP BY year ORDER BY year")
        years = [r[0] for r in cur.fetchall()]
        print(f"Found {len(years)} years in parcel history: {years}")

        for ldr_col, hist_table, key_col, conflict_col in geo_levels:
            print(f"\n=== Rebuilding {hist_table} ===")
            for yr in years:
                t0 = time.time()
                try:
                    cur.execute(f"""
                        INSERT INTO {s}.{hist_table} ({key_col}, year, value, p50, series_kind, variant_id, jurisdiction)
                        SELECT
                            l.{ldr_col}, ph.year,
                            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ph.value),
                            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ph.value),
                            ph.series_kind, ph.variant_id, 'hcad'
                        FROM {s}.metrics_parcel_history ph
                        JOIN public.parcel_ladder_v1 l ON l.acct = ph.acct
                        WHERE l.{ldr_col} IS NOT NULL AND ph.year = {yr} AND ph.jurisdiction = 'hcad'
                        GROUP BY l.{ldr_col}, ph.year, ph.series_kind, ph.variant_id
                        ON CONFLICT ({conflict_col}, year, series_kind, variant_id)
                        DO UPDATE SET value = EXCLUDED.value, p50 = EXCLUDED.p50, jurisdiction = 'hcad'
                    """)
                    n = cur.rowcount
                    total += n
                    print(f"  year={yr}: {n} rows [{time.time()-t0:.1f}s]")
                except Exception as e:
                    print(f"  year={yr}: ERROR - {e}")

    conn.close()
    print(f"\n✅ Done — {total} total rows upserted")
    return {"total_rows": total}


# ─────────────────────────────────────────────────────────────────────────────
# Job 3: Download ACS B25077 + FHFA HPI to GCS
# ─────────────────────────────────────────────────────────────────────────────
@app.function(
    image=base_image,
    secrets=[gcs_secret],
    timeout=3600,
)
def run_reference_downloads():
    import os, json, time, requests
    from google.cloud import storage
    from google.oauth2 import service_account

    t0_total = time.time()
    creds_info = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    creds = service_account.Credentials.from_service_account_info(creds_info)
    client = storage.Client(credentials=creds, project="properlytic-data")
    bucket = client.bucket("properlytic-raw-data")
    results = {}

    # ACS B25077: 2018-2023
    for year in range(2018, 2024):
        blob_name = f"census/ACS_B25077_home_value_{year}.zip"
        blob = bucket.blob(blob_name)
        if blob.exists():
            print(f"ACS {year}: already exists, skipping")
            results[f"ACS_{year}"] = "skipped"
            continue
        url = (f"https://www2.census.gov/programs-surveys/acs/summary_file/{year}"
               f"/table-based-SF/{year}_ACS_Detailed_Tables_Group_B25077.zip")
        print(f"ACS {year}: downloading {url}")
        try:
            r = requests.get(url, timeout=300, stream=True)
            r.raise_for_status()
            data = b"".join(r.iter_content(1024 * 1024))
            blob.upload_from_string(data, content_type="application/zip")
            mb = len(data) / 1e6
            print(f"ACS {year}: ✅ {mb:.1f} MB → {blob_name}")
            results[f"ACS_{year}"] = f"ok ({mb:.1f} MB)"
        except Exception as e:
            print(f"ACS {year}: ❌ {e}")
            results[f"ACS_{year}"] = f"error: {e}"

    # FHFA HPI
    fhfa = {
        "fhfa/HPI_AT_state.csv": "https://www.fhfa.gov/DataTools/Downloads/Documents/HPI/HPI_AT_state.csv",
        "fhfa/HPI_AT_metro.csv": "https://www.fhfa.gov/DataTools/Downloads/Documents/HPI/HPI_AT_metro.csv",
        "fhfa/HPI_AT_3zip.csv":  "https://www.fhfa.gov/DataTools/Downloads/Documents/HPI/HPI_AT_3zip.csv",
    }
    headers = {"User-Agent": "Mozilla/5.0 Properlytic-DataBot/1.0"}
    for blob_name, url in fhfa.items():
        blob = bucket.blob(blob_name)
        if blob.exists():
            print(f"FHFA {blob_name}: already exists, skipping")
            results[blob_name] = "skipped"
            continue
        print(f"FHFA: downloading {url}")
        try:
            r = requests.get(url, timeout=120, headers=headers)
            r.raise_for_status()
            if b"<html" in r.content[:200].lower():
                raise ValueError("Got HTML instead of CSV")
            blob.upload_from_string(r.content, content_type="text/csv")
            mb = len(r.content) / 1e6
            print(f"FHFA {blob_name}: ✅ {mb:.2f} MB")
            results[blob_name] = f"ok ({mb:.2f} MB)"
        except Exception as e:
            print(f"FHFA {blob_name}: ❌ {e}")
            results[blob_name] = f"error: {e}"

    print(f"\n✅ Downloads done in {(time.time()-t0_total)/60:.1f} min")
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Job 4: DB diagnostic — run on Modal so connection is reliable (AWS→Supabase)
# Never run DB diagnostics via local psycopg2 through pooler — they hang
# ─────────────────────────────────────────────────────────────────────────────
@app.function(
    image=base_image,
    secrets=[supabase_secret],
    timeout=120,
)
def run_db_diagnostic():
    import os, psycopg2, time

    conn = psycopg2.connect(os.environ["SUPABASE_DB_URL"], connect_timeout=10)
    conn.autocommit = True
    s = SCHEMA
    results = {}

    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '30s'")

        # Tables in forecast schema
        cur.execute("""
            SELECT t.tablename,
                   pg_size_pretty(pg_total_relation_size(quote_ident(t.schemaname)||'.'||quote_ident(t.tablename))) AS size,
                   (SELECT reltuples::bigint FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = t.schemaname AND c.relname = t.tablename) AS est_rows
            FROM pg_tables t
            WHERE t.schemaname = %s
            ORDER BY t.tablename
        """, (s,))
        tables = cur.fetchall()
        print(f"\n=== Tables in {s} ===")
        for name, size, est_rows in tables:
            print(f"  {name:45s} {size:>10s}  ~{est_rows or 0:>12,} rows")
            results[name] = {"size": size, "est_rows": est_rows}

        # Indexes
        cur.execute("""
            SELECT tablename, indexname
            FROM pg_indexes
            WHERE schemaname = %s
            ORDER BY tablename, indexname
        """, (s,))
        idxs = cur.fetchall()
        print(f"\n=== Indexes in {s} ===")
        for tbl, idx in idxs:
            print(f"  {tbl}: {idx}")

        # Sample from each history table (existence check)
        print("\n=== History table samples ===")
        for tbl in ["metrics_zcta_history", "metrics_tract_history",
                    "metrics_tabblock_history", "metrics_parcel_history"]:
            try:
                cur.execute(f"SELECT year, value FROM {s}.{tbl} LIMIT 1")
                row = cur.fetchone()
                print(f"  {tbl}: {'HAS DATA — ' + str(row) if row else 'EMPTY'}")
            except Exception as e:
                print(f"  {tbl}: ERROR — {e}")

    conn.close()
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Orchestrator: runs on Modal cloud, calls all sub-jobs via .remote()
# ─────────────────────────────────────────────────────────────────────────────
@app.function(
    image=base_image,
    secrets=[supabase_secret, gcs_secret],
    timeout=21600,  # 6h
)
def run_all_maintenance():
    """Cloud orchestrator — calls all sub-jobs. Safe for --detach."""
    r1 = run_create_indexes.remote()
    print(f"indexes: {r1}")
    r2 = run_fix_hcad_history.remote()
    print(f"history: {r2}")
    r3 = run_reference_downloads.remote()
    print(f"downloads: {r3}")
    return {"indexes": r1, "history": r2, "downloads": r3}


# ─────────────────────────────────────────────────────────────────────────────
# Entrypoints — always use .remote() + `modal run --detach`
# .remote() blocks the entrypoint (keeping app alive)
# --detach means app survives laptop close
# ─────────────────────────────────────────────────────────────────────────────
@app.local_entrypoint()
def main():
    """Run all maintenance jobs. Use: modal run --detach"""
    print("🚀 Running all maintenance (indexes + history + downloads)...")
    result = run_all_maintenance.remote()
    print(f"✅ Done: {result}")


@app.local_entrypoint()
def indexes():
    print(run_create_indexes.remote())


@app.local_entrypoint()
def fix_history():
    print(run_fix_hcad_history.remote())


@app.local_entrypoint()
def downloads():
    print(run_reference_downloads.remote())


@app.local_entrypoint()
def diagnose():
    print(run_db_diagnostic.remote())


