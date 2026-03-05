#!/usr/bin/env python3
"""
Historical Value Uploader for Forecast Choropleth Map
=====================================================
Reads the HCAD master panel parquet, aggregates `tot_appr_val` by
geography level (zcta, tract, tabblock), and upserts into the
`metrics_*_history` tables in the forecast schema.

Usage:
  pip install psycopg2-binary pandas pyarrow
  cd v0-properlytic-8v
  python scripts/upload_history.py [--levels zcta,tract,tabblock] [--parcel]

By default uploads zcta + tract + tabblock aggregate history.
Use --parcel to also upload parcel-level history (30M rows — slow).
"""
import os, sys, time, argparse
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

# --- Deps ---
try:
    import psycopg2, psycopg2.extras
except ImportError:
    os.system(f"{sys.executable} -m pip install psycopg2-binary")
    import psycopg2, psycopg2.extras
try:
    import pandas as pd
except ImportError:
    os.system(f"{sys.executable} -m pip install pandas pyarrow")
    import pandas as pd
try:
    import numpy as np
except ImportError:
    os.system(f"{sys.executable} -m pip install numpy")
    import numpy as np

# --- Config ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCHEMA = "forecast_20260220_7f31c6e4"
BATCH_SIZE = 500

# Panel parquet on G: drive (Google Drive mount)
# Colab path: /content/drive/MyDrive/HCAD_Archive_Aggregates/...
PANEL_PARQUET = Path(r"G:\My Drive\HCAD_Archive_Aggregates\hcad_master_panel_2005_2025_leakage_strict_FIXEDYR_WITHGIS.parquet")

# Level definitions: key_col is the column in the metrics_*_history table
# crosswalk_col is the column in parcel_ladder_v1 that maps acct -> geo key
LEVEL_DEFS = {
    "zcta":     {"table": f"{SCHEMA}.metrics_zcta_history",     "key_col": "zcta5",            "crosswalk_col": "zcta5"},
    "tract":    {"table": f"{SCHEMA}.metrics_tract_history",    "key_col": "tract_geoid20",    "crosswalk_col": "tract_geoid20"},
    "tabblock": {"table": f"{SCHEMA}.metrics_tabblock_history", "key_col": "tabblock_geoid20", "crosswalk_col": "tabblock_geoid20"},
}


def ts():
    return time.strftime("%H:%M:%S")


# --- DB Connection (same pattern as upload_geo.py) ---
def get_db_connection():
    env_path = PROJECT_ROOT / ".env.local"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                v = v.strip().strip('"').strip("'")
                os.environ.setdefault(k.strip(), v)

    db_url = ""
    for key in ["SUPABASE_DB_URL", "POSTGRES_URL_NON_POOLING", "POSTGRES_URL"]:
        raw = os.environ.get(key, "").strip()
        if raw:
            parts = urlsplit(raw)
            q = dict(parse_qsl(parts.query, keep_blank_values=True))
            allowed = {"sslmode": q.get("sslmode")} if "sslmode" in q else {}
            db_url = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(allowed), parts.fragment)).strip()
            break

    if not db_url:
        raise RuntimeError("No database URL found in environment")

    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '120min';")
        try:
            cur.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE;")
        except:
            pass
    print(f"[{ts()}] Connected to database")
    return conn


def load_panel():
    """Load the master parquet — only the columns we need."""
    print(f"[{ts()}] Loading panel from {PANEL_PARQUET} ...")
    cols = ["acct", "yr", "tot_appr_val"]
    df = pd.read_parquet(PANEL_PARQUET, columns=cols, engine="pyarrow")
    # Rename yr -> year for clarity
    df = df.rename(columns={"yr": "year"})
    # Drop rows with missing values
    df = df.dropna(subset=["acct", "year", "tot_appr_val"])
    df["acct"] = df["acct"].astype(str).str.strip()
    df["year"] = df["year"].astype(int)
    print(f"[{ts()}] Loaded {len(df):,} rows, year range {df['year'].min()}-{df['year'].max()}")
    return df


def load_crosswalk(conn):
    """Download the parcel_ladder_v1 crosswalk from Supabase."""
    print(f"[{ts()}] Downloading crosswalk (parcel_ladder_v1) ...")
    query = "SELECT acct, zcta5, tract_geoid20, tabblock_geoid20 FROM public.parcel_ladder_v1"
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
    cw = pd.DataFrame(rows, columns=["acct", "zcta5", "tract_geoid20", "tabblock_geoid20"])
    cw["acct"] = cw["acct"].astype(str).str.strip()
    print(f"[{ts()}] Crosswalk: {len(cw):,} parcels")
    return cw


def upsert_history(conn, table, key_col, records):
    """
    Batch upsert records into a history table.
    records: list of (key_value, year, value, p50, n)
    """
    sql = f"""
        INSERT INTO {table} ({key_col}, year, value, p50, n, series_kind, variant_id)
        VALUES %s
        ON CONFLICT ({key_col}, year, series_kind, variant_id)
        DO UPDATE SET value = EXCLUDED.value, p50 = EXCLUDED.p50, n = EXCLUDED.n,
                      updated_at = now()
    """
    template = "(%s, %s, %s, %s, %s, 'history', '__history__')"

    inserted = 0
    with conn.cursor() as cur:
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i : i + BATCH_SIZE]
            psycopg2.extras.execute_values(cur, sql, batch, template=template)
            inserted += len(batch)
            if inserted % 5000 < BATCH_SIZE:
                print(f"[{ts()}]   {table}: {inserted:,} / {len(records):,}")

    print(f"[{ts()}] ✅ Upserted {inserted:,} rows into {table}")
    return inserted


def upload_aggregate_history(conn, panel, crosswalk, level_name):
    """Aggregate panel to a geography level and upload."""
    defn = LEVEL_DEFS[level_name]
    cw_col = defn["crosswalk_col"]
    key_col = defn["key_col"]
    table = defn["table"]

    print(f"\n[{ts()}] === Aggregating {level_name.upper()} history ===")

    # Join panel with crosswalk
    merged = panel.merge(crosswalk[["acct", cw_col]], on="acct", how="inner")
    merged = merged.dropna(subset=[cw_col])
    print(f"[{ts()}] Joined {len(merged):,} rows with crosswalk ({cw_col})")

    # Group by (geo_key, year) -> median value, count
    agg = merged.groupby([cw_col, "year"]).agg(
        value=("tot_appr_val", "median"),
        n=("tot_appr_val", "count"),
    ).reset_index()

    # p50 = median = value
    agg["p50"] = agg["value"]
    agg["n"] = agg["n"].astype(int)

    print(f"[{ts()}] Aggregated to {len(agg):,} (geo, year) rows")

    # Build records list
    records = list(agg[[cw_col, "year", "value", "p50", "n"]].itertuples(index=False, name=None))

    return upsert_history(conn, table, key_col, records)


def upload_parcel_history(conn, panel):
    """Upload parcel-level history (1 row per acct-year)."""
    table = f"{SCHEMA}.metrics_parcel_history"
    key_col = "acct"

    print(f"\n[{ts()}] === Uploading PARCEL history ({len(panel):,} rows) ===")

    records = list(
        panel[["acct", "year", "tot_appr_val"]].assign(
            p50=panel["tot_appr_val"],
            n=1,
        )[["acct", "year", "tot_appr_val", "p50", "n"]].itertuples(index=False, name=None)
    )

    return upsert_history(conn, table, key_col, records)


def apply_grants(conn):
    """Grant SELECT on history tables to Supabase roles."""
    tables = [d["table"] for d in LEVEL_DEFS.values()]
    tables.append(f"{SCHEMA}.metrics_parcel_history")
    with conn.cursor() as cur:
        for t in tables:
            try:
                cur.execute(f"GRANT SELECT ON {t} TO anon, authenticated, service_role;")
            except Exception as e:
                print(f"[{ts()}] WARN: Grant failed for {t}: {e}")
    print(f"[{ts()}] ✅ Grants applied to history tables")


def main():
    parser = argparse.ArgumentParser(description="Upload historical values to Supabase forecast schema")
    parser.add_argument(
        "--levels", type=str, default="zcta,tract,tabblock",
        help="Comma-separated list of levels to upload (default: zcta,tract,tabblock)"
    )
    parser.add_argument("--parcel", action="store_true", help="Also upload parcel-level history (very large)")
    parser.add_argument("--panel", type=str, default=None, help="Override path to panel parquet")
    args = parser.parse_args()

    global PANEL_PARQUET
    if args.panel:
        PANEL_PARQUET = Path(args.panel)

    if not PANEL_PARQUET.exists():
        print(f"ERROR: Panel parquet not found at {PANEL_PARQUET}")
        sys.exit(1)

    conn = get_db_connection()

    try:
        panel = load_panel()
        crosswalk = load_crosswalk(conn)

        levels = [l.strip() for l in args.levels.split(",") if l.strip()]
        total = 0

        for level_name in levels:
            if level_name not in LEVEL_DEFS:
                print(f"[{ts()}] WARN: Unknown level '{level_name}', skipping")
                continue
            total += upload_aggregate_history(conn, panel, crosswalk, level_name)

        if args.parcel:
            total += upload_parcel_history(conn, panel)

        apply_grants(conn)

        print(f"\n[{ts()}] 🎉 All done! Total rows upserted: {total:,}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
