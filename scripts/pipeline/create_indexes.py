#!/usr/bin/env python3
"""
create_indexes.py
=================
Creates and ensures all necessary indexes on Supabase forecast schema tables.

Why this matters:
- Without indexes, `LIMIT 1` queries hang on large tables (full sequential scan)
- Our history and forecast tables have millions of rows but only a PK/unique constraint
- This script adds single-column btree indexes on the geo key columns used in WHERE clauses
- Safe to re-run — uses CREATE INDEX IF NOT EXISTS (no-op if already exists)
- Also runs ANALYZE on each table so the query planner has up-to-date statistics

Usage:
    python scripts/pipeline/create_indexes.py
    python scripts/pipeline/create_indexes.py --schema forecast_20260220_7f31c6e4
"""

import os, sys, time, argparse
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

try:
    import psycopg2
except ImportError:
    os.system(f"{sys.executable} -m pip install psycopg2-binary -q")
    import psycopg2

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCHEMA = "forecast_20260220_7f31c6e4"

def ts(): return time.strftime("%H:%M:%S")

def get_db_connection(schema: str):
    env_path = PROJECT_ROOT / ".env.local"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                v = v.strip().strip('"').strip("'")
                os.environ.setdefault(k.strip(), v)

    for key in ["POSTGRES_URL_NON_POOLING", "SUPABASE_DB_URL", "POSTGRES_URL"]:
        raw = os.environ.get(key, "").strip()
        if raw:
            parts = urlsplit(raw)
            q = dict(parse_qsl(parts.query, keep_blank_values=True))
            allowed = {"sslmode": q["sslmode"]} if "sslmode" in q else {}
            db_url = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(allowed), parts.fragment)).strip()
            conn = psycopg2.connect(db_url, connect_timeout=10)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = '30min'")
            print(f"[{ts()}] Connected via {key}")
            return conn
    raise RuntimeError("No DB URL found in environment")


# Index definitions: (table, index_name, column_expression)
# Using CREATE INDEX CONCURRENTLY where possible to avoid locking
def get_index_specs(schema: str) -> list[tuple[str, str, str]]:
    s = schema
    return [
        # ── History tables ──────────────────────────────────────────────────
        # The ON CONFLICT clause covers (key, year, series_kind, variant_id),
        # but we also need fast single-column lookups for the API WHERE clause
        (f"{s}.metrics_zcta_history",     f"idx_{s[:8]}_zcta_hist_key",      "zcta5"),
        (f"{s}.metrics_zcta_history",     f"idx_{s[:8]}_zcta_hist_year",     "year"),
        (f"{s}.metrics_tract_history",    f"idx_{s[:8]}_tract_hist_key",     "tract_geoid20"),
        (f"{s}.metrics_tract_history",    f"idx_{s[:8]}_tract_hist_year",    "year"),
        (f"{s}.metrics_tabblock_history", f"idx_{s[:8]}_tblk_hist_key",      "tabblock_geoid20"),
        (f"{s}.metrics_tabblock_history", f"idx_{s[:8]}_tblk_hist_year",     "year"),
        (f"{s}.metrics_parcel_history",   f"idx_{s[:8]}_prcl_hist_key",      "acct"),
        (f"{s}.metrics_parcel_history",   f"idx_{s[:8]}_prcl_hist_year",     "year"),

        # ── Forecast tables ─────────────────────────────────────────────────
        (f"{s}.metrics_zcta_forecast",     f"idx_{s[:8]}_zcta_fc_key",        "zcta5"),
        (f"{s}.metrics_zcta_forecast",     f"idx_{s[:8]}_zcta_fc_origin",     "origin_year"),
        (f"{s}.metrics_tract_forecast",    f"idx_{s[:8]}_tract_fc_key",       "tract_geoid20"),
        (f"{s}.metrics_tract_forecast",    f"idx_{s[:8]}_tract_fc_origin",    "origin_year"),
        (f"{s}.metrics_tabblock_forecast", f"idx_{s[:8]}_tblk_fc_key",        "tabblock_geoid20"),
        (f"{s}.metrics_tabblock_forecast", f"idx_{s[:8]}_tblk_fc_origin",     "origin_year"),
        (f"{s}.metrics_parcel_forecast",   f"idx_{s[:8]}_prcl_fc_key",        "acct"),
        (f"{s}.metrics_parcel_forecast",   f"idx_{s[:8]}_prcl_fc_origin",     "origin_year"),

        # ── Public tables used in API joins ──────────────────────────────────
        ("public.parcel_ladder_v1",       "idx_pl1_acct",                     "acct"),
        ("public.parcel_ladder_v1",       "idx_pl1_zcta5",                    "zcta5"),
        ("public.parcel_ladder_v1",       "idx_pl1_tract",                    "tract_geoid20"),
        ("public.parcel_ladder_v1",       "idx_pl1_tabblock",                 "tabblock_geoid20"),
    ]


def table_exists(cur, full_table: str) -> bool:
    """Fast catalog check — no scan."""
    parts = full_table.split(".", 1)
    schema, table = (parts[0], parts[1]) if len(parts) == 2 else ("public", parts[0])
    cur.execute(
        "SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname=%s AND tablename=%s)",
        (schema, table)
    )
    return cur.fetchone()[0]


def index_exists(cur, index_name: str, full_table: str) -> bool:
    parts = full_table.split(".", 1)
    schema = parts[0] if len(parts) == 2 else "public"
    cur.execute(
        "SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE schemaname=%s AND indexname=%s)",
        (schema, index_name)
    )
    return cur.fetchone()[0]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", default=SCHEMA)
    parser.add_argument("--analyze", action="store_true", default=True,
                        help="Run ANALYZE after indexing (default: True)")
    parser.add_argument("--no-analyze", dest="analyze", action="store_false")
    args = parser.parse_args()

    conn = get_db_connection(args.schema)
    specs = get_index_specs(args.schema)

    created = 0
    skipped_no_table = 0
    skipped_exists = 0
    errors = 0

    with conn.cursor() as cur:
        seen_tables = set()
        for (table, idx_name, col) in specs:
            t0 = time.time()
            if not table_exists(cur, table):
                print(f"[{ts()}]   SKIP {table} — table does not exist")
                skipped_no_table += 1
                continue

            if index_exists(cur, idx_name, table):
                print(f"[{ts()}]   EXISTS {idx_name}")
                skipped_exists += 1
                seen_tables.add(table)
                continue

            try:
                # CONCURRENTLY is safest — doesn't lock the table while building
                sql = f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {idx_name} ON {table} ({col})"
                cur.execute(sql)
                elapsed = time.time() - t0
                print(f"[{ts()}]   ✅ Created {idx_name} on {table}({col})  [{elapsed:.1f}s]")
                created += 1
                seen_tables.add(table)
            except Exception as e:
                print(f"[{ts()}]   ⚠️  {idx_name}: {e}")
                errors += 1

        # ANALYZE each table so stats are fresh for query planner
        if args.analyze:
            print(f"\n[{ts()}] Running ANALYZE on {len(seen_tables)} tables...")
            for table in sorted(seen_tables):
                if not table_exists(cur, table):
                    continue
                try:
                    cur.execute(f"ANALYZE {table}")
                    print(f"[{ts()}]   ✅ ANALYZE {table}")
                except Exception as e:
                    print(f"[{ts()}]   ⚠️  ANALYZE {table}: {e}")

    conn.close()
    print(f"\n[{ts()}] Done — created: {created}, skipped (exists): {skipped_exists}, "
          f"skipped (no table): {skipped_no_table}, errors: {errors}")


if __name__ == "__main__":
    main()
