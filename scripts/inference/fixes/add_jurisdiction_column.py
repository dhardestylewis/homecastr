"""Add jurisdiction column to all metrics tables and update unique constraints."""
import psycopg2

CONN_STR = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"
SCHEMA = "forecast_20260220_7f31c6e4"

MIGRATIONS = [
    # ── metrics_parcel_history ──
    f"ALTER TABLE {SCHEMA}.metrics_parcel_history ADD COLUMN IF NOT EXISTS jurisdiction TEXT DEFAULT 'hcad'",
    f"ALTER TABLE {SCHEMA}.metrics_parcel_history DROP CONSTRAINT IF EXISTS pk_parcel_history",
    f"ALTER TABLE {SCHEMA}.metrics_parcel_history ADD CONSTRAINT pk_parcel_history PRIMARY KEY (acct, year, series_kind, variant_id, jurisdiction)",

    # ── metrics_parcel_forecast ──
    f"ALTER TABLE {SCHEMA}.metrics_parcel_forecast ADD COLUMN IF NOT EXISTS jurisdiction TEXT DEFAULT 'hcad'",
    f"ALTER TABLE {SCHEMA}.metrics_parcel_forecast DROP CONSTRAINT IF EXISTS pk_parcel_forecast",
    f"ALTER TABLE {SCHEMA}.metrics_parcel_forecast ADD CONSTRAINT pk_parcel_forecast PRIMARY KEY (acct, origin_year, horizon_m, series_kind, variant_id, jurisdiction)",

    # ── metrics_zcta_history ──
    f"ALTER TABLE {SCHEMA}.metrics_zcta_history ADD COLUMN IF NOT EXISTS jurisdiction TEXT DEFAULT 'hcad'",

    # ── metrics_zcta_forecast ──
    f"ALTER TABLE {SCHEMA}.metrics_zcta_forecast ADD COLUMN IF NOT EXISTS jurisdiction TEXT DEFAULT 'hcad'",

    # ── metrics_tract_history ──
    f"ALTER TABLE {SCHEMA}.metrics_tract_history ADD COLUMN IF NOT EXISTS jurisdiction TEXT DEFAULT 'hcad'",

    # ── metrics_tract_forecast ──
    f"ALTER TABLE {SCHEMA}.metrics_tract_forecast ADD COLUMN IF NOT EXISTS jurisdiction TEXT DEFAULT 'hcad'",

    # ── metrics_tabblock_history ──
    f"ALTER TABLE {SCHEMA}.metrics_tabblock_history ADD COLUMN IF NOT EXISTS jurisdiction TEXT DEFAULT 'hcad'",
]

def main():
    conn = psycopg2.connect(CONN_STR)
    conn.autocommit = True
    cur = conn.cursor()

    for i, sql in enumerate(MIGRATIONS):
        print(f"[{i+1}/{len(MIGRATIONS)}] {sql[:80]}...")
        try:
            cur.execute(sql)
            print(f"  ✅ OK")
        except Exception as e:
            print(f"  ❌ {e}")

    # Verify
    print("\n=== Verification ===")
    for tbl in ['metrics_parcel_history', 'metrics_parcel_forecast']:
        cur.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_name='{tbl}' AND table_schema='{SCHEMA}' AND column_name='jurisdiction'
        """)
        has_jurisdiction = len(cur.fetchall()) > 0
        cur.execute(f"""
            SELECT c.conname, pg_get_constraintdef(c.oid)
            FROM pg_constraint c
            WHERE c.conrelid = (
                SELECT oid FROM pg_class WHERE relname = '{tbl}'
                AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{SCHEMA}')
            ) AND c.contype = 'p'
        """)
        pk = cur.fetchall()
        print(f"  {tbl}: jurisdiction={'YES' if has_jurisdiction else 'NO'}, PK={pk}")

    conn.close()
    print("\nDone!")

if __name__ == "__main__":
    main()
