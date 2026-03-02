"""Re-add dropped PK constraints. Use original PKs first to restore API, 
then update to include jurisdiction later with longer timeout."""
import psycopg2

CONN_STR = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"
SCHEMA = "forecast_20260220_7f31c6e4"

def main():
    conn = psycopg2.connect(CONN_STR)
    conn.autocommit = True
    cur = conn.cursor()

    # Set a longer statement timeout (10 minutes)
    cur.execute("SET statement_timeout = '600s'")

    migrations = [
        # Re-add original PKs (without jurisdiction for now — to restore API ASAP)
        (f"ALTER TABLE {SCHEMA}.metrics_parcel_history DROP CONSTRAINT IF EXISTS pk_parcel_history", "Drop old PK (if any)"),
        (f"ALTER TABLE {SCHEMA}.metrics_parcel_history ADD CONSTRAINT pk_parcel_history PRIMARY KEY (acct, year, series_kind, variant_id)", "Re-add parcel history PK"),
        
        (f"ALTER TABLE {SCHEMA}.metrics_parcel_forecast DROP CONSTRAINT IF EXISTS pk_parcel_forecast", "Drop old PK (if any)"),
        (f"ALTER TABLE {SCHEMA}.metrics_parcel_forecast ADD CONSTRAINT pk_parcel_forecast PRIMARY KEY (acct, origin_year, horizon_m, series_kind, variant_id)", "Re-add parcel forecast PK"),
    ]

    for sql, desc in migrations:
        print(f"[{desc}]...")
        try:
            cur.execute(sql)
            print(f"  ✅ OK")
        except Exception as e:
            print(f"  ❌ {e}")

    # Verify
    print("\n=== Verification ===")
    for tbl in ['metrics_parcel_history', 'metrics_parcel_forecast']:
        cur.execute(f"""
            SELECT c.conname, pg_get_constraintdef(c.oid)
            FROM pg_constraint c
            WHERE c.conrelid = (
                SELECT oid FROM pg_class WHERE relname = '{tbl}'
                AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{SCHEMA}')
            ) AND c.contype = 'p'
        """)
        pk = cur.fetchall()
        print(f"  {tbl}: PK={pk}")

    conn.close()
    print("\nDone!")

if __name__ == "__main__":
    main()
