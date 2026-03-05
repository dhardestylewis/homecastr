"""Check constraints only."""
import psycopg2

CONN_STR = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"
SCHEMA = "forecast_20260220_7f31c6e4"

def main():
    conn = psycopg2.connect(CONN_STR)
    cur = conn.cursor()

    for tbl in ['metrics_parcel_history', 'metrics_parcel_forecast',
                'metrics_zcta_history', 'metrics_zcta_forecast']:
        print(f"\n=== {tbl} ===")
        cur.execute(f"""
            SELECT c.conname, c.contype, pg_get_constraintdef(c.oid)
            FROM pg_constraint c
            WHERE c.conrelid = (
                SELECT oid FROM pg_class
                WHERE relname = %s
                AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = %s)
            )
            ORDER BY c.contype
        """, (tbl, SCHEMA))
        for r in cur.fetchall():
            kind = {'p': 'PRIMARY KEY', 'u': 'UNIQUE', 'f': 'FOREIGN KEY', 'c': 'CHECK'}.get(r[1], r[1])
            print(f"  [{kind}] {r[0]}: {r[2]}")

    conn.close()

if __name__ == "__main__":
    main()
