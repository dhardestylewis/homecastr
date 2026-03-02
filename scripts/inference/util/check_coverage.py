"""Quick check: parcel history coverage and API readiness."""
import psycopg2

CONN_STR = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"
SCHEMA = "forecast_20260220_7f31c6e4"

def main():
    conn = psycopg2.connect(CONN_STR)
    cur = conn.cursor()

    # 1. PK check
    print("=== PK Check ===")
    for tbl in ['metrics_parcel_history', 'metrics_parcel_forecast']:
        cur.execute(f"""
            SELECT c.conname FROM pg_constraint c
            WHERE c.conrelid = (
                SELECT oid FROM pg_class WHERE relname = '{tbl}'
                AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{SCHEMA}')
            ) AND c.contype = 'p'
        """)
        pk = cur.fetchall()
        print(f"  {tbl}: PK={'YES' if pk else 'NO'} ({pk})")

    # 2. How many unique parcels have history vs forecast
    cur.execute(f"SELECT COUNT(DISTINCT acct) FROM {SCHEMA}.metrics_parcel_history")
    hist_parcels = cur.fetchone()[0]
    cur.execute(f"SELECT COUNT(DISTINCT acct) FROM {SCHEMA}.metrics_parcel_forecast")
    fc_parcels = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM public.parcel_ladder_v1")
    ladder_parcels = cur.fetchone()[0]
    print(f"\n=== Parcel Coverage ===")
    print(f"  parcel_ladder_v1: {ladder_parcels:,} parcels")
    print(f"  parcel_history:   {hist_parcels:,} unique parcels ({hist_parcels/ladder_parcels*100:.1f}% of ladder)")
    print(f"  parcel_forecast:  {fc_parcels:,} unique parcels ({fc_parcels/ladder_parcels*100:.1f}% of ladder)")

    # 3. Sample parcels WITH forecast but WITHOUT history
    cur.execute(f"""
        SELECT f.acct, COUNT(DISTINCT f.origin_year) as fc_years
        FROM {SCHEMA}.metrics_parcel_forecast f
        LEFT JOIN {SCHEMA}.metrics_parcel_history h ON h.acct = f.acct
        WHERE h.acct IS NULL
        GROUP BY f.acct
        LIMIT 5
    """)
    missing = cur.fetchall()
    print(f"\n=== Parcels with forecast but no history (sample) ===")
    for r in missing:
        print(f"  acct={r[0]} forecast_origins={r[1]}")

    # 4. Sample parcels WITH both
    cur.execute(f"""
        SELECT h.acct, COUNT(DISTINCT h.year) as hist_years
        FROM {SCHEMA}.metrics_parcel_history h
        LIMIT 5
    """)

    conn.close()

if __name__ == "__main__":
    main()
