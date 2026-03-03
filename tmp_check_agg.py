"""Quick check: what's already in the aggregate tables after partial run."""
import psycopg2

db_url = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"
conn = psycopg2.connect(db_url)
conn.autocommit = True
cur = conn.cursor()

SCHEMA = "forecast_20260220_7f31c6e4"

print("=== FORECAST aggregates ===")
for tbl in ['metrics_neighborhood_forecast','metrics_unsd_forecast','metrics_zcta_forecast',
            'metrics_tract_forecast','metrics_tabblock_forecast']:
    try:
        cur.execute(f"SELECT count(*), count(DISTINCT horizon_m) FROM {SCHEMA}.{tbl} WHERE series_kind='forecast'")
        cnt, n_h = cur.fetchone()
        cur.execute(f"SELECT DISTINCT horizon_m FROM {SCHEMA}.{tbl} WHERE series_kind='forecast' ORDER BY 1")
        horizons = [r[0] for r in cur.fetchall()]
        print(f"  {tbl:<40} {cnt:>8,} rows  horizons={horizons}")
    except Exception as e:
        conn.rollback()
        print(f"  {tbl:<40} ERROR: {e}")

print("\n=== HISTORY aggregates ===")
for tbl in ['metrics_neighborhood_history','metrics_unsd_history','metrics_zcta_history',
            'metrics_tract_history','metrics_tabblock_history']:
    try:
        cur.execute(f"SELECT count(*) FROM {SCHEMA}.{tbl}")
        cnt = cur.fetchone()[0]
        print(f"  {tbl:<40} {cnt:>8,} rows")
    except Exception as e:
        conn.rollback()
        print(f"  {tbl:<40} ERROR: {e}")

print("\n=== parcel_history combos ===")
cur.execute(f"SELECT series_kind, variant_id, count(*) FROM {SCHEMA}.metrics_parcel_history GROUP BY 1,2 ORDER BY 1,2")
for r in cur.fetchall():
    print(f"  {r[0]}/{r[1]}: {r[2]:,}")

# ACS quick check
print("\n=== ACS in parcel_forecast ===")
cur.execute(f"""SELECT count(DISTINCT acct) FROM {SCHEMA}.metrics_parcel_forecast 
    WHERE jurisdiction = 'acs_nationwide'""")
acs_cnt = cur.fetchone()[0]
print(f"  ACS distinct accts: {acs_cnt:,}")

# Check if ACS accts are in parcel_ladder
cur.execute(f"""SELECT count(DISTINCT mp.acct)
    FROM {SCHEMA}.metrics_parcel_forecast mp
    JOIN public.parcel_ladder_v1 pl ON pl.acct = mp.acct
    WHERE mp.jurisdiction = 'acs_nationwide'""")
acs_in_ladder = cur.fetchone()[0]
print(f"  ACS accts in parcel_ladder_v1: {acs_in_ladder:,}")

# Sample ACS acct format
cur.execute(f"SELECT DISTINCT acct FROM {SCHEMA}.metrics_parcel_forecast WHERE jurisdiction='acs_nationwide' LIMIT 5")
print(f"  ACS acct samples: {[r[0] for r in cur.fetchall()]}")

# Sample parcel_ladder acct format
cur.execute("SELECT acct FROM public.parcel_ladder_v1 LIMIT 5")
print(f"  Ladder acct samples: {[r[0] for r in cur.fetchall()]}")

conn.close()
print("\nDone!")
