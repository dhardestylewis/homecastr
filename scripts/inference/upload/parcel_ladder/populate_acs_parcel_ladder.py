"""
Populate parcel_ladder_v1 for ACS nationwide accounts.

ACS accounts are 11-digit Census tract FIPS codes (SSCCCTTTTTT).
Each account IS a tract, so:
    tract_geoid20 = acct  (the 11-digit FIPS)
    zcta5          = NULL (would need Census crosswalk file — future work) 
    tabblock_geoid20 = NULL (ACS is tract-level, not block-level)

Tracts already covered by HCAD parcel-level data are EXCLUDED to avoid
double-counting during aggregation.

Uses ON CONFLICT (acct) DO NOTHING to be safe for re-runs and to never
overwrite existing HCAD entries.

Usage:
    python scripts/inference/upload/populate_acs_parcel_ladder.py
"""
import psycopg2

CONN = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"
SCHEMA = "forecast_20260220_7f31c6e4"


def main():
    conn = psycopg2.connect(CONN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SET statement_timeout = 300000")  # 5 min

    # ── 1. Current ladder state ───────────────────────────────────────────
    print("=== Current parcel_ladder_v1 by jurisdiction ===")
    cur.execute("SELECT jurisdiction, count(*) FROM public.parcel_ladder_v1 GROUP BY 1 ORDER BY 2 DESC")
    for jur, cnt in cur.fetchall():
        print(f"  {jur}: {cnt:,}")

    # ── 2. Get tracts already covered by HCAD parcel data ─────────────────
    # These tracts have parcel-granularity; we don't want ACS tract-level
    # averages to compete with them in the aggregate tables.
    print("\nGathering HCAD-covered tracts...")
    cur.execute("""
        SELECT DISTINCT tract_geoid20
        FROM public.parcel_ladder_v1
        WHERE jurisdiction NOT IN ('acs_nationwide', 'france_dvf', 'seattle_wa')
          AND tract_geoid20 IS NOT NULL
    """)
    hcad_tracts = {r[0] for r in cur.fetchall()}
    print(f"  {len(hcad_tracts):,} tracts already have parcel-level coverage")

    # ── 3. Get all distinct ACS accounts ──────────────────────────────────
    print("\nGathering ACS accounts from metrics_parcel_forecast...")
    cur.execute(f"""
        SELECT DISTINCT acct
        FROM {SCHEMA}.metrics_parcel_forecast
        WHERE jurisdiction = 'acs_nationwide'
    """)
    acs_accts = [r[0] for r in cur.fetchall()]
    print(f"  {len(acs_accts):,} distinct ACS accounts")

    # ── 4. Filter out HCAD-overlapping tracts ─────────────────────────────
    to_insert = [a for a in acs_accts if a not in hcad_tracts]
    skipped = len(acs_accts) - len(to_insert)
    print(f"  Skipping {skipped:,} tracts that overlap with HCAD")
    print(f"  Inserting {len(to_insert):,} ACS tracts into parcel_ladder_v1")

    # ── 5. Batch insert ───────────────────────────────────────────────────
    inserted = 0
    batch_size = 1000
    for i in range(0, len(to_insert), batch_size):
        batch = to_insert[i:i + batch_size]
        args = [(acct, acct, "acs_nationwide", 2024) for acct in batch]
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO public.parcel_ladder_v1 (acct, tract_geoid20, jurisdiction, gis_year)
            VALUES %s
            ON CONFLICT (acct) DO NOTHING
            """,
            args,
            template="(%s, %s, %s, %s)",
            page_size=1000,
        )
        inserted += len(batch)
        if inserted % 10000 == 0 or inserted == len(to_insert):
            print(f"  {inserted:,} / {len(to_insert):,} inserted...")

    # ── 6. Verify ─────────────────────────────────────────────────────────
    print("\n=== Updated parcel_ladder_v1 by jurisdiction ===")
    cur.execute("SELECT jurisdiction, count(*) FROM public.parcel_ladder_v1 GROUP BY 1 ORDER BY 2 DESC")
    for jur, cnt in cur.fetchall():
        print(f"  {jur}: {cnt:,}")

    # Spot check: confirm some ACS entries
    cur.execute("""
        SELECT acct, tract_geoid20, zcta5, tabblock_geoid20
        FROM public.parcel_ladder_v1
        WHERE jurisdiction = 'acs_nationwide'
        LIMIT 5
    """)
    print("\nSample ACS ladder entries:")
    for r in cur.fetchall():
        print(f"  acct={r[0]} tract={r[1]} zcta={r[2]} tabblock={r[3]}")

    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    from psycopg2 import extras  # noqa — needed for execute_values
    main()
