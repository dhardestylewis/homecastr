import os
import psycopg2

url = None
with open(".env.local", "r", encoding="utf-8", errors="ignore") as f:
    for line in f:
        for key in ["SUPABASE_DB_URL=", "POSTGRES_URL=", "POSTGRES_URL_NON_POOLING="]:
            if line.startswith(key):
                val = line.strip().split("=", 1)[1].strip("'\" ")
                url = val.split("?")[0].split(" ")[0]
                break
        if url: break

if not url:
    print("NO URL")
    exit(1)

# Connect to the session mode 5432 which might be full, or just try 6543 (current URL)
try:
    conn = psycopg2.connect(url)
    conn.autocommit = True
    cur = conn.cursor()
    
    # Create the zip3 tables if they don't exist
    schema = "forecast_20260220_7f31c6e4"
    print("2. Creating metrics_zip3_* tables...")
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {schema}.metrics_zip3_forecast (
        zip3 character varying NOT NULL,
        origin_year int4 NOT NULL,
        horizon_m int4 NOT NULL,
        series_kind character varying NOT NULL,
        variant_id character varying NOT NULL,
        forecast_year int4 NOT NULL,
        value float8 NULL,
        p10 float8 NULL,
        p25 float8 NULL,
        p50 float8 NULL,
        p75 float8 NULL,
        p90 float8 NULL,
        n int4 NULL,
        run_id character varying NULL,
        backtest_id character varying NULL,
        model_version character varying NULL,
        as_of_date date NULL,
        n_scenarios int4 NULL,
        is_backtest bool NULL,
        inserted_at timestamp with time zone NULL,
        updated_at timestamp with time zone NULL,
        CONSTRAINT zip3_forecast_pkey PRIMARY KEY (zip3, origin_year, horizon_m, series_kind, variant_id)
    );
    """)

    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {schema}.metrics_zip3_history (
        zip3 character varying NOT NULL,
        year int4 NOT NULL,
        series_kind character varying NOT NULL,
        variant_id character varying NOT NULL,
        value float8 NULL,
        p50 float8 NULL,
        n int4 NULL,
        run_id character varying NULL,
        backtest_id character varying NULL,
        model_version character varying NULL,
        as_of_date date NULL,
        inserted_at timestamp with time zone NULL,
        updated_at timestamp with time zone NULL,
        CONSTRAINT zip3_history_pkey PRIMARY KEY (zip3, year, series_kind, variant_id)
    );
    """)
    
    print("DONE! Created tables successfully.")
except Exception as e:
    print(f"FAILED: {e}")
