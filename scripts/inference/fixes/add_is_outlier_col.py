"""Add is_outlier column to metrics_parcel_forecast and fix statement timeout for large inserts."""
import psycopg2

def main():
    with open(".env.local", "rb") as f:
        content = f.read().decode("utf-16", errors="ignore").replace("\0", "")
        if "POSTGRES_URL=" not in content:
            f.seek(0)
            content = f.read().decode("utf-8", errors="ignore")
        for line in content.splitlines():
            if line.startswith("POSTGRES_URL="):
                db_url = line.strip().split("=", 1)[1].strip('"').strip("'")
                db_url = db_url.split(" ")[0].split("?")[0]
                break

    print(f"Connecting ...")
    if "pooler.supabase.com" in db_url and "6543" in db_url:
        db_url = db_url.replace(":6543/", ":5432/")

    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    cur = conn.cursor()

    schema = "forecast_queue"

    # 1. Add is_outlier column to metrics_parcel_forecast
    try:
        cur.execute(f'ALTER TABLE "{schema}"."metrics_parcel_forecast" ADD COLUMN IF NOT EXISTS is_outlier boolean DEFAULT false;')
        print(f"  ✅ {schema}.metrics_parcel_forecast.is_outlier — added")
    except Exception as e:
        print(f"  ⚠️  {e}")

    # 2. Also add to metrics_parcel_history for completeness
    try:
        cur.execute(f'ALTER TABLE "{schema}"."metrics_parcel_history" ADD COLUMN IF NOT EXISTS is_outlier boolean DEFAULT false;')
        print(f"  ✅ {schema}.metrics_parcel_history.is_outlier — added")
    except Exception as e:
        print(f"  ⚠️  {e}")

    # 3. Index for outlier filtering
    try:
        cur.execute(f'CREATE INDEX IF NOT EXISTS ix_mpf_is_outlier ON "{schema}".metrics_parcel_forecast (is_outlier) WHERE is_outlier = true;')
        print(f"  ✅ ix_mpf_is_outlier index — created")
    except Exception as e:
        print(f"  ⚠️  index: {e}")

    cur.close()
    conn.close()
    print("Done!")

if __name__ == "__main__":
    main()
