"""Add jurisdiction column to metrics_parcel_history and metrics_parcel_forecast in forecast_queue."""
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

    print(f"Connecting to: {db_url[:20]}...")
    if "pooler.supabase.com" in db_url and "6543" in db_url:
        db_url = db_url.replace(":6543/", ":5432/")

    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    cur = conn.cursor()

    schema = "forecast_queue"
    tables = [
        "metrics_parcel_history",
        "metrics_parcel_forecast",
        "metrics_tabblock_forecast",
        "metrics_tabblock_history",
        "metrics_tract_forecast",
        "metrics_tract_history",
        "metrics_zcta_forecast",
        "metrics_zcta_history",
        "metrics_unsd_forecast",
        "metrics_unsd_history",
        "metrics_neighborhood_forecast",
        "metrics_neighborhood_history",
        "metrics_zip3_forecast",
        "metrics_zip3_history",
    ]

    for tbl in tables:
        try:
            cur.execute(f'ALTER TABLE "{schema}"."{tbl}" ADD COLUMN IF NOT EXISTS jurisdiction text;')
            print(f"  ✅ {schema}.{tbl}.jurisdiction — OK")
        except Exception as e:
            print(f"  ⚠️  {schema}.{tbl}: {e}")

    cur.close()
    conn.close()
    print("Done!")

if __name__ == "__main__":
    main()
