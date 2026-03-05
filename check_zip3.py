import os, psycopg2

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

conn = psycopg2.connect(url.replace(":6543/", ":5432/"))
cur = conn.cursor()
cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='public' AND table_name='parcel_ladder_v1'")
cols = [row[0] for row in cur.fetchall()]
print(f"Columns in parcel_ladder_v1: {cols}")
if "zip3" in cols:
    print("YES_ZIP3")
else:
    print("NO_ZIP3")
