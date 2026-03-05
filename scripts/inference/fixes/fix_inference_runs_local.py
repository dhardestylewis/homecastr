import os
import psycopg2

def main():
    db_url = None
    with open(".env.local", "rb") as f:
        content = f.read().decode("utf-16", errors="ignore").replace("\0", "")
        if "POSTGRES_URL=" not in content:
            f.seek(0)
            content = f.read().decode("utf-8", errors="ignore")
        for line in content.splitlines():
            if line.startswith("POSTGRES_URL="):
                db_url = line.strip().split("=", 1)[1].strip('"').strip("'")
                # Remove inline comments if any
                db_url = db_url.split(" ")[0]
                db_url = db_url.split("?")[0]
                break
    
    if not db_url:
        print("SUPABASE_DB_URL not found in .env.local")
        return
        
    print(f"Connecting to: {db_url[:20]}...")
    
    if "pooler.supabase.com" in db_url and "6543" in db_url:
        db_url = db_url.replace(":6543/", ":5432/")
        
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    cur = conn.cursor()
    
    schema_name = "forecast_queue"
    print(f"Adding inference_runs to {schema_name}...")
    
    sql = f"""
    create table if not exists "{schema_name}".inference_runs (
      run_id          text primary key,
      level_name      text not null,
      mode            text not null,
      origin_year     integer,
      horizon_m       integer,
      as_of_date      date,
      model_version   text,
      n_scenarios     integer,

      status          text not null default 'running',
      started_at      timestamptz not null default now(),
      completed_at    timestamptz,
      notes           text,

      inserted_at     timestamptz not null default now(),
      updated_at      timestamptz not null default now(),

      constraint ck_inference_runs_status check (status in ('running','completed','failed','cancelled'))
    );

    create index if not exists ix_inference_runs_status on "{schema_name}".inference_runs(status);
    create index if not exists ix_inference_runs_level on "{schema_name}".inference_runs(level_name);
    create index if not exists ix_inference_runs_origin on "{schema_name}".inference_runs(origin_year);
    create index if not exists ix_inference_runs_started_at on "{schema_name}".inference_runs(started_at desc);

    create table if not exists "{schema_name}".inference_run_progress (
      run_id              text not null references "{schema_name}".inference_runs(run_id) on delete cascade,
      chunk_seq           integer not null,
      level_name          text not null,
      status              text not null default 'running',
      series_kind         text,
      variant_id          text,
      origin_year         integer,
      horizon_m           integer,
      year                integer,

      rows_upserted_total bigint,
      keys_upserted_total bigint,
      chunk_rows          integer,
      chunk_keys          integer,

      min_key             text,
      max_key             text,
      heartbeat_at        timestamptz not null default now(),

      inserted_at         timestamptz not null default now(),
      updated_at          timestamptz not null default now(),

      primary key (run_id, chunk_seq)
    );

    create index if not exists ix_inference_progress_run on "{schema_name}".inference_run_progress(run_id);
    create index if not exists ix_inference_progress_heartbeat on "{schema_name}".inference_run_progress(heartbeat_at desc);
    """
    
    cur.execute(sql)
    print("Done!")

if __name__ == "__main__":
    main()
