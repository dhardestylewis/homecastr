import modal
import os
import psycopg2

app = modal.App("fix-schema-inference-runs")
image = modal.Image.debian_slim().pip_install("psycopg2-binary")
supabase_secret = modal.Secret.from_name("supabase-creds", required_keys=["SUPABASE_DB_URL"])

@app.function(image=image, secrets=[supabase_secret])
def fix_schema(schema_name: str):
    db_url = os.environ["SUPABASE_DB_URL"]
    # Handle connection pooler URL if needed
    if "pooler.supabase.com" in db_url and "6543" in db_url:
        db_url = db_url.replace(":6543/", ":5432/")
        
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
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
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()
    conn.close()
    print(f"✅ Schema {schema_name} patched with inference_runs tables.")

@app.local_entrypoint()
def main(schema_name: str = "forecast_queue"):
    fix_schema.remote(schema_name)
