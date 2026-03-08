-- =====================================================================
-- HCAD SCHEMA — Separate schema for Harris County Appraisal District data
-- 
-- This creates a `forecast_hcad` schema that mirrors `forecast_queue`
-- (which is an alias for `forecast_20260220_7f31c6e4`).
--
-- Purpose: Isolate HCAD data from ACS data to prevent accidental mixing
-- when switching origin_year. HCAD data uses origin_year=2025,
-- ACS uses origin_year=2024. Keeping them in separate schemas
-- makes the data source explicit via the schema parameter.
--
-- Usage:
--   - ACS data → forecast_queue (default, origin_year=2024)
--   - HCAD data → forecast_hcad (origin_year=2025)
--   - Frontend switches via ?schema=forecast_hcad
--
-- This script is idempotent (IF NOT EXISTS everywhere).
-- =====================================================================

create schema if not exists forecast_hcad;
comment on schema forecast_hcad is
  'HCAD (Harris County Appraisal District) forecast data - isolated from ACS';

set search_path = forecast_hcad, public;

-- ----- Trigger helpers -----
create or replace function forecast_hcad.touch_updated_at_generic()
returns trigger language plpgsql as $$
begin new.updated_at := now(); return new; end; $$;

create or replace function forecast_hcad.touch_updated_at_utc_generic()
returns trigger language plpgsql as $$
begin new.updated_at_utc := now(); return new; end; $$;

-- Helper: pick simplified geom table if it exists and has rows
create or replace function forecast_hcad._pick_geom_table(p_preferred text, p_fallback text)
returns text language plpgsql stable as $$
declare v_has boolean := false; v_sql text;
begin
  if to_regclass(p_preferred) is not null then
    v_sql := format('select exists (select 1 from %s limit 1)', p_preferred);
    execute v_sql into v_has;
    if coalesce(v_has, false) then return p_preferred; end if;
  end if;
  return p_fallback;
end; $$;

-- ----- Metrics tables (same structure as forecast_queue) -----
do $$
declare
  rec record;
  levels text[][] := array[
    array['parcel',       'acct',              'forecast_hcad.metrics_parcel_forecast',       'forecast_hcad.metrics_parcel_history'],
    array['tabblock',     'tabblock_geoid20',  'forecast_hcad.metrics_tabblock_forecast',     'forecast_hcad.metrics_tabblock_history'],
    array['tract',        'tract_geoid20',     'forecast_hcad.metrics_tract_forecast',        'forecast_hcad.metrics_tract_history'],
    array['zcta',         'zcta5',             'forecast_hcad.metrics_zcta_forecast',         'forecast_hcad.metrics_zcta_history'],
    array['unsd',         'unsd_geoid',        'forecast_hcad.metrics_unsd_forecast',         'forecast_hcad.metrics_unsd_history'],
    array['neighborhood', 'neighborhood_id',   'forecast_hcad.metrics_neighborhood_forecast', 'forecast_hcad.metrics_neighborhood_history']
  ];
  level_alias text;
  key_col text;
  forecast_fqtn text;
  history_fqtn text;
begin
  for i in 1..array_length(levels, 1) loop
    level_alias   := levels[i][1];
    key_col       := levels[i][2];
    forecast_fqtn := levels[i][3];
    history_fqtn  := levels[i][4];

    execute format($sql$
      create table if not exists %s (
        %I            text not null,
        origin_year   integer not null,
        horizon_m     integer not null,
        forecast_year integer,
        value         double precision,
        p10           double precision,
        p25           double precision,
        p50           double precision,
        p75           double precision,
        p90           double precision,
        n             integer,
        run_id        text,
        backtest_id   text,
        variant_id    text not null default '__forecast__',
        model_version text,
        as_of_date    date,
        n_scenarios   integer,
        is_backtest   boolean not null default false,
        series_kind   text not null default 'forecast',
        inserted_at   timestamptz not null default now(),
        updated_at    timestamptz not null default now(),
        constraint %I primary key (%I, origin_year, horizon_m, series_kind, variant_id),
        constraint %I check (series_kind in ('forecast','backtest')),
        constraint %I check (
          (series_kind = 'forecast' and variant_id = '__forecast__')
          or (series_kind = 'backtest' and variant_id <> '__forecast__')
        )
      )
    $sql$,
      forecast_fqtn, key_col,
      'pk_hcad_' || level_alias || '_forecast', key_col,
      'ck_hcad_' || level_alias || '_forecast_sk',
      'ck_hcad_' || level_alias || '_forecast_var'
    );

    execute format($sql$
      create table if not exists %s (
        %I            text not null,
        year          integer not null,
        value         double precision,
        p50           double precision,
        n             integer,
        run_id        text,
        backtest_id   text,
        variant_id    text not null default '__history__',
        model_version text,
        as_of_date    date,
        series_kind   text not null default 'history',
        inserted_at   timestamptz not null default now(),
        updated_at    timestamptz not null default now(),
        constraint %I primary key (%I, year, series_kind, variant_id),
        constraint %I check (series_kind in ('history','backtest')),
        constraint %I check (
          (series_kind = 'history' and variant_id = '__history__')
          or (series_kind = 'backtest' and variant_id <> '__history__')
        )
      )
    $sql$,
      history_fqtn, key_col,
      'pk_hcad_' || level_alias || '_history', key_col,
      'ck_hcad_' || level_alias || '_history_sk',
      'ck_hcad_' || level_alias || '_history_var'
    );

    -- Indexes
    execute format('create index if not exists %I on %s (series_kind, origin_year, horizon_m, %I)',
      'ix_hcad_' || level_alias || '_f_query', forecast_fqtn, key_col);
    execute format('create index if not exists %I on %s (series_kind, forecast_year, %I)',
      'ix_hcad_' || level_alias || '_f_fyear', forecast_fqtn, key_col);
    execute format('create index if not exists %I on %s (run_id)',
      'ix_hcad_' || level_alias || '_f_runid', forecast_fqtn);
    execute format('create index if not exists %I on %s (series_kind, year, %I)',
      'ix_hcad_' || level_alias || '_h_query', history_fqtn, key_col);

    -- Triggers
    begin
      execute format('create trigger %I before update on %s for each row execute function forecast_hcad.touch_updated_at_generic()',
        'trg_hcad_' || level_alias || '_f_upd', forecast_fqtn);
    exception when duplicate_object then null;
    end;
    begin
      execute format('create trigger %I before update on %s for each row execute function forecast_hcad.touch_updated_at_generic()',
        'trg_hcad_' || level_alias || '_h_upd', history_fqtn);
    exception when duplicate_object then null;
    end;
  end loop;
end $$;

-- ----- Inference runs tracking -----
create table if not exists forecast_hcad.inference_runs (
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
  constraint ck_hcad_runs_status check (status in ('running','completed','failed','cancelled'))
);

create table if not exists forecast_hcad.inference_run_progress (
  run_id              text not null references forecast_hcad.inference_runs(run_id) on delete cascade,
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

-- ----- Clone the MVT choropleth function into forecast_hcad -----
-- This copies the routing logic from the original schema so tiles work
-- identically when queried via schema=forecast_hcad.
-- NOTE: This function references public.geo_* geometry tables (shared).
-- The metrics tables are schema-local (forecast_hcad.metrics_*).

-- We need the _mvt_forecast_generic helper and mvt_choropleth_forecast router.
-- Rather than duplicating 500+ lines, we create a thin wrapper that delegates
-- to the original schema's functions with table name substitution.
-- For now, we just ensure the schema exists and tables are ready.
-- The mvt_choropleth_forecast function will be cloned separately if needed.

reset search_path;
