-- =====================================================================
-- CONSOLIDATED GEOSPATIAL FORECASTING SCHEMA (ISOLATED / NON-DESTRUCTIVE)
-- Supabase / Postgres / PostGIS
--
-- UNIQUE SCHEMA (collision-resistant):
--   forecast_20260220_7f31c6e4
--
-- Purpose:
--   - Replace H3-first serving with political/census geographies + parcel lotlines
--   - Support forecast + history + backtests side-by-side
--   - Support full fan quantiles (p10/p25/p50/p75/p90)
--   - Support live inference progress tracking
--   - Support vector MVT choropleths at multiple zoom levels
--   - Provide optional raster tile cache for heatmap overlays
--
-- IMPORTANT:
--   - This script does NOT drop or replace prod metrics/views/functions in public.
--   - Shared geometry/crosswalk tables remain in public (additive create/alter only).
--   - All new metrics/views/functions/progress/cache live in forecast_20260220_7f31c6e4.
--   - No large UPDATE backfills are executed (timeout-safe).
--
-- Default zoom routing:
--   z <= 7    : ZCTA
--   z <= 11   : Tract
--   z <= 16   : Tabblock
--   z >= 17   : Parcel polygons (capped per tile)
--
-- School district layer:
--   - Implemented as UNSD (Unified School Districts).
--   - If you later need ESD/SSD, clone the UNSD blocks.
-- =====================================================================

-- ---------------------------------------------------------------------
-- 0) Extensions + isolated schema
-- ---------------------------------------------------------------------
create extension if not exists postgis;

create schema if not exists forecast_20260220_7f31c6e4;
comment on schema forecast_20260220_7f31c6e4 is
  'Isolated geospatial forecasting schema (created 2026-02-20, suffix 7f31c6e4)';

set search_path = forecast_20260220_7f31c6e4, public;

-- ---------------------------------------------------------------------
-- 1) Local trigger helpers (schema-local; does not alter public helpers)
-- ---------------------------------------------------------------------
create or replace function forecast_20260220_7f31c6e4.touch_updated_at_generic()
returns trigger
language plpgsql
as $$
begin
  new.updated_at := now();
  return new;
end;
$$;

create or replace function forecast_20260220_7f31c6e4.touch_updated_at_utc_generic()
returns trigger
language plpgsql
as $$
begin
  new.updated_at_utc := now();
  return new;
end;
$$;

-- Helper: pick simplified geom table if it exists and has rows, else fallback
create or replace function forecast_20260220_7f31c6e4._pick_geom_table(p_preferred text, p_fallback text)
returns text
language plpgsql
stable
as $$
declare
  v_has boolean := false;
  v_sql text;
begin
  if to_regclass(p_preferred) is not null then
    v_sql := format('select exists (select 1 from %s limit 1)', p_preferred);
    execute v_sql into v_has;
    if coalesce(v_has, false) then
      return p_preferred;
    end if;
  end if;
  return p_fallback;
end;
$$;

-- ---------------------------------------------------------------------
-- 2) Crosswalk (parcel -> geohierarchy)
--    Shared table in public. Additive only.
-- ---------------------------------------------------------------------
create table if not exists public.parcel_ladder_v1 (
  acct              text primary key,
  tabblock_geoid20  text,
  tract_geoid20     text,
  unsd_geoid        text,
  unsd_name         text,
  zcta5             text,
  neighborhood_id   text,
  neighborhood_name text,
  parent_rule       text not null default 'centroid_within',
  gis_year          integer not null,
  updated_at_utc    timestamptz not null default now()
);

alter table public.parcel_ladder_v1 add column if not exists neighborhood_id text;
alter table public.parcel_ladder_v1 add column if not exists neighborhood_name text;
alter table public.parcel_ladder_v1 add column if not exists updated_at_utc timestamptz not null default now();

create index if not exists parcel_ladder_v1_gis_year_idx     on public.parcel_ladder_v1 (gis_year);
create index if not exists parcel_ladder_v1_tabblock_idx     on public.parcel_ladder_v1 (tabblock_geoid20);
create index if not exists parcel_ladder_v1_tract_idx        on public.parcel_ladder_v1 (tract_geoid20);
create index if not exists parcel_ladder_v1_zcta5_idx        on public.parcel_ladder_v1 (zcta5);
create index if not exists parcel_ladder_v1_unsd_idx         on public.parcel_ladder_v1 (unsd_geoid);
create index if not exists parcel_ladder_v1_neighborhood_idx on public.parcel_ladder_v1 (neighborhood_id);

-- Add a uniquely named trigger on the shared table (no drops; no collision with prod trigger names)
do $$
begin
  if to_regclass('public.parcel_ladder_v1') is not null
     and not exists (
       select 1
       from pg_trigger
       where tgrelid = 'public.parcel_ladder_v1'::regclass
         and tgname = 'trg_touch_updated_at_utc_f7f31c6e4'
         and not tgisinternal
     )
  then
    execute $sql$
      create trigger trg_touch_updated_at_utc_f7f31c6e4
      before update on public.parcel_ladder_v1
      for each row execute function forecast_20260220_7f31c6e4.touch_updated_at_utc_generic()
    $sql$;
  end if;
end $$;

-- ---------------------------------------------------------------------
-- 3) Geometry tables (shared in public; additive create-if-missing)
--    Assumed EPSG:4326
-- ---------------------------------------------------------------------

-- 3A) Parcel polygons
create table if not exists public.geo_parcel_poly (
  acct text primary key,
  geom geometry(MultiPolygon, 4326) not null
);
create index if not exists geo_parcel_poly_geom_gix
  on public.geo_parcel_poly using gist (geom);

-- Optional simplified parcels for z>=17
create table if not exists public.geo_parcel_poly_z17 (
  acct text primary key,
  geom geometry(MultiPolygon, 4326) not null
);
create index if not exists geo_parcel_poly_z17_geom_gix
  on public.geo_parcel_poly_z17 using gist (geom);

-- 3B) Census + school district + neighborhood geometries
create table if not exists public.geo_tabblock20_tx (
  geoid20 text primary key,
  geom geometry(MultiPolygon, 4326) not null
);
create index if not exists geo_tabblock20_tx_geom_gix
  on public.geo_tabblock20_tx using gist (geom);

create table if not exists public.geo_tract20_tx (
  geoid text primary key,
  geom geometry(MultiPolygon, 4326) not null
);
create index if not exists geo_tract20_tx_geom_gix
  on public.geo_tract20_tx using gist (geom);

create table if not exists public.geo_zcta20_us (
  zcta5 text primary key,
  geom geometry(MultiPolygon, 4326) not null
);
create index if not exists geo_zcta20_us_geom_gix
  on public.geo_zcta20_us using gist (geom);

create table if not exists public.geo_unsd23_tx (
  geoid text primary key,
  name text,
  geom geometry(MultiPolygon, 4326) not null
);
create index if not exists geo_unsd23_tx_geom_gix
  on public.geo_unsd23_tx using gist (geom);

create table if not exists public.geo_neighborhood_tx (
  neighborhood_id   text primary key,
  neighborhood_name text,
  geom geometry(MultiPolygon, 4326) not null
);
create index if not exists geo_neighborhood_tx_geom_gix
  on public.geo_neighborhood_tx using gist (geom);

-- 3C) Optional simplified geometry tables (recommended)
create table if not exists public.geo_zcta20_us_z7 (
  zcta5 text primary key,
  geom geometry(MultiPolygon, 4326) not null
);
create index if not exists geo_zcta20_us_z7_geom_gix
  on public.geo_zcta20_us_z7 using gist (geom);

create table if not exists public.geo_tract20_tx_z10 (
  geoid text primary key,
  geom geometry(MultiPolygon, 4326) not null
);
create index if not exists geo_tract20_tx_z10_geom_gix
  on public.geo_tract20_tx_z10 using gist (geom);

create table if not exists public.geo_tabblock20_tx_z13 (
  geoid20 text primary key,
  geom geometry(MultiPolygon, 4326) not null
);
create index if not exists geo_tabblock20_tx_z13_geom_gix
  on public.geo_tabblock20_tx_z13 using gist (geom);

create table if not exists public.geo_neighborhood_tx_z12 (
  neighborhood_id text primary key,
  geom geometry(MultiPolygon, 4326) not null
);
create index if not exists geo_neighborhood_tx_z12_geom_gix
  on public.geo_neighborhood_tx_z12 using gist (geom);

-- ---------------------------------------------------------------------
-- 4) Metrics tables (isolated schema; no drops)
--    Supports:
--      - forecast/history
--      - backtests alongside forecast (series_kind + variant_id)
--      - full fan quantiles
--      - run metadata
-- ---------------------------------------------------------------------

create temp table tmp_metric_levels_f7f31c6e4 (
  level_alias       text primary key,
  key_col           text not null,
  forecast_fqtn     text not null,
  history_fqtn      text not null
) on commit drop;

insert into tmp_metric_levels_f7f31c6e4(level_alias, key_col, forecast_fqtn, history_fqtn) values
  ('parcel'      , 'acct'            , 'forecast_20260220_7f31c6e4.metrics_parcel_forecast'      , 'forecast_20260220_7f31c6e4.metrics_parcel_history'),
  ('tabblock'    , 'tabblock_geoid20', 'forecast_20260220_7f31c6e4.metrics_tabblock_forecast'    , 'forecast_20260220_7f31c6e4.metrics_tabblock_history'),
  ('tract'       , 'tract_geoid20'   , 'forecast_20260220_7f31c6e4.metrics_tract_forecast'       , 'forecast_20260220_7f31c6e4.metrics_tract_history'),
  ('zcta'        , 'zcta5'           , 'forecast_20260220_7f31c6e4.metrics_zcta_forecast'        , 'forecast_20260220_7f31c6e4.metrics_zcta_history'),
  ('unsd'        , 'unsd_geoid'      , 'forecast_20260220_7f31c6e4.metrics_unsd_forecast'        , 'forecast_20260220_7f31c6e4.metrics_unsd_history'),
  ('neighborhood', 'neighborhood_id' , 'forecast_20260220_7f31c6e4.metrics_neighborhood_forecast', 'forecast_20260220_7f31c6e4.metrics_neighborhood_history');

do $$
declare
  rec record;
begin
  for rec in select * from tmp_metric_levels_f7f31c6e4 order by level_alias loop
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
          or
          (series_kind = 'backtest' and variant_id <> '__forecast__')
        )
      )
    $sql$,
      rec.forecast_fqtn,
      rec.key_col,
      'pk_' || rec.level_alias || '_forecast',
      rec.key_col,
      'ck_' || rec.level_alias || '_forecast_series_kind',
      'ck_' || rec.level_alias || '_forecast_variant'
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
          or
          (series_kind = 'backtest' and variant_id <> '__history__')
        )
      )
    $sql$,
      rec.history_fqtn,
      rec.key_col,
      'pk_' || rec.level_alias || '_history',
      rec.key_col,
      'ck_' || rec.level_alias || '_history_series_kind',
      'ck_' || rec.level_alias || '_history_variant'
    );

    -- Forecast indexes
    execute format(
      'create index if not exists %I on %s (series_kind, origin_year, horizon_m, %I)',
      'ix_' || rec.level_alias || '_f_query',
      rec.forecast_fqtn,
      rec.key_col
    );

    execute format(
      'create index if not exists %I on %s (series_kind, forecast_year, %I)',
      'ix_' || rec.level_alias || '_f_fyear',
      rec.forecast_fqtn,
      rec.key_col
    );

    execute format(
      'create index if not exists %I on %s (run_id)',
      'ix_' || rec.level_alias || '_f_runid',
      rec.forecast_fqtn
    );

    execute format(
      'create index if not exists %I on %s (backtest_id)',
      'ix_' || rec.level_alias || '_f_backtestid',
      rec.forecast_fqtn
    );

    execute format(
      'create index if not exists %I on %s (variant_id)',
      'ix_' || rec.level_alias || '_f_variant',
      rec.forecast_fqtn
    );

    -- History indexes
    execute format(
      'create index if not exists %I on %s (series_kind, year, %I)',
      'ix_' || rec.level_alias || '_h_query',
      rec.history_fqtn,
      rec.key_col
    );

    execute format(
      'create index if not exists %I on %s (run_id)',
      'ix_' || rec.level_alias || '_h_runid',
      rec.history_fqtn
    );

    execute format(
      'create index if not exists %I on %s (backtest_id)',
      'ix_' || rec.level_alias || '_h_backtestid',
      rec.history_fqtn
    );

    execute format(
      'create index if not exists %I on %s (variant_id)',
      'ix_' || rec.level_alias || '_h_variant',
      rec.history_fqtn
    );

    -- updated_at triggers (safe on rerun via duplicate_object handler)
    begin
      execute format(
        'create trigger %I before update on %s for each row execute function forecast_20260220_7f31c6e4.touch_updated_at_generic()',
        'trg_' || rec.level_alias || '_forecast_touch_updated_at',
        rec.forecast_fqtn
      );
    exception when duplicate_object then
      null;
    end;

    begin
      execute format(
        'create trigger %I before update on %s for each row execute function forecast_20260220_7f31c6e4.touch_updated_at_generic()',
        'trg_' || rec.level_alias || '_history_touch_updated_at',
        rec.history_fqtn
      );
    exception when duplicate_object then
      null;
    end;
  end loop;
end $$;

-- ---------------------------------------------------------------------
-- 5) Live inference progress / status tables (isolated schema; no drops)
-- ---------------------------------------------------------------------
create table if not exists forecast_20260220_7f31c6e4.inference_runs (
  run_id          text primary key,
  level_name      text not null,     -- parcel/tabblock/tract/zcta/unsd/neighborhood
  mode            text not null,     -- forecast/backtest/history/etc.
  origin_year     integer,
  horizon_m       integer,           -- optional (single-horizon runs)
  as_of_date      date,
  model_version   text,
  n_scenarios     integer,

  status          text not null default 'running', -- running/completed/failed/cancelled
  started_at      timestamptz not null default now(),
  completed_at    timestamptz,
  notes           text,

  inserted_at     timestamptz not null default now(),
  updated_at      timestamptz not null default now(),

  constraint ck_inference_runs_status check (status in ('running','completed','failed','cancelled'))
);

create index if not exists ix_inference_runs_status
  on forecast_20260220_7f31c6e4.inference_runs(status);
create index if not exists ix_inference_runs_level
  on forecast_20260220_7f31c6e4.inference_runs(level_name);
create index if not exists ix_inference_runs_origin
  on forecast_20260220_7f31c6e4.inference_runs(origin_year);
create index if not exists ix_inference_runs_started_at
  on forecast_20260220_7f31c6e4.inference_runs(started_at desc);

create table if not exists forecast_20260220_7f31c6e4.inference_run_progress (
  run_id              text not null references forecast_20260220_7f31c6e4.inference_runs(run_id) on delete cascade,
  chunk_seq           integer not null,
  level_name          text not null,
  status              text not null default 'running',
  series_kind         text,
  variant_id          text,
  origin_year         integer,
  horizon_m           integer,
  year                integer,  -- for history fills

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

create index if not exists ix_inference_progress_run
  on forecast_20260220_7f31c6e4.inference_run_progress(run_id);
create index if not exists ix_inference_progress_heartbeat
  on forecast_20260220_7f31c6e4.inference_run_progress(heartbeat_at desc);

do $$
begin
  begin
    create trigger trg_inference_runs_touch_updated_at
    before update on forecast_20260220_7f31c6e4.inference_runs
    for each row execute function forecast_20260220_7f31c6e4.touch_updated_at_generic();
  exception when duplicate_object then
    null;
  end;

  begin
    create trigger trg_inference_progress_touch_updated_at
    before update on forecast_20260220_7f31c6e4.inference_run_progress
    for each row execute function forecast_20260220_7f31c6e4.touch_updated_at_generic();
  exception when duplicate_object then
    null;
  end;
end $$;

-- ---------------------------------------------------------------------
-- 6) Optional raster tile cache (isolated schema; no drops)
-- ---------------------------------------------------------------------
create table if not exists forecast_20260220_7f31c6e4.raster_tile_cache (
  id            bigserial primary key,
  layer_name    text not null,      -- e.g. 'forecast_value', 'backtest_error'
  level_name    text not null,      -- parcel/tabblock/tract/zcta/unsd/neighborhood
  series_kind   text not null,      -- forecast/backtest/history
  variant_id    text,               -- __forecast__ / backtest id / __history__
  run_id        text,
  backtest_id   text,
  origin_year   integer,
  horizon_m     integer,
  year          integer,            -- for history rasters
  z             integer not null,
  x             integer not null,
  y             integer not null,
  img_format    text not null default 'png', -- png/webp
  tile_bytes    bytea not null,
  inserted_at   timestamptz not null default now(),
  updated_at    timestamptz not null default now()
);

create unique index if not exists ux_raster_tile_cache_lookup
  on forecast_20260220_7f31c6e4.raster_tile_cache (
    layer_name,
    level_name,
    series_kind,
    coalesce(variant_id,''),
    coalesce(run_id,''),
    coalesce(backtest_id,''),
    coalesce(origin_year,-1),
    coalesce(horizon_m,-1),
    coalesce(year,-1),
    z, x, y
  );

create index if not exists ix_raster_tile_cache_lookup_fast
  on forecast_20260220_7f31c6e4.raster_tile_cache (layer_name, level_name, series_kind, z, x, y);

do $$
begin
  begin
    create trigger trg_raster_tile_cache_touch_updated_at
    before update on forecast_20260220_7f31c6e4.raster_tile_cache
    for each row execute function forecast_20260220_7f31c6e4.touch_updated_at_generic();
  exception when duplicate_object then
    null;
  end;
end $$;

create or replace function forecast_20260220_7f31c6e4.get_raster_tile_cache(
  p_layer_name  text,
  p_level_name  text,
  p_series_kind text,
  p_z           integer,
  p_x           integer,
  p_y           integer,
  p_variant_id  text default null,
  p_run_id      text default null,
  p_backtest_id text default null,
  p_origin_year integer default null,
  p_horizon_m   integer default null,
  p_year        integer default null
)
returns bytea
language sql
stable
as $$
select t.tile_bytes
from forecast_20260220_7f31c6e4.raster_tile_cache t
where t.layer_name = p_layer_name
  and t.level_name = p_level_name
  and t.series_kind = p_series_kind
  and t.z = p_z and t.x = p_x and t.y = p_y
  and coalesce(t.variant_id,'')  = coalesce(p_variant_id,'')
  and coalesce(t.run_id,'')      = coalesce(p_run_id,'')
  and coalesce(t.backtest_id,'') = coalesce(p_backtest_id,'')
  and coalesce(t.origin_year,-1) = coalesce(p_origin_year,-1)
  and coalesce(t.horizon_m,-1)   = coalesce(p_horizon_m,-1)
  and coalesce(t.year,-1)        = coalesce(p_year,-1)
limit 1;
$$;

-- ---------------------------------------------------------------------
-- 7) Parcel frontend views (isolated schema)
-- ---------------------------------------------------------------------
create or replace view forecast_20260220_7f31c6e4.v_metrics_parcel_fan_latest as
with latest as (
  select acct, max(origin_year) as origin_year
  from forecast_20260220_7f31c6e4.metrics_parcel_forecast
  where series_kind = 'forecast'
    and variant_id = '__forecast__'
  group by acct
)
select
  f.acct,
  f.origin_year,
  f.horizon_m,
  coalesce(f.forecast_year, f.origin_year + ((f.horizon_m + 11) / 12))::integer as forecast_year,
  f.value,
  f.p10,
  f.p25,
  coalesce(f.p50, f.value) as p50,
  f.p75,
  f.p90,
  f.n,
  f.run_id,
  f.backtest_id,
  f.variant_id,
  f.series_kind,
  f.model_version,
  f.as_of_date,
  f.n_scenarios,
  f.is_backtest,
  f.inserted_at,
  f.updated_at
from forecast_20260220_7f31c6e4.metrics_parcel_forecast f
join latest l
  on l.acct = f.acct
 and l.origin_year = f.origin_year
where f.series_kind = 'forecast'
  and f.variant_id = '__forecast__';

create or replace view forecast_20260220_7f31c6e4.v_metrics_parcel_history_norm as
select
  acct,
  year,
  value,
  coalesce(p50, value) as p50,
  n,
  run_id,
  backtest_id,
  variant_id,
  series_kind,
  model_version,
  as_of_date,
  inserted_at,
  updated_at
from forecast_20260220_7f31c6e4.metrics_parcel_history;

create or replace view forecast_20260220_7f31c6e4.v_metrics_parcel_timeseries_frontend as
select
  h.acct,
  null::integer as origin_year,
  null::integer as horizon_m,
  h.year        as year,
  'history'::text as series_kind,
  h.variant_id,
  h.value,
  null::double precision as p10,
  null::double precision as p25,
  h.p50,
  null::double precision as p75,
  null::double precision as p90,
  h.n,
  h.run_id,
  h.backtest_id,
  h.model_version,
  h.as_of_date,
  null::integer as n_scenarios,
  null::boolean as is_backtest,
  h.inserted_at,
  h.updated_at
from forecast_20260220_7f31c6e4.v_metrics_parcel_history_norm h
where h.series_kind = 'history'
  and h.variant_id = '__history__'

union all

select
  f.acct,
  f.origin_year,
  f.horizon_m,
  coalesce(f.forecast_year, f.origin_year + ((f.horizon_m + 11) / 12))::integer as year,
  f.series_kind,
  f.variant_id,
  f.value,
  f.p10,
  f.p25,
  coalesce(f.p50, f.value) as p50,
  f.p75,
  f.p90,
  f.n,
  f.run_id,
  f.backtest_id,
  f.model_version,
  f.as_of_date,
  f.n_scenarios,
  f.is_backtest,
  f.inserted_at,
  f.updated_at
from forecast_20260220_7f31c6e4.v_metrics_parcel_fan_latest f;

-- ---------------------------------------------------------------------
-- 8) Generic MVT builders (forecast/history) (isolated schema functions)
-- ---------------------------------------------------------------------
create or replace function forecast_20260220_7f31c6e4._mvt_forecast_generic(
  p_layer_name       text,
  p_geom_fqtn        text,
  p_geom_key_col     text,
  p_metrics_fqtn     text,
  p_metrics_key_col  text,
  z                  integer,
  x                  integer,
  y                  integer,
  p_origin_year      integer,
  p_horizon_m        integer,
  p_series_kind      text default 'forecast',
  p_variant_id       text default '__forecast__',
  p_run_id           text default null,
  p_backtest_id      text default null,
  p_limit            integer default null
)
returns bytea
language plpgsql
stable
as $$
declare
  v_sql text;
  v_mvt bytea;
  v_limit_sql text := '';
begin
  if p_limit is not null and p_limit > 0 then
    v_limit_sql := format(' limit %s', p_limit);
  end if;

  v_sql := format($fmt$
    with bounds as (
      select
        ST_TileEnvelope($1, $2, $3) as b3857,
        ST_Transform(ST_TileEnvelope($1, $2, $3), 4326) as b4326
    ),
    src as (
      select
        g.%1$I::text as id,
        m.origin_year,
        m.horizon_m,
        coalesce(m.forecast_year, m.origin_year + ((m.horizon_m + 11) / 12))::integer as forecast_year,

        m.value,
        m.p10,
        m.p25,
        coalesce(m.p50, m.value) as p50,
        m.p75,
        m.p90,
        m.n,

        m.series_kind,
        m.variant_id,
        m.run_id,
        m.backtest_id,
        m.model_version,
        m.as_of_date,
        m.n_scenarios,
        m.is_backtest,

        ST_AsMVTGeom(ST_Transform(g.geom, 3857), bounds.b3857, 4096, 256, true) as geom
      from %2$s g
      join %3$s m
        on m.%4$I = g.%1$I
      cross join bounds
      where g.geom && bounds.b4326
        and ST_Intersects(g.geom, bounds.b4326)
        and m.origin_year = $4
        and m.horizon_m   = $5
        and m.series_kind = $6
        and m.variant_id  = $7
        and ($8 is null or m.run_id = $8)
        and ($9 is null or m.backtest_id = $9)
      %5$s
    )
    select ST_AsMVT(src, %6$L, 4096, 'geom')
    from src
  $fmt$,
    p_geom_key_col,
    p_geom_fqtn,
    p_metrics_fqtn,
    p_metrics_key_col,
    v_limit_sql,
    p_layer_name
  );

  execute v_sql
    using z, x, y, p_origin_year, p_horizon_m, p_series_kind, p_variant_id, p_run_id, p_backtest_id
    into v_mvt;

  return coalesce(v_mvt, ''::bytea);
end;
$$;

create or replace function forecast_20260220_7f31c6e4._mvt_history_generic(
  p_layer_name       text,
  p_geom_fqtn        text,
  p_geom_key_col     text,
  p_metrics_fqtn     text,
  p_metrics_key_col  text,
  z                  integer,
  x                  integer,
  y                  integer,
  p_year             integer,
  p_series_kind      text default 'history',
  p_variant_id       text default '__history__',
  p_run_id           text default null,
  p_backtest_id      text default null,
  p_limit            integer default null
)
returns bytea
language plpgsql
stable
as $$
declare
  v_sql text;
  v_mvt bytea;
  v_limit_sql text := '';
begin
  if p_limit is not null and p_limit > 0 then
    v_limit_sql := format(' limit %s', p_limit);
  end if;

  v_sql := format($fmt$
    with bounds as (
      select
        ST_TileEnvelope($1, $2, $3) as b3857,
        ST_Transform(ST_TileEnvelope($1, $2, $3), 4326) as b4326
    ),
    src as (
      select
        g.%1$I::text as id,
        m.year,

        m.value,
        coalesce(m.p50, m.value) as p50,
        m.n,

        m.series_kind,
        m.variant_id,
        m.run_id,
        m.backtest_id,
        m.model_version,
        m.as_of_date,

        ST_AsMVTGeom(ST_Transform(g.geom, 3857), bounds.b3857, 4096, 256, true) as geom
      from %2$s g
      join %3$s m
        on m.%4$I = g.%1$I
      cross join bounds
      where g.geom && bounds.b4326
        and ST_Intersects(g.geom, bounds.b4326)
        and m.year        = $4
        and m.series_kind = $5
        and m.variant_id  = $6
        and ($7 is null or m.run_id = $7)
        and ($8 is null or m.backtest_id = $8)
      %5$s
    )
    select ST_AsMVT(src, %6$L, 4096, 'geom')
    from src
  $fmt$,
    p_geom_key_col,
    p_geom_fqtn,
    p_metrics_fqtn,
    p_metrics_key_col,
    v_limit_sql,
    p_layer_name
  );

  execute v_sql
    using z, x, y, p_year, p_series_kind, p_variant_id, p_run_id, p_backtest_id
    into v_mvt;

  return coalesce(v_mvt, ''::bytea);
end;
$$;

-- ---------------------------------------------------------------------
-- 9) Forecast MVT leaf functions
-- ---------------------------------------------------------------------
create or replace function forecast_20260220_7f31c6e4.mvt_zcta_choropleth_forecast(
  z integer, x integer, y integer,
  p_origin_year integer,
  p_horizon_m integer,
  p_series_kind text default 'forecast',
  p_variant_id  text default '__forecast__',
  p_run_id      text default null,
  p_backtest_id text default null
)
returns bytea
language plpgsql
stable
as $$
declare
  v_geom text;
begin
  v_geom := forecast_20260220_7f31c6e4._pick_geom_table('public.geo_zcta20_us_z7', 'public.geo_zcta20_us');
  return forecast_20260220_7f31c6e4._mvt_forecast_generic(
    'zcta', v_geom, 'zcta5',
    'forecast_20260220_7f31c6e4.metrics_zcta_forecast', 'zcta5',
    z, x, y, p_origin_year, p_horizon_m,
    p_series_kind, p_variant_id, p_run_id, p_backtest_id, null
  );
end;
$$;

create or replace function forecast_20260220_7f31c6e4.mvt_tract_choropleth_forecast(
  z integer, x integer, y integer,
  p_origin_year integer,
  p_horizon_m integer,
  p_series_kind text default 'forecast',
  p_variant_id  text default '__forecast__',
  p_run_id      text default null,
  p_backtest_id text default null
)
returns bytea
language plpgsql
stable
as $$
declare
  v_geom text;
begin
  v_geom := forecast_20260220_7f31c6e4._pick_geom_table('public.geo_tract20_tx_z10', 'public.geo_tract20_tx');
  return forecast_20260220_7f31c6e4._mvt_forecast_generic(
    'tract', v_geom, 'geoid',
    'forecast_20260220_7f31c6e4.metrics_tract_forecast', 'tract_geoid20',
    z, x, y, p_origin_year, p_horizon_m,
    p_series_kind, p_variant_id, p_run_id, p_backtest_id, null
  );
end;
$$;

create or replace function forecast_20260220_7f31c6e4.mvt_tabblock_choropleth_forecast(
  z integer, x integer, y integer,
  p_origin_year integer,
  p_horizon_m integer,
  p_series_kind text default 'forecast',
  p_variant_id  text default '__forecast__',
  p_run_id      text default null,
  p_backtest_id text default null
)
returns bytea
language plpgsql
stable
as $$
declare
  v_geom text;
begin
  v_geom := forecast_20260220_7f31c6e4._pick_geom_table('public.geo_tabblock20_tx_z13', 'public.geo_tabblock20_tx');
  return forecast_20260220_7f31c6e4._mvt_forecast_generic(
    'tabblock', v_geom, 'geoid20',
    'forecast_20260220_7f31c6e4.metrics_tabblock_forecast', 'tabblock_geoid20',
    z, x, y, p_origin_year, p_horizon_m,
    p_series_kind, p_variant_id, p_run_id, p_backtest_id, null
  );
end;
$$;

create or replace function forecast_20260220_7f31c6e4.mvt_unsd_choropleth_forecast(
  z integer, x integer, y integer,
  p_origin_year integer,
  p_horizon_m integer,
  p_series_kind text default 'forecast',
  p_variant_id  text default '__forecast__',
  p_run_id      text default null,
  p_backtest_id text default null
)
returns bytea
language plpgsql
stable
as $$
begin
  return forecast_20260220_7f31c6e4._mvt_forecast_generic(
    'unsd', 'public.geo_unsd23_tx', 'geoid',
    'forecast_20260220_7f31c6e4.metrics_unsd_forecast', 'unsd_geoid',
    z, x, y, p_origin_year, p_horizon_m,
    p_series_kind, p_variant_id, p_run_id, p_backtest_id, null
  );
end;
$$;

create or replace function forecast_20260220_7f31c6e4.mvt_neighborhood_choropleth_forecast(
  z integer, x integer, y integer,
  p_origin_year integer,
  p_horizon_m integer,
  p_series_kind text default 'forecast',
  p_variant_id  text default '__forecast__',
  p_run_id      text default null,
  p_backtest_id text default null
)
returns bytea
language plpgsql
stable
as $$
declare
  v_geom text;
begin
  v_geom := forecast_20260220_7f31c6e4._pick_geom_table('public.geo_neighborhood_tx_z12', 'public.geo_neighborhood_tx');
  return forecast_20260220_7f31c6e4._mvt_forecast_generic(
    'neighborhood', v_geom, 'neighborhood_id',
    'forecast_20260220_7f31c6e4.metrics_neighborhood_forecast', 'neighborhood_id',
    z, x, y, p_origin_year, p_horizon_m,
    p_series_kind, p_variant_id, p_run_id, p_backtest_id, null
  );
end;
$$;

create or replace function forecast_20260220_7f31c6e4.mvt_parcel_choropleth_forecast(
  z integer, x integer, y integer,
  p_origin_year integer,
  p_horizon_m integer,
  p_series_kind text default 'forecast',
  p_variant_id  text default '__forecast__',
  p_run_id      text default null,
  p_backtest_id text default null,
  p_limit       integer default 3500
)
returns bytea
language plpgsql
stable
as $$
declare
  v_geom text;
begin
  v_geom := forecast_20260220_7f31c6e4._pick_geom_table('public.geo_parcel_poly_z17', 'public.geo_parcel_poly');
  return forecast_20260220_7f31c6e4._mvt_forecast_generic(
    'parcel', v_geom, 'acct',
    'forecast_20260220_7f31c6e4.metrics_parcel_forecast', 'acct',
    z, x, y, p_origin_year, p_horizon_m,
    p_series_kind, p_variant_id, p_run_id, p_backtest_id, p_limit
  );
end;
$$;

-- ---------------------------------------------------------------------
-- 10) History MVT leaf functions
-- ---------------------------------------------------------------------
create or replace function forecast_20260220_7f31c6e4.mvt_zcta_choropleth_history(
  z integer, x integer, y integer,
  p_year integer,
  p_series_kind text default 'history',
  p_variant_id  text default '__history__',
  p_run_id      text default null,
  p_backtest_id text default null
)
returns bytea
language plpgsql
stable
as $$
declare
  v_geom text;
begin
  v_geom := forecast_20260220_7f31c6e4._pick_geom_table('public.geo_zcta20_us_z7', 'public.geo_zcta20_us');
  return forecast_20260220_7f31c6e4._mvt_history_generic(
    'zcta', v_geom, 'zcta5',
    'forecast_20260220_7f31c6e4.metrics_zcta_history', 'zcta5',
    z, x, y, p_year,
    p_series_kind, p_variant_id, p_run_id, p_backtest_id, null
  );
end;
$$;

create or replace function forecast_20260220_7f31c6e4.mvt_tract_choropleth_history(
  z integer, x integer, y integer,
  p_year integer,
  p_series_kind text default 'history',
  p_variant_id  text default '__history__',
  p_run_id      text default null,
  p_backtest_id text default null
)
returns bytea
language plpgsql
stable
as $$
declare
  v_geom text;
begin
  v_geom := forecast_20260220_7f31c6e4._pick_geom_table('public.geo_tract20_tx_z10', 'public.geo_tract20_tx');
  return forecast_20260220_7f31c6e4._mvt_history_generic(
    'tract', v_geom, 'geoid',
    'forecast_20260220_7f31c6e4.metrics_tract_history', 'tract_geoid20',
    z, x, y, p_year,
    p_series_kind, p_variant_id, p_run_id, p_backtest_id, null
  );
end;
$$;

create or replace function forecast_20260220_7f31c6e4.mvt_tabblock_choropleth_history(
  z integer, x integer, y integer,
  p_year integer,
  p_series_kind text default 'history',
  p_variant_id  text default '__history__',
  p_run_id      text default null,
  p_backtest_id text default null
)
returns bytea
language plpgsql
stable
as $$
declare
  v_geom text;
begin
  v_geom := forecast_20260220_7f31c6e4._pick_geom_table('public.geo_tabblock20_tx_z13', 'public.geo_tabblock20_tx');
  return forecast_20260220_7f31c6e4._mvt_history_generic(
    'tabblock', v_geom, 'geoid20',
    'forecast_20260220_7f31c6e4.metrics_tabblock_history', 'tabblock_geoid20',
    z, x, y, p_year,
    p_series_kind, p_variant_id, p_run_id, p_backtest_id, null
  );
end;
$$;

create or replace function forecast_20260220_7f31c6e4.mvt_unsd_choropleth_history(
  z integer, x integer, y integer,
  p_year integer,
  p_series_kind text default 'history',
  p_variant_id  text default '__history__',
  p_run_id      text default null,
  p_backtest_id text default null
)
returns bytea
language plpgsql
stable
as $$
begin
  return forecast_20260220_7f31c6e4._mvt_history_generic(
    'unsd', 'public.geo_unsd23_tx', 'geoid',
    'forecast_20260220_7f31c6e4.metrics_unsd_history', 'unsd_geoid',
    z, x, y, p_year,
    p_series_kind, p_variant_id, p_run_id, p_backtest_id, null
  );
end;
$$;

create or replace function forecast_20260220_7f31c6e4.mvt_neighborhood_choropleth_history(
  z integer, x integer, y integer,
  p_year integer,
  p_series_kind text default 'history',
  p_variant_id  text default '__history__',
  p_run_id      text default null,
  p_backtest_id text default null
)
returns bytea
language plpgsql
stable
as $$
declare
  v_geom text;
begin
  v_geom := forecast_20260220_7f31c6e4._pick_geom_table('public.geo_neighborhood_tx_z12', 'public.geo_neighborhood_tx');
  return forecast_20260220_7f31c6e4._mvt_history_generic(
    'neighborhood', v_geom, 'neighborhood_id',
    'forecast_20260220_7f31c6e4.metrics_neighborhood_history', 'neighborhood_id',
    z, x, y, p_year,
    p_series_kind, p_variant_id, p_run_id, p_backtest_id, null
  );
end;
$$;

create or replace function forecast_20260220_7f31c6e4.mvt_parcel_choropleth_history(
  z integer, x integer, y integer,
  p_year integer,
  p_series_kind text default 'history',
  p_variant_id  text default '__history__',
  p_run_id      text default null,
  p_backtest_id text default null,
  p_limit       integer default 3500
)
returns bytea
language plpgsql
stable
as $$
declare
  v_geom text;
begin
  v_geom := forecast_20260220_7f31c6e4._pick_geom_table('public.geo_parcel_poly_z17', 'public.geo_parcel_poly');
  return forecast_20260220_7f31c6e4._mvt_history_generic(
    'parcel', v_geom, 'acct',
    'forecast_20260220_7f31c6e4.metrics_parcel_history', 'acct',
    z, x, y, p_year,
    p_series_kind, p_variant_id, p_run_id, p_backtest_id, p_limit
  );
end;
$$;

-- ---------------------------------------------------------------------
-- 11) Parcel lotline overlay MVT (no metrics; outline only)
-- ---------------------------------------------------------------------
create or replace function forecast_20260220_7f31c6e4.mvt_parcel_lotlines(
  z integer,
  x integer,
  y integer,
  p_limit integer default 6000
)
returns bytea
language plpgsql
stable
as $$
declare
  v_geom_table text;
  v_mvt bytea;
begin
  v_geom_table := forecast_20260220_7f31c6e4._pick_geom_table('public.geo_parcel_poly_z17', 'public.geo_parcel_poly');

  execute format($fmt$
    with bounds as (
      select
        ST_TileEnvelope($1, $2, $3) as b3857,
        ST_Transform(ST_TileEnvelope($1, $2, $3), 4326) as b4326
    ),
    src as (
      select
        g.acct::text as id,
        ST_AsMVTGeom(
          ST_Transform(ST_Boundary(g.geom), 3857),
          bounds.b3857,
          4096,
          256,
          true
        ) as geom
      from %1$s g
      cross join bounds
      where g.geom && bounds.b4326
        and ST_Intersects(g.geom, bounds.b4326)
      limit %2$s
    )
    select ST_AsMVT(src, 'parcel_lotlines', 4096, 'geom')
    from src
  $fmt$, v_geom_table, greatest(p_limit,1))
  using z, x, y
  into v_mvt;

  return coalesce(v_mvt, ''::bytea);
end;
$$;

-- ---------------------------------------------------------------------
-- 12) Routers (forecast + history)
-- ---------------------------------------------------------------------
create or replace function forecast_20260220_7f31c6e4.mvt_choropleth_forecast(
  z integer, x integer, y integer,
  p_origin_year integer,
  p_horizon_m integer,
  p_level_override text default null,
  p_series_kind text default 'forecast',
  p_variant_id  text default '__forecast__',
  p_run_id      text default null,
  p_backtest_id text default null,
  p_parcel_limit integer default 3500
)
returns bytea
language plpgsql
stable
as $$
begin
  if p_level_override is not null then
    case lower(p_level_override)
      when 'zcta' then
        return forecast_20260220_7f31c6e4.mvt_zcta_choropleth_forecast(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
      when 'tract' then
        return forecast_20260220_7f31c6e4.mvt_tract_choropleth_forecast(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
      when 'tabblock' then
        return forecast_20260220_7f31c6e4.mvt_tabblock_choropleth_forecast(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
      when 'unsd' then
        return forecast_20260220_7f31c6e4.mvt_unsd_choropleth_forecast(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
      when 'neighborhood' then
        return forecast_20260220_7f31c6e4.mvt_neighborhood_choropleth_forecast(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
      when 'parcel' then
        return forecast_20260220_7f31c6e4.mvt_parcel_choropleth_forecast(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id,p_run_id,p_backtest_id,p_parcel_limit);
      else
        return ''::bytea;
    end case;
  end if;

  if z <= 7 then
    return forecast_20260220_7f31c6e4.mvt_zcta_choropleth_forecast(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
  elsif z <= 11 then
    return forecast_20260220_7f31c6e4.mvt_tract_choropleth_forecast(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
  elsif z <= 16 then
    return forecast_20260220_7f31c6e4.mvt_tabblock_choropleth_forecast(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
  else
    return forecast_20260220_7f31c6e4.mvt_parcel_choropleth_forecast(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id,p_run_id,p_backtest_id,p_parcel_limit);
  end if;
end;
$$;

create or replace function forecast_20260220_7f31c6e4.mvt_choropleth_history(
  z integer, x integer, y integer,
  p_year integer,
  p_level_override text default null,
  p_series_kind text default 'history',
  p_variant_id  text default '__history__',
  p_run_id      text default null,
  p_backtest_id text default null,
  p_parcel_limit integer default 3500
)
returns bytea
language plpgsql
stable
as $$
begin
  if p_level_override is not null then
    case lower(p_level_override)
      when 'zcta' then
        return forecast_20260220_7f31c6e4.mvt_zcta_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
      when 'tract' then
        return forecast_20260220_7f31c6e4.mvt_tract_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
      when 'tabblock' then
        return forecast_20260220_7f31c6e4.mvt_tabblock_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
      when 'unsd' then
        return forecast_20260220_7f31c6e4.mvt_unsd_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
      when 'neighborhood' then
        return forecast_20260220_7f31c6e4.mvt_neighborhood_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
      when 'parcel' then
        return forecast_20260220_7f31c6e4.mvt_parcel_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id,p_parcel_limit);
      else
        return ''::bytea;
    end case;
  end if;

  if z <= 7 then
    return forecast_20260220_7f31c6e4.mvt_zcta_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
  elsif z <= 11 then
    return forecast_20260220_7f31c6e4.mvt_tract_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
  elsif z <= 16 then
    return forecast_20260220_7f31c6e4.mvt_tabblock_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
  else
    return forecast_20260220_7f31c6e4.mvt_parcel_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id,p_parcel_limit);
  end if;
end;
$$;

-- Convenience wrappers for explicit UNSD mode
create or replace function forecast_20260220_7f31c6e4.mvt_choropleth_unsd_forecast(
  z integer, x integer, y integer,
  p_origin_year integer,
  p_horizon_m integer,
  p_series_kind text default 'forecast',
  p_variant_id  text default '__forecast__',
  p_run_id      text default null,
  p_backtest_id text default null
)
returns bytea
language sql
stable
as $$
select forecast_20260220_7f31c6e4.mvt_unsd_choropleth_forecast(
  z, x, y, p_origin_year, p_horizon_m, p_series_kind, p_variant_id, p_run_id, p_backtest_id
);
$$;

create or replace function forecast_20260220_7f31c6e4.mvt_choropleth_unsd_history(
  z integer, x integer, y integer,
  p_year integer,
  p_series_kind text default 'history',
  p_variant_id  text default '__history__',
  p_run_id      text default null,
  p_backtest_id text default null
)
returns bytea
language sql
stable
as $$
select forecast_20260220_7f31c6e4.mvt_unsd_choropleth_history(
  z, x, y, p_year, p_series_kind, p_variant_id, p_run_id, p_backtest_id
);
$$;

-- ---------------------------------------------------------------------
-- 13) Lightweight schema diagnostics (isolated schema)
-- ---------------------------------------------------------------------

-- 13A) Forecast tables: fan + backtest columns present
select
  t.level_alias,
  t.forecast_fqtn,
  max((c.column_name = 'p10')::int)         as has_p10,
  max((c.column_name = 'p25')::int)         as has_p25,
  max((c.column_name = 'p50')::int)         as has_p50,
  max((c.column_name = 'p75')::int)         as has_p75,
  max((c.column_name = 'p90')::int)         as has_p90,
  max((c.column_name = 'series_kind')::int) as has_series_kind,
  max((c.column_name = 'variant_id')::int)  as has_variant_id,
  max((c.column_name = 'run_id')::int)      as has_run_id,
  max((c.column_name = 'backtest_id')::int) as has_backtest_id
from tmp_metric_levels_f7f31c6e4 t
left join information_schema.columns c
  on c.table_schema = split_part(t.forecast_fqtn, '.', 1)
 and c.table_name   = split_part(t.forecast_fqtn, '.', 2)
 and c.column_name in ('p10','p25','p50','p75','p90','series_kind','variant_id','run_id','backtest_id')
group by 1,2
order by 1;

-- 13B) History tables present
select
  t.level_alias,
  t.history_fqtn,
  max((c.column_name = 'year')::int)        as has_year,
  max((c.column_name = 'p50')::int)         as has_p50,
  max((c.column_name = 'series_kind')::int) as has_series_kind,
  max((c.column_name = 'variant_id')::int)  as has_variant_id
from tmp_metric_levels_f7f31c6e4 t
left join information_schema.columns c
  on c.table_schema = split_part(t.history_fqtn, '.', 1)
 and c.table_name   = split_part(t.history_fqtn, '.', 2)
 and c.column_name in ('year','p50','series_kind','variant_id')
group by 1,2
order by 1;

-- 13C) Quick row estimates (after you load data, ANALYZE first)
select
  n.nspname as schema_name,
  c.relname as table_name,
  c.reltuples::bigint as est_rows
from pg_class c
join pg_namespace n on n.oid = c.relnamespace
where n.nspname = 'forecast_20260220_7f31c6e4'
  and c.relname in (
    'metrics_parcel_forecast','metrics_parcel_history',
    'metrics_tabblock_forecast','metrics_tabblock_history',
    'metrics_tract_forecast','metrics_tract_history',
    'metrics_zcta_forecast','metrics_zcta_history',
    'metrics_unsd_forecast','metrics_unsd_history',
    'metrics_neighborhood_forecast','metrics_neighborhood_history'
  )
order by c.relname;

-- ---------------------------------------------------------------------
-- 14) Grants for Supabase roles (optional but typically needed)
-- ---------------------------------------------------------------------
grant usage on schema forecast_20260220_7f31c6e4 to anon, authenticated, service_role;
grant select on all tables in schema forecast_20260220_7f31c6e4 to anon, authenticated, service_role;
grant usage, select on all sequences in schema forecast_20260220_7f31c6e4 to anon, authenticated, service_role;
grant execute on all functions in schema forecast_20260220_7f31c6e4 to anon, authenticated, service_role;

alter default privileges in schema forecast_20260220_7f31c6e4
grant select on tables to anon, authenticated, service_role;

alter default privileges in schema forecast_20260220_7f31c6e4
grant usage, select on sequences to anon, authenticated, service_role;

alter default privileges in schema forecast_20260220_7f31c6e4
grant execute on functions to anon, authenticated, service_role;

-- ---------------------------------------------------------------------
-- 15) PostgREST schema cache reload
-- ---------------------------------------------------------------------
notify pgrst, 'reload schema';

-- =====================================================================
-- UPLOAD CONTRACT NOTES (isolated schema)
-- ---------------------------------------------------------------------
-- Forecast production rows:
--   series_kind = 'forecast'
--   variant_id  = '__forecast__'
--
-- Forecast backtest rows:
--   series_kind = 'backtest'
--   variant_id  = '<backtest_id or run-scoped id>'
--   backtest_id = '<same id>'   -- recommended
--
-- History rows:
--   series_kind = 'history'
--   variant_id  = '__history__'
--
-- ON CONFLICT targets:
--   Forecast: (<key>, origin_year, horizon_m, series_kind, variant_id)
--   History : (<key>, year,       series_kind, variant_id)
--
-- SQL tile calls (schema-qualified):
--   Forecast default:
--     select forecast_20260220_7f31c6e4.mvt_choropleth_forecast(z,x,y,2025,12);
--
--   Forecast backtest:
--     select forecast_20260220_7f31c6e4.mvt_choropleth_forecast(z,x,y,2025,12,null,'backtest','bt_2025q1');
--
--   History:
--     select forecast_20260220_7f31c6e4.mvt_choropleth_history(z,x,y,2024);
--
--   UNSD explicit:
--     select forecast_20260220_7f31c6e4.mvt_choropleth_forecast(z,x,y,2025,12,'unsd');
--
--   Parcel lotlines overlay:
--     select forecast_20260220_7f31c6e4.mvt_parcel_lotlines(z,x,y);
--
-- Performance note (1M parcels):
--   - Parcel tiles are only routed at z>=17
--   - Parcel tile rows are capped (default 3500)
--   - Keep public.geo_parcel_poly_z17 populated with simplified geometries
--   - Use vector MVT + client heatmap as the default "fast on-the-fly" path
--   - Raster cache is optional for low-zoom precomputed overlays
-- =====================================================================
