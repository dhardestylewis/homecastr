-- =============================================================================
-- Layer 1: Clamp growth_pct to [-50, +100] in _mvt_forecast_generic
-- Purpose: Prevent extreme outlier predictions from dominating heatmap colors.
-- Bounds are conservative defaults; refine using growth_distribution_diagnostic.sql
-- 
-- Deploy via Supabase SQL Editor.
-- =============================================================================

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
  v_hist_fqtn text;
begin
  if p_limit is not null and p_limit > 0 then
    v_limit_sql := format(' limit %s', p_limit);
  end if;

  v_hist_fqtn := replace(p_metrics_fqtn, '_forecast', '_history');

  if p_horizon_m <= 0 then
    -- ===== HISTORICAL MODE =====
    -- Slider year from history, "now" (2026) from forecast at horizon_m=12
    -- growth = (now_2026 - past) / past * 100

    perform 1 from pg_catalog.pg_class c
      join pg_catalog.pg_namespace n on n.oid = c.relnamespace
      where (n.nspname || '.' || c.relname) = v_hist_fqtn;
    if not found then return ''::bytea; end if;

    v_sql := format($fmt$
      with bounds as (
        select ST_TileEnvelope($1,$2,$3) as b3857,
               ST_Transform(ST_TileEnvelope($1,$2,$3),4326) as b4326
      ), src as (
        select g.%1$I::text as id, $4 as origin_year, $5 as horizon_m,
          ($4+($5/12))::integer as forecast_year,
          coalesce(h_past.p50,h_past.value) as value,
          null::double precision as p10, null::double precision as p25,
          coalesce(h_past.p50,h_past.value) as p50,
          null::double precision as p75, null::double precision as p90,
          null::bigint as n,
          CASE 
            WHEN coalesce(f_now.p50,f_now.value) IS NULL OR coalesce(h_past.p50,h_past.value) IS NULL THEN null
            ELSE least(100, greatest(-50,
              round((100.0*(coalesce(f_now.p50,f_now.value)-coalesce(h_past.p50,h_past.value))
                /nullif(coalesce(h_past.p50,h_past.value),0))::numeric,1)
            ))
          END as growth_pct,
          'historical'::text as series_kind, null::text as variant_id,
          null::text as run_id, null::text as backtest_id,
          null::text as model_version, null::date as as_of_date,
          null::integer as n_scenarios, false as is_backtest,
          ST_AsMVTGeom(ST_Transform(g.geom,3857),bounds.b3857,4096,256,true) as geom
        from %2$s g
        join %3$s h_past on h_past.%4$I=g.%1$I
          and h_past.year=($4+($5/12))::integer
        left join %5$s f_now on f_now.%4$I=g.%1$I
          and f_now.origin_year=$4 and f_now.horizon_m=12
          and f_now.series_kind=$6 and f_now.variant_id=$7
        cross join bounds
        where g.geom && bounds.b4326 and ST_Intersects(g.geom,bounds.b4326)
        %6$s
      ) select ST_AsMVT(src,%7$L,4096,'geom') from src
    $fmt$,
      p_geom_key_col,    -- %1$I
      p_geom_fqtn,       -- %2$s
      v_hist_fqtn,       -- %3$s  (history table)
      p_metrics_key_col, -- %4$I
      p_metrics_fqtn,    -- %5$s  (forecast table for "now" value)
      v_limit_sql,       -- %6$s
      p_layer_name       -- %7$L
    );

  else
    -- ===== FORECAST MODE =====
    -- growth = (forecast_at_horizon - now_2026) / now_2026 * 100
    -- "now" = forecast at horizon_m=12

    v_sql := format($fmt$
      with bounds as (
        select ST_TileEnvelope($1,$2,$3) as b3857,
               ST_Transform(ST_TileEnvelope($1,$2,$3),4326) as b4326
      ), src as (
        select g.%1$I::text as id, m.origin_year, m.horizon_m,
          coalesce(m.forecast_year,m.origin_year+((m.horizon_m+11)/12))::integer as forecast_year,
          m.value, m.p10, m.p25, coalesce(m.p50,m.value) as p50, m.p75, m.p90, m.n,
          CASE 
            WHEN coalesce(m.p50,m.value) IS NULL OR coalesce(f_now.p50,f_now.value) IS NULL THEN null
            ELSE least(100, greatest(-50,
              round((100.0*(coalesce(m.p50,m.value)-coalesce(f_now.p50,f_now.value))
                /nullif(coalesce(f_now.p50,f_now.value),0))::numeric,1)
            ))
          END as growth_pct,
          m.series_kind, m.variant_id, m.run_id, m.backtest_id,
          m.model_version, m.as_of_date, m.n_scenarios, m.is_backtest,
          ST_AsMVTGeom(ST_Transform(g.geom,3857),bounds.b3857,4096,256,true) as geom
        from %2$s g
        join %3$s m on m.%4$I=g.%1$I
        left join %3$s f_now on f_now.%4$I=g.%1$I
          and f_now.origin_year=$4 and f_now.horizon_m=12
          and f_now.series_kind=$6 and f_now.variant_id=$7
        cross join bounds
        where g.geom && bounds.b4326 and ST_Intersects(g.geom,bounds.b4326)
          and m.origin_year=$4 and m.horizon_m=$5
          and m.series_kind=$6 and m.variant_id=$7
          and ($8 is null or m.run_id=$8) and ($9 is null or m.backtest_id=$9)
        %5$s
      ) select ST_AsMVT(src,%6$L,4096,'geom') from src
    $fmt$,
      p_geom_key_col,    -- %1$I
      p_geom_fqtn,       -- %2$s
      p_metrics_fqtn,    -- %3$s
      p_metrics_key_col, -- %4$I
      v_limit_sql,       -- %5$s
      p_layer_name       -- %6$L
    );
  end if;

  execute v_sql
    using z, x, y, p_origin_year, p_horizon_m, p_series_kind, p_variant_id, p_run_id, p_backtest_id
    into v_mvt;

  return coalesce(v_mvt, ''::bytea);
end;
$$;
