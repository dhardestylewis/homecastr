-- Fix MVT wrapper functions that were missed during the geo_tract20_tx rename
-- and fix the column name bug (tract_geoid20 -> geoid) in the public wrapper.

CREATE OR REPLACE FUNCTION forecast_queue.mvt_tract_choropleth(z integer, x integer, y integer, p_origin_year integer DEFAULT 2025, p_horizon_m integer DEFAULT 12, p_series_kind text DEFAULT 'forecast'::text, p_variant_id text DEFAULT '__forecast__'::text)
 RETURNS bytea
 LANGUAGE plpgsql
 STABLE
AS $function$
DECLARE v_geom text;
BEGIN
  v_geom := forecast_queue._pick_geom_table('public.geo_tract20_us_z10', 'public.geo_tract20_us');
  RETURN forecast_queue._mvt_forecast_generic(
    'tract', v_geom, 'geoid',
    'forecast_queue.metrics_tract_forecast', 'tract_geoid20',
    z,x,y,p_origin_year,p_horizon_m,
    p_series_kind,p_variant_id,null,null,null
  );
END;
$function$;

CREATE OR REPLACE FUNCTION forecast_queue.mvt_tract_choropleth_forecast(z integer, x integer, y integer, p_origin_year integer, p_horizon_m integer, p_series_kind text DEFAULT 'forecast'::text, p_variant_id text DEFAULT '__forecast__'::text, p_run_id text DEFAULT NULL::text, p_backtest_id text DEFAULT NULL::text)
 RETURNS bytea
 LANGUAGE plpgsql
 STABLE
AS $function$
DECLARE v_geom text;
BEGIN
  v_geom := forecast_queue._pick_geom_table('public.geo_tract20_us_z10', 'public.geo_tract20_us');
  RETURN forecast_queue._mvt_forecast_generic(
    'tract', v_geom, 'geoid',
    'forecast_queue.metrics_tract_forecast', 'tract_geoid20',
    z,x,y,p_origin_year,p_horizon_m,
    p_series_kind,p_variant_id,p_run_id,p_backtest_id,null
  );
END;
$function$;

CREATE OR REPLACE FUNCTION forecast_queue.mvt_tract_choropleth_history(z integer, x integer, y integer, p_year integer, p_series_kind text DEFAULT 'history'::text, p_variant_id text DEFAULT '__history__'::text, p_run_id text DEFAULT NULL::text, p_backtest_id text DEFAULT NULL::text)
 RETURNS bytea
 LANGUAGE plpgsql
 STABLE
AS $function$
DECLARE v_geom text;
BEGIN
  v_geom := forecast_queue._pick_geom_table('public.geo_tract20_us_z10', 'public.geo_tract20_us');
  RETURN forecast_queue._mvt_history_generic(
    'tract', v_geom, 'geoid',
    'forecast_queue.metrics_tract_history', 'tract_geoid20',
    z,x,y,p_year,
    p_series_kind,p_variant_id,p_run_id,p_backtest_id,null
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.mvt_tract_choropleth_forecast(z integer, x integer, y integer, p_origin_year integer, p_horizon_m integer)
 RETURNS bytea
 LANGUAGE sql
 STABLE
AS $function$
with
bounds as (select ST_TileEnvelope(z, x, y) as b),
geom_src as (
  select geoid::text, geom from public.geo_tract20_us_z10
  union all
  select geoid::text, geom from public.geo_tract20_us
  where not exists (select 1 from public.geo_tract20_us_z10 limit 1)
),
src as (
  select
    g.geoid as id,
    m.value, m.p10, m.p90, m.n,
    ST_AsMVTGeom(g.geom, bounds.b, 4096, 256, true) as geom
  from geom_src g
  join public.metrics_tract_forecast m
    on m.geoid = g.geoid
  cross join bounds
  where g.geom && bounds.b
    and m.origin_year = p_origin_year
    and m.horizon_m = p_horizon_m
)
select ST_AsMVT(src, 'tract', 4096, 'geom') from src;
$function$;
