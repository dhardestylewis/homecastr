-- =====================================================================
-- Add State-level MVT tile support to forecast_queue schema
--
-- State metrics tables (metrics_state_forecast, metrics_state_history)
-- and geometry table (geo_state_us) already exist. This migration adds:
--   1) State MVT leaf functions (forecast + history)
--   2) Updates the zoom router to serve state tiles at z <= 4
--   3) Adds 'state' to the level_override dispatch
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1) State MVT leaf functions
-- ---------------------------------------------------------------------

-- Forecast
CREATE OR REPLACE FUNCTION forecast_queue.mvt_state_choropleth_forecast(
  z integer, x integer, y integer,
  p_origin_year integer,
  p_horizon_m integer,
  p_series_kind text DEFAULT 'forecast'::text,
  p_variant_id  text DEFAULT '__forecast__'::text,
  p_run_id      text DEFAULT NULL::text,
  p_backtest_id text DEFAULT NULL::text
)
RETURNS bytea
LANGUAGE plpgsql
STABLE
AS $function$
BEGIN
  RETURN forecast_queue._mvt_forecast_generic(
    'state', 'public.geo_state_us', 'state_fips',
    'forecast_queue.metrics_state_forecast', 'state_fips',
    z, x, y, p_origin_year, p_horizon_m,
    p_series_kind, p_variant_id, p_run_id, p_backtest_id, null
  );
END;
$function$;

-- History
CREATE OR REPLACE FUNCTION forecast_queue.mvt_state_choropleth_history(
  z integer, x integer, y integer,
  p_year integer,
  p_series_kind text DEFAULT 'history'::text,
  p_variant_id  text DEFAULT '__history__'::text,
  p_run_id      text DEFAULT NULL::text,
  p_backtest_id text DEFAULT NULL::text
)
RETURNS bytea
LANGUAGE plpgsql
STABLE
AS $function$
BEGIN
  RETURN forecast_queue._mvt_history_generic(
    'state', 'public.geo_state_us', 'state_fips',
    'forecast_queue.metrics_state_history', 'state_fips',
    z, x, y, p_year,
    p_series_kind, p_variant_id, p_run_id, p_backtest_id, null
  );
END;
$function$;

-- ---------------------------------------------------------------------
-- 2) Update the forecast router: add state at z <= 4, shift ZCTA to z <= 7
-- ---------------------------------------------------------------------

CREATE OR REPLACE FUNCTION forecast_queue.mvt_choropleth_forecast(
  z integer, x integer, y integer,
  p_origin_year integer,
  p_horizon_m integer,
  p_level_override text DEFAULT null,
  p_series_kind text DEFAULT 'forecast',
  p_variant_id  text DEFAULT '__forecast__',
  p_run_id      text DEFAULT null,
  p_backtest_id text DEFAULT null,
  p_parcel_limit integer DEFAULT 3500
)
RETURNS bytea
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_result bytea;
BEGIN
  -- Level override dispatch
  IF p_level_override IS NOT NULL THEN
    CASE lower(p_level_override)
      WHEN 'state' THEN
        RETURN forecast_queue.mvt_state_choropleth_forecast(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
      WHEN 'zcta' THEN
        RETURN forecast_queue.mvt_zcta_choropleth_forecast(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
      WHEN 'tract' THEN
        RETURN forecast_queue.mvt_tract_choropleth_forecast(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
      WHEN 'tabblock' THEN
        RETURN forecast_queue.mvt_tabblock_choropleth_forecast(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
      WHEN 'unsd' THEN
        RETURN forecast_queue.mvt_unsd_choropleth_forecast(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
      WHEN 'neighborhood' THEN
        RETURN forecast_queue.mvt_neighborhood_choropleth_forecast(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
      WHEN 'parcel' THEN
        RETURN forecast_queue.mvt_parcel_choropleth_forecast(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id,p_run_id,p_backtest_id,p_parcel_limit);
      ELSE
        RETURN ''::bytea;
    END CASE;
  END IF;

  -- Zoom-based auto-routing
  IF z <= 4 THEN
    -- Try state; fall back to ZCTA if state has no data
    v_result := forecast_queue.mvt_state_choropleth_forecast(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
    IF v_result IS NOT NULL AND length(v_result) > 0 THEN RETURN v_result; END IF;
    RETURN forecast_queue.mvt_zcta_choropleth_forecast(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
  ELSIF z <= 7 THEN
    RETURN forecast_queue.mvt_zcta_choropleth_forecast(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
  ELSIF z <= 11 THEN
    RETURN forecast_queue.mvt_tract_choropleth_forecast(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
  ELSIF z <= 16 THEN
    -- Try tabblock; fall back to tract if empty
    v_result := forecast_queue.mvt_tabblock_choropleth_forecast(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
    IF v_result IS NOT NULL AND length(v_result) > 0 THEN RETURN v_result; END IF;
    RETURN forecast_queue.mvt_tract_choropleth_forecast(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
  ELSE
    -- Try parcel -> tabblock -> tract
    v_result := forecast_queue.mvt_parcel_choropleth_forecast(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id,p_run_id,p_backtest_id,p_parcel_limit);
    IF v_result IS NOT NULL AND length(v_result) > 0 THEN RETURN v_result; END IF;
    v_result := forecast_queue.mvt_tabblock_choropleth_forecast(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
    IF v_result IS NOT NULL AND length(v_result) > 0 THEN RETURN v_result; END IF;
    RETURN forecast_queue.mvt_tract_choropleth_forecast(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
  END IF;
END;
$$;

-- ---------------------------------------------------------------------
-- 3) Update the history router: add state at z <= 4
-- ---------------------------------------------------------------------

CREATE OR REPLACE FUNCTION forecast_queue.mvt_choropleth_history(
  z integer, x integer, y integer,
  p_year integer,
  p_level_override text DEFAULT null,
  p_series_kind text DEFAULT 'history',
  p_variant_id  text DEFAULT '__history__',
  p_run_id      text DEFAULT null,
  p_backtest_id text DEFAULT null,
  p_parcel_limit integer DEFAULT 3500
)
RETURNS bytea
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_result bytea;
BEGIN
  IF p_level_override IS NOT NULL THEN
    CASE lower(p_level_override)
      WHEN 'state' THEN
        RETURN forecast_queue.mvt_state_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
      WHEN 'zcta' THEN
        RETURN forecast_queue.mvt_zcta_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
      WHEN 'tract' THEN
        RETURN forecast_queue.mvt_tract_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
      WHEN 'tabblock' THEN
        RETURN forecast_queue.mvt_tabblock_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
      WHEN 'unsd' THEN
        RETURN forecast_queue.mvt_unsd_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
      WHEN 'neighborhood' THEN
        RETURN forecast_queue.mvt_neighborhood_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
      WHEN 'parcel' THEN
        RETURN forecast_queue.mvt_parcel_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id,p_parcel_limit);
      ELSE
        RETURN ''::bytea;
    END CASE;
  END IF;

  IF z <= 4 THEN
    -- Try state; fall back to ZCTA if state has no data
    v_result := forecast_queue.mvt_state_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
    IF v_result IS NOT NULL AND length(v_result) > 0 THEN RETURN v_result; END IF;
    RETURN forecast_queue.mvt_zcta_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
  ELSIF z <= 7 THEN
    RETURN forecast_queue.mvt_zcta_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
  ELSIF z <= 11 THEN
    RETURN forecast_queue.mvt_tract_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
  ELSIF z <= 16 THEN
    v_result := forecast_queue.mvt_tabblock_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
    IF v_result IS NOT NULL AND length(v_result) > 0 THEN RETURN v_result; END IF;
    RETURN forecast_queue.mvt_tract_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
  ELSE
    v_result := forecast_queue.mvt_parcel_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id,p_parcel_limit);
    IF v_result IS NOT NULL AND length(v_result) > 0 THEN RETURN v_result; END IF;
    v_result := forecast_queue.mvt_tabblock_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
    IF v_result IS NOT NULL AND length(v_result) > 0 THEN RETURN v_result; END IF;
    RETURN forecast_queue.mvt_tract_choropleth_history(z,x,y,p_year,p_series_kind,p_variant_id,p_run_id,p_backtest_id);
  END IF;
END;
$$;

-- Reload PostgREST schema cache
NOTIFY pgrst, 'reload schema';
