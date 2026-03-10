-- =====================================================================
-- RPC: get_feature_bounds
-- Purpose: Dynamically retrieve a BBOX array [min_lng, min_lat, max_lng, max_lat]
--          for a given feature level, geoid, and optional state scope.
--
-- Actual table inventory (geometry_columns):
--   geo_state_us        (state_fips, state_abbr, state_name, geom)
--   geo_zcta20_us       (zcta5ce20, geom)         -- ZIP code areas (national)
--   geo_tract20_us      (geoid, geom)              -- ALL US tracts (national, legacy _tx name)
--   geo_tabblock20_tx   (geoid20, geom)            -- census blocks
--   geo_parcel_poly     (acct, geom)               -- parcels
--   geo_neighborhood_tx (neighborhood_id, geom)    -- neighborhoods
--
-- NOTE: geo_tract20_us contains tracts for ALL 53 states/territories despite
--       the _tx suffix (legacy naming from initial Texas onboarding).
--       No geo_county table exists — county bounds are derived from
--       the extent of all tracts whose geoid starts with the 5-digit county FIPS.
-- =====================================================================

create or replace function public.get_feature_bounds(
  p_level text,       -- 'state', 'county', 'zcta', 'tract', 'parcel', 'neighborhood'
  p_geoid text,       -- e.g., '23', '23005', '2300500100', '12345678'
  p_state_slug text default null  -- currently unused (kept for API compatibility)
)
returns float[]
language plpgsql
security definer
stable
as $$
declare
  v_query text;
  v_res float[];
begin

  -- ================================================================
  -- 1) STATE → geo_state_us (state_fips)
  -- ================================================================
  if p_level = 'state' then
    v_query := format(
      'SELECT ARRAY[ST_XMin(b), ST_YMin(b), ST_XMax(b), ST_YMax(b)]
       FROM (SELECT ST_Extent(geom) as b FROM public.geo_state_us WHERE state_fips = %L) sub
       WHERE b IS NOT NULL',
      p_geoid
    );

  -- ================================================================
  -- 2) COUNTY → extent of all tracts with matching 5-digit county FIPS prefix
  -- ================================================================
  elsif p_level = 'county' then
    v_query := format(
      'SELECT ARRAY[ST_XMin(b), ST_YMin(b), ST_XMax(b), ST_YMax(b)]
       FROM (SELECT ST_Extent(geom) as b FROM public.geo_tract20_us WHERE geoid LIKE %L) sub
       WHERE b IS NOT NULL',
      p_geoid || '%'
    );

  -- ================================================================
  -- 3) ZCTA → geo_zcta20_us (zcta5ce20)
  -- ================================================================
  elsif p_level = 'zcta' then
    v_query := format(
      'SELECT ARRAY[ST_XMin(b), ST_YMin(b), ST_XMax(b), ST_YMax(b)]
       FROM (SELECT ST_Extent(geom) as b FROM public.geo_zcta20_us WHERE zcta5ce20 = %L) sub
       WHERE b IS NOT NULL',
      p_geoid
    );

  -- ================================================================
  -- 4) TRACT → geo_tract20_us (national table, geoid column)
  -- ================================================================
  elsif p_level = 'tract' then
    v_query := format(
      'SELECT ARRAY[ST_XMin(b), ST_YMin(b), ST_XMax(b), ST_YMax(b)]
       FROM (SELECT ST_Extent(geom) as b FROM public.geo_tract20_us WHERE geoid = %L) sub
       WHERE b IS NOT NULL',
      p_geoid
    );

  -- ================================================================
  -- 5) PARCEL → geo_parcel_poly (acct)
  -- ================================================================
  elsif p_level = 'parcel' then
    v_query := format(
      'SELECT ARRAY[ST_XMin(b), ST_YMin(b), ST_XMax(b), ST_YMax(b)]
       FROM (SELECT ST_Extent(geom) as b FROM public.geo_parcel_poly WHERE acct = %L) sub
       WHERE b IS NOT NULL',
      p_geoid
    );

  -- ================================================================
  -- 6) NEIGHBORHOOD → geo_neighborhood_tx (neighborhood_id)
  -- ================================================================
  elsif p_level = 'neighborhood' then
    v_query := format(
      'SELECT ARRAY[ST_XMin(b), ST_YMin(b), ST_XMax(b), ST_YMax(b)]
       FROM (SELECT ST_Extent(geom) as b FROM public.geo_neighborhood_tx WHERE neighborhood_id = %L) sub
       WHERE b IS NOT NULL',
      p_geoid
    );

  else
    return null;
  end if;

  execute v_query into v_res;
  return v_res;

exception
  when others then
    return null;
end;
$$;
