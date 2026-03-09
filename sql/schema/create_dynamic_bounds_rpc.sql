-- =====================================================================
-- RPC: get_feature_bounds
-- Purpose: Dynamically retrieve a BBOX array [min_lng, min_lat, max_lng, max_lat]
--          for a given feature level, geoid, and optional state scope.
--
-- Actual table inventory (geometry_columns):
--   geo_state_us        (state_fips, state_abbr, state_name, geom)
--   geo_zcta20_us       (zcta5ce20, geom)            -- ZIP code areas
--   geo_tract20_{st}    (geoid, geom)                 -- tract per state
--   geo_tabblock20_{st} (geoid20, geom)               -- block per state
--   geo_parcel_poly     (acct, geom)                  -- parcels (TX only)
--   geo_neighborhood_tx (neighborhood_id, geom)       -- neighborhoods (TX only)
--
-- No geo_county table exists — county bounds are derived from
-- the union/extent of tracts whose geoid starts with the 5-digit county FIPS.
-- =====================================================================

create or replace function public.get_feature_bounds(
  p_level text,       -- 'state', 'county', 'zcta', 'tract', 'parcel', 'neighborhood'
  p_geoid text,       -- e.g., '23', '23005', '2300500100', '12345678', 'tract-240001'
  p_state_slug text default null
)
returns float[]
language plpgsql
security definer
stable
as $$
declare
  v_table_name text;
  v_id_col text;
  v_query text;
  v_res float[];
  v_fallback_slug text;
  v_exists boolean;
  v_fips2 text;
begin
  -- Helper: derive 2-letter state slug from 2-digit FIPS
  v_fips2 := substring(p_geoid from 1 for 2);

  -- ================================================================
  -- 1) STATE → geo_state_us (state_fips)
  -- ================================================================
  if p_level = 'state' then
    v_table_name := 'public.geo_state_us';
    v_id_col := 'state_fips';

  -- ================================================================
  -- 2) COUNTY → derive from tract extent (no county geometry table)
  --    County FIPS = first 5 digits of each tract GEOID.
  --    We compute ST_Extent over all tracts in that county.
  -- ================================================================
  elsif p_level = 'county' then
    -- Resolve state slug from the 2-digit prefix of the county FIPS
    if p_state_slug is null then
      select abbr into v_fallback_slug from (
        values
        ('01','al'),('02','ak'),('04','az'),('05','ar'),('06','ca'),('08','co'),('09','ct'),
        ('10','de'),('11','dc'),('12','fl'),('13','ga'),('15','hi'),('16','id'),('17','il'),
        ('18','in'),('19','ia'),('20','ks'),('21','ky'),('22','la'),('23','me'),('24','md'),
        ('25','ma'),('26','mi'),('27','mn'),('28','ms'),('29','mo'),('30','mt'),('31','ne'),
        ('32','nv'),('33','nh'),('34','nj'),('35','nm'),('36','ny'),('37','nc'),('38','nd'),
        ('39','oh'),('40','ok'),('41','or'),('42','pa'),('44','ri'),('45','sc'),('46','sd'),
        ('47','tn'),('48','tx'),('49','ut'),('50','vt'),('51','va'),('53','wa'),('54','wv'),
        ('55','wi'),('56','wy'),('72','pr')
      ) as t(fips, abbr) where fips = v_fips2;
      v_fallback_slug := coalesce(v_fallback_slug, 'tx');
    else
      v_fallback_slug := p_state_slug;
    end if;

    v_table_name := 'public.geo_tract20_' || v_fallback_slug;

    -- Check table exists
    select exists (
      select from pg_tables
      where schemaname = 'public'
        and tablename = 'geo_tract20_' || v_fallback_slug
    ) into v_exists;

    if not v_exists then
      -- Fallback: use ZCTA instead (national)
      -- Filter ZCTAs by coarse overlap with state — skip for now, just return null
      return null;
    end if;

    -- County FIPS is p_geoid (5 chars). Tracts have geoid starting with county FIPS.
    v_query := format(
      'SELECT ARRAY[ST_XMin(b), ST_YMin(b), ST_XMax(b), ST_YMax(b)]
       FROM (SELECT ST_Extent(geom) as b FROM %s WHERE geoid LIKE %L) sub
       WHERE b IS NOT NULL',
      v_table_name, p_geoid || '%'
    );

    execute v_query into v_res;
    return v_res;

  -- ================================================================
  -- 3) ZCTA → geo_zcta20_us
  -- ================================================================
  elsif p_level = 'zcta' then
    v_table_name := 'public.geo_zcta20_us';
    v_id_col := 'zcta5ce20';

  -- ================================================================
  -- 4) TRACT → geo_tract20_{state_slug}
  -- ================================================================
  elsif p_level = 'tract' then
    if p_state_slug is not null then
      v_fallback_slug := p_state_slug;
    else
      select abbr into v_fallback_slug from (
        values
        ('01','al'),('02','ak'),('04','az'),('05','ar'),('06','ca'),('08','co'),('09','ct'),
        ('10','de'),('11','dc'),('12','fl'),('13','ga'),('15','hi'),('16','id'),('17','il'),
        ('18','in'),('19','ia'),('20','ks'),('21','ky'),('22','la'),('23','me'),('24','md'),
        ('25','ma'),('26','mi'),('27','mn'),('28','ms'),('29','mo'),('30','mt'),('31','ne'),
        ('32','nv'),('33','nh'),('34','nj'),('35','nm'),('36','ny'),('37','nc'),('38','nd'),
        ('39','oh'),('40','ok'),('41','or'),('42','pa'),('44','ri'),('45','sc'),('46','sd'),
        ('47','tn'),('48','tx'),('49','ut'),('50','vt'),('51','va'),('53','wa'),('54','wv'),
        ('55','wi'),('56','wy'),('72','pr')
      ) as t(fips, abbr) where fips = v_fips2;
      v_fallback_slug := coalesce(v_fallback_slug, 'tx');
    end if;
    v_table_name := 'public.geo_tract20_' || v_fallback_slug;
    v_id_col := 'geoid';

  -- ================================================================
  -- 5) PARCEL → geo_parcel_poly
  -- ================================================================
  elsif p_level = 'parcel' then
    v_table_name := 'public.geo_parcel_poly';
    v_id_col := 'acct';

  -- ================================================================
  -- 6) NEIGHBORHOOD → geo_neighborhood_{state_slug}
  -- ================================================================
  elsif p_level = 'neighborhood' then
    v_fallback_slug := coalesce(p_state_slug, 'tx');
    v_table_name := 'public.geo_neighborhood_' || v_fallback_slug;
    v_id_col := 'neighborhood_id';

  else
    return null;
  end if;

  -- ================================================================
  -- Generic path: exact match on v_id_col = p_geoid
  -- ================================================================

  -- Check table exists
  select exists (
    select from pg_tables
    where schemaname = split_part(v_table_name, '.', 1)
      and tablename = split_part(v_table_name, '.', 2)
  ) into v_exists;

  if not v_exists then
    return null;
  end if;

  v_query := format(
    'SELECT ARRAY[ST_XMin(b), ST_YMin(b), ST_XMax(b), ST_YMax(b)]
     FROM (SELECT ST_Extent(geom) as b FROM %s WHERE %I = %L) sub
     WHERE b IS NOT NULL',
    v_table_name, v_id_col, p_geoid
  );

  execute v_query into v_res;
  return v_res;

exception
  when others then
    return null;
end;
$$;
