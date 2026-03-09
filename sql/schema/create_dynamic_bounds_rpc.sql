-- =====================================================================
-- RPC: get_feature_bounds
-- Purpose: Dynamically retrieve a BBOX array [min_lng, min_lat, max_lng, max_lat]
--          for a given feature level, geoid, and optional state scope.
-- =====================================================================

create or replace function public.get_feature_bounds(
  p_level text,       -- 'state', 'county', 'tract', 'parcel', 'neighborhood'
  p_geoid text,       -- e.g., '48', '48201', '48201100000', '12345678', 'tract-240001'
  p_state_slug text default null -- e.g., 'tx' (used to determine the specific partitioned table like geo_tract20_tx)
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
  v_fallback_fips text;
  v_exists boolean;
begin
  -- 1) Determine table and ID column based on level
  if p_level = 'state' then
    v_table_name := 'public.geo_state20';
    v_id_col := 'geoid20';

  elsif p_level = 'county' then
    v_table_name := 'public.geo_county20';
    v_id_col := 'geoid20';

  elsif p_level = 'tract' then
    if p_state_slug is not null then
      v_table_name := 'public.geo_tract20_' || p_state_slug;
    else
      -- Assume national table exists or derive state from geoid
      v_fallback_fips := substring(p_geoid from 1 for 2);
      select abbr into p_state_slug from (
          values
          ('01','al'),('02','ak'),('04','az'),('05','ar'),('06','ca'),('08','co'),('09','ct'),
          ('10','de'),('11','dc'),('12','fl'),('13','ga'),('15','hi'),('16','id'),('17','il'),
          ('18','in'),('19','ia'),('20','ks'),('21','ky'),('22','la'),('23','me'),('24','md'),
          ('25','ma'),('26','mi'),('27','mn'),('28','ms'),('29','mo'),('30','mt'),('31','ne'),
          ('32','nv'),('33','nh'),('34','nj'),('35','nm'),('36','ny'),('37','nc'),('38','nd'),
          ('39','oh'),('40','ok'),('41','or'),('42','pa'),('44','ri'),('45','sc'),('46','sd'),
          ('47','tn'),('48','tx'),('49','ut'),('50','vt'),('51','va'),('53','wa'),('54','wv'),
          ('55','wi'),('56','wy'),('72','pr')
      ) as t(fips, abbr) where fips = v_fallback_fips;
      v_table_name := 'public.geo_tract20_' || coalesce(p_state_slug, 'tx');
    end if;
    v_id_col := 'geoid';

  elsif p_level = 'parcel' then
    v_table_name := 'public.geo_parcel_poly';
    v_id_col := 'acct';

  elsif p_level = 'neighborhood' then
    if p_state_slug is not null then
      v_table_name := 'public.geo_neighborhood_' || p_state_slug;
    else
      v_table_name := 'public.geo_neighborhood_tx';
    end if;
    v_id_col := 'neighborhood_id';

  else
    return null;
  end if;

  -- 2) Check if table exists
  select exists (
    select from pg_tables
    where schemaname = split_part(v_table_name, '.', 1)
      and tablename = split_part(v_table_name, '.', 2)
  ) into v_exists;

  if not v_exists then
    return null;
  end if;

  -- 3) Query the extent and compute bbox
  -- PostGIS ST_Extent returns a BOX2D object. We use ST_XMin/Max/YMin/Max to extract coords.
  -- Format -> [minLng, minLat, maxLng, maxLat]
  v_query := format('
    WITH ext AS (
      SELECT ST_Extent(geom) as b FROM %s WHERE %I = %L
    )
    SELECT ARRAY[ST_XMin(b), ST_YMin(b), ST_XMax(b), ST_YMax(b)]
    FROM ext
    WHERE b IS NOT NULL
  ', v_table_name, v_id_col, p_geoid);

  execute v_query into v_res;

  return v_res;
exception
  when others then
    return null; -- safe fallback instead of hard-crashing on missing table/column
end;
$$;
