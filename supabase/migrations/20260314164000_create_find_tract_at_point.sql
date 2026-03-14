-- Create find_tract_at_point RPC to allow address-to-forecast lookup by lat/lng
-- Returns the census tract geoid that contains the given coordinates.

CREATE OR REPLACE FUNCTION public.find_tract_at_point(p_lat double precision, p_lng double precision)
RETURNS TABLE (geoid text)
LANGUAGE sql
STABLE
AS $$
  SELECT geoid::text
  FROM public.geo_tract20_us
  WHERE ST_Contains(geom, ST_SetSRID(ST_Point(p_lng, p_lat), 4326))
  LIMIT 1;
$$;
