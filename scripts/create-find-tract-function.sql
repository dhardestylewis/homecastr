-- Create a function to find which census tract contains a given lat/lng point
-- Uses PostGIS ST_Contains to do point-in-polygon lookup

CREATE OR REPLACE FUNCTION public.find_tract_at_point(p_lat double precision, p_lng double precision)
RETURNS TABLE(geoid text) 
LANGUAGE sql
STABLE
AS $$
  SELECT geoid
  FROM public.geo_tract20_us
  WHERE ST_Contains(geom, ST_SetSRID(ST_MakePoint(p_lng, p_lat), 4326))
  LIMIT 1;
$$;

-- Grant execute permission to anon and authenticated roles
GRANT EXECUTE ON FUNCTION public.find_tract_at_point(double precision, double precision) TO anon;
GRANT EXECUTE ON FUNCTION public.find_tract_at_point(double precision, double precision) TO authenticated;

-- Add a comment for documentation
COMMENT ON FUNCTION public.find_tract_at_point IS 'Finds the census tract (geoid) that contains the given lat/lng point using PostGIS point-in-polygon lookup';
