-- Create a lightweight RPC for fetching H3 grid data for the map
-- Only fetches essential columns (id, opportunity, reliability) to minimize payload

CREATE OR REPLACE FUNCTION get_h3_compact_grid(
  min_lat FLOAT,
  min_lng FLOAT,
  max_lat FLOAT,
  max_lng FLOAT,
  resolution INTEGER,
  year INTEGER
)
RETURNS TABLE (
  h3_id TEXT,
  o FLOAT, -- opportunity (score)
  r FLOAT  -- reliability
)
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
  SELECT
    h.h3_id,
    h.opportunity as o,
    h.reliability as r
  FROM
    h3_precomputed_hex_details h
  WHERE
    h.forecast_year = year
    AND h.h3_res = resolution
    AND h.lat >= min_lat
    AND h.lat <= max_lat
    AND h.lng >= min_lng
    AND h.lng <= max_lng;
END;
$$;
