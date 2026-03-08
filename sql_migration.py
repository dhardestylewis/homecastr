import psycopg2
import os
from dotenv import load_dotenv

load_dotenv(".env.local")

db_url = os.getenv("POSTGRES_URL_NON_POOLING")

# Connect to the database
conn = psycopg2.connect(db_url)
conn.autocommit = True
cur = conn.cursor()

# Create the RPC function
rpc_sql = """
CREATE OR REPLACE FUNCTION get_state_outlooks(target_schema text, state_fips text)
RETURNS TABLE (
    county_count bigint,
    neighborhood_count bigint,
    median_value numeric,
    median_appreciation numeric,
    highest_upside numeric
) AS $$
BEGIN
    RETURN QUERY EXECUTE format('
        WITH h12 AS (
            SELECT tract_geoid20, p50 as value
            FROM %I.metrics_tract_forecast
            WHERE tract_geoid20 >= $1 AND tract_geoid20 < $1 || ''z''
              AND horizon_m = 12
              AND series_kind = ''forecast''
              AND p50 IS NOT NULL
        ),
        h60 AS (
            SELECT tract_geoid20, p50 as value
            FROM %I.metrics_tract_forecast
            WHERE tract_geoid20 >= $1 AND tract_geoid20 < $1 || ''z''
              AND horizon_m = 60
              AND series_kind = ''forecast''
              AND p50 IS NOT NULL
        ),
        valid_tracts AS (
            SELECT h12.tract_geoid20, 
                   h12.value as h12_val, 
                   h60.value as h60_val,
                   ((h60.value - h12.value) / h12.value) * 100 as appreciation
            FROM h12
            JOIN h60 ON h12.tract_geoid20 = h60.tract_geoid20
            WHERE h12.value >= 20000 AND h12.value < 5000000
        ),
        filtered_tracts AS (
            SELECT * FROM valid_tracts WHERE appreciation > -95
        )
        SELECT 
            (SELECT COUNT(DISTINCT SUBSTRING(tract_geoid20 FROM 1 FOR 5))::bigint FROM h12) as county_count,
            (SELECT COUNT(*)::bigint FROM h12) as neighborhood_count,
            (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY h12_val)::numeric FROM filtered_tracts) as median_value,
            (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY appreciation)::numeric FROM filtered_tracts) as median_appreciation,
            (SELECT percentile_cont(0.99) WITHIN GROUP (ORDER BY appreciation)::numeric FROM filtered_tracts) as highest_upside
    ', target_schema, target_schema) USING state_fips;
END;
$$ LANGUAGE plpgsql;
"""

cur.execute(rpc_sql)
print("Function get_state_outlooks created successfully.")

cur.close()
conn.close()
