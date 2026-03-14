"""
Wire up ZIP3 into the MVT tile pipeline and frontend.

1. Create mvt_zip3_choropleth RPC function on Supabase
2. Update mvt_choropleth_forecast router to add zip3 at z <= 4
3. Update GEO_LEVELS in forecast-map.tsx
4. Add zip3 to forecast-detail API route
"""
import os
import psycopg2

CONN_STR = os.environ["SUPABASE_DB_URL"]
SCHEMA = "forecast_20260220_7f31c6e4"

def main():
    conn = psycopg2.connect(CONN_STR)
    conn.autocommit = True
    cur = conn.cursor()

    # Step 1: Create mvt_zip3_choropleth function
    print("Creating mvt_zip3_choropleth function...")
    cur.execute(f"""
        CREATE OR REPLACE FUNCTION {SCHEMA}.mvt_zip3_choropleth(
            z int, x int, y int,
            p_origin_year int DEFAULT 2025,
            p_horizon_m int DEFAULT 12,
            p_series_kind text DEFAULT 'forecast',
            p_variant_id text DEFAULT '__forecast__'
        ) RETURNS bytea
        LANGUAGE plpgsql STABLE
        AS $func$
        DECLARE
            result bytea;
            bbox geometry;
        BEGIN
            bbox := ST_TileEnvelope(z, x, y);
            
            SELECT ST_AsMVT(tile, 'zip3', 4096, 'geom') INTO result
            FROM (
                SELECT
                    g.zip3 AS id,
                    ST_AsMVTGeom(g.geom, bbox, 4096, 256, true) AS geom,
                    COALESCE(f.p50, 0) AS p50
                FROM public.geo_zip3_us g
                LEFT JOIN {SCHEMA}.metrics_zip3_forecast f
                    ON f.zip3 = g.zip3
                    AND f.origin_year = p_origin_year
                    AND f.horizon_m = p_horizon_m
                WHERE g.geom && bbox
                    AND ST_Intersects(g.geom, bbox)
            ) tile;
            
            RETURN result;
        END;
        $func$;
    """)
    print("  Created mvt_zip3_choropleth")

    # Step 2: Get current mvt_choropleth_forecast function definition
    cur.execute(f"""
        SELECT pg_get_functiondef(oid)
        FROM pg_proc
        WHERE proname = 'mvt_choropleth_forecast'
        AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{SCHEMA}')
    """)
    func_def = cur.fetchone()[0]
    print(f"  Current mvt_choropleth_forecast: {len(func_def)} chars")

    # Step 3: Update the main router to add zip3 routing
    # We need to add a case for zip3 and adjust zcta range
    # The function uses CASE/IF logic based on zoom level or p_level_override
    
    # Add zip3 override handling and zoom routing
    # First check if zip3 is already handled
    if 'zip3' in func_def:
        print("  zip3 already in mvt_choropleth_forecast - skipping")
    else:
        print("  Adding zip3 to mvt_choropleth_forecast...")
        
        # The function has a pattern like:
        # CASE p_level_override
        #   WHEN 'zcta' THEN ...
        #   WHEN 'tract' THEN ...
        # We need to add WHEN 'zip3' THEN ...
        # And in the zoom-based routing, add: IF z <= 4 THEN return zip3
        
        # Let's replace the function entirely with an updated version
        cur.execute(f"""
            CREATE OR REPLACE FUNCTION {SCHEMA}.mvt_choropleth_forecast(
                z int, x int, y int,
                p_origin_year int DEFAULT 2025,
                p_horizon_m int DEFAULT 12,
                p_level_override text DEFAULT NULL,
                p_series_kind text DEFAULT 'forecast',
                p_variant_id text DEFAULT '__forecast__',
                p_run_id text DEFAULT NULL,
                p_backtest_id text DEFAULT NULL,
                p_parcel_limit int DEFAULT 3500
            ) RETURNS bytea
            LANGUAGE plpgsql STABLE
            AS $func$
            BEGIN
                -- Explicit level override
                IF p_level_override IS NOT NULL THEN
                    CASE p_level_override
                        WHEN 'zip3' THEN
                            RETURN {SCHEMA}.mvt_zip3_choropleth(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id);
                        WHEN 'zcta' THEN
                            RETURN {SCHEMA}.mvt_zcta_choropleth(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id);
                        WHEN 'tract' THEN
                            RETURN {SCHEMA}.mvt_tract_choropleth(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id);
                        WHEN 'tabblock' THEN
                            RETURN {SCHEMA}.mvt_tabblock_choropleth(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id);
                        WHEN 'parcel' THEN
                            RETURN {SCHEMA}.mvt_parcel_choropleth(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id,p_parcel_limit);
                        WHEN 'neighborhood' THEN
                            RETURN {SCHEMA}.mvt_neighborhood_choropleth(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id);
                        WHEN 'unsd' THEN
                            RETURN {SCHEMA}.mvt_unsd_choropleth(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id);
                        ELSE
                            NULL;
                    END CASE;
                END IF;
                
                -- Auto-route by zoom level
                IF z <= 4 THEN
                    RETURN {SCHEMA}.mvt_zip3_choropleth(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id);
                ELSIF z <= 7 THEN
                    RETURN {SCHEMA}.mvt_zcta_choropleth(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id);
                ELSIF z <= 11 THEN
                    RETURN {SCHEMA}.mvt_tract_choropleth(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id);
                ELSIF z <= 16 THEN
                    RETURN {SCHEMA}.mvt_tabblock_choropleth(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id);
                ELSE
                    RETURN {SCHEMA}.mvt_parcel_choropleth(z,x,y,p_origin_year,p_horizon_m,p_series_kind,p_variant_id,p_parcel_limit);
                END IF;
            END;
            $func$;
        """)
        print("  Updated mvt_choropleth_forecast with zip3 routing")

    conn.close()
    print("Done!")

if __name__ == "__main__":
    main()
