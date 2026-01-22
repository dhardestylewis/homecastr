
-- Drop function if exists to allow updates
DROP FUNCTION IF EXISTS public.get_h3_tile_mvt;

-- Function to generate MVT for a given Tile Z/X/Y and parameters
CREATE OR REPLACE FUNCTION public.get_h3_tile_mvt(
    z integer,
    x integer,
    y integer,
    query_year integer,
    query_res integer
)
RETURNS bytea
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
AS $function$
DECLARE
    mvt bytea;
    min_x float8;
    min_y float8;
    max_x float8;
    max_y float8;
    world_size float8 := 20037508.3427892;
    tile_size float8;
    tile_bbox_3857 geometry;
    tile_bbox_4326 geometry;
BEGIN
    -- 1. Calculate Tile Bounding Box (Web Mercator EPSG:3857)
    tile_size := (world_size * 2) / (2.0 ^ z);
    min_x := -world_size + x * tile_size;
    max_x := -world_size + (x + 1) * tile_size;
    max_y := world_size - y * tile_size;
    min_y := world_size - (y + 1) * tile_size;

    tile_bbox_3857 := ST_MakeEnvelope(min_x, min_y, max_x, max_y, 3857);
    tile_bbox_4326 := ST_Transform(tile_bbox_3857, 4326);

    -- 2. Query and Generate MVT
    WITH mvtgeom AS (
        SELECT 
            g.h3_id,
            d.opportunity as opp,
            d.reliability as rel,
            d.predicted_value as val,
            d.property_count as count,
            d.sample_accuracy as acc,
            d.med_years as ny,
            d.hard_fail as hf,
            d.alert_pct as ap,
            ST_AsMVTGeom(
                ST_Transform(g.geom, 3857),
                tile_bbox_3857,
                4096,
                256,
                true
            ) as geom
        FROM h3_aoi_grid g
        JOIN h3_precomputed_hex_details d 
            ON g.h3_id = d.h3_id 
            AND d.h3_res = g.h3_res
            AND d.forecast_year = query_year
        WHERE 
            g.h3_res = query_res
            -- Use bounding box operator for index performance
            AND g.geom && tile_bbox_4326
            AND ST_Intersects(g.geom, tile_bbox_4326)
    )
    SELECT INTO mvt ST_AsMVT(mvtgeom.*, 'default', 4096, 'geom')
    FROM mvtgeom;

    RETURN COALESCE(mvt, '');
END;
$function$;
