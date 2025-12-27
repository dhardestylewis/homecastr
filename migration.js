const { Client } = require('pg');

// User provided connection string
const connectionString = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:6543/postgres?sslmode=require&supa=base-pooler.x";

const client = new Client({
    connectionString: connectionString,
});

async function runMigration() {
    try {
        console.log("Connecting to Supabase...");
        await client.connect();
        console.log("Connected.");

        const sql = `
      -- 1. Create function to generate MVT tiles directly from H3 data
      create or replace function get_h3_tile(
        z int, 
        x int, 
        y int
      )
      returns bytea
      language plpgsql
      as $$
      declare
        mvt bytea;
        bbox geometry;
      begin
        -- Calculate tile bounds in Web Mercator (3857)
        bbox := ST_TileEnvelope(z, x, y);

        select ST_AsMVT(grid.*, 'h3_layer', 4096, 'geom')
        into mvt
        from (
          select 
            h3_id,
            opportunity,
            reliability,
            -- Transform geometry to MVT coordinate space
            ST_AsMVTGeom(
              ST_Transform(geom, 3857), 
              bbox, 
              4096, 
              256, 
              true
            ) as geom
          from h3_precomputed_hex_details
          where 
            -- Dynamic resolution based on Zoom level
            -- z=0-4 -> res 4 (too coarse?) - handled by frontend limit usually
            -- z=5-8 -> res 5
            -- z=9-10 -> res 7
            -- z=11-12 -> res 9
            -- z=13+ -> res 10/11
            h3_res = (
              case 
                when z < 5 then 4
                when z < 9 then 5
                when z < 11 then 7
                when z < 13 then 9
                else 10
              end
            )
            and forecast_year = 2026 -- Default year for now, pass as param later if needed
            and ST_Intersects(geom, ST_Transform(bbox, 4326))
        ) as grid;

        return mvt;
      end;
      $$;
    `;

        console.log("Running SQL...");
        await client.query(sql);
        console.log("Successfully created function: get_h3_tile");

    } catch (err) {
        console.error("Migration failed:", err);
    } finally {
        await client.end();
    }
}

runMigration();
