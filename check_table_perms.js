const { Pool } = require('pg');

const pool = new Pool({
    connectionString: process.env.POSTGRES_URL
});

async function checkPermissions() {
    const client = await pool.connect();
    try {
        console.log("--- Table Grants in forecast_queue ---");
        const tblRes = await client.query(`
      SELECT relname, relacl
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'forecast_queue' AND c.relkind IN ('r', 'v', 'm');
    `);
        console.table(tblRes.rows);

        // The MVT functions also access public tables like parcel_ladder_v1, parcel_geometry_v1
        console.log("\n--- Geometry Table Grants in public ---");
        const geomRes = await client.query(`
      SELECT relname, relacl
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'public' AND c.relname IN ('parcel_ladder_v1', 'parcel_geometry_v1', 'zcta5_polygons_v1');
    `);
        console.table(geomRes.rows);

    } catch (err) {
        console.error("Query Error", err);
    } finally {
        client.release();
        pool.end();
    }
}

checkPermissions();
