const { Pool } = require('pg');

const pool = new Pool({
    connectionString: process.env.POSTGRES_URL
});

async function runFixes() {
    const client = await pool.connect();
    try {
        console.log("--- Apply Grants ---");
        // Ensure the schema itself has usage
        await client.query(`GRANT USAGE ON SCHEMA forecast_queue TO anon, authenticated;`);

        // Ensure all tables/views in the schema can be read
        await client.query(`GRANT SELECT ON ALL TABLES IN SCHEMA forecast_queue TO anon, authenticated;`);
        await client.query(`ALTER DEFAULT PRIVILEGES IN SCHEMA forecast_queue GRANT SELECT ON TABLES TO anon, authenticated;`);

        // Ensure all functions in the schema can be executed
        await client.query(`GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA forecast_queue TO anon, authenticated;`);
        await client.query(`ALTER DEFAULT PRIVILEGES IN SCHEMA forecast_queue GRANT EXECUTE ON FUNCTIONS TO anon, authenticated;`);

        // IMPORTANT: When accessing the schema via the API directly, we need to ensure the authenticator role has access 
        // to switch to the schema, and that the schema is in the search path for the API roles
        await client.query(`ALTER ROLE authenticator SET search_path TO public, forecast_queue, forecast_20260220_7f31c6e4;`);
        await client.query(`ALTER ROLE anon SET search_path TO public, forecast_queue, forecast_20260220_7f31c6e4;`);
        await client.query(`ALTER ROLE authenticated SET search_path TO public, forecast_queue, forecast_20260220_7f31c6e4;`);

        // Reload PostgREST schema cache
        await client.query(`NOTIFY pgrst, 'reload schema';`);
        console.log("Grants applied and schema reloaded.");

    } catch (err) {
        console.error("Query Error", err);
    } finally {
        client.release();
        pool.end();
    }
}

runFixes();
