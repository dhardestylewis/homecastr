const { Pool } = require('pg');

const pool = new Pool({
    connectionString: process.env.POSTGRES_URL // Make sure to run this with the right env var
});

async function checkPermissions() {
    const client = await pool.connect();
    try {
        console.log("--- Schema Owner & ACL ---");
        const schemaRes = await client.query(`
      SELECT nspname, nspacl, pg_get_userbyid(nspowner) AS owner
      FROM pg_namespace
      WHERE nspname = 'forecast_queue';
    `);
        console.table(schemaRes.rows);

        console.log("\n--- Authenticator Role check ---");
        const roleRes = await client.query(`
      SELECT rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin
      FROM pg_roles
      WHERE rolname IN ('authenticator', 'anon', 'authenticated', 'postgres');
    `);
        console.table(roleRes.rows);

        console.log("\n--- Function Grants ---");
        const funcRes = await client.query(`
      SELECT p.proname, pg_get_userbyid(p.proowner) AS owner, p.proacl 
      FROM pg_proc p
      JOIN pg_namespace n ON p.pronamespace = n.oid
      WHERE n.nspname = 'forecast_queue';
    `);
        console.table(funcRes.rows);

    } catch (err) {
        console.error("Query Error", err);
    } finally {
        client.release();
        pool.end();
    }
}

checkPermissions();
