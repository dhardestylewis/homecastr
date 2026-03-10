import { loadEnvConfig } from '@next/env';
loadEnvConfig(process.cwd());
import { Client } from 'pg';
import fs from 'fs';
import path from 'path';

process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

async function run() {
    const dbUrl = process.env.POSTGRES_URL_NON_POOLING;
    if (!dbUrl) {
        console.error('Missing POSTGRES_URL_NON_POOLING variable');
        return;
    }
    const client = new Client({
        connectionString: dbUrl,
        ssl: { rejectUnauthorized: false }
    });
    try {
        await client.connect();

        // Step 1: Rename the tables and indexes
        console.log('Step 1: Renaming tables and indexes...');
        const renameSql = fs.readFileSync(path.join(process.cwd(), 'sql/schema/rename_tract_tx_to_us.sql'), 'utf-8');
        await client.query(renameSql);
        console.log('  Tables and indexes renamed.');

        // Step 2: Update the get_feature_bounds RPC
        console.log('Step 2: Updating get_feature_bounds RPC...');
        const boundsSql = fs.readFileSync(path.join(process.cwd(), 'sql/schema/create_dynamic_bounds_rpc.sql'), 'utf-8');
        await client.query(boundsSql);
        console.log('  RPC updated.');

        // Step 3: Update the MVT tile functions (need to re-create the tract functions)
        // These live inside the forecast schema, so we need to extract and re-run just those functions.
        // Actually, the _pick_geom_table references are string literals inside PL/pgSQL, so we need
        // to recreate the functions that use them.
        console.log('Step 3: Updating MVT tract tile functions...');

        // mvt_tract_choropleth_forecast — read the function from the schema and re-create
        // We'll do it by extracting the function block and replacing the table name
        const schemaSql = fs.readFileSync(path.join(process.cwd(), 'sql/schema/forecast_20260220_7f31c6e4_schema.sql'), 'utf-8');

        // Find and run the mvt_tract_choropleth_forecast function (forecast version)
        const forecastFnMatch = schemaSql.match(
            /create or replace function forecast_20260220_7f31c6e4\.mvt_tract_choropleth_forecast\b[\s\S]*?(?=\ncreate or replace function|\n-- \d)/
        );
        if (forecastFnMatch) {
            await client.query(forecastFnMatch[0]);
            console.log('  mvt_tract_choropleth_forecast updated.');
        } else {
            console.log('  WARN: Could not extract mvt_tract_choropleth_forecast');
        }

        // Find and run the mvt_tract_choropleth_history function
        const historyFnMatch = schemaSql.match(
            /create or replace function forecast_20260220_7f31c6e4\.mvt_tract_choropleth_history\b[\s\S]*?(?=\ncreate or replace function|\n-- \d)/
        );
        if (historyFnMatch) {
            await client.query(historyFnMatch[0]);
            console.log('  mvt_tract_choropleth_history updated.');
        } else {
            console.log('  WARN: Could not extract mvt_tract_choropleth_history');
        }

        console.log('Migration complete!');
    } catch (err) {
        console.error('Error:', err);
    } finally {
        await client.end();
    }
}
run();
