import { loadEnvConfig } from '@next/env';
loadEnvConfig(process.cwd());
import { Client } from 'pg';
import fs from 'fs';
import path from 'path';

async function run() {
    const dbUrl = process.env.POSTGRES_URL_NON_POOLING;
    if (!dbUrl) {
        console.error('Missing POSTGRES_URL_NON_POOLING variable');
        return;
    }
    console.log('Connecting to', dbUrl.split('@')[1] || dbUrl);
    const client = new Client({
        connectionString: dbUrl,
        ssl: { rejectUnauthorized: false }
    });
    try {
        await client.connect();
        const sql = fs.readFileSync(path.join(process.cwd(), 'sql/schema/create_dynamic_bounds_rpc.sql'), 'utf-8');
        await client.query(sql);
        console.log('Successfully applied SQL migration.');
    } catch (err) {
        console.error('Error applying SQL:', err);
    } finally {
        await client.end();
    }
}
run();
