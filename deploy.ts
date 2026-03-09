import { loadEnvConfig } from '@next/env';
loadEnvConfig(process.cwd());
import { Client } from 'pg';
import fs from 'fs';

async function main() {
    process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";
    const client = new Client({
        connectionString: process.env.POSTGRES_URL_NON_POOLING || process.env.DATABASE_URL,
        ssl: { rejectUnauthorized: false }
    });
    await client.connect();
    const sql = fs.readFileSync('sql/schema/fix_baseline_fq.sql', 'utf8');
    await client.query(sql);
    console.log('Deployed SQL successfully.');
    await client.end();
}
main().catch(console.error);
