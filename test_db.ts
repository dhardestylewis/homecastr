import { loadEnvConfig } from "@next/env"
loadEnvConfig(process.cwd())

import { Client } from "pg"

process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0"

async function main() {
    const client = new Client({
        connectionString: process.env.POSTGRES_URL_NON_POOLING,
        ssl: { rejectUnauthorized: false }
    })

    await client.connect()

    // Check distinct state FIPS prefixes in geo_tract20_tx
    const res = await client.query(`
        SELECT DISTINCT substring(geoid from 1 for 2) as state_fips, count(*) as tract_count
        FROM public.geo_tract20_tx 
        GROUP BY substring(geoid from 1 for 2)
        ORDER BY state_fips;
    `)
    console.log("States in geo_tract20_tx:", res.rows.length, "states")
    for (const r of res.rows) {
        console.log(`  ${r.state_fips}: ${r.tract_count} tracts`)
    }

    await client.end()
}

main().catch(console.error)