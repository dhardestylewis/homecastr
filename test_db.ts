import { loadEnvConfig } from "@next/env"
loadEnvConfig(process.cwd())

import { Client } from "pg"
import * as fs from "fs"

process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0"

async function main() {
    const client = new Client({
        connectionString: process.env.POSTGRES_URL_NON_POOLING,
        ssl: { rejectUnauthorized: false }
    })

    await client.connect()

    const res = await client.query(`
        SELECT f_table_name, f_geometry_column 
        FROM geometry_columns 
        ORDER BY f_table_name;
    `)

    fs.writeFileSync("/tmp/geo_tables.json", JSON.stringify(res.rows, null, 2))

    const res2 = await client.query(`
        SELECT routine_name FROM information_schema.routines 
        WHERE routine_name LIKE 'mvt_%' OR routine_name LIKE 'get_feature%'
        ORDER BY routine_name;
    `)
    fs.writeFileSync("/tmp/geo_funcs.json", JSON.stringify(res2.rows, null, 2))

    await client.end()
    console.log("Done")
}

main().catch(console.error)