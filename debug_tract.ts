import { loadEnvConfig } from "@next/env"
loadEnvConfig(process.cwd())
import { Client } from "pg"

async function main() {
  process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0"
  const client = new Client({
    connectionString: process.env.POSTGRES_URL_NON_POOLING,
    ssl: { rejectUnauthorized: false }
  })
  await client.connect()

  // Verify forecast_queue._mvt_forecast_generic now has baseline_value
  const res = await client.query(`
        SELECT prosrc FROM pg_proc 
        WHERE proname = '_mvt_forecast_generic' 
        AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'forecast_queue');
    `)
  const src = res.rows[0]?.prosrc || ''
  console.log("forecast_queue._mvt_forecast_generic:")
  console.log("  Contains baseline_value?", src.includes('baseline_value'))
  console.log("  Contains horizon_m=24?", src.includes('horizon_m=24'))

  await client.end()
}

main().catch(console.error)
