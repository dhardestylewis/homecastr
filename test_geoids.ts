import { loadEnvConfig } from '@next/env'
loadEnvConfig(process.cwd())

import { getSupabaseAdmin } from "./lib/supabase/admin"

async function main() {
    const supabase = getSupabaseAdmin()
    const SCHEMA = process.env.FORECAST_SCHEMA || "forecast_queue"
    console.log("SCHEMA:", SCHEMA)

    const { data: ca25 } = await supabase
        .schema(SCHEMA)
        .from("metrics_tract_forecast")
        .select("tract_geoid20")
        .like("tract_geoid20", "%R")
        .limit(10)

    console.log("CA R-suffix tracts:")
    console.log(ca25)
}

main().catch(console.error)
