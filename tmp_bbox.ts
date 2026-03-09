import { loadEnvConfig } from "@next/env"
loadEnvConfig(process.cwd())

import { createClient } from "@supabase/supabase-js"

const supabase = createClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL!,
    process.env.SUPABASE_SERVICE_ROLE_KEY!
)

async function run() {
    // Test 1: Direct RPC call
    const { data: stateData, error: stateErr } = await supabase.rpc("get_feature_bounds", {
        p_level: "state",
        p_geoid: "23",
        p_state_slug: null
    })
    console.log("State ME RPC result:", JSON.stringify(stateData), "error:", stateErr?.message)

    // Test 2: County
    const { data: countyData, error: countyErr } = await supabase.rpc("get_feature_bounds", {
        p_level: "county",
        p_geoid: "23005",
        p_state_slug: null
    })
    console.log("County 23005 RPC result:", JSON.stringify(countyData), "error:", countyErr?.message)

    // Test 3: Check what tables exist related to geo_state
    const { data: tables, error: tablesErr } = await supabase.rpc("get_feature_bounds", {
        p_level: "state",
        p_geoid: "48",
        p_state_slug: null
    })
    console.log("State TX RPC result:", JSON.stringify(tables), "error:", tablesErr?.message)
}

run()
