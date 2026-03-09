import { loadEnvConfig } from '@next/env'
loadEnvConfig(process.cwd())

import { getSupabaseAdmin } from "./lib/supabase/admin"

async function main() {
    const supabase = getSupabaseAdmin()

    // check if it exists in our master geography table
    const { data: bnd } = await supabase
        .from("geo_tract20_tx")
        .select("geoid20, namelsad20")
        .eq("geoid20", "0611570025R")

    console.log("Boundary table:", bnd)

    // check CA bounds simply to see if R is common
    const { data: bndCa } = await supabase
        .from("geo_tract20_tx")
        .select("geoid20, namelsad20")
        .like("geoid20", "061157%")
        .limit(10)

    console.log("Boundary table CA:", bndCa)
}

main().catch(console.error)
