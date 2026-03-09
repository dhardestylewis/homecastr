import { loadEnvConfig } from '@next/env'
loadEnvConfig(process.cwd())
import { getSupabaseAdmin } from "./lib/supabase/admin"

async function main() {
    const supabase = getSupabaseAdmin()

    // 1. Check what tables exist that start with geo_
    const { data: tables, error: e1 } = await supabase.rpc('get_tables_by_prefix', { prefix: 'geo_' })
    console.log("Geo tables (if RPC exists):", tables || e1?.message)

    // Alternatively try to query geo_state
    const { data: st, error: e2 } = await supabase.from('geo_state').select('*').limit(1)
    console.log("geo_state:", st ? "EXISTS" : e2?.message)

    const { data: st20, error: e3 } = await supabase.from('geo_state20').select('*').limit(1)
    console.log("geo_state20:", st20 ? "EXISTS" : e3?.message)
}

main().catch(console.error)
