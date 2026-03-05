
import dotenv from "dotenv"
import fs from "fs"
import path from "path"
import { createClient } from "@supabase/supabase-js"

const envLocalPath = path.resolve(process.cwd(), ".env.local")
if (fs.existsSync(envLocalPath)) {
    const envConfig = dotenv.parse(fs.readFileSync(envLocalPath))
    for (const k in envConfig) {
        process.env[k] = envConfig[k]
    }
}

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL
const supabaseKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY

async function auditMetrics() {
    if (!supabaseUrl || !supabaseKey) return console.error("Missing credentials")
    const supabase = createClient(supabaseUrl, supabaseKey)

    console.log("--- High Opportunity (> 0.5) Sample ---")
    const { data: highOpp } = await supabase
        .from("h3_precomputed_hex_details")
        .select("h3_id, opportunity, predicted_value, forecast_year")
        .gt("opportunity", 0.5)
        .limit(5)
    console.table(highOpp)

    console.log("--- Comparison of 2026 vs 2029 for hex in Montrose area ---")
    // Let's pick a known hex or search for one
    const { data: montrose } = await supabase
        .from("h3_precomputed_hex_details")
        .select("h3_id, forecast_year, opportunity, predicted_value")
        .eq("forecast_year", 2026)
        .limit(1)

    if (montrose && montrose.length > 0) {
        const id = montrose[0].h3_id
        const { data: history } = await supabase
            .from("h3_precomputed_hex_details")
            .select("h3_id, forecast_year, opportunity, predicted_value")
            .eq("h3_id", id)
            .order("forecast_year", { ascending: true })
        console.table(history)
    }
}

auditMetrics()
