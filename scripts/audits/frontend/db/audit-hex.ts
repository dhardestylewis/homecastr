
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

async function auditHex() {
    if (!supabaseUrl || !supabaseKey) return console.error("Missing credentials")
    const supabase = createClient(supabaseUrl, supabaseKey)

    const hexId = "89446c1435fffff" // Montrose
    console.log(`--- Audit for Hex: ${hexId} ---`)
    const { data: results, error } = await supabase
        .from("h3_precomputed_hex_details")
        .select("h3_id, forecast_year, opportunity, predicted_value, property_count")
        .eq("h3_id", hexId)
        .order("forecast_year", { ascending: true })

    if (error) console.error(error)
    else console.log(JSON.stringify(results, null, 2))
}

auditHex()
