
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

async function auditSamples() {
    if (!supabaseUrl || !supabaseKey) return console.error("Missing credentials")
    const supabase = createClient(supabaseUrl, supabaseKey)

    console.log("--- 2026 Sample ---")
    const { data: s2026 } = await supabase
        .from("h3_precomputed_hex_details")
        .select("*")
        .eq("forecast_year", 2026)
        .limit(5)
    console.log(JSON.stringify(s2026, null, 2))

    console.log("--- 2029 Sample ---")
    const { data: s2029 } = await supabase
        .from("h3_precomputed_hex_details")
        .select("*")
        .eq("forecast_year", 2029)
        .limit(5)
    console.log(JSON.stringify(s2029, null, 2))
}

auditSamples()
