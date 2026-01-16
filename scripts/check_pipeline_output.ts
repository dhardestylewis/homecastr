
import { createClient } from '@supabase/supabase-js'

// Hardcoded for quick check
const supabaseUrl = "https://earrhbknfjnhbudsucch.supabase.co"
const supabaseKey = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVhcnJoYmtuZmpuaGJ1ZHN1Y2NoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjY3MDQ3NTEsImV4cCI6MjA4MjI4MDc1MX0.qlmpskXzZImPJ3l9-Ze4CkaXnQXSdyVW7oWaoUn8Np0"

if (!supabaseUrl || !supabaseKey) {
    console.error('Missing credentials')
    process.exit(1)
}

const supabase = createClient(supabaseUrl, supabaseKey)

async function checkForecastData() {
    console.log("Checking Forecast Data (2026) for Fan Charts & Reliability...")
    const year = 2026

    // 1. Check Fan Chart Columns (p50_y1, p50_y5)
    // We expect these to be NON-NULL now if v14.7 ran.
    const { count: fanCount, error: fanErr } = await supabase
        .from('h3_precomputed_hex_details')
        .select('*', { count: 'exact', head: true })
        .eq('forecast_year', year)
        .not('fan_p50_y1', 'is', null)
        .not('fan_p50_y5', 'is', null)

    if (fanErr) console.error("Fan Check Error:", fanErr.message)
    console.log(`[FORECAST] Rows with populated Fan Charts (y1 & y5): ${fanCount}`)

    // 2. Check Reliability Decomposition
    const { count: relCount, error: relErr } = await supabase
        .from('h3_precomputed_hex_details')
        .select('*', { count: 'exact', head: true })
        .eq('forecast_year', year)
        .not('accuracy_term', 'is', null)
        .not('stability_term', 'is', null)

    if (relErr) console.error("Rel Check Error:", relErr.message)
    console.log(`[FORECAST] Rows with populated Reliability Terms: ${relCount}`)

    // 3. Sample a row
    const { data: sample } = await supabase
        .from('h3_precomputed_hex_details')
        .select('h3_id, fan_p50_y1, fan_p90_y5, accuracy_term, robustness_term')
        .eq('forecast_year', year)
        .limit(1)

    if (sample && sample.length > 0) {
        console.log("Sample 2026 Row:", sample[0])
    }
}

checkForecastData()
