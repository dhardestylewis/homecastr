
import { createClient } from '@supabase/supabase-js'

// Hardcoded for quick check (same as check_2020_data.ts)
const supabaseUrl = "https://earrhbknfjnhbudsucch.supabase.co"
const supabaseKey = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVhcnJoYmtuZmpuaGJ1ZHN1Y2NoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjY3MDQ3NTEsImV4cCI6MjA4MjI4MDc1MX0.qlmpskXzZImPJ3l9-Ze4CkaXnQXSdyVW7oWaoUn8Np0"

if (!supabaseUrl || !supabaseKey) {
    console.error('Missing credentials')
    process.exit(1)
}

const supabase = createClient(supabaseUrl, supabaseKey)

async function checkData() {
    console.log("Checking data for Year 2017...")
    const year = 2017

    // 1. Count rows in h3_precomputed_hex_details
    const { count: detailsCount, error: detailsErr } = await supabase
        .from('h3_precomputed_hex_details')
        .select('*', { count: 'exact', head: true })
        .eq('forecast_year', year)

    if (detailsErr) console.error("Details Error:", detailsErr.message)
    console.log(`[DETAILS] Rows for 2017: ${detailsCount}`)

    // 2. Sample a row to check columns
    const { data: sample, error: sampleErr } = await supabase
        .from('h3_precomputed_hex_details')
        .select('h3_id, predicted_value, opportunity, reliability')
        .eq('forecast_year', year)
        .limit(1)

    if (sample && sample.length > 0) {
        console.log("Sample Row:", sample[0])
    } else {
        console.log("No rows found to sample.")
    }

    // 3. Check for valid predicted_value (should correspond to Actual Value in our new logic)
    if (detailsCount && detailsCount > 0) {
        const { count: nonNullCount } = await supabase
            .from('h3_precomputed_hex_details')
            .select('*', { count: 'exact', head: true })
            .eq('forecast_year', year)
            .not('predicted_value', 'is', null)

        console.log(`[DETAILS] Rows with non-null predicted_value: ${nonNullCount}`)
    }
}

checkData()
