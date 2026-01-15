
import { createClient } from '@supabase/supabase-js'

// Hardcoded for quick check
const supabaseUrl = "https://earrhbknfjnhbudsucch.supabase.co"
const supabaseKey = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVhcnJoYmtuZmpuaGJ1ZHN1Y2NoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjY3MDQ3NTEsImV4cCI6MjA4MjI4MDc1MX0.qlmpskXzZImPJ3l9-Ze4CkaXnQXSdyVW7oWaoUn8Np0"

if (!supabaseUrl || !supabaseKey) {
    console.error('Missing credentials')
    process.exit(1)
}

const supabase = createClient(supabaseUrl, supabaseKey)

async function checkData() {
    const year = 2020

    // 1. Try to fetch ONE row with * to see available columns
    const { data: sample, error: sampleErr } = await supabase
        .from('h3_precomputed_hex_rows')
        .select('*')
        .eq('forecast_year', year)
        .limit(1)

    // 2. Count total rows for 2020
    const { count, error: countErr } = await supabase
        .from('h3_precomputed_hex_rows')
        .select('*', { count: 'exact', head: true })
        .eq('forecast_year', year)

    // 3. Check for ANY non-null med_predicted_value if rows exist
    if (count && count > 0) {
        const { count: validCount, error: validErr } = await supabase
            .from('h3_precomputed_hex_rows')
            .select('*', { count: 'exact', head: true })
            .eq('forecast_year', year)
            .not('med_predicted_value', 'is', null)
    }

    // 4. Check 2026 as control group
    const year2 = 2026
    const { count: count2, error: countErr2 } = await supabase
        .from('h3_precomputed_hex_rows')
        .select('*', { count: 'exact', head: true })
        .eq('forecast_year', year2)

    // Concise Output
    console.log(`\nRESULTS:`)
    console.log(`YEAR=2020 COUNT=${count ?? 'Error'}`)
    console.log(`YEAR=2026 COUNT=${count2 ?? 'Error'}`)
}

checkData()
