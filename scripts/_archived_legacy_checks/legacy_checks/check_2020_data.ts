
import { createClient } from '@supabase/supabase-js'

// Hardcoded for quick check
const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL
const supabaseKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY

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
