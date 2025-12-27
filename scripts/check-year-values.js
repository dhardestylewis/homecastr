// Quick script to check if predicted_value differs by year
const { createClient } = require('@supabase/supabase-js')
require('dotenv').config({ path: '.env.local' })

const supabase = createClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL,
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY
)

async function checkYearValues() {
    const h3Id = '89446ca8873ffff'

    const { data, error } = await supabase
        .from('h3_precomputed_hex_details')
        .select('forecast_year, predicted_value, opportunity, reliability')
        .eq('h3_id', h3Id)
        .order('forecast_year')

    if (error) {
        console.error('Error:', error)
        return
    }

    console.log(`\nValues for ${h3Id}:`)
    console.table(data)
}

checkYearValues()
