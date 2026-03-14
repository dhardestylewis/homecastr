// Check all available tables in Supabase for potential fan chart data
const fs = require('fs')
const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL
const supabaseKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY

// Check common table names that might have fan chart / prediction data
const tablesToCheck = [
    'h3_precomputed_hex_details',
    'h3_precomputed_hex_rows',
    'h3_predictions',
    'h3_forecasts',
    'predictions',
    'forecasts',
    'fan_chart_data',
    'property_predictions'
]

async function checkTables() {
    let output = "=== Checking tables for fan chart data ===\n\n"

    for (const table of tablesToCheck) {
        try {
            const url = `${supabaseUrl}/rest/v1/${table}?select=*&limit=1`
            const res = await fetch(url, {
                headers: { apikey: supabaseKey, Authorization: `Bearer ${supabaseKey}` }
            })
            const data = await res.json()

            if (res.ok && data[0]) {
                const cols = Object.keys(data[0]).sort().join(', ')
                output += `✓ ${table}:\n  Columns: ${cols}\n\n`

                // Check for fan-related columns
                const fanCols = Object.keys(data[0]).filter(k => k.includes('fan') || k.includes('p10') || k.includes('p50') || k.includes('p90'))
                if (fanCols.length > 0) {
                    output += `  FAN CHART COLUMNS FOUND: ${fanCols.join(', ')}\n\n`
                }
            } else {
                output += `✗ ${table}: Not found or empty\n\n`
            }
        } catch (e) {
            output += `✗ ${table}: Error - ${e.message}\n\n`
        }
    }

    console.log(output)
    fs.writeFileSync('scripts/fan-chart-tables-check.txt', output)
}

checkTables().catch(console.error)
