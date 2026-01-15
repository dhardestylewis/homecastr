// Check all available tables in Supabase for potential fan chart data
const fs = require('fs')
const supabaseUrl = "https://earrhbknfjnhbudsucch.supabase.co"
const supabaseKey = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVhcnJoYmtuZmpuaGJ1ZHN1Y2NoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjY3MDQ3NTEsImV4cCI6MjA4MjI4MDc1MX0.qlmpskXzZImPJ3l9-Ze4CkaXnQXSdyVW7oWaoUn8Np0"

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
