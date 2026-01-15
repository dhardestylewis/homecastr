// Check fan chart columns specifically with actual values
const fs = require('fs')
const supabaseUrl = "https://earrhbknfjnhbudsucch.supabase.co"
const supabaseKey = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVhcnJoYmtuZmpuaGJ1ZHN1Y2NoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjY3MDQ3NTEsImV4cCI6MjA4MjI4MDc1MX0.qlmpskXzZImPJ3l9-Ze4CkaXnQXSdyVW7oWaoUn8Np0"

async function checkFanChartData() {
    let output = "=== FAN CHART COLUMN ANALYSIS ===\n\n"

    // Fan chart columns
    const fanCols = [
        'fan_p10_y1', 'fan_p10_y2', 'fan_p10_y3', 'fan_p10_y4', 'fan_p10_y5',
        'fan_p50_y1', 'fan_p50_y2', 'fan_p50_y3', 'fan_p50_y4', 'fan_p50_y5',
        'fan_p90_y1', 'fan_p90_y2', 'fan_p90_y3', 'fan_p90_y4', 'fan_p90_y5'
    ]

    output += "Expected columns in h3_precomputed_hex_details:\n"
    fanCols.forEach(col => {
        output += `  - ${col}\n`
    })
    output += "\n"

    // Get sample rows with fan chart columns
    const select = ['h3_id', 'forecast_year', 'predicted_value', ...fanCols].join(',')
    const url = `${supabaseUrl}/rest/v1/h3_precomputed_hex_details?select=${select}&limit=5`

    const res = await fetch(url, {
        headers: { apikey: supabaseKey, Authorization: `Bearer ${supabaseKey}` }
    })
    const data = await res.json()

    output += "Sample rows (5):\n"
    output += "-".repeat(80) + "\n"

    for (const row of data) {
        output += `h3_id: ${row.h3_id}, year: ${row.forecast_year}\n`
        output += `  predicted_value: ${row.predicted_value}\n`
        output += `  Fan chart values:\n`
        for (const col of fanCols) {
            output += `    ${col}: ${row[col] ?? 'NULL'}\n`
        }
        output += "\n"
    }

    // Check if ANY rows have non-null fan chart values
    const checkUrl = `${supabaseUrl}/rest/v1/h3_precomputed_hex_details?select=h3_id,fan_p50_y1&fan_p50_y1=neq.null&limit=5`
    const checkRes = await fetch(checkUrl, {
        headers: { apikey: supabaseKey, Authorization: `Bearer ${supabaseKey}` }
    })
    const checkData = await checkRes.json()

    output += "-".repeat(80) + "\n"
    output += `Rows with non-null fan_p50_y1: ${checkData.length > 0 ? checkData.length + ' found' : 'NONE'}\n`

    console.log(output)
    fs.writeFileSync('scripts/fan-chart-columns.txt', output)
}

checkFanChartData().catch(console.error)
