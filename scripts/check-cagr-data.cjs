// Check if predicted_value exists across years for CAGR calculation
const fs = require('fs')
const supabaseUrl = "https://earrhbknfjnhbudsucch.supabase.co"
const supabaseKey = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVhcnJoYmtuZmpuaGJ1ZHN1Y2NoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjY3MDQ3NTEsImV4cCI6MjA4MjI4MDc1MX0.qlmpskXzZImPJ3l9-Ze4CkaXnQXSdyVW7oWaoUn8Np0"

async function checkHistoricalValues() {
    let output = "=== Historical Predicted Values Check (for CAGR) ===\n\n"

    // Pick a sample h3_id and check its values across years
    const sampleUrl = `${supabaseUrl}/rest/v1/h3_precomputed_hex_details?select=h3_id&limit=1`
    const sampleRes = await fetch(sampleUrl, {
        headers: { apikey: supabaseKey, Authorization: `Bearer ${supabaseKey}` }
    })
    const sampleData = await sampleRes.json()

    if (!sampleData[0]) {
        output += "No data in hex_details\n"
        console.log(output)
        return
    }

    const h3Id = sampleData[0].h3_id
    output += `Sample H3: ${h3Id}\n\n`

    // Check values across all years
    const yearsUrl = `${supabaseUrl}/rest/v1/h3_precomputed_hex_details?select=forecast_year,predicted_value,opportunity&h3_id=eq.${h3Id}&order=forecast_year`
    const yearsRes = await fetch(yearsUrl, {
        headers: { apikey: supabaseKey, Authorization: `Bearer ${supabaseKey}` }
    })
    const yearsData = await yearsRes.json()

    output += "Year\tPredicted Value\t\tOpportunity\n"
    output += "-".repeat(50) + "\n"

    for (const row of yearsData) {
        const val = row.predicted_value ? `$${row.predicted_value.toLocaleString()}` : 'NULL'
        output += `${row.forecast_year}\t${val}\t\t${row.opportunity}\n`
    }

    // Calculate CAGR if we have multiple years
    if (yearsData.length >= 2) {
        const firstYear = yearsData[0]
        const lastYear = yearsData[yearsData.length - 1]

        if (firstYear.predicted_value && lastYear.predicted_value) {
            const years = lastYear.forecast_year - firstYear.forecast_year
            const cagr = Math.pow(lastYear.predicted_value / firstYear.predicted_value, 1 / years) - 1
            output += `\nComputed CAGR: ${(cagr * 100).toFixed(2)}% over ${years} years\n`
        }
    }

    console.log(output)
    fs.writeFileSync('scripts/cagr-check.txt', output)
}

checkHistoricalValues().catch(console.error)
