// Check for confidence factor columns in both tables
const fs = require('fs')
const supabaseUrl = "https://earrhbknfjnhbudsucch.supabase.co"
const supabaseKey = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVhcnJoYmtuZmpuaGJ1ZHN1Y2NoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjY3MDQ3NTEsImV4cCI6MjA4MjI4MDc1MX0.qlmpskXzZImPJ3l9-Ze4CkaXnQXSdyVW7oWaoUn8Np0"

async function checkConfidenceFactors() {
    let output = "=== Checking Confidence Factor columns ===\n\n"

    // Check hex_rows for all columns
    const rowsUrl = `${supabaseUrl}/rest/v1/h3_precomputed_hex_rows?select=*&limit=1`
    const rowsRes = await fetch(rowsUrl, {
        headers: { apikey: supabaseKey, Authorization: `Bearer ${supabaseKey}` }
    })
    const rowsData = await rowsRes.json()

    output += "h3_precomputed_hex_rows columns:\n"
    if (rowsData[0]) {
        output += Object.keys(rowsData[0]).sort().join(', ') + "\n\n"
    } else {
        output += "No data\n\n"
    }

    // Check hex_details for all columns
    const detailsUrl = `${supabaseUrl}/rest/v1/h3_precomputed_hex_details?select=*&limit=1`
    const detailsRes = await fetch(detailsUrl, {
        headers: { apikey: supabaseKey, Authorization: `Bearer ${supabaseKey}` }
    })
    const detailsData = await detailsRes.json()

    output += "h3_precomputed_hex_details columns:\n"
    if (detailsData[0]) {
        output += Object.keys(detailsData[0]).sort().join(', ') + "\n\n"
        // Check specific confidence columns
        const confCols = ['accuracy_term', 'confidence_term', 'stability_term', 'robustness_term', 'support_term']
        output += "Confidence factor values:\n"
        for (const col of confCols) {
            output += `  ${col}: ${detailsData[0][col]}\n`
        }
    } else {
        output += "No data (table may be empty or not exist)\n\n"
    }

    console.log(output)
    fs.writeFileSync('scripts/confidence-factors-check.txt', output)
}

checkConfidenceFactors().catch(console.error)
