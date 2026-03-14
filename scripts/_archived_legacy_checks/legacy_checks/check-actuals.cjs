// Check for actual values vs predicted, residuals, and error metrics
const fs = require('fs')
const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL
const supabaseKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY

async function checkActualsAndResiduals() {
    let output = "=== Checking for Actual Values and Residual Error Data ===\n\n"

    // Columns that might relate to actuals/residuals
    const columnsOfInterest = [
        'current_value',      // Might be current actual
        'predicted_value',    // Model prediction
        'ape',                // Absolute Percentage Error
        'sample_accuracy',    // Some accuracy metric
        'pred_cv',            // Prediction coefficient of variation
        'medae_z',            // Median Absolute Error z-score
        'tail_gap_z'          // Tail gap z-score
    ]

    // Get sample with these columns
    const select = columnsOfInterest.join(',')
    const url = `${supabaseUrl}/rest/v1/h3_precomputed_hex_details?select=${select},h3_id,forecast_year&limit=5`

    const res = await fetch(url, {
        headers: { apikey: supabaseKey, Authorization: `Bearer ${supabaseKey}` }
    })
    const data = await res.json()

    output += "Sample rows from hex_details:\n"
    output += "-".repeat(80) + "\n"

    for (const row of data) {
        output += `H3: ${row.h3_id}, Year: ${row.forecast_year}\n`
        for (const col of columnsOfInterest) {
            const val = row[col]
            const status = val === null ? 'NULL' : val
            output += `  ${col}: ${status}\n`
        }
        output += "\n"
    }

    // Check if there's a separate actuals table
    output += "\n=== Checking for potential actuals tables ===\n"
    const tableCandidates = ['h3_actuals', 'property_actuals', 'assessment_values', 'actual_values', 'market_values']

    for (const table of tableCandidates) {
        try {
            const tUrl = `${supabaseUrl}/rest/v1/${table}?select=*&limit=1`
            const tRes = await fetch(tUrl, {
                headers: { apikey: supabaseKey, Authorization: `Bearer ${supabaseKey}` }
            })
            if (tRes.ok) {
                const tData = await tRes.json()
                if (tData[0]) {
                    output += `✓ ${table}: ${Object.keys(tData[0]).join(', ')}\n`
                } else {
                    output += `○ ${table}: exists but empty\n`
                }
            } else {
                output += `✗ ${table}: not found\n`
            }
        } catch (e) {
            output += `✗ ${table}: error\n`
        }
    }

    console.log(output)
    fs.writeFileSync('scripts/actuals-check.txt', output)
}

checkActualsAndResiduals().catch(console.error)
