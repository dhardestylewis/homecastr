// Check for any property-level or prediction data that could populate fan charts
const fs = require('fs')
const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL
const supabaseKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY

// Check for property-level tables
const tablesToCheck = [
    'properties',
    'property_data',
    'property_values',
    'property_predictions',
    'parcel_data',
    'parcels',
    'h3_property_values',
    'h3_raw_predictions'
]

async function checkPredictionTables() {
    let output = "=== Checking for raw prediction data ===\n\n"

    // First check what tables exist - REST API returns 404 for non-existent tables
    for (const table of tablesToCheck) {
        try {
            const url = `${supabaseUrl}/rest/v1/${table}?select=*&limit=1`
            const res = await fetch(url, {
                headers: { apikey: supabaseKey, Authorization: `Bearer ${supabaseKey}` }
            })

            if (res.ok) {
                const data = await res.json()
                if (data[0]) {
                    output += `✓ ${table}:\n  Columns: ${Object.keys(data[0]).sort().join(', ')}\n\n`
                } else {
                    output += `○ ${table}: Exists but empty\n\n`
                }
            } else {
                output += `✗ ${table}: Not found\n`
            }
        } catch (e) {
            output += `✗ ${table}: Error\n`
        }
    }

    // Check predicted_value column in hex_details - could create fan chart from historical predictions
    output += "\n=== Checking predicted_value across years ===\n"
    const years = [2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026, 2027, 2028, 2029, 2030]

    for (const year of years) {
        const url = `${supabaseUrl}/rest/v1/h3_precomputed_hex_details?select=predicted_value&forecast_year=eq.${year}&predicted_value=neq.null&limit=5`
        const res = await fetch(url, {
            headers: { apikey: supabaseKey, Authorization: `Bearer ${supabaseKey}` }
        })
        const data = await res.json()
        output += `Year ${year}: ${data.length} rows with predicted_value\n`
        if (data[0]) {
            output += `  Sample: ${data.slice(0, 3).map(d => d.predicted_value).join(', ')}\n`
        }
    }

    console.log(output)
    fs.writeFileSync('scripts/prediction-data-check.txt', output)
}

checkPredictionTables().catch(console.error)
