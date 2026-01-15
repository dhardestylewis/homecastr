// Comprehensive DB scan - check all accessible tables and their columns
const fs = require('fs')
const supabaseUrl = "https://earrhbknfjnhbudsucch.supabase.co"
const supabaseKey = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVhcnJoYmtuZmpuaGJ1ZHN1Y2NoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjY3MDQ3NTEsImV4cCI6MjA4MjI4MDc1MX0.qlmpskXzZImPJ3l9-Ze4CkaXnQXSdyVW7oWaoUn8Np0"

// Common table name patterns to try
const tablesToCheck = [
    // H3 related
    'h3_aoi_grid', 'h3_precomputed_hex_details', 'h3_precomputed_hex_rows',
    'h3_predictions', 'h3_forecasts', 'h3_property_values', 'h3_raw',
    // Property/parcel related  
    'properties', 'property', 'parcels', 'parcel', 'lots',
    'property_data', 'property_values', 'property_predictions',
    // Values/assessments
    'assessments', 'assessment_data', 'valuations', 'values',
    'appraisals', 'market_values', 'tax_values',
    // Predictions/forecasts
    'predictions', 'forecasts', 'projections', 'estimates',
    'model_outputs', 'model_predictions',
    // Sales/transactions
    'sales', 'transactions', 'transfers', 'deeds',
    // Geographic
    'neighborhoods', 'blocks', 'zip_codes', 'school_districts',
    // Raw data
    'raw_data', 'source_data', 'staging', 'import'
]

async function comprehensiveScan() {
    let output = "=== COMPREHENSIVE DB SCAN ===\n"
    output += `Timestamp: ${new Date().toISOString()}\n\n`

    const foundTables = []

    for (const table of tablesToCheck) {
        try {
            const url = `${supabaseUrl}/rest/v1/${table}?select=*&limit=1`
            const res = await fetch(url, {
                headers: { apikey: supabaseKey, Authorization: `Bearer ${supabaseKey}` }
            })

            if (res.ok) {
                const data = await res.json()
                if (data[0]) {
                    const cols = Object.keys(data[0]).sort()
                    foundTables.push({ name: table, columns: cols, sample: data[0] })
                    output += `\n${'='.repeat(60)}\n`
                    output += `TABLE: ${table}\n`
                    output += `${'='.repeat(60)}\n`
                    output += `Columns (${cols.length}): ${cols.join(', ')}\n`
                    output += `\nSample values:\n`
                    for (const col of cols) {
                        const val = data[0][col]
                        const valStr = val === null ? 'NULL' : JSON.stringify(val).slice(0, 80)
                        output += `  ${col}: ${valStr}\n`
                    }
                }
            }
        } catch (e) {
            // Ignore errors for non-existent tables
        }
    }

    output += `\n\n${'='.repeat(60)}\n`
    output += `SUMMARY: Found ${foundTables.length} accessible tables\n`
    output += `${'='.repeat(60)}\n`
    for (const t of foundTables) {
        output += `- ${t.name} (${t.columns.length} columns)\n`
    }

    console.log(output)
    fs.writeFileSync('scripts/comprehensive-db-scan.txt', output)
    console.log('\nWritten to scripts/comprehensive-db-scan.txt')
}

comprehensiveScan().catch(console.error)
