// Simple check script using fetch - output to file
const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL
const supabaseKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY
const fs = require('fs')

async function checkTable(tableName) {
    const url = `${supabaseUrl}/rest/v1/${tableName}?select=*&forecast_year=eq.2026&limit=1`
    const res = await fetch(url, {
        headers: {
            apikey: supabaseKey,
            Authorization: `Bearer ${supabaseKey}`,
        },
    })
    const data = await res.json()
    return data[0] || null
}

async function main() {
    let output = ""

    output += "=== h3_precomputed_hex_details ===\n"
    const details = await checkTable("h3_precomputed_hex_details")
    if (details) {
        output += "Columns: " + Object.keys(details).sort().join(", ") + "\n\n"
        output += "Sample values:\n"
        for (const [k, v] of Object.entries(details)) {
            output += `  ${k}: ${JSON.stringify(v)}\n`
        }
    } else {
        output += "No data found or table does not exist\n"
    }

    output += "\n=== h3_precomputed_hex_rows ===\n"
    const rows = await checkTable("h3_precomputed_hex_rows")
    if (rows) {
        output += "Columns: " + Object.keys(rows).sort().join(", ") + "\n\n"
        output += "Sample values:\n"
        for (const [k, v] of Object.entries(rows)) {
            output += `  ${k}: ${JSON.stringify(v)}\n`
        }
    } else {
        output += "No data found or table does not exist\n"
    }

    console.log(output)
    fs.writeFileSync('scripts/supabase-columns.txt', output)
    console.log("\nWritten to scripts/supabase-columns.txt")
}

main().catch(console.error)
