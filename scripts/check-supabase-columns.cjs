// Simple check script using fetch - output to file
const supabaseUrl = "https://earrhbknfjnhbudsucch.supabase.co"
const supabaseKey = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVhcnJoYmtuZmpuaGJ1ZHN1Y2NoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjY3MDQ3NTEsImV4cCI6MjA4MjI4MDc1MX0.qlmpskXzZImPJ3l9-Ze4CkaXnQXSdyVW7oWaoUn8Np0"
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
