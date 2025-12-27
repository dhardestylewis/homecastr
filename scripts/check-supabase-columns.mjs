// Simple check script using fetch
const supabaseUrl = "https://earrhbknfjnhbudsucch.supabase.co"
const supabaseKey = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVhcnJoYmtuZmpuaGJ1ZHN1Y2NoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjY3MDQ3NTEsImV4cCI6MjA4MjI4MDc1MX0.qlmpskXzZImPJ3l9-Ze4CkaXnQXSdyVW7oWaoUn8Np0"

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
    console.log("=== h3_precomputed_hex_details ===")
    const details = await checkTable("h3_precomputed_hex_details")
    if (details) {
        console.log("Columns:", Object.keys(details).sort().join(", "))
        console.log("\nSample values:")
        for (const [k, v] of Object.entries(details)) {
            console.log(`  ${k}: ${v}`)
        }
    } else {
        console.log("No data found or table does not exist")
    }

    console.log("\n=== h3_precomputed_hex_rows ===")
    const rows = await checkTable("h3_precomputed_hex_rows")
    if (rows) {
        console.log("Columns:", Object.keys(rows).sort().join(", "))
        console.log("\nSample values:")
        for (const [k, v] of Object.entries(rows)) {
            console.log(`  ${k}: ${v}`)
        }
    } else {
        console.log("No data found or table does not exist")
    }
}

main().catch(console.error)
