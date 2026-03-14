// Simple check script using fetch
const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL
const supabaseKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY

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
