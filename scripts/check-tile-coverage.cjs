// Check tile data coverage in Supabase
const supabaseUrl = "https://earrhbknfjnhbudsucch.supabase.co"
const supabaseKey = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVhcnJoYmtuZmpuaGJ1ZHN1Y2NoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjY3MDQ3NTEsImV4cCI6MjA4MjI4MDc1MX0.qlmpskXzZImPJ3l9-Ze4CkaXnQXSdyVW7oWaoUn8Np0"
const fs = require('fs')

async function query(sql) {
    const url = `${supabaseUrl}/rest/v1/rpc/exec_sql`
    // Use regular REST API instead
    return null
}

async function countByResolution(tableName, forecastYear) {
    const url = `${supabaseUrl}/rest/v1/${tableName}?select=h3_res,count&forecast_year=eq.${forecastYear}&order=h3_res`

    // Get distinct h3_res values and counts
    const results = {}
    for (let res = 5; res <= 11; res++) {
        const countUrl = `${supabaseUrl}/rest/v1/${tableName}?select=h3_id&forecast_year=eq.${forecastYear}&h3_res=eq.${res}&limit=1`
        const headUrl = `${supabaseUrl}/rest/v1/${tableName}?select=count&forecast_year=eq.${forecastYear}&h3_res=eq.${res}`

        try {
            const response = await fetch(headUrl, {
                method: 'HEAD',
                headers: {
                    apikey: supabaseKey,
                    Authorization: `Bearer ${supabaseKey}`,
                    Prefer: 'count=exact'
                },
            })
            const count = response.headers.get('content-range')
            results[res] = count ? count.split('/')[1] : '0'
        } catch (e) {
            results[res] = 'error'
        }
    }
    return results
}

async function main() {
    let output = "=== Tile Data Coverage Check ===\n\n"

    output += "Checking h3_precomputed_hex_rows by resolution for year 2026:\n"
    const rowsCounts = await countByResolution('h3_precomputed_hex_rows', 2026)
    for (const [res, count] of Object.entries(rowsCounts)) {
        output += `  H3 Res ${res}: ${count} tiles\n`
    }

    output += "\nChecking h3_precomputed_hex_details by resolution for year 2026:\n"
    const detailsCounts = await countByResolution('h3_precomputed_hex_details', 2026)
    for (const [res, count] of Object.entries(detailsCounts)) {
        output += `  H3 Res ${res}: ${count} tiles\n`
    }

    output += "\n=== UI Zoom to Resolution Mapping ===\n"
    output += "From map-view.tsx getH3ResolutionForZoom():\n"
    output += "  zoom < 10.5  → H3 res 7\n"
    output += "  zoom < 12.0  → H3 res 8\n"
    output += "  zoom < 13.5  → H3 res 9\n"
    output += "  zoom < 15.0  → H3 res 10\n"
    output += "  zoom >= 15.0 → H3 res 11\n"

    output += "\nFrom map-view.tsx getContinuousBasemapZoom():\n"
    output += "  scale 1000  → zoom ~9\n"
    output += "  scale 3000  → zoom ~10.5 (initial view)\n"
    output += "  scale 15000 → zoom ~14.2\n"
    output += "  scale 50000 → zoom ~18\n"

    console.log(output)
    fs.writeFileSync('scripts/tile-coverage.txt', output)
    console.log("\nWritten to scripts/tile-coverage.txt")
}

main().catch(console.error)
