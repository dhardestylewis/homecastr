// Check grid cell counts by resolution
const fs = require('fs')
const supabaseUrl = "https://earrhbknfjnhbudsucch.supabase.co"
const supabaseKey = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVhcnJoYmtuZmpuaGJ1ZHN1Y2NoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjY3MDQ3NTEsImV4cCI6MjA4MjI4MDc1MX0.qlmpskXzZImPJ3l9-Ze4CkaXnQXSdyVW7oWaoUn8Np0"

async function checkGridCounts() {
    let output = "=== Grid Cell Counts by Resolution ===\n\n"

    for (let res = 6; res <= 11; res++) {
        // Use head request to get count
        const url = `${supabaseUrl}/rest/v1/h3_aoi_grid?select=h3_id&h3_res=eq.${res}&aoi_id=eq.harris_county`
        const res2 = await fetch(url, {
            headers: {
                apikey: supabaseKey,
                Authorization: `Bearer ${supabaseKey}`,
                Prefer: 'count=exact',
                Range: '0-0'  // Only get count, not data
            }
        })

        const contentRange = res2.headers.get('content-range')
        output += `Resolution ${res}: ${contentRange}\n`
    }

    console.log(output)
    fs.writeFileSync('scripts/grid-counts.txt', output)
}

checkGridCounts().catch(console.error)
