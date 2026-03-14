// Check grid cell counts by resolution
const fs = require('fs')
const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL
const supabaseKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY

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
