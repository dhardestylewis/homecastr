// Check how many tiles are filtered by showUnderperformers=false
const supabaseUrl = "https://earrhbknfjnhbudsucch.supabase.co"
const supabaseKey = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVhcnJoYmtuZmpuaGJ1ZHN1Y2NoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjY3MDQ3NTEsImV4cCI6MjA4MjI4MDc1MX0.qlmpskXzZImPJ3l9-Ze4CkaXnQXSdyVW7oWaoUn8Np0"

async function countWithFilter(h3Res, opFilter) {
    const url = `${supabaseUrl}/rest/v1/h3_precomputed_hex_details?select=h3_id&h3_res=eq.${h3Res}&forecast_year=eq.2026&opportunity=${opFilter}`
    const res = await fetch(url, {
        method: 'HEAD',
        headers: {
            apikey: supabaseKey,
            Authorization: `Bearer ${supabaseKey}`,
            Prefer: 'count=exact'
        },
    })
    const range = res.headers.get('content-range')
    return range ? parseInt(range.split('/')[1]) : 0
}

async function main() {
    console.log("=== Underperformer Analysis ===\n")
    console.log("Default filter: showUnderperformers=false means opportunity < 0 are HIDDEN\n")

    for (const h3Res of [7, 8, 9, 10, 11]) {
        const negative = await countWithFilter(h3Res, 'lt.0')
        const positive = await countWithFilter(h3Res, 'gte.0')
        const total = negative + positive
        const pctNegative = ((negative / total) * 100).toFixed(1)

        console.log(`H3 Res ${h3Res}:`)
        console.log(`  Total: ${total.toLocaleString()}`)
        console.log(`  Positive (shown): ${positive.toLocaleString()}`)
        console.log(`  Negative (hidden): ${negative.toLocaleString()} (${pctNegative}%)`)
        console.log()
    }
}

main().catch(console.error)
