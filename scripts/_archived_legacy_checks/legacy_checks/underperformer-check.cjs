// Check how many tiles are filtered by showUnderperformers=false
const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL
const supabaseKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY

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
