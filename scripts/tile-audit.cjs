// Comprehensive tile data audit
const supabaseUrl = "https://earrhbknfjnhbudsucch.supabase.co"
const supabaseKey = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVhcnJoYmtuZmpuaGJ1ZHN1Y2NoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjY3MDQ3NTEsImV4cCI6MjA4MjI4MDc1MX0.qlmpskXzZImPJ3l9-Ze4CkaXnQXSdyVW7oWaoUn8Np0"
const fs = require('fs')

// Expected H3 cell counts for Houston metro area (~10,000 sq km)
// H3 res 7: ~0.5 km² per cell -> ~20,000 cells for metro
// H3 res 8: ~0.07 km² per cell -> ~143,000 cells
// H3 res 9: ~0.01 km² per cell -> ~1,000,000 cells
// H3 res 10: ~0.0015 km² per cell -> ~6,700,000 cells
// H3 res 11: ~0.0002 km² per cell -> ~50,000,000 cells
// These are rough estimates for full metro, actual populated areas will be less

async function fetchSample(tableName, h3Res, forecastYear, limit = 100) {
    const url = `${supabaseUrl}/rest/v1/${tableName}?select=*&h3_res=eq.${h3Res}&forecast_year=eq.${forecastYear}&limit=${limit}`
    const res = await fetch(url, {
        headers: {
            apikey: supabaseKey,
            Authorization: `Bearer ${supabaseKey}`,
        },
    })
    return await res.json()
}

async function countRecords(tableName, h3Res, forecastYear) {
    const url = `${supabaseUrl}/rest/v1/${tableName}?select=h3_id&h3_res=eq.${h3Res}&forecast_year=eq.${forecastYear}`
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

function analyzeNulls(records, fieldName) {
    let nullCount = 0
    let nanCount = 0
    let zeroCount = 0
    let validCount = 0

    for (const r of records) {
        const val = r[fieldName]
        if (val === null || val === undefined) {
            nullCount++
        } else if (typeof val === 'number' && isNaN(val)) {
            nanCount++
        } else if (val === 0) {
            zeroCount++
        } else if (typeof val === 'number' && isFinite(val)) {
            validCount++
        } else {
            // String "NaN" or other
            if (String(val).toLowerCase() === 'nan') nanCount++
            else validCount++
        }
    }

    return { nullCount, nanCount, zeroCount, validCount, total: records.length }
}

async function main() {
    let output = "=== Comprehensive Tile Data Audit ===\n\n"
    output += `Timestamp: ${new Date().toISOString()}\n`
    output += `Table: h3_precomputed_hex_details\n`
    output += `Forecast Year: 2026\n\n`

    const criticalFields = ['opportunity', 'reliability', 'lat', 'lng', 'property_count']

    for (const h3Res of [7, 8, 9, 10, 11]) {
        output += `\n=== H3 Resolution ${h3Res} ===\n`

        // Count total records
        const totalCount = await countRecords('h3_precomputed_hex_details', h3Res, 2026)
        output += `Total records: ${totalCount.toLocaleString()}\n`

        if (totalCount === 0) {
            output += `  ❌ NO DATA for this resolution!\n`
            continue
        }

        // Fetch sample to analyze nulls
        const sample = await fetchSample('h3_precomputed_hex_details', h3Res, 2026, 200)

        output += `Sample size: ${sample.length}\n\n`
        output += `Field analysis (from sample):\n`

        for (const field of criticalFields) {
            const analysis = analyzeNulls(sample, field)
            const pctValid = ((analysis.validCount / analysis.total) * 100).toFixed(1)
            const pctNull = ((analysis.nullCount / analysis.total) * 100).toFixed(1)
            const pctNaN = ((analysis.nanCount / analysis.total) * 100).toFixed(1)
            const pctZero = ((analysis.zeroCount / analysis.total) * 100).toFixed(1)

            let status = '✅'
            if (analysis.nullCount > 0 || analysis.nanCount > 0) status = '⚠️'
            if (analysis.validCount === 0) status = '❌'

            output += `  ${status} ${field}: valid=${pctValid}%, null=${pctNull}%, NaN=${pctNaN}%, zero=${pctZero}%\n`
        }

        // Check if any records have invalid coordinates
        const badCoords = sample.filter(r =>
            r.lat === null || r.lng === null ||
            !isFinite(r.lat) || !isFinite(r.lng) ||
            r.lat === 0 || r.lng === 0
        )
        if (badCoords.length > 0) {
            output += `  ❌ ${badCoords.length} records with invalid/missing coordinates!\n`
            output += `     Sample IDs: ${badCoords.slice(0, 3).map(r => r.h3_id).join(', ')}\n`
        }

        // Check opportunity/reliability ranges
        const badOpportunity = sample.filter(r =>
            r.opportunity === null || !isFinite(r.opportunity)
        )
        const badReliability = sample.filter(r =>
            r.reliability === null || !isFinite(r.reliability) || r.reliability < 0 || r.reliability > 1
        )

        if (badOpportunity.length > 0) {
            output += `  ⚠️ ${badOpportunity.length} records with invalid opportunity\n`
        }
        if (badReliability.length > 0) {
            output += `  ⚠️ ${badReliability.length} records with invalid reliability (outside 0-1)\n`
        }
    }

    // Also check what the UI filtering would reject
    output += `\n=== UI Filter Simulation ===\n`
    output += `The map-view.tsx filters out records where:\n`
    output += `  - isNaN(reliability) OR isNaN(opportunity) OR isNaN(lat) OR isNaN(lng) OR !h3_id\n\n`

    for (const h3Res of [7, 8, 9, 10]) {
        const sample = await fetchSample('h3_precomputed_hex_details', h3Res, 2026, 500)
        const valid = sample.filter(h =>
            !isNaN(h.reliability) &&
            !isNaN(h.opportunity) &&
            !isNaN(h.lat) &&
            !isNaN(h.lng) &&
            h.h3_id
        )
        const rejected = sample.length - valid.length
        output += `  Res ${h3Res}: ${valid.length}/${sample.length} pass UI filter (${rejected} rejected)\n`
    }

    console.log(output)
    fs.writeFileSync('scripts/tile-audit.txt', output)
    console.log("\nWritten to scripts/tile-audit.txt")
}

main().catch(console.error)
