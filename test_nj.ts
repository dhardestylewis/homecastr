import { loadEnvConfig } from '@next/env'
loadEnvConfig(process.cwd())

import { getSupabaseAdmin } from "./lib/supabase/admin"
import * as fs from 'fs'

async function fetchAllRows(queryBuilder: () => any, pageSize = 1000): Promise<any[]> {
    const all: any[] = []
    let offset = 0
    while (true) {
        const { data, error } = await queryBuilder().range(offset, offset + pageSize - 1)
        if (error || !data || data.length === 0) break
        all.push(...data)
        if (data.length < pageSize) break
        offset += pageSize
    }
    return all
}

const log: string[] = []
function L(msg: string) { log.push(msg) }

async function main() {
    const stateFips = "41" // Oregon
    const SCHEMA = process.env.FORECAST_SCHEMA || "forecast_queue"
    const supabase = getSupabaseAdmin()

    L("=== Oregon (41) diagnostic ===")

    // 1. Distinct origin_years
    const allRows = await fetchAllRows(() =>
        supabase
            .schema(SCHEMA as any)
            .from("metrics_tract_forecast")
            .select("tract_geoid20, origin_year")
            .gte("tract_geoid20", stateFips)
            .lt("tract_geoid20", stateFips + "z")
            .eq("horizon_m", 12)
            .eq("series_kind", "forecast")
            .not("p50", "is", null)
            .order("tract_geoid20")
    )

    const years = new Set(allRows.map((r: any) => r.origin_year))
    L(`Distinct origin_years: ${JSON.stringify([...years])}`)

    // 2. Group by county FIPS per year
    for (const y of [...years].sort()) {
        const yRows = allRows.filter((r: any) => r.origin_year === y)
        const counties = new Map<string, number>()
        for (const r of yRows) {
            const c = r.tract_geoid20.substring(0, 5)
            counties.set(c, (counties.get(c) || 0) + 1)
        }
        L(`\nYear ${y}: ${yRows.length} tracts, ${counties.size} counties`)
        const sorted = [...counties.entries()].sort((a, b) => b[1] - a[1])
        for (const [fips, count] of sorted) {
            L(`  ${fips}: ${count} tracts`)
        }
    }

    // 3. Simulate FIXED getCountyOutlooks
    L("\n=== FIXED getCountyOutlooks simulation ===")
    const [h12Rows, h60Rows] = await Promise.all([
        fetchAllRows(() =>
            supabase
                .schema(SCHEMA as any)
                .from("metrics_tract_forecast")
                .select("tract_geoid20, origin_year, p50")
                .gte("tract_geoid20", stateFips)
                .lt("tract_geoid20", stateFips + "z")
                .eq("horizon_m", 12)
                .eq("series_kind", "forecast")
                .not("p50", "is", null)
                .order("tract_geoid20")
        ),
        fetchAllRows(() =>
            supabase
                .schema(SCHEMA as any)
                .from("metrics_tract_forecast")
                .select("tract_geoid20, origin_year, p50")
                .gte("tract_geoid20", stateFips)
                .lt("tract_geoid20", stateFips + "z")
                .eq("horizon_m", 60)
                .eq("series_kind", "forecast")
                .not("p50", "is", null)
                .order("tract_geoid20")
        ),
    ])

    L(`h12Rows total: ${h12Rows.length}, h60Rows total: ${h60Rows.length}`)

    // Dedup: keep latest origin_year per tract
    const h12Map = new Map<string, { year: number, p50: number }>()
    for (const row of h12Rows) {
        const existing = h12Map.get(row.tract_geoid20)
        if (!existing || row.origin_year > existing.year) {
            h12Map.set(row.tract_geoid20, { year: row.origin_year, p50: row.p50 })
        }
    }

    const h60Map = new Map<string, { year: number, p50: number }>()
    for (const row of h60Rows) {
        const existing = h60Map.get(row.tract_geoid20)
        if (!existing || row.origin_year > existing.year) {
            h60Map.set(row.tract_geoid20, { year: row.origin_year, p50: row.p50 })
        }
    }

    L(`Unique tracts in h12: ${h12Map.size}, h60: ${h60Map.size}`)

    // Check year distribution after dedup
    const yearDist = new Map<number, number>()
    for (const [, v] of h12Map) {
        yearDist.set(v.year, (yearDist.get(v.year) || 0) + 1)
    }
    L(`h12 year distribution after dedup: ${JSON.stringify([...yearDist.entries()])}`)

    // Compute county outlooks
    const countyData = new Map<string, { appreciations: number[]; values: number[] }>()
    let skippedYearMismatch = 0
    let skippedNoH60 = 0
    let skippedFilter = 0

    for (const [tractId, h12Data] of h12Map) {
        const h60Data = h60Map.get(tractId)
        const countyFips = tractId.substring(0, 5)
        if (!h60Data) { skippedNoH60++; continue }
        if (h12Data.year !== h60Data.year) { skippedYearMismatch++; continue }
        const h12 = h12Data.p50
        const h60 = h60Data.p50
        if (h12 >= 20_000 && h60 && h12 < 5_000_000) {
            const appr = ((h60 - h12) / h12) * 100
            if (appr > -95 && appr <= 300) {
                if (!countyData.has(countyFips)) countyData.set(countyFips, { appreciations: [], values: [] })
                const cd = countyData.get(countyFips)!
                cd.appreciations.push(appr)
                cd.values.push(h12)
            } else { skippedFilter++ }
        } else { skippedFilter++ }
    }

    L(`\nSkipped: noH60=${skippedNoH60}, yearMismatch=${skippedYearMismatch}, filter=${skippedFilter}`)
    L(`Counties with outlook data: ${countyData.size}`)

    const sortedCounties = [...countyData.entries()].sort((a, b) => a[0].localeCompare(b[0]))
    for (const [fips, data] of sortedCounties) {
        L(`  ${fips}: ${data.appreciations.length} tracts`)
    }

    // Standard Oregon counties
    const orStdCounties = [
        "41001", "41003", "41005", "41007", "41009", "41011", "41013", "41015",
        "41017", "41019", "41021", "41023", "41025", "41027", "41029", "41031",
        "41033", "41035", "41037", "41039", "41041", "41043", "41045", "41047",
        "41049", "41051", "41053", "41055", "41057", "41059", "41061", "41063",
        "41065", "41067", "41069", "41071"
    ]
    L("\n=== Standard county coverage ===")
    let covered = 0
    for (const fips of orStdCounties) {
        const has = countyData.has(fips)
        if (has) covered++
        else L(`  MISSING: ${fips}`)
    }
    L(`Covered: ${covered}/${orStdCounties.length}`)

    fs.writeFileSync("test_output.txt", log.join("\n"), "utf-8")
    console.log("Wrote test_output.txt")
}

main().catch(console.error)
