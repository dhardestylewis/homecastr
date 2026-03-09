import { loadEnvConfig } from '@next/env'
loadEnvConfig(process.cwd())

import { getSupabaseAdmin } from "./lib/supabase/admin"
import * as fs from 'fs'

const log: string[] = []
function L(msg: string) { log.push(msg) }

async function main() {
    const SCHEMA = process.env.FORECAST_SCHEMA || "forecast_queue"
    L("SCHEMA: " + SCHEMA)
    const supabase = getSupabaseAdmin()

    // Test tract 0611570025R (Yuba CA)
    const tractGeoid = "0611570025R"

    L(`\n=== History for ${tractGeoid} ===`)
    const { data: histData, error } = await supabase
        .schema(SCHEMA as any)
        .from("metrics_tract_history")
        .select("year, value, p50")
        .eq("tract_geoid20", tractGeoid)
        .gte("year", 2015)
        .lte("year", 2025)
        .order("year", { ascending: true })

    L(`Error: ${error ? JSON.stringify(error) : "none"}`)
    L(`Rows: ${histData?.length || 0}`)
    if (histData && histData.length > 0) {
        for (const r of histData) L(`  year=${r.year} value=${r.value} p50=${r.p50}`)
    }

    // Check total count in history table
    L(`\n=== Overall metrics_tract_history stats ===`)
    const { count: totalHist } = await supabase
        .schema(SCHEMA as any)
        .from("metrics_tract_history")
        .select('*', { count: 'exact', head: true })
    L(`Total rows in metrics_tract_history: ${totalHist}`)

    // Sample some rows
    const { data: sampleData } = await supabase
        .schema(SCHEMA as any)
        .from("metrics_tract_history")
        .select("tract_geoid20, year, value, p50")
        .limit(10)
    L(`\nSample rows:`)
    for (const r of (sampleData || [])) {
        L(`  tract=${r.tract_geoid20} year=${r.year} value=${r.value} p50=${r.p50}`)
    }

    // Check a standard tract that worked before (e.g. Houston area)
    const houstonTract = "4820110100"
    L(`\n=== History for Houston tract ${houstonTract} ===`)
    const { data: houstData } = await supabase
        .schema(SCHEMA as any)
        .from("metrics_tract_history")
        .select("year, value, p50")
        .eq("tract_geoid20", houstonTract)
        .gte("year", 2015)
        .lte("year", 2025)
        .order("year", { ascending: true })
    L(`Rows: ${houstData?.length || 0}`)
    for (const r of (houstData || [])) L(`  year=${r.year} value=${r.value} p50=${r.p50}`)

    // Check if the history table even exists in this schema
    L(`\n=== Checking table in public schema ===`)
    const { data: pubHist, error: pubErr } = await supabase
        .from("metrics_tract_history")
        .select("tract_geoid20, year, value")
        .limit(5)
    L(`Public schema error: ${pubErr ? JSON.stringify(pubErr) : "none"}`)
    L(`Public schema rows: ${pubHist?.length || 0}`)
    for (const r of (pubHist || [])) L(`  tract=${r.tract_geoid20} year=${r.year} value=${r.value}`)

    fs.writeFileSync("test_history_output.txt", log.join("\n"), "utf-8")
    console.log("Done. See test_history_output.txt")
}

main().catch(console.error)
