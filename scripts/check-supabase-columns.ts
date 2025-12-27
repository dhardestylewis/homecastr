/**
 * Debug script to check what columns exist in Supabase tables
 * Run with: npx tsx scripts/check-supabase-columns.ts
 */

import { createClient } from "@supabase/supabase-js"

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
const supabaseKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!

async function main() {
    const supabase = createClient(supabaseUrl, supabaseKey)

    console.log("=== Checking h3_precomputed_hex_details ===")
    const { data: detailsSample, error: detailsError } = await supabase
        .from("h3_precomputed_hex_details")
        .select("*")
        .eq("forecast_year", 2026)
        .limit(1)
        .single()

    if (detailsError) {
        console.error("Details table error:", detailsError.message)
    } else if (detailsSample) {
        console.log("Details table columns:", Object.keys(detailsSample).sort().join(", "))
        console.log("\nSample row:")
        console.log(JSON.stringify(detailsSample, null, 2))
    }

    console.log("\n=== Checking h3_precomputed_hex_rows ===")
    const { data: rowsSample, error: rowsError } = await supabase
        .from("h3_precomputed_hex_rows")
        .select("*")
        .eq("forecast_year", 2026)
        .limit(1)
        .single()

    if (rowsError) {
        console.error("Rows table error:", rowsError.message)
    } else if (rowsSample) {
        console.log("Rows table columns:", Object.keys(rowsSample).sort().join(", "))
        console.log("\nSample row:")
        console.log(JSON.stringify(rowsSample, null, 2))
    }

    // Compare columns
    if (detailsSample && rowsSample) {
        const detailsCols = new Set(Object.keys(detailsSample))
        const rowsCols = new Set(Object.keys(rowsSample))

        const onlyInDetails = [...detailsCols].filter(c => !rowsCols.has(c))
        const onlyInRows = [...rowsCols].filter(c => !detailsCols.has(c))

        console.log("\n=== Column Comparison ===")
        console.log("Only in details:", onlyInDetails.join(", ") || "(none)")
        console.log("Only in rows:", onlyInRows.join(", ") || "(none)")
    }

    // Check expected columns from implementation plan
    console.log("\n=== Checking Expected Columns ===")
    const expectedColumns = [
        "accuracy_term", "confidence_term", "stability_term", "support_term",
        "predicted_value", "noi", "monthly_rent", "dscr", "cap_rate", "breakeven_occ",
        "risk_score", "score", "alert_pct",
        "fan_p10_y1", "fan_p50_y1", "fan_p90_y1"
    ]

    if (detailsSample) {
        const cols = Object.keys(detailsSample)
        for (const expected of expectedColumns) {
            const found = cols.includes(expected)
            console.log(`  ${found ? "✅" : "❌"} ${expected}`)
        }
    }
}

main().catch(console.error)
