import { NextResponse } from "next/server"
import { getSupabaseAdmin } from "@/lib/supabase/admin"

/**
 * Returns global percentile breakpoints for forecast p50 values.
 * Queries the tract-level forecast table (good middle ground between
 * granularity and speed) and computes p5/p10/p25/p50/p75/p90/p95.
 *
 * Used to calibrate the map color ramp to the actual data distribution.
 *
 * GET /api/forecast-stats?originYear=2025&horizonM=12
 */
export async function GET(request: Request) {
    const { searchParams } = new URL(request.url)
    const originYear = parseInt(searchParams.get("originYear") || "2025")
    const horizonM = parseInt(searchParams.get("horizonM") || "12")
    const schemaName = searchParams.get("schema") || "forecast_20260220_7f31c6e4"

    const supabase = getSupabaseAdmin()

    try {
        // Query all p50 values at tract level for the given horizon
        // Tract is a good level: enough rows for statistics, fast enough to aggregate
        const { data, error } = await supabase
            .schema(schemaName as any)
            .from("metrics_tract_forecast")
            .select("p50")
            .eq("origin_year", originYear)
            .eq("horizon_m", horizonM)
            .not("p50", "is", null)
            .order("p50", { ascending: true })

        if (error) {
            console.error("[FORECAST-STATS] Query error:", error)
            return NextResponse.json({ error: error.message }, { status: 500 })
        }

        if (!data || data.length === 0) {
            return NextResponse.json({ error: "No data found" }, { status: 404 })
        }

        const values = data.map((r: any) => r.p50 as number).filter((v: number) => v > 0)
        values.sort((a: number, b: number) => a - b)

        const pct = (p: number) => {
            const idx = Math.floor(p * values.length)
            return values[Math.min(idx, values.length - 1)]
        }

        const stats = {
            count: values.length,
            min: values[0],
            p5: pct(0.05),
            p10: pct(0.10),
            p25: pct(0.25),
            p50: pct(0.50),
            p75: pct(0.75),
            p90: pct(0.90),
            p95: pct(0.95),
            max: values[values.length - 1],
            originYear,
            horizonM,
        }

        return NextResponse.json(stats, {
            headers: { "Cache-Control": "public, max-age=86400" },
        })
    } catch (e: any) {
        console.error("[FORECAST-STATS] Error:", e)
        return NextResponse.json({ error: e.message }, { status: 500 })
    }
}
