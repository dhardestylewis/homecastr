import { NextResponse } from "next/server"
import { getSupabaseAdmin } from "@/lib/supabase/admin"

/**
 * Returns global percentile breakpoints for forecast p50 values AND
 * growth_pct distributions at each geographic level.
 *
 * Used to calibrate the map color ramp to the actual data distribution.
 *
 * GET /api/forecast-stats?originYear=2024&horizonM=60
 * GET /api/forecast-stats?originYear=2024&horizonM=60&mode=growth
 *
 * mode=growth returns growth_pct percentiles per geo level:
 *   { levels: { state: { p5, p10, p25, p50, p75, p90, p95 }, zcta: {...}, ... } }
 */
export async function GET(request: Request) {
    const { searchParams } = new URL(request.url)
    const originYear = parseInt(searchParams.get("originYear") || "2024")
    const horizonM = parseInt(searchParams.get("horizonM") || "12")
    const schemaName = searchParams.get("schema") || "forecast_queue"
    const mode = searchParams.get("mode") || "value"

    const supabase = getSupabaseAdmin()

    try {
        if (mode === "growth") {
            // Compute growth_pct percentiles per geo level
            // growth_pct = (p50_at_horizon - p50_at_h12) / p50_at_h12 * 100
            // Levels match tile zoom routing: z<=7→zcta, z<=11→tract, z<=16→tabblock
            const levels = [
                { name: "zcta", table: "metrics_zcta_forecast", key: "zcta5" },
                { name: "tract", table: "metrics_tract_forecast", key: "tract_geoid20" },
                { name: "tabblock", table: "metrics_tabblock_forecast", key: "tabblock_geoid20" },
            ]

            const result: Record<string, any> = {}
            const pct = (sorted: number[], p: number) => {
                if (sorted.length === 0) return null
                const idx = Math.floor(p * sorted.length)
                return sorted[Math.min(idx, sorted.length - 1)]
            }

            for (const level of levels) {
                try {
                    // Get p50 at requested horizon and at baseline (h=12)
                    const [horizonRes, baselineRes] = await Promise.all([
                        supabase
                            .schema(schemaName as any)
                            .from(level.table)
                            .select(`${level.key}, p50`)
                            .eq("origin_year", originYear)
                            .eq("horizon_m", horizonM)
                            .eq("series_kind", "forecast")
                            .not("p50", "is", null),
                        supabase
                            .schema(schemaName as any)
                            .from(level.table)
                            .select(`${level.key}, p50`)
                            .eq("origin_year", originYear)
                            .eq("horizon_m", 24) // baseline = 2026 for origin 2024 (origin + 24m)
                            .eq("series_kind", "forecast")
                            .not("p50", "is", null),
                    ])

                    if (horizonRes.error || baselineRes.error) {
                        console.warn(`[FORECAST-STATS] Error for ${level.name}:`, horizonRes.error || baselineRes.error)
                        result[level.name] = null
                        continue
                    }

                    // Build baseline lookup
                    const baselineMap = new Map<string, number>()
                    for (const row of (baselineRes.data || [])) {
                        baselineMap.set((row as any)[level.key], row.p50)
                    }

                    // Compute growth_pct for each feature
                    const growthValues: number[] = []
                    for (const row of (horizonRes.data || [])) {
                        const key = (row as any)[level.key]
                        const baseline = baselineMap.get(key)
                        if (baseline && baseline > 0 && row.p50) {
                            const growth = ((row.p50 - baseline) / baseline) * 100
                            // Clip extreme outliers
                            if (growth >= -100 && growth <= 500) {
                                growthValues.push(Math.round(growth * 10) / 10)
                            }
                        }
                    }

                    growthValues.sort((a, b) => a - b)

                    result[level.name] = {
                        count: growthValues.length,
                        p5: pct(growthValues, 0.05),
                        p10: pct(growthValues, 0.10),
                        p25: pct(growthValues, 0.25),
                        p50: pct(growthValues, 0.50),
                        p75: pct(growthValues, 0.75),
                        p90: pct(growthValues, 0.90),
                        p95: pct(growthValues, 0.95),
                        min: growthValues[0] ?? null,
                        max: growthValues[growthValues.length - 1] ?? null,
                    }
                } catch (e: any) {
                    console.warn(`[FORECAST-STATS] Error computing ${level.name} growth:`, e.message)
                    result[level.name] = null
                }
            }

            return NextResponse.json(
                { levels: result, originYear, horizonM, mode: "growth" },
                { headers: { "Cache-Control": "public, max-age=86400" } }
            )
        }

        // Default: absolute value percentiles (original behavior)
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
