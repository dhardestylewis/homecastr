import { NextResponse } from "next/server"
import { getSupabaseAdmin } from "@/lib/supabase/admin"

/**
 * Fetch ALL horizons for a given geography ID so the tooltip can render a FanChart.
 * Also fetches historical values (2019-2025) from the history table.
 *
 * Query params:
 *   level   = zcta | tract | tabblock | parcel
 *   id      = the geography key (e.g. "77079" for zcta, "48201..." for tract)
 *   originYear (default 2025)
 *
 * Returns { years, p10, p25, p50, p75, p90, y_med, historicalValues } arrays.
 */

const LEVEL_TABLE: Record<string, { table: string; key: string }> = {
    zip3: { table: "metrics_zip3_forecast", key: "zip3" },
    zcta: { table: "metrics_zcta_forecast", key: "zcta5" },
    tract: { table: "metrics_tract_forecast", key: "tract_geoid20" },
    tabblock: { table: "metrics_tabblock_forecast", key: "tabblock_geoid20" },
    parcel: { table: "metrics_parcel_forecast", key: "acct" },
}

const HISTORY_TABLE: Record<string, { table: string; key: string }> = {
    zip3: { table: "metrics_zip3_history", key: "zip3" },
    zcta: { table: "metrics_zcta_history", key: "zcta5" },
    tract: { table: "metrics_tract_history", key: "tract_geoid20" },
    tabblock: { table: "metrics_tabblock_history", key: "tabblock_geoid20" },
    parcel: { table: "metrics_parcel_history", key: "acct" },
}

export async function GET(request: Request) {
    const { searchParams } = new URL(request.url)
    const levelStr = searchParams.get("level") || "zcta"
    const level = levelStr === "zip" ? "zcta" : levelStr
    const id = searchParams.get("id")

    if (!id) {
        return NextResponse.json({ error: "id is required" }, { status: 400 })
    }

    const meta = LEVEL_TABLE[level]
    if (!meta) {
        return NextResponse.json({ error: `Unknown level: ${level}` }, { status: 400 })
    }

    const supabase = getSupabaseAdmin()

    try {
        // --- 1. Fetch forecast data for the requested origin year ---
        const originYear = parseInt(searchParams.get("originYear") || "2025")

        const { data, error } = await supabase
            .schema("forecast_20260220_7f31c6e4" as any)
            .from(meta.table)
            .select("horizon_m, p10, p25, p50, p75, p90, origin_year")
            .eq(meta.key, id)
            .eq("origin_year", originYear)
            .order("horizon_m", { ascending: true })

        if (error) {
            console.error("[FORECAST-DETAIL] Forecast error:", error)
            return NextResponse.json({ error: error.message }, { status: 500 })
        }

        // --- Fetch historical data (years 2019-2025) ---
        let historicalValues: (number | null)[] = [null, null, null, null, null, null, null]

        const histMeta = HISTORY_TABLE[level]
        if (histMeta) {
            try {
                const { data: histData, error: histError } = await supabase
                    .schema("forecast_20260220_7f31c6e4" as any)
                    .from(histMeta.table)
                    .select("year, value, p50")
                    .eq(histMeta.key, id)
                    .gte("year", 2019)
                    .lte("year", 2025)
                    .order("year", { ascending: true })

                if (!histError && histData && histData.length > 0) {
                    // Map into fixed 7-slot array [2019, 2020, ..., 2025]
                    const histMap = new Map<number, number>()
                    for (const row of histData as any[]) {
                        const val = row.p50 ?? row.value
                        if (val != null) {
                            histMap.set(row.year, val)
                        }
                    }

                    // Backfill 2024/2025 from forecast data if history table is missing them.
                    // ACS origin_year=2024 forecasted 2025 at horizon_m=12 — now past, treat as history.
                    if (data && data.length > 0) {
                        for (const row of data as any[]) {
                            const forecastYear = row.origin_year + row.horizon_m / 12
                            // Only backfill years that are in the past (≤2025) and missing from history
                            if (forecastYear <= 2025 && !histMap.has(forecastYear)) {
                                histMap.set(forecastYear, row.p50 ?? 0)
                            }
                        }
                    }

                    historicalValues = [2019, 2020, 2021, 2022, 2023, 2024, 2025].map(
                        (yr) => histMap.get(yr) ?? null
                    )
                }
            } catch (histErr) {
                // Non-fatal: just skip history if table doesn't exist yet
                console.warn("[FORECAST-DETAIL] History fetch warning:", histErr)
            }
        }

        // --- Build response ---
        // Group available forecast streams by their origin_year
        const forecastVariants: Record<number, any> = {}

        if (data && data.length > 0) {
            const yearsSet = new Set(data.map(d => d.origin_year))
            for (const oy of Array.from(yearsSet) as number[]) {
                const oyData = data.filter(d => d.origin_year === oy)
                forecastVariants[oy] = {
                    years: oyData.map((r: any) => oy + r.horizon_m / 12),
                    p10: oyData.map((r: any) => r.p10 ?? 0),
                    p25: oyData.map((r: any) => r.p25 ?? 0),
                    p50: oyData.map((r: any) => r.p50 ?? 0),
                    p75: oyData.map((r: any) => r.p75 ?? 0),
                    p90: oyData.map((r: any) => r.p90 ?? 0),
                    y_med: oyData.map((r: any) => r.p50 ?? 0)
                }
            }
        }

        // Return structured payload
        return NextResponse.json({
            historicalValues,
            forecastVariants, // Contains {2024: {...bands}, 2025: {...bands}} arrays dynamically

            // Provide a legacy flattened fallback layout using the latest year so un-migrated frontend components don't instantly crash
            years: forecastVariants[2025]?.years || forecastVariants[2024]?.years || [],
            p10: forecastVariants[2025]?.p10 || forecastVariants[2024]?.p10 || [],
            p25: forecastVariants[2025]?.p25 || forecastVariants[2024]?.p25 || [],
            p50: forecastVariants[2025]?.p50 || forecastVariants[2024]?.p50 || [],
            p75: forecastVariants[2025]?.p75 || forecastVariants[2024]?.p75 || [],
            p90: forecastVariants[2025]?.p90 || forecastVariants[2024]?.p90 || [],
            y_med: forecastVariants[2025]?.y_med || forecastVariants[2024]?.y_med || [],
        }, {
            headers: {
                "Cache-Control": "public, max-age=3600",
            },
        })
    } catch (e: any) {
        console.error("[FORECAST-DETAIL] Error:", e)
        return NextResponse.json({ error: e.message }, { status: 500 })
    }
}
