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
    state: { table: "metrics_state_forecast", key: "state_fips" },
    zip3: { table: "metrics_zip3_forecast", key: "zip3" },
    zcta: { table: "metrics_zcta_forecast", key: "zcta5" },
    tract: { table: "metrics_tract_forecast", key: "tract_geoid20" },
    tabblock: { table: "metrics_tabblock_forecast", key: "tabblock_geoid20" },
    parcel: { table: "metrics_parcel_forecast", key: "acct" },
}

const HISTORY_TABLE: Record<string, { table: string; key: string }> = {
    state: { table: "metrics_state_history", key: "state_fips" },
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
    const schemaName = searchParams.get("schema") || "forecast_queue"

    if (!id) {
        return NextResponse.json({ error: "id is required" }, { status: 400 })
    }

    const meta = LEVEL_TABLE[level]
    if (!meta) {
        return NextResponse.json({ error: `Unknown level: ${level}` }, { status: 400 })
    }

    const supabase = getSupabaseAdmin()

    try {
        // --- 1. Fetch forecast data for the requested origin year, with fallback ---
        const originYear = parseInt(searchParams.get("originYear") || "2025")
        const fallbackYear = originYear === 2025 ? 2024 : 2025

        let { data, error } = await supabase
            .schema(schemaName as any)
            .from(meta.table)
            .select("horizon_m, p10, p25, p50, p75, p90, origin_year")
            .eq(meta.key, id)
            .eq("origin_year", originYear)
            .order("horizon_m", { ascending: true })

        // For state level: if the primary query fails or returns empty, try alternate column names
        // The metrics_state_forecast table has had column name changes (origin_year↔forecast_year, horizon_m↔horizon_months)
        if (level === "state" && (error || !data || data.length === 0)) {
            console.log(`[FORECAST-DETAIL] State primary query ${error ? 'errored' : 'empty'} for id=${id}, originYear=${originYear}. Trying alternate columns...`)
            const altResult = await supabase
                .schema(schemaName as any)
                .from(meta.table)
                .select("*")
                .eq(meta.key, id)
                .limit(5)
            if (!altResult.error && altResult.data && altResult.data.length > 0) {
                const sample = altResult.data[0]
                const cols = Object.keys(sample)
                console.log(`[FORECAST-DETAIL] State table columns: ${cols.join(', ')}. Sample row origin check: origin_year=${sample.origin_year}, forecast_year=${(sample as any).forecast_year}`)
                // Try with forecast_year column if origin_year doesn't exist
                if (cols.includes('forecast_year') && !cols.includes('origin_year')) {
                    const altQuery = await supabase
                        .schema(schemaName as any)
                        .from(meta.table)
                        .select("horizon_m, p10, p25, p50, p75, p90, forecast_year")
                        .eq(meta.key, id)
                        .eq("forecast_year", originYear)
                        .order("horizon_m", { ascending: true })
                    if (!altQuery.error && altQuery.data && altQuery.data.length > 0) {
                        // Map forecast_year → origin_year for downstream compatibility
                        data = altQuery.data.map((r: any) => ({ ...r, origin_year: r.forecast_year }))
                        error = null
                        console.log(`[FORECAST-DETAIL] State fallback with forecast_year succeeded: ${data!.length} rows`)
                    }
                }
            } else {
                console.log(`[FORECAST-DETAIL] State table sample query also ${altResult.error ? 'errored: ' + altResult.error.message : 'empty'}`)
            }
        }

        // Fallback to alternate origin year if no data found
        if (!error && (!data || data.length === 0)) {
            const fb = await supabase
                .schema(schemaName as any)
                .from(meta.table)
                .select("horizon_m, p10, p25, p50, p75, p90, origin_year")
                .eq(meta.key, id)
                .eq("origin_year", fallbackYear)
                .order("horizon_m", { ascending: true })
            if (!fb.error && fb.data && fb.data.length > 0) {
                data = fb.data
            }
            // State fallback: also try forecast_year for the fallback year
            if (level === "state" && (!data || data.length === 0)) {
                const fbAlt = await supabase
                    .schema(schemaName as any)
                    .from(meta.table)
                    .select("horizon_m, p10, p25, p50, p75, p90, forecast_year")
                    .eq(meta.key, id)
                    .eq("forecast_year", fallbackYear)
                    .order("horizon_m", { ascending: true })
                if (!fbAlt.error && fbAlt.data && fbAlt.data.length > 0) {
                    data = fbAlt.data.map((r: any) => ({ ...r, origin_year: r.forecast_year }))
                    error = null
                }
            }
        }

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
                    .schema(schemaName as any)
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

        // Provide a legacy flattened fallback layout — ONLY forecast years (2026+, h>=24)
        // so fan chart p50[0] = 2026 forecast, not 2024 ACS baseline
        const primaryOy = forecastVariants[2025] || forecastVariants[2024]
        const forecastOnly = primaryOy ? (() => {
            const idxStart = primaryOy.years.findIndex((yr: number) => yr >= 2026)
            if (idxStart < 0) return primaryOy
            return {
                years: primaryOy.years.slice(idxStart),
                p10: primaryOy.p10.slice(idxStart),
                p25: primaryOy.p25.slice(idxStart),
                p50: primaryOy.p50.slice(idxStart),
                p75: primaryOy.p75.slice(idxStart),
                p90: primaryOy.p90.slice(idxStart),
                y_med: primaryOy.y_med.slice(idxStart),
            }
        })() : null
        const empty: number[] = []
        return NextResponse.json({
            historicalValues,
            forecastVariants,
            years: forecastOnly?.years || empty,
            p10: forecastOnly?.p10 || empty,
            p25: forecastOnly?.p25 || empty,
            p50: forecastOnly?.p50 || empty,
            p75: forecastOnly?.p75 || empty,
            p90: forecastOnly?.p90 || empty,
            y_med: forecastOnly?.y_med || empty,
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
