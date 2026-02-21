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
    zcta: { table: "metrics_zcta_forecast", key: "zcta5" },
    tract: { table: "metrics_tract_forecast", key: "tract_geoid20" },
    tabblock: { table: "metrics_tabblock_forecast", key: "tabblock_geoid20" },
    parcel: { table: "metrics_parcel_forecast", key: "acct" },
}

const HISTORY_TABLE: Record<string, { table: string; key: string }> = {
    zcta: { table: "metrics_zcta_history", key: "zcta5" },
    tract: { table: "metrics_tract_history", key: "tract_geoid20" },
    tabblock: { table: "metrics_tabblock_history", key: "tabblock_geoid20" },
    parcel: { table: "metrics_parcel_history", key: "acct" },
}

export async function GET(request: Request) {
    const { searchParams } = new URL(request.url)
    const level = searchParams.get("level") || "zcta"
    const id = searchParams.get("id")
    const originYear = parseInt(searchParams.get("originYear") || "2025")

    if (!id) {
        return NextResponse.json({ error: "id is required" }, { status: 400 })
    }

    const meta = LEVEL_TABLE[level]
    if (!meta) {
        return NextResponse.json({ error: `Unknown level: ${level}` }, { status: 400 })
    }

    const supabase = getSupabaseAdmin()

    try {
        // --- Fetch forecast data ---
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
        if (!data || data.length === 0) {
            return NextResponse.json({
                years: [], p10: [], p25: [], p50: [], p75: [], p90: [], y_med: [],
                historicalValues,
            })
        }

        const years = data.map((r: any) => originYear + r.horizon_m / 12)
        const p10 = data.map((r: any) => r.p10 ?? 0)
        const p25 = data.map((r: any) => r.p25 ?? 0)
        const p50 = data.map((r: any) => r.p50 ?? 0)
        const p75 = data.map((r: any) => r.p75 ?? 0)
        const p90 = data.map((r: any) => r.p90 ?? 0)

        return NextResponse.json({
            years,
            p10,
            p25,
            p50,
            p75,
            p90,
            y_med: p50,
            historicalValues,
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
