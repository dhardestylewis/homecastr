import { NextResponse } from "next/server"
import { getSupabaseAdmin } from "@/lib/supabase/admin"

/**
 * Fetch ALL origin years of forecast data for a single geography,
 * plus historical actuals. Used by the methodology page backtest chart.
 *
 * Query params:
 *   level   = zcta | tract | parcel (default: zcta)
 *   id      = geography key (e.g. "77079" for zcta)
 *   schema  = supabase schema (default: forecast_queue)
 *
 * Returns:
 *   { historicalValues: number[], vintages: { [originYear]: { p10, p50, p90 } } }
 */

const LEVEL_TABLE: Record<string, { table: string; key: string }> = {
    zcta: { table: "metrics_zcta_forecast", key: "zcta5" },
    tract: { table: "metrics_tract_forecast", key: "tract_geoid20" },
    parcel: { table: "metrics_parcel_forecast", key: "acct" },
}

const HISTORY_TABLE: Record<string, { table: string; key: string }> = {
    zcta: { table: "metrics_zcta_history", key: "zcta5" },
    tract: { table: "metrics_tract_history", key: "tract_geoid20" },
    parcel: { table: "metrics_parcel_history", key: "acct" },
}

export async function GET(request: Request) {
    const { searchParams } = new URL(request.url)
    const level = searchParams.get("level") || "zcta"
    const id = searchParams.get("id")
    const schemaName = searchParams.get("schema") || "forecast_queue"

    if (!id) {
        return NextResponse.json({ error: "id is required" }, { status: 400 })
    }

    const meta = LEVEL_TABLE[level]
    const histMeta = HISTORY_TABLE[level]
    if (!meta || !histMeta) {
        return NextResponse.json({ error: `Unknown level: ${level}` }, { status: 400 })
    }

    const supabase = getSupabaseAdmin()

    try {
        // 1. Fetch ALL forecast rows for this geography (all origin years)
        const { data: forecastData, error: fErr } = await supabase
            .schema(schemaName as any)
            .from(meta.table)
            .select("horizon_m, p10, p50, p90, origin_year")
            .eq(meta.key, id)
            .order("origin_year", { ascending: true })
            .order("horizon_m", { ascending: true })

        if (fErr) {
            return NextResponse.json({ error: fErr.message }, { status: 500 })
        }

        // 2. Fetch historical actuals (2019–2025)
        const { data: histData, error: hErr } = await supabase
            .schema(schemaName as any)
            .from(histMeta.table)
            .select("year, value, p50")
            .eq(histMeta.key, id)
            .gte("year", 2015)
            .lte("year", 2025)
            .order("year", { ascending: true })

        // Build historical array (2015–2025 = 11 slots)
        const histMap = new Map<number, number>()
        if (!hErr && histData) {
            for (const row of histData as any[]) {
                const val = row.p50 ?? row.value
                if (val != null) histMap.set(row.year, val)
            }
        }
        const histYears = Array.from({ length: 11 }, (_, i) => 2015 + i)
        const historicalValues = histYears.map(yr => histMap.get(yr) ?? null)

        // 3. Group forecasts by origin_year into vintages
        const vintages: Record<number, { years: number[]; p10: number[]; p50: number[]; p90: number[] }> = {}
        if (forecastData) {
            for (const row of forecastData as any[]) {
                const oy = row.origin_year
                if (!vintages[oy]) {
                    vintages[oy] = { years: [], p10: [], p50: [], p90: [] }
                }
                const targetYear = oy + row.horizon_m / 12
                vintages[oy].years.push(targetYear)
                vintages[oy].p10.push(row.p10 ?? 0)
                vintages[oy].p50.push(row.p50 ?? 0)
                vintages[oy].p90.push(row.p90 ?? 0)
            }
        }

        // 4. Filter out degenerate vintages (broken checkpoints)
        // Drop any vintage where any p50 exceeds 5× the max historical value
        const validHist = (historicalValues as (number | null)[]).filter(
            (v): v is number => v != null && Number.isFinite(v) && v > 0
        )
        const histMax = validHist.length > 0 ? Math.max(...validHist) : Infinity
        const ceiling = histMax * 5
        for (const oy of Object.keys(vintages).map(Number)) {
            const v = vintages[oy]
            const hasDegenerate = v.p50.some(p => p > ceiling) || v.p90.some(p => p > ceiling * 2)
            if (hasDegenerate) {
                console.log(`[BACKTEST-COVERAGE] Dropping degenerate vintage o=${oy} (predictions exceed ${ceiling.toFixed(0)})`)
                delete vintages[oy]
            }
        }

        return NextResponse.json({
            historicalYears: histYears,
            historicalValues,
            vintages,
        }, {
            headers: { "Cache-Control": "public, max-age=3600" },
        })
    } catch (e: any) {
        console.error("[BACKTEST-COVERAGE] Error:", e)
        return NextResponse.json({ error: e.message }, { status: 500 })
    }
}
