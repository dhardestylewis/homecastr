import { getSupabaseAdmin } from "@/lib/supabase/admin"
import { withRedisCache } from "@/lib/redis"

const SCHEMA = process.env.FORECAST_SCHEMA || "forecast_queue"

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface ForecastHorizon {
    horizon_m: number
    forecastYear: number
    p10: number
    p25: number
    p50: number
    p75: number
    p90: number
    spread: number       // p90 - p10
    appreciation: number // ((p50 / baselineP50) - 1) * 100
}

export interface HistoryPoint {
    year: number
    value: number
}

export interface ComparableTract {
    tractGeoid: string
    name: string
    p50_12m: number
    p50_60m: number
    appreciation_60m: number
    spread_60m: number
    url: string
}

export interface Rankings {
    metroRank: number
    metroTotal: number
    nationalPercentile: number
}

export interface ForecastPageData {
    forecast: {
        originYear: number
        horizons: ForecastHorizon[]
        baselineP50: number   // p50 at horizon_m=12 (current year value)
    }
    history: HistoryPoint[]
    comparables: {
        similar: ComparableTract[]
        higherUpside: ComparableTract[]
        lowerRisk: ComparableTract[]
    }
    rankings: Rankings
    uniqueDataTokens: number
}

// ---------------------------------------------------------------------------
// Main data fetcher
// ---------------------------------------------------------------------------

export async function fetchForecastPageData(
    tractGeoid: string,
    originYear = 2025,
    schema = SCHEMA
): Promise<ForecastPageData | null> {
    return withRedisCache(`forecast_page:${tractGeoid}:${originYear}:${schema}`, async () => {
        const supabase = getSupabaseAdmin()

        // --- 1. Fetch all forecast horizons ---
        let { data: forecastData, error } = await supabase
            .schema(schema as any)
            .from("metrics_tract_forecast")
            .select("horizon_m, p10, p25, p50, p75, p90, origin_year")
            .eq("tract_geoid20", tractGeoid)
            .eq("origin_year", originYear)
            .eq("series_kind", "forecast")
            .order("horizon_m", { ascending: true })

        // Fallback origin year
        if (!error && (!forecastData || forecastData.length === 0)) {
            const fallback = originYear === 2025 ? 2024 : 2025
            const fb = await supabase
                .schema(schema as any)
                .from("metrics_tract_forecast")
                .select("horizon_m, p10, p25, p50, p75, p90, origin_year")
                .eq("tract_geoid20", tractGeoid)
                .eq("origin_year", fallback)
                .eq("series_kind", "forecast")
                .order("horizon_m", { ascending: true })
            if (!fb.error && fb.data && fb.data.length > 0) {
                forecastData = fb.data
            }
        }

        if (error || !forecastData || forecastData.length === 0) return null

        const effectiveOrigin = (forecastData[0] as any).origin_year as number
        const baseRow = forecastData.find((r: any) => r.horizon_m === 12)
        const baselineP50 = (baseRow as any)?.p50 || (forecastData[0] as any).p50 || 1

        const horizons: ForecastHorizon[] = forecastData.map((r: any) => ({
            horizon_m: r.horizon_m,
            forecastYear: effectiveOrigin + r.horizon_m / 12,
            p10: r.p10 || 0,
            p25: r.p25 || 0,
            p50: r.p50 || 0,
            p75: r.p75 || 0,
            p90: r.p90 || 0,
            spread: (r.p90 || 0) - (r.p10 || 0),
            appreciation: baselineP50 > 0 ? (((r.p50 || 0) / baselineP50) - 1) * 100 : 0,
        }))

        // Fire history and rankings in parallel
        const historyPromise = supabase
            .schema(schema as any)
            .from("metrics_tract_history")
            .select("year, value, p50")
            .eq("tract_geoid20", tractGeoid)
            .gte("year", 2015)
            .lte("year", 2025)
            .order("year", { ascending: true })

        const rankingsPromise = computeRankings(
            tractGeoid, effectiveOrigin, schema, supabase
        )

        // --- 3. Fetch comparables (same county first, widen to state if sparse) ---
        const countyFips = tractGeoid.substring(0, 5)
        const stateFips = tractGeoid.substring(0, 2)

        let { data: countyTracts } = await supabase
            .schema(schema as any)
            .from("metrics_tract_forecast")
            .select("tract_geoid20, p10, p50, p75, p90, horizon_m")
            .like("tract_geoid20", `${countyFips}%`)
            .eq("origin_year", effectiveOrigin)
            .eq("series_kind", "forecast")
            .in("horizon_m", [12, 60])
            .not("p50", "is", null)

        // Fallback: if county returns no comparable tracts, widen to state
        if (!countyTracts || countyTracts.length === 0) {
            const { data: stateTracts } = await supabase
                .schema(schema as any)
                .from("metrics_tract_forecast")
                .select("tract_geoid20, p10, p50, p75, p90, horizon_m")
                .like("tract_geoid20", `${stateFips}%`)
                .eq("origin_year", effectiveOrigin)
                .eq("series_kind", "forecast")
                .in("horizon_m", [12, 60])
                .not("p50", "is", null)
                .limit(2000)
            if (stateTracts && stateTracts.length > 0) {
                countyTracts = stateTracts
            }
        }

        const comparables = buildComparables(
            tractGeoid, countyTracts || [], baselineP50
        )

        const [{ data: histData }, rankings] = await Promise.all([historyPromise, rankingsPromise])

        // --- 2. Process history ---
        const history: HistoryPoint[] = (histData || []).map((r: any) => ({
            year: r.year,
            value: r.p50 ?? r.value ?? 0,
        }))

        // --- 5. Compute unique data tokens ---
        const uniqueDataTokens = countUniqueTokens(horizons, history, comparables, rankings)

        return {
            forecast: { originYear: effectiveOrigin, horizons, baselineP50 },
            history,
            comparables,
            rankings,
            uniqueDataTokens,
        }
    })
}

// ---------------------------------------------------------------------------
// Comparables builder
// ---------------------------------------------------------------------------

function buildComparables(
    targetGeoid: string,
    countyData: any[],
    targetBaseline: number
) {
    // Group by tract
    const tractMap = new Map<string, { p50_12m: number; p50_60m: number; p10_60m: number; p90_60m: number }>()
    for (const row of countyData) {
        const tid = row.tract_geoid20
        if (tid === targetGeoid) continue
        const entry = tractMap.get(tid) || { p50_12m: 0, p50_60m: 0, p10_60m: 0, p90_60m: 0 }
        if (row.horizon_m === 12) {
            entry.p50_12m = row.p50 || 0
        } else if (row.horizon_m === 60) {
            entry.p50_60m = row.p50 || 0
            entry.p10_60m = row.p10 || 0
            entry.p90_60m = row.p90 || 0
        }
        tractMap.set(tid, entry)
    }

    // Filter to tracts with both horizons
    const tracts = [...tractMap.entries()]
        .filter(([, v]) => v.p50_12m > 0 && v.p50_60m > 0)
        .map(([id, v]) => ({
            tractGeoid: id,
            name: `Tract ${id.substring(5)}`,
            p50_12m: v.p50_12m,
            p50_60m: v.p50_60m,
            appreciation_60m: v.p50_12m > 0 ? ((v.p50_60m / v.p50_12m) - 1) * 100 : 0,
            spread_60m: v.p90_60m - v.p10_60m,
            url: "",  // Will be enriched by the page component
        }))

    // Similar: closest p50_12m to target
    const similar = [...tracts]
        .sort((a, b) => Math.abs(a.p50_12m - targetBaseline) - Math.abs(b.p50_12m - targetBaseline))
        .slice(0, 5)

    // Higher upside: highest 5yr appreciation
    const higherUpside = [...tracts]
        .sort((a, b) => b.appreciation_60m - a.appreciation_60m)
        .slice(0, 5)

    // Lower risk: smallest spread
    const lowerRisk = [...tracts]
        .filter(t => t.spread_60m > 0)
        .sort((a, b) => a.spread_60m - b.spread_60m)
        .slice(0, 5)

    return { similar, higherUpside, lowerRisk }
}

// ---------------------------------------------------------------------------
// Rankings
// ---------------------------------------------------------------------------

async function computeRankings(
    tractGeoid: string,
    originYear: number,
    schema: string,
    supabase: ReturnType<typeof getSupabaseAdmin>
): Promise<Rankings> {
    const countyFips = tractGeoid.substring(0, 5)

    // Get target tract's p50 at 60m horizon
    const { data: targetRow } = await supabase
        .schema(schema as any)
        .from("metrics_tract_forecast")
        .select("p50")
        .eq("tract_geoid20", tractGeoid)
        .eq("origin_year", originYear)
        .eq("horizon_m", 60)
        .eq("series_kind", "forecast")
        .single()

    if (!targetRow) return { metroRank: 0, metroTotal: 0, nationalPercentile: 0 }
    const targetP50 = (targetRow as any).p50

    // Metro rank: count tracts in same county with higher p50 at 60m
    const { count: metroHigher } = await supabase
        .schema(schema as any)
        .from("metrics_tract_forecast")
        .select("*", { count: "exact", head: true })
        .like("tract_geoid20", `${countyFips}%`)
        .eq("origin_year", originYear)
        .eq("horizon_m", 60)
        .eq("series_kind", "forecast")
        .gt("p50", targetP50)

    const { count: metroTotal } = await supabase
        .schema(schema as any)
        .from("metrics_tract_forecast")
        .select("*", { count: "exact", head: true })
        .like("tract_geoid20", `${countyFips}%`)
        .eq("origin_year", originYear)
        .eq("horizon_m", 60)
        .eq("series_kind", "forecast")
        .not("p50", "is", null)

    // National percentile: count tracts nationally with lower p50 at 60m
    const { count: nationalLower } = await supabase
        .schema(schema as any)
        .from("metrics_tract_forecast")
        .select("*", { count: "exact", head: true })
        .eq("origin_year", originYear)
        .eq("horizon_m", 60)
        .eq("series_kind", "forecast")
        .lt("p50", targetP50)
        .not("p50", "is", null)

    const { count: nationalTotal } = await supabase
        .schema(schema as any)
        .from("metrics_tract_forecast")
        .select("*", { count: "exact", head: true })
        .eq("origin_year", originYear)
        .eq("horizon_m", 60)
        .eq("series_kind", "forecast")
        .not("p50", "is", null)

    return {
        metroRank: (metroHigher || 0) + 1,
        metroTotal: metroTotal || 0,
        nationalPercentile: nationalTotal
            ? Math.round(((nationalLower || 0) / nationalTotal) * 100)
            : 0,
    }
}

// ---------------------------------------------------------------------------
// Quality gate: unique data token counter
// ---------------------------------------------------------------------------

function countUniqueTokens(
    horizons: ForecastHorizon[],
    history: HistoryPoint[],
    comparables: ForecastPageData["comparables"],
    rankings: Rankings
): number {
    let tokens = 0

    // Each horizon contributes: p10, p25, p50, p75, p90, spread, appreciation = 7 tokens
    tokens += horizons.length * 7

    // Each history point = 1 token
    tokens += history.length

    // Each comparable = 4 tokens (name, p50_12m, appreciation, spread)
    tokens += comparables.similar.length * 4
    tokens += comparables.higherUpside.length * 4
    tokens += comparables.lowerRisk.length * 4

    // Rankings = 3 tokens
    if (rankings.metroRank > 0) tokens += 3

    return tokens
}
