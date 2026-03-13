import type { Metadata } from "next"
import { getStatesWithData } from "@/lib/publishing/geo-crosswalk"
import { withRedisCache } from "@/lib/redis"
import { getSupabaseAdmin } from "@/lib/supabase/admin"
import { SortableStateTable, type StateRow } from "@/components/publishing/SortableStateTable"
import { DownloadButton } from "@/components/publishing/DownloadButton"
import { DatasetJsonLd } from "@/components/publishing/DatasetJsonLd"
import { isOutlierTract, logFlaggedOutliers, createOutlierTag, type OutlierTag } from "@/lib/publishing/forecast-outlier-filter"

export const revalidate = 3600

const SCHEMA = process.env.FORECAST_SCHEMA || "forecast_queue"

export const metadata: Metadata = {
    title: "Homecastr Forecasts",
    description: "Home price forecasts for neighborhoods across the United States. Data-driven outlook powered by the Homecastr World Model.",
}

// State slug → FIPS mapping
const SLUG_TO_FIPS: Record<string, string> = {
    al: "01", ak: "02", az: "04", ar: "05", ca: "06", co: "08", ct: "09",
    de: "10", dc: "11", fl: "12", ga: "13", hi: "15", id: "16", il: "17",
    in: "18", ia: "19", ks: "20", ky: "21", la: "22", me: "23", md: "24",
    ma: "25", mi: "26", mn: "27", ms: "28", mo: "29", mt: "30", ne: "31",
    nv: "32", nh: "33", nj: "34", nm: "35", ny: "36", nc: "37", nd: "38",
    oh: "39", ok: "40", or: "41", pa: "42", pr: "72", ri: "44", sc: "45",
    sd: "46", tn: "47", tx: "48", ut: "49", vt: "50", va: "51", wa: "53",
    wv: "54", wi: "55", wy: "56",
}

/**
 * Paginated Supabase fetch using efficient range queries (.gte/.lt).
 */
async function fetchAllRows(
    queryBuilder: () => any,
    pageSize = 1000
): Promise<any[]> {
    const all: any[] = []
    let offset = 0
    while (true) {
        const { data, error } = await queryBuilder()
            .range(offset, offset + pageSize - 1)

        if (error || !data || data.length === 0) break
        all.push(...data)
        if (data.length < pageSize) break  // last page
        offset += pageSize
    }
    return all
}

/**
 * Compute state-level outlook.
 * Tries RPC first; falls back to client-side range queries (.gte/.lt)
 * which are efficient on indexed tract_geoid20 columns.
 */
async function getStateOutlooksFast(stateFips: string) {
    return withRedisCache(`state_outlooks_v2:${stateFips}:${SCHEMA}`, async () => {
        const supabase = getSupabaseAdmin()

        // --- Try RPC first ---
        try {
            const { data, error } = await supabase.rpc('get_state_outlooks', {
                target_schema: SCHEMA,
                state_fips: stateFips
            })
            if (!error && data && data.length > 0) {
                const row = data[0]
                const highestUpside = row.highest_upside !== null ? Number(row.highest_upside) : null
                // If RPC highest_upside looks corrupt (>100%), fall through to JS recomputation
                // so we get a real interpolated p99 from valid tracts rather than a capped junk value.
                if (highestUpside !== null && highestUpside > 100) throw new Error('outlier')
                return {
                    countyCount: Number(row.county_count || 0),
                    neighborhoodCount: Number(row.neighborhood_count || 0),
                    medianValue: row.median_value !== null ? Number(row.median_value) : null,
                    medianAppreciation: row.median_appreciation !== null ? Number(row.median_appreciation) : null,
                    highestUpside,
                }
            }
        } catch { /* fall through to client-side */ }

        // --- Fallback: client-side range queries ---
        // Use .gte/.lt (index-friendly) instead of .like() (full scan)
        const nextFips = String(Number(stateFips) + 1).padStart(2, "0")

        const [h12Rows, h60Rows] = await Promise.all([
            fetchAllRows(() =>
                supabase
                    .schema(SCHEMA as any)
                    .from("metrics_tract_forecast")
                    .select("tract_geoid20, p50")
                    .gte("tract_geoid20", stateFips)
                    .lt("tract_geoid20", nextFips)
                    .eq("origin_year", 2025)
                    .eq("horizon_m", 12)
                    .eq("series_kind", "forecast")
                    .not("p50", "is", null)
                    .order("tract_geoid20")
            ),
            fetchAllRows(() =>
                supabase
                    .schema(SCHEMA as any)
                    .from("metrics_tract_forecast")
                    .select("tract_geoid20, p50")
                    .gte("tract_geoid20", stateFips)
                    .lt("tract_geoid20", nextFips)
                    .eq("origin_year", 2025)
                    .eq("horizon_m", 60)
                    .eq("series_kind", "forecast")
                    .not("p50", "is", null)
                    .order("tract_geoid20")
            ),
        ])

        // County & neighborhood counts from h12
        const counties = new Set<string>()
        const tracts = new Set<string>()
        for (const row of h12Rows) {
            counties.add(row.tract_geoid20.substring(0, 5))
            tracts.add(row.tract_geoid20)
        }

        if (h12Rows.length === 0) return null

        // Build lookup maps
        const h12Map = new Map<string, number>()
        for (const row of h12Rows) h12Map.set(row.tract_geoid20, row.p50)
        const h60Map = new Map<string, number>()
        for (const row of h60Rows) h60Map.set(row.tract_geoid20, row.p50)

        // Compute per-tract appreciation (centralized outlier filter)
        const appreciations: number[] = []
        const values: number[] = []
        const flagged: OutlierTag[] = []
        for (const [tractId, h12] of h12Map) {
            const h60 = h60Map.get(tractId) ?? null
            const check = isOutlierTract(h12, h60)
            if (check.outlier) {
                flagged.push(createOutlierTag(tractId, check.reason!, h12, h60, SCHEMA))
                continue
            }
            if (h60 != null) {
                const appr = ((h60 - h12) / h12) * 100
                appreciations.push(appr)
                values.push(h12)
            }
        }
        await logFlaggedOutliers(flagged)

        if (appreciations.length === 0) {
            return {
                countyCount: counties.size,
                neighborhoodCount: tracts.size,
                medianValue: null,
                medianAppreciation: null,
                highestUpside: null,
            }
        }

        appreciations.sort((a, b) => a - b)
        values.sort((a, b) => a - b)
        const p99Idx = Math.floor(appreciations.length * 0.99)

        return {
            countyCount: counties.size,
            neighborhoodCount: tracts.size,
            medianValue: values[Math.floor(values.length / 2)],
            medianAppreciation: appreciations[Math.floor(appreciations.length / 2)],
            highestUpside: appreciations[p99Idx],
        }
    }, 7200) // 2 hours — per-state aggregate, ~50 keys
}

const fmtPct = (v: number) => `${v >= 0 ? "+" : ""}${v.toFixed(1)}%`

export default async function ForecastsIndexPage() {
    const states = await getStatesWithData(SCHEMA)

    // Fetch all state details in parallel (batched to avoid overwhelming DB)
    const BATCH = 6
    const stateDetails: StateRow[] = []

    for (let i = 0; i < states.length; i += BATCH) {
        const batch = states.slice(i, i + BATCH)
        const results = await Promise.all(
            batch.map(async (s) => {
                const fips = SLUG_TO_FIPS[s.stateSlug] || "00"
                const outlook = await getStateOutlooksFast(fips)

                return {
                    ...s,
                    countyCount: outlook?.countyCount ?? 0,
                    neighborhoodCount: outlook?.neighborhoodCount ?? 0,
                    medianValue: outlook?.medianValue ?? null,
                    medianAppreciation: outlook?.medianAppreciation ?? null,
                    highestUpside: outlook?.highestUpside ?? null,
                }
            })
        )
        stateDetails.push(...results)
    }

    // Summary stats
    const totalCounties = stateDetails.reduce((s, d) => s + d.countyCount, 0)
    const totalNeighborhoods = stateDetails.reduce((s, d) => s + d.neighborhoodCount, 0)
    const allAppreciations = stateDetails.filter(s => s.medianAppreciation !== null).map(s => s.medianAppreciation!)
    const avgOutlook = allAppreciations.length > 0
        ? allAppreciations.reduce((s, v) => s + v, 0) / allAppreciations.length
        : null

    return (
        <div className="space-y-8">
            <DatasetJsonLd
                name="United States Home Price Forecasts"
                description="State-level home price forecast summaries across the United States, generated by the Homecastr World Model."
                path="/forecasts"
                geographyCovered="United States"
            />
            <header className="space-y-3">
                <h1 className="text-3xl font-bold tracking-tight sm:text-4xl text-foreground">
                    Home Price Forecasts
                </h1>
                <p className="text-base text-muted-foreground max-w-2xl">
                    Explore neighborhood-level home price forecasts across the United States. Each page shows forecast distributions, uncertainty, comparables, and historical trends — all generated by the Homecastr World Model.
                </p>
                <DownloadButton />
            </header>

            {/* Summary stats */}
            <div className="grid gap-4 sm:grid-cols-4">
                <div className="glass-panel rounded-xl p-4">
                    <p className="text-xs uppercase tracking-wider text-muted-foreground mb-1">States & Territories</p>
                    <p className="text-2xl font-bold text-foreground">{stateDetails.length}</p>
                </div>
                <div className="glass-panel rounded-xl p-4">
                    <p className="text-xs uppercase tracking-wider text-muted-foreground mb-1">Total Counties</p>
                    <p className="text-2xl font-bold text-foreground">{totalCounties.toLocaleString()}</p>
                </div>
                <div className="glass-panel rounded-xl p-4">
                    <p className="text-xs uppercase tracking-wider text-muted-foreground mb-1">Total Neighborhoods</p>
                    <p className="text-2xl font-bold text-foreground">{totalNeighborhoods.toLocaleString()}</p>
                </div>
                <div className="glass-panel rounded-xl p-4">
                    <p className="text-xs uppercase tracking-wider text-muted-foreground mb-1">Avg 5yr Outlook</p>
                    <p className={`text-2xl font-bold ${avgOutlook !== null && avgOutlook >= 0 ? "text-chart-high" : "text-chart-low"}`}>
                        {avgOutlook !== null ? fmtPct(avgOutlook) : "N/A"}
                    </p>
                </div>
            </div>

            {stateDetails.length > 0 ? (
                <SortableStateTable rows={stateDetails} />
            ) : (
                <div className="glass-panel rounded-xl p-10 text-center">
                    <p className="text-muted-foreground">Forecast data is being processed. Check back soon.</p>
                </div>
            )}
        </div>
    )
}
