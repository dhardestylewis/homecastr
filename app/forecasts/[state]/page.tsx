import type { Metadata } from "next"
import { notFound } from "next/navigation"
import Link from "next/link"
import { getCitiesForState, getStatesWithData } from "@/lib/publishing/geo-crosswalk"
import { withRedisCache } from "@/lib/redis"
import { getSupabaseAdmin } from "@/lib/supabase/admin"
import { SortableCountyTable, type CountyRow } from "@/components/publishing/SortableCountyTable"

export const revalidate = 3600

const SCHEMA = process.env.FORECAST_SCHEMA || "forecast_queue"

/**
 * Pre-build all state pages at deploy time (SSG).
 */
export async function generateStaticParams() {
    const states = await getStatesWithData(SCHEMA)
    return states.map((s) => ({ state: s.stateSlug }))
}

const STATE_NAMES: Record<string, string> = {
    al: "Alabama", ak: "Alaska", az: "Arizona", ar: "Arkansas", ca: "California",
    co: "Colorado", ct: "Connecticut", de: "Delaware", dc: "District of Columbia",
    fl: "Florida", ga: "Georgia", hi: "Hawaii", id: "Idaho", il: "Illinois",
    in: "Indiana", ia: "Iowa", ks: "Kansas", ky: "Kentucky", la: "Louisiana",
    me: "Maine", md: "Maryland", ma: "Massachusetts", mi: "Michigan", mn: "Minnesota",
    ms: "Mississippi", mo: "Missouri", mt: "Montana", ne: "Nebraska", nv: "Nevada",
    nh: "New Hampshire", nj: "New Jersey", nm: "New Mexico", ny: "New York",
    nc: "North Carolina", nd: "North Dakota", oh: "Ohio", ok: "Oklahoma", or: "Oregon",
    pa: "Pennsylvania", ri: "Rhode Island", sc: "South Carolina", sd: "South Dakota",
    tn: "Tennessee", tx: "Texas", ut: "Utah", vt: "Vermont", va: "Virginia",
    wa: "Washington", wv: "West Virginia", wi: "Wisconsin", wy: "Wyoming", pr: "Puerto Rico",
}

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

interface PageProps {
    params: Promise<{ state: string }>
}

/**
 * Paginated Supabase fetch — bypasses the default 1000-row server limit.
 */
async function fetchAllRows(queryBuilder: () => any, pageSize = 1000): Promise<any[]> {
    const all: any[] = []
    let offset = 0
    while (true) {
        const { data, error } = await queryBuilder().range(offset, offset + pageSize - 1)
        if (error || !data || data.length === 0) break
        all.push(...data)
        if (data.length < pageSize) break
        offset += pageSize
    }
    return all
}

/**
 * Compute per-county outlook from tract-level forecasts for a given state.
 */
async function getCountyOutlooks(stateFips: string) {
    return withRedisCache(`county_outlooks:${stateFips}:${SCHEMA}`, async () => {
        const supabase = getSupabaseAdmin()

        const [h12Rows, h60Rows] = await Promise.all([
            fetchAllRows(() =>
                supabase
                    .schema(SCHEMA as any)
                    .from("metrics_tract_forecast")
                    .select("tract_geoid20, p50")
                    .like("tract_geoid20", `${stateFips}%`)
                    .eq("horizon_m", 12)
                    .eq("series_kind", "forecast")
                    .not("p50", "is", null)
            ),
            fetchAllRows(() =>
                supabase
                    .schema(SCHEMA as any)
                    .from("metrics_tract_forecast")
                    .select("tract_geoid20, p50")
                    .like("tract_geoid20", `${stateFips}%`)
                    .eq("horizon_m", 60)
                    .eq("series_kind", "forecast")
                    .not("p50", "is", null)
            ),
        ])

        const h12Map = new Map<string, number>()
        for (const row of h12Rows) h12Map.set(row.tract_geoid20, row.p50)
        const h60Map = new Map<string, number>()
        for (const row of h60Rows) h60Map.set(row.tract_geoid20, row.p50)

        // Group by county FIPS and compute appreciation
        const countyData = new Map<string, { appreciations: number[]; values: number[] }>()

        for (const [tractId, h12] of h12Map) {
            const h60 = h60Map.get(tractId)
            const countyFips = tractId.substring(0, 5)

            if (h12 >= 20_000 && h60 && h12 < 5_000_000) {
                const appr = ((h60 - h12) / h12) * 100
                if (appr > -95) {
                    if (!countyData.has(countyFips)) countyData.set(countyFips, { appreciations: [], values: [] })
                    const cd = countyData.get(countyFips)!
                    cd.appreciations.push(appr)
                    cd.values.push(h12)
                }
            }
        }

        const result: Record<string, { medianAppreciation: number; highestUpside: number; medianValue: number }> = {}

        for (const [countyFips, data] of countyData) {
            if (data.appreciations.length === 0) continue
            data.appreciations.sort((a, b) => a - b)
            data.values.sort((a, b) => a - b)
            const p99Idx = Math.min(Math.floor(data.appreciations.length * 0.99), data.appreciations.length - 1)

            result[countyFips] = {
                medianAppreciation: data.appreciations[Math.floor(data.appreciations.length / 2)],
                highestUpside: data.appreciations[p99Idx],
                medianValue: data.values[Math.floor(data.values.length / 2)],
            }
        }

        return result
    })
}

const fmtPct = (v: number) => `${v >= 0 ? "+" : ""}${v.toFixed(1)}%`
const fmtVal = (v: number) => {
    if (v >= 1_000_000) return `$${(v / 1_000_000).toFixed(2)}M`
    if (v >= 1_000) return `$${(v / 1_000).toFixed(0)}K`
    return `$${v.toFixed(0)}`
}

export async function generateMetadata({ params }: PageProps): Promise<Metadata> {
    const { state } = await params
    const stateName = STATE_NAMES[state] || state.toUpperCase()
    return {
        title: `${stateName} Home Price Forecasts`,
        description: `Home price forecasts for cities and neighborhoods across ${stateName}. Explore which areas have the strongest outlook.`,
    }
}

export default async function StateHubPage({ params }: PageProps) {
    const { state } = await params
    const stateName = STATE_NAMES[state] || state.toUpperCase()
    const stateFips = SLUG_TO_FIPS[state] || "00"

    const [cities, countyOutlooks] = await Promise.all([
        getCitiesForState(state, SCHEMA),
        getCountyOutlooks(stateFips),
    ])

    if (cities.length === 0) notFound()

    const totalTracts = cities.reduce((s, c) => s + c.tractCount, 0)

    // Build rows with outlook data, using countyFips for direct lookup
    const countyRows: CountyRow[] = cities.map(c => {
        const outlook = countyOutlooks[c.countyFips]
        return {
            city: c.city,
            citySlug: c.citySlug,
            tractCount: c.tractCount,
            medianValue: outlook?.medianValue ?? null,
            medianAppreciation: outlook?.medianAppreciation ?? null,
            highestUpside: outlook?.highestUpside ?? null,
        }
    })

    // Summary stats
    const allAppreciations = countyRows.filter(c => c.medianAppreciation !== null).map(c => c.medianAppreciation!)
    const avgOutlook = allAppreciations.length > 0
        ? allAppreciations.reduce((s, v) => s + v, 0) / allAppreciations.length
        : null

    const allValues = countyRows.filter(c => c.medianValue !== null).map(c => c.medianValue!)
    allValues.sort((a, b) => a - b)
    const stateMedianValue = allValues.length > 0 ? allValues[Math.floor(allValues.length / 2)] : null

    return (
        <div className="space-y-8">
            {/* Breadcrumbs */}
            <nav aria-label="Breadcrumb" className="text-xs text-muted-foreground flex items-center gap-1.5">
                <Link href="/forecasts" className="hover:text-foreground transition-colors">Forecasts</Link>
                <span>/</span>
                <span className="text-foreground/70">{stateName}</span>
            </nav>

            <header className="space-y-2">
                <h1 className="text-3xl font-bold tracking-tight sm:text-4xl text-foreground">
                    {stateName} Home Price Forecasts
                </h1>
                <p className="text-base text-muted-foreground">
                    {cities.length} counties · {totalTracts.toLocaleString()} neighborhoods with forecast data
                </p>
            </header>

            {/* Summary stats */}
            <div className="grid gap-4 sm:grid-cols-4">
                <div className="glass-panel rounded-xl p-4">
                    <p className="text-xs uppercase tracking-wider text-muted-foreground mb-1">Counties</p>
                    <p className="text-2xl font-bold text-foreground">{cities.length}</p>
                </div>
                <div className="glass-panel rounded-xl p-4">
                    <p className="text-xs uppercase tracking-wider text-muted-foreground mb-1">Neighborhoods</p>
                    <p className="text-2xl font-bold text-foreground">{totalTracts.toLocaleString()}</p>
                </div>
                <div className="glass-panel rounded-xl p-4">
                    <p className="text-xs uppercase tracking-wider text-muted-foreground mb-1">Median Value</p>
                    <p className="text-2xl font-bold text-foreground">
                        {stateMedianValue !== null ? fmtVal(stateMedianValue) : "N/A"}
                    </p>
                </div>
                <div className="glass-panel rounded-xl p-4">
                    <p className="text-xs uppercase tracking-wider text-muted-foreground mb-1">Avg 5yr Outlook</p>
                    <p className={`text-2xl font-bold ${avgOutlook !== null && avgOutlook >= 0 ? "text-chart-high" : "text-chart-low"}`}>
                        {avgOutlook !== null ? fmtPct(avgOutlook) : "N/A"}
                    </p>
                </div>
            </div>

            <SortableCountyTable rows={countyRows} state={state} />
        </div>
    )
}
