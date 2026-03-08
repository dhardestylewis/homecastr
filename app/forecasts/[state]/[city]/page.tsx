import type { Metadata } from "next"
import { notFound } from "next/navigation"
import Link from "next/link"
import { getTractsForCity, batchEnrichTracts, getStatesWithData, getCitiesForState } from "@/lib/publishing/geo-crosswalk"
import { getSupabaseAdmin } from "@/lib/supabase/admin"
import { SortableNeighborhoodTable } from "@/components/publishing/SortableNeighborhoodTable"

export const revalidate = 3600

const SCHEMA = process.env.FORECAST_SCHEMA || "forecast_queue"

/**
 * Pre-build all state/city pages at deploy time (SSG).
 */
export async function generateStaticParams() {
    const states = await getStatesWithData(SCHEMA)
    const params: { state: string; city: string }[] = []

    // Batch to avoid overwhelming DB during build
    const BATCH = 4
    for (let i = 0; i < states.length; i += BATCH) {
        const batch = states.slice(i, i + BATCH)
        const results = await Promise.all(
            batch.map(async (s) => {
                const cities = await getCitiesForState(s.stateSlug, SCHEMA)
                return cities.map((c) => ({ state: s.stateSlug, city: c.citySlug }))
            })
        )
        params.push(...results.flat())
    }

    return params
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

interface PageProps {
    params: Promise<{ state: string; city: string }>
    searchParams: Promise<{ schema?: string }>
}

export async function generateMetadata({ params }: PageProps): Promise<Metadata> {
    const { state, city } = await params
    const cityName = city.split("-").map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(" ")
    const stateName = STATE_NAMES[state] || state.toUpperCase()

    return {
        title: `${cityName} Neighborhood Forecasts`,
        description: `Home price forecasts for neighborhoods in ${cityName}, ${stateName}. See which areas have the strongest upside and lowest risk.`,
    }
}

export default async function CityHubPage({ params, searchParams }: PageProps) {
    const { state, city } = await params
    const sp = await searchParams
    const schema = sp.schema || SCHEMA
    const tracts = await getTractsForCity(state, city, schema)

    if (tracts.length === 0) notFound()

    const cityName = tracts[0]?.city || city.split("-").map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(" ")
    const stateName = tracts[0]?.stateName || STATE_NAMES[state] || state.toUpperCase()
    const stateAbbr = tracts[0]?.stateAbbr || state.toUpperCase()

    // Batch fetch forecast data and neighborhood names in parallel
    const supabase = getSupabaseAdmin()
    const tractIds = tracts.map(t => t.tractGeoid)

    const [{ data: forecastRows }, enrichedNames] = await Promise.all([
        supabase
            .schema(schema as any)
            .from("metrics_tract_forecast")
            .select("tract_geoid20, horizon_m, p50, p10, p90")
            .in("tract_geoid20", tractIds)
            .in("horizon_m", [12, 60])
            .eq("series_kind", "forecast")
            .not("p50", "is", null),
        batchEnrichTracts(tractIds),
    ])

    // Build lookup: tractId -> { p50_current, appreciation_5yr, spread_5yr }
    const tractLookup = new Map<string, {
        p50_current: number
        appreciation_5yr: number
        spread_5yr: number
    }>()

    if (forecastRows) {
        const grouped = new Map<string, typeof forecastRows>()
        for (const row of forecastRows) {
            const existing = grouped.get(row.tract_geoid20) || []
            existing.push(row)
            grouped.set(row.tract_geoid20, existing)
        }

        for (const [tractId, rows] of grouped) {
            const h12 = rows.find((r: any) => r.horizon_m === 12)
            const h60 = rows.find((r: any) => r.horizon_m === 60)
            if (h12) {
                tractLookup.set(tractId, {
                    p50_current: h12.p50,
                    appreciation_5yr: h60 ? ((h60.p50 - h12.p50) / h12.p50 * 100) : 0,
                    spread_5yr: h60 ? (h60.p90 - h60.p10) : 0,
                })
            }
        }
    }

    // Build slug from enriched name, with disambiguation for duplicates
    const slugify = (s: string) => s.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '')

    const tractDataRaw = tracts
        .map(t => {
            const enriched = enrichedNames.get(t.tractGeoid)
            return {
                ...t,
                neighborhoodName: enriched?.name || t.neighborhoodName,
                zcta5: enriched?.zcta5 || null,
                ...tractLookup.get(t.tractGeoid) || { p50_current: 0, appreciation_5yr: 0, spread_5yr: 0 },
            }
        })
        // Filter: must have data, and filter outliers (likely commercial/institutional tracts)
        .filter(t => t.p50_current >= 20_000 && t.p50_current < 5_000_000 && t.appreciation_5yr > -95 && t.appreciation_5yr <= 100)
        .sort((a, b) => b.appreciation_5yr - a.appreciation_5yr)

    // Two-pass disambiguation: use ZIP code as qualifier for duplicate names
    const nameFreq = new Map<string, number>()
    for (const t of tractDataRaw) {
        const baseName = t.neighborhoodName || `Tract ${t.tractGeoid.substring(5)}`
        nameFreq.set(baseName, (nameFreq.get(baseName) || 0) + 1)
    }

    // For duplicates: check if ZIP codes differ (useful qualifier) or if we need tract suffix
    const tractData = (() => {
        // Collect all items sharing each name, with their ZCTAs
        const nameGroups = new Map<string, typeof tractDataRaw>()
        for (const t of tractDataRaw) {
            const baseName = t.neighborhoodName || `Tract ${t.tractGeoid.substring(5)}`
            if (!nameGroups.has(baseName)) nameGroups.set(baseName, [])
            nameGroups.get(baseName)!.push(t)
        }

        return tractDataRaw.map(t => {
            const baseName = t.neighborhoodName || `Tract ${t.tractGeoid.substring(5)}`
            const baseSlug = slugify(baseName)
            const total = nameFreq.get(baseName) || 1

            // Unique name: keep clean
            if (total === 1) {
                return { ...t, neighborhoodName: baseName, neighborhoodSlug: baseSlug }
            }

            // Duplicate name: disambiguate with ZIP code
            const zip = t.zcta5 || ''
            const group = nameGroups.get(baseName) || []
            const zipFreq = group.filter(g => g.zcta5 === zip).length

            if (zip && zipFreq === 1) {
                // ZIP alone is unique within this name group
                const displayName = `${baseName} · ${zip}`
                const neighborhoodSlug = `${baseSlug}-${zip}`
                return { ...t, neighborhoodName: displayName, neighborhoodSlug }
            }

            // ZIP is shared too — add tract suffix for full uniqueness
            const tractSuffix = t.tractGeoid.substring(5)
            const displayName = zip
                ? `${baseName} · ${zip} · Tr.${tractSuffix}`
                : `${baseName} · Tr.${tractSuffix}`
            const neighborhoodSlug = `${baseSlug}-tr-${tractSuffix}`
            return { ...t, neighborhoodName: displayName, neighborhoodSlug }
        })
    })()

    const fmtVal = (v: number) => {
        if (v >= 1_000_000) return `$${(v / 1_000_000).toFixed(2)}M`
        if (v >= 1_000) return `$${(v / 1_000).toFixed(0)}K`
        return `$${v.toFixed(0)}`
    }

    const fmtPct = (v: number) => `${v >= 0 ? "+" : ""}${v.toFixed(1)}%`

    // Compute metro average for "vs. Metro" column
    const avgAppreciation = tractData.length > 0
        ? tractData.reduce((s, t) => s + t.appreciation_5yr, 0) / tractData.length
        : 0

    // Compute outlier-capped median value range (10th to 90th percentile)
    const sortedValues = tractData.map(t => t.p50_current).filter(v => v > 0).sort((a, b) => a - b)
    const p10Idx = Math.floor(sortedValues.length * 0.1)
    const p90Idx = Math.floor(sortedValues.length * 0.9)
    const medianLow = sortedValues[p10Idx] || 0
    const medianHigh = sortedValues[p90Idx] || 0

    return (
        <div className="space-y-8">
            {/* Breadcrumbs */}
            <nav aria-label="Breadcrumb" className="text-xs text-muted-foreground flex items-center gap-1.5">
                <Link href="/forecasts" className="hover:text-foreground transition-colors">Forecasts</Link>
                <span>/</span>
                <Link href={`/forecasts/${state}`} className="hover:text-foreground transition-colors">{stateName}</Link>
                <span>/</span>
                <span className="text-foreground/70">{cityName}</span>
            </nav>

            <header className="space-y-2">
                <h1 className="text-3xl font-bold tracking-tight sm:text-4xl text-foreground">
                    {cityName} Neighborhood Forecasts
                </h1>
                <p className="text-base text-muted-foreground">
                    {tractData.length} neighborhoods with forecast data · {stateAbbr}
                </p>
            </header>

            {/* Summary stats */}
            <div className="grid gap-4 sm:grid-cols-3">
                <div className="glass-panel rounded-xl p-4">
                    <p className="text-xs uppercase tracking-wider text-muted-foreground mb-1">Neighborhoods Covered</p>
                    <p className="text-2xl font-bold text-foreground">{tractData.length}</p>
                </div>
                <div className="glass-panel rounded-xl p-4">
                    <p className="text-xs uppercase tracking-wider text-muted-foreground mb-1">Highest 5yr Upside</p>
                    <p className="text-2xl font-bold text-chart-high">
                        {tractData.length > 0 ? fmtPct(tractData[0].appreciation_5yr) : "N/A"}
                    </p>
                </div>
                <div className="glass-panel rounded-xl p-4">
                    <p className="text-xs uppercase tracking-wider text-muted-foreground mb-1">Typical Value Range</p>
                    <p className="text-lg font-bold text-foreground/70">
                        {sortedValues.length > 0
                            ? `${fmtVal(medianLow)} to ${fmtVal(medianHigh)}`
                            : "N/A"}
                    </p>
                </div>
            </div>

            {/* Tract listing — sortable table */}
            <SortableNeighborhoodTable
                rows={tractData.map(t => ({
                    tractGeoid: t.tractGeoid,
                    neighborhoodName: t.neighborhoodName,
                    neighborhoodSlug: t.neighborhoodSlug,
                    p50_current: t.p50_current,
                    appreciation_5yr: t.appreciation_5yr,
                    vsMetro: t.appreciation_5yr - avgAppreciation,
                }))}
                state={state}
                city={city}
                avgAppreciation={avgAppreciation}
            />
        </div>
    )
}
