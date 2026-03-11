import type { Metadata } from "next"
import { notFound } from "next/navigation"

import { resolveSlugToTract, parseTractGeoid, enrichWithNeighborhood, batchEnrichTracts, slugify, ZIP_NAMES } from "@/lib/publishing/geo-crosswalk"
import { fetchForecastPageData, isForecastOutlier } from "@/lib/publishing/forecast-data"
import { fetchSeoNarrative } from "@/lib/publishing/seo-narratives"

import { ForecastSummaryCard } from "@/components/publishing/ForecastSummaryCard"
import { UncertaintyBand } from "@/components/publishing/UncertaintyBand"
import { InterpretationSection } from "@/components/publishing/InterpretationSection"
import { ComparablesTable } from "@/components/publishing/ComparablesTable"
import { HistoryForecastChart } from "@/components/publishing/HistoryForecastChart"
import { MethodCaveat } from "@/components/publishing/MethodCaveat"
import { ForecastMapEmbed } from "@/components/publishing/ForecastMapEmbed"
import { RequestAnalysisModal } from "@/components/request-analysis-modal"
import { getCenterForCity } from "@/lib/publishing/geo-centroids"
import { getDynamicBounds } from "@/lib/publishing/geo-bounds"
import Link from "next/link"

// ISR: revalidate every hour so pages auto-update when ACS forecasts change
export const revalidate = 3600

const SCHEMA = process.env.FORECAST_SCHEMA || "forecast_queue"

interface PageProps {
    params: Promise<{
        state: string
        city: string
        neighborhood: string
    }>
    searchParams: Promise<{ schema?: string }>
}

export async function generateMetadata({ params }: PageProps): Promise<Metadata> {
    const { state, city, neighborhood } = await params
    const tractGeoid = await resolveSlugToTract(state, city, neighborhood, SCHEMA)
    if (!tractGeoid) return { title: "Forecast Not Found" }

    let geo = parseTractGeoid(tractGeoid)
    geo = await enrichWithNeighborhood(geo)

    const data = await fetchForecastPageData(tractGeoid, 2025, SCHEMA)
    const isOutlier = data ? isForecastOutlier(data) : false
    const h5 = data?.forecast.horizons.find(h => h.horizon_m === 60)
    const appreciation = (h5 && !isOutlier) ? `${h5.appreciation > 0 ? "+" : ""}${h5.appreciation.toFixed(1)}%` : ""

    // Outlook = consistent 2027–2030 range regardless of origin vintage
    const minForecastYear = 2027
    const maxForecastYear = 2030
    const title = `${geo.neighborhoodName} Home Price Forecast, ${minForecastYear}–${maxForecastYear}`
    const description = isOutlier
        ? `Detailed modeling and market outlook for ${geo.neighborhoodName} (${geo.city}, ${geo.stateAbbr}). Data access available by request.`
        : `Homecastr forecasts ${geo.neighborhoodName} (${geo.city}, ${geo.stateAbbr}) home values ${appreciation ? `to change ${appreciation}` : ""} by ${maxForecastYear} (p50). See upside, downside, comparables, and uncertainty.`
    return {
        title,
        description,
        alternates: {
            canonical: `/forecasts/${state}/${city}/${neighborhood}/home-price-forecast`,
        },
        openGraph: {
            title: `${title} | Homecastr`,
            description,
            type: "article",
            siteName: "Homecastr",
        },
        twitter: {
            card: "summary",
            title: `${title} | Homecastr`,
            description,
        },
    }
}

export default async function NeighborhoodForecastPage({ params, searchParams }: PageProps) {
    const { state, city, neighborhood } = await params
    const sp = await searchParams
    const schema = sp.schema || SCHEMA

    // Resolve URL slug → tract GeoID
    const tractGeoid = await resolveSlugToTract(state, city, neighborhood, schema)
    console.log(`[forecast-page] resolve: state=${state} city=${city} neighborhood=${neighborhood} → tractGeoid=${tractGeoid}`)
    if (!tractGeoid) notFound()

    // Get geo info
    let geo = parseTractGeoid(tractGeoid)
    geo = await enrichWithNeighborhood(geo)

    // RECONSTRUCT DISAMBIGUATED NAME FROM URL SLUG (mirrors city page logic)
    const baseSlug = slugify(geo.neighborhoodName)
    let displayName = geo.neighborhoodName
    
    if (neighborhood !== baseSlug && neighborhood.startsWith(baseSlug + "-")) {
        const remainder = neighborhood.substring(baseSlug.length + 1) // e.g. "lakehead", "tr-970200"
        const tractSuffix = geo.tractGeoid.substring(5)
        
        if (remainder === `tr-${tractSuffix}`) {
            // It fell back to Tr.XXXXX. The city page would have appended the ZIP/ZIP_NAME if available.
            let qualifier = geo.zcta5 || ""
            if (qualifier && ZIP_NAMES[qualifier]) {
                const resolvedName = ZIP_NAMES[qualifier]
                if (resolvedName.toLowerCase() !== geo.neighborhoodName.toLowerCase()) {
                    qualifier = resolvedName
                }
            }
            displayName = qualifier 
                ? `${geo.neighborhoodName} · ${qualifier} · Tr.${tractSuffix}`
                : `${geo.neighborhoodName} · Tr.${tractSuffix}`
        } else {
            // It's a unique ZIP or ZIP_NAME (e.g. "lakehead" or "83638")
            const qualifierSlug = remainder
            let qualifierStr = qualifierSlug
            if (geo.zcta5 && slugify(geo.zcta5) === qualifierSlug) qualifierStr = geo.zcta5
            if (geo.zcta5 && ZIP_NAMES[geo.zcta5] && slugify(ZIP_NAMES[geo.zcta5]) === qualifierSlug) qualifierStr = ZIP_NAMES[geo.zcta5]
            
            displayName = `${geo.neighborhoodName} · ${qualifierStr}`
        }
    }
    geo.neighborhoodName = displayName

    // Fetch all forecast data + AI narrative in parallel
    const countyFips = tractGeoid.substring(0, 5)
    const [data, narrative, mapCenter, tractBounds] = await Promise.all([
        fetchForecastPageData(tractGeoid, 2025, schema),
        fetchSeoNarrative(tractGeoid, "tract"),
        Promise.resolve(getCenterForCity(countyFips, state)),
        getDynamicBounds("tract", tractGeoid),
    ])
    console.log(`[forecast-page] data for ${tractGeoid}: exists=${!!data} tokens=${data?.uniqueDataTokens} narrative=${!!narrative}`)
    if (!data) notFound()

    // Determine outlier status and scrub data for the DOM if necessary
    const isOutlier = isForecastOutlier(data)
    if (isOutlier) {
        console.log(`[forecast-page] tract ${tractGeoid} is defined as an outlier: baselineP50=${data.forecast.baselineP50}`)
        // Redact data to ensure extreme numbers don't show up in SEO or source code while keeping enough shape for the blur.
        data.forecast.baselineP50 = 500000;
        data.forecast.horizons = data.forecast.horizons.map(h => ({
            ...h,
            p10: 450000,
            p25: 480000,
            p50: 520000,
            p75: 560000,
            p90: 600000,
            appreciation: 4,
            spread: 150000
        }))
        data.history = data.history.map(h => ({ ...h, value: Math.max(100000, h.value) }))
        data.comparables.similar = []
        data.comparables.higherUpside = []
        data.comparables.lowerRisk = []
    }

    const h5 = data.forecast.horizons.find(h => h.horizon_m === 60)

    // Outlook = consistent 2027–2030 range regardless of origin vintage
    const minForecastYear = 2027
    const maxForecastYear = 2030

    // Enrich comparable tract names using crosswalks
    const allCompIds = [
        ...data.comparables.similar,
        ...data.comparables.higherUpside,
        ...data.comparables.lowerRisk,
    ].map(c => c.tractGeoid)
    if (allCompIds.length > 0) {
        const enrichedCompNames = await batchEnrichTracts([...new Set(allCompIds)])
        for (const list of [data.comparables.similar, data.comparables.higherUpside, data.comparables.lowerRisk]) {
            for (const comp of list) {
                const enriched = enrichedCompNames.get(comp.tractGeoid)
                if (enriched) comp.name = enriched.name
            }
        }
    }

    // Build JSON-LD structured data
    const jsonLd = {
        "@context": "https://schema.org",
        "@type": "FAQPage",
        mainEntity: [
            {
                "@type": "Question",
                name: `What is the home price forecast for ${geo.neighborhoodName}?`,
                acceptedAnswer: {
                    "@type": "Answer",
                    text: `Homecastr's model forecasts ${geo.neighborhoodName} median home values at $${Math.round(data.forecast.baselineP50).toLocaleString()} currently, with a 5-year expected change of ${h5 ? `${h5.appreciation.toFixed(1)}%` : "N/A"}.`,
                },
            },
            {
                "@type": "Question",
                name: `How confident is the ${geo.neighborhoodName} forecast?`,
                acceptedAnswer: {
                    "@type": "Answer",
                    text: h5
                        ? `The 5-year forecast range spans from $${Math.round(h5.p10).toLocaleString()} (downside) to $${Math.round(h5.p90).toLocaleString()} (upside), with a spread of ${((h5.spread / h5.p50) * 100).toFixed(0)}% of median.`
                        : "Forecast confidence data not available.",
                },
            },
        ],
    }

    return (
        <div className="relative">
            {isOutlier && <RequestAnalysisModal neighborhoodName={geo.neighborhoodName} />}
            <div className={`space-y-10 ${isOutlier ? "pointer-events-none blur-sm select-none opacity-80 overflow-hidden max-h-screen" : ""}`}>
                {/* JSON-LD */}
                {!isOutlier && (
                    <script
                        type="application/ld+json"
                        dangerouslySetInnerHTML={{ __html: JSON.stringify(jsonLd) }}
                    />
                )}

                {/* Map embed */}
                <ForecastMapEmbed
                    lat={mapCenter.lat}
                    lng={mapCenter.lng}
                    zoom={12}
                    bbox={tractBounds}
                    label={`${geo.neighborhoodName} Forecast Map`}
                    height={350}
                />

                {/* Breadcrumbs */}
                <nav aria-label="Breadcrumb" className="text-xs text-muted-foreground flex items-center gap-1.5 flex-wrap">
                    <Link href="/forecasts" className="hover:text-foreground transition-colors">Forecasts</Link>
                    <span>/</span>
                    <Link href={`/forecasts/${state}`} className="hover:text-foreground transition-colors">
                        {geo.stateName}
                    </Link>
                    <span>/</span>
                    <Link href={`/forecasts/${state}/${city}`} className="hover:text-foreground transition-colors">
                        {geo.city}
                    </Link>
                    <span>/</span>
                    <span className="text-foreground/70">{geo.neighborhoodName}</span>
                </nav>

                {/* Page title */}
                <header className="space-y-3">
                    <h1 className="text-3xl font-bold tracking-tight sm:text-4xl text-foreground">
                        {geo.neighborhoodName} Home Price Forecast
                    </h1>
                    <p className="text-base text-muted-foreground">
                        {geo.city}, {geo.stateAbbr} · {minForecastYear}–{maxForecastYear} outlook · Census Tract {geo.tractGeoid}
                    </p>
                    {/* AI market summary (when available) */}
                    {narrative?.market_summary && !isOutlier && (
                        <p className="text-sm text-muted-foreground leading-relaxed mt-2">
                            {narrative.market_summary}
                        </p>
                    )}
                    <div className="flex items-center gap-3">
                        {data.rankings.metroRank > 0 && data.rankings.metroTotal > 1 && (
                            <span className="inline-flex items-center gap-1.5 rounded-full bg-primary/10 px-3 py-1 text-xs font-medium text-primary border border-primary/20">
                                #{data.rankings.metroRank} of {data.rankings.metroTotal} in metro
                            </span>
                        )}
                        {data.rankings.nationalPercentile > 0 && (() => {
                            const topPct = 100 - data.rankings.nationalPercentile
                            const rounded = topPct <= 5 ? 5 : Math.ceil(topPct / 5) * 5
                            return (
                                <span className="inline-flex items-center gap-1.5 rounded-full bg-accent px-3 py-1 text-xs font-medium text-accent-foreground border border-border">
                                    Top {rounded}% nationally
                                </span>
                            )
                        })()}
                    </div>
                </header>

                {/* 1. Historical Trend & Forecast (visual first) */}
                <HistoryForecastChart
                    history={data.history}
                    horizons={data.forecast.horizons}
                    originYear={data.forecast.originYear}
                    suppressConfidence={data.forecast.originYear >= 2026}
                />

                {/* 2. Forecast Summary */}
                <ForecastSummaryCard
                    horizons={data.forecast.horizons}
                    baselineP50={data.forecast.baselineP50}
                    neighborhoodName={geo.neighborhoodName}
                    suppressConfidence={data.forecast.originYear >= 2026}
                />

                {/* 3. Interpretation */}
                <InterpretationSection
                    horizons={data.forecast.horizons}
                    neighborhoodName={geo.neighborhoodName}
                    city={geo.city}
                    stateAbbr={geo.stateAbbr}
                    aiNarrative={isOutlier ? null : narrative}
                />

                {/* 4. Uncertainty Band */}
                {data.forecast.originYear < 2026 && (
                    <UncertaintyBand horizons={data.forecast.horizons} />
                )}

                {/* 5. Comparable Alternatives */}
                <ComparablesTable
                    similar={data.comparables.similar}
                    higherUpside={data.comparables.higherUpside}
                    lowerRisk={data.comparables.lowerRisk}
                    currentTractGeoid={tractGeoid}
                    stateSlug={state}
                    citySlug={city}
                    aiComparableNarrative={isOutlier ? null : narrative?.comparable_narrative}
                />

                {/* 6. Method / Caveat */}
                <MethodCaveat
                    schemaVersion={SCHEMA}
                    originYear={data.forecast.originYear}
                    minForecastYear={minForecastYear}
                    maxForecastYear={maxForecastYear}
                />

                {/* Internal linking */}
                <nav className="glass-panel rounded-xl p-5 space-y-3">
                    <h2 className="text-sm font-medium text-muted-foreground">Explore More Forecasts</h2>
                    <div className="flex flex-wrap gap-2">
                        <Link
                            href={`/forecasts/${state}/${city}`}
                            className="text-xs px-3 py-1.5 rounded-lg bg-secondary hover:bg-accent text-muted-foreground hover:text-foreground border border-border transition-all"
                        >
                            All {geo.city} neighborhoods →
                        </Link>
                        <Link
                            href={`/forecasts/${state}`}
                            className="text-xs px-3 py-1.5 rounded-lg bg-secondary hover:bg-accent text-muted-foreground hover:text-foreground border border-border transition-all"
                        >
                            All {geo.stateName} cities →
                        </Link>
                        <Link
                            href="/"
                            className="text-xs px-3 py-1.5 rounded-lg bg-primary/10 hover:bg-primary/20 text-primary border border-primary/20 transition-all"
                        >
                            Explore interactive map →
                        </Link>
                    </div>
                </nav>
            </div>
        </div>
    )
}
