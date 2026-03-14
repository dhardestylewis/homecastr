import type { Metadata } from "next"
import { notFound } from "next/navigation"
import Link from "next/link"

import { resolveSlugToTract, parseTractGeoid, enrichWithNeighborhood, batchEnrichTracts, buildDisplayNameForTract } from "@/lib/publishing/geo-crosswalk"
import { fetchForecastPageData, isForecastOutlier } from "@/lib/publishing/forecast-data"
import { fetchSeoNarrative } from "@/lib/publishing/seo-narratives"

import { KeyTakeaway } from "@/components/publishing/KeyTakeaway"
import { HistoryForecastChart } from "@/components/publishing/HistoryForecastChart"
import { UncertaintyBand } from "@/components/publishing/UncertaintyBand"
import { InterpretationSection } from "@/components/publishing/InterpretationSection"
import { ComparablesTable } from "@/components/publishing/ComparablesTable"
import { MethodCaveat } from "@/components/publishing/MethodCaveat"
import { ForecastMapEmbed } from "@/components/publishing/ForecastMapEmbed"
import { RequestAnalysisModal } from "@/components/request-analysis-modal"
import { ForecastContextSync } from "@/components/assistant/ForecastContextSync"
import { getCenterForCity } from "@/lib/publishing/geo-centroids"
import { getDynamicBounds } from "@/lib/publishing/geo-bounds"

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

    const minForecastYear = 2027
    const maxForecastYear = 2030
    const title = `${geo.neighborhoodName} Home Price Forecast`
    const description = isOutlier
        ? `Detailed housing market modeling and outlook for ${geo.neighborhoodName} (${geo.city}, ${geo.stateAbbr}). Data access available by request.`
        : `${geo.neighborhoodName} home prices are forecast ${appreciation ? `to change ${appreciation}` : ""} by ${maxForecastYear}. See downside, base case, upside scenarios, and market drivers.`
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
    if (!tractGeoid) notFound()

    // Get geo info
    let geo = parseTractGeoid(tractGeoid)
    geo = await enrichWithNeighborhood(geo)
    geo.neighborhoodSlug = neighborhood
    geo.neighborhoodName = buildDisplayNameForTract(geo)

    // Fetch all forecast data + AI narrative in parallel
    const countyFips = tractGeoid.substring(0, 5)
    const [data, narrative, mapCenter, tractBounds] = await Promise.all([
        fetchForecastPageData(tractGeoid, 2025, schema),
        fetchSeoNarrative(tractGeoid, "tract"),
        Promise.resolve(getCenterForCity(countyFips, state)),
        getDynamicBounds("tract", tractGeoid),
    ])
    if (!data) notFound()

    // Determine outlier status
    const isOutlier = isForecastOutlier(data)
    if (isOutlier) {
        data.forecast.baselineP50 = 500000;
        data.forecast.horizons = data.forecast.horizons.map(h => ({
            ...h,
            p10: 450000, p25: 480000, p50: 520000, p75: 560000, p90: 600000,
            appreciation: 4, spread: 150000
        }))
        data.history = data.history.map(h => ({ ...h, value: Math.max(100000, h.value) }))
        data.comparables.similar = []
        data.comparables.higherUpside = []
        data.comparables.lowerRisk = []
    }

    const h5 = data.forecast.horizons.find(h => h.horizon_m === 60)
    const minForecastYear = 2027
    const maxForecastYear = 2030

    // Enrich comparable tract names
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

    // JSON-LD structured data
    const jsonLd = {
        "@context": "https://schema.org",
        "@type": "FAQPage",
        mainEntity: [
            {
                "@type": "Question",
                name: `What is the home price forecast for ${geo.neighborhoodName}?`,
                acceptedAnswer: {
                    "@type": "Answer",
                    text: `Homecastr forecasts ${geo.neighborhoodName} median home values at $${Math.round(data.forecast.baselineP50).toLocaleString()} currently, with a 5-year expected change of ${h5 ? `${h5.appreciation.toFixed(1)}%` : "N/A"}.`,
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
        <div className="relative max-w-4xl mx-auto">
            {/* Sync context to assistant for grounded chat */}
            <ForecastContextSync 
                context={{
                    tractGeoid,
                    neighborhoodName: geo.neighborhoodName,
                    city: geo.city,
                    state: geo.stateAbbr,
                    currentUrl: `/forecasts/${state}/${city}/${neighborhood}/home-price-forecast`,
                }}
            />
            
            {isOutlier && <RequestAnalysisModal neighborhoodName={geo.neighborhoodName} />}
            <div className={`space-y-8 ${isOutlier ? "pointer-events-none blur-sm select-none opacity-80 overflow-hidden max-h-screen" : ""}`}>
                {/* JSON-LD */}
                {!isOutlier && (
                    <script
                        type="application/ld+json"
                        dangerouslySetInnerHTML={{ __html: JSON.stringify(jsonLd) }}
                    />
                )}

                {/* ===== 1. TITLE HERO - Human name first, geography metadata secondary ===== */}
                <header className="space-y-3 pt-6">
                    <h1 className="text-3xl md:text-4xl font-bold tracking-tight text-foreground text-balance">
                        {geo.neighborhoodName} Home Price Forecast
                    </h1>
                    <p className="text-sm text-muted-foreground">
                        {geo.city}, {geo.stateAbbr}
                        {geo.zcta5 && <> · ZIP {geo.zcta5}</>}
                        {' '}· Census Tract {geo.tractGeoid}
                        {' '}· {minForecastYear}–{maxForecastYear} outlook
                    </p>
                    {/* AI market summary when available */}
                    {narrative?.market_summary && !isOutlier && (
                        <p className="text-sm text-muted-foreground leading-relaxed max-w-2xl">
                            {narrative.market_summary}
                        </p>
                    )}
                </header>

                {/* ===== 2. KEY TAKEAWAY HERO - Forecast summary cards + one-paragraph insight ===== */}
                <section id="key-takeaway">
                <KeyTakeaway
                    horizons={data.forecast.horizons}
                    baselineP50={data.forecast.baselineP50}
                    neighborhoodName={geo.neighborhoodName}
                    city={geo.city}
                    stateAbbr={geo.stateAbbr}
                />
                </section>

                {/* ===== 3. FAN CHART - Visual forecast first ===== */}
                <section className="space-y-3">
                    <h2 className="text-lg font-semibold text-foreground">Forecast Timeline</h2>
                    <HistoryForecastChart
                        history={data.history}
                        horizons={data.forecast.horizons}
                        originYear={data.forecast.originYear}
                        suppressConfidence={data.forecast.originYear >= 2026}
                    />
                </section>

                {/* ===== 4. UNCERTAINTY - Why this range is wide/narrow ===== */}
                {data.forecast.originYear < 2026 && (
                    <section id="uncertainty">
                    <UncertaintyBand horizons={data.forecast.horizons} />
                    </section>
                )}

                {/* ===== 5. INTERPRETATION - What this means ===== */}
                <section id="interpretation">
                <InterpretationSection
                    horizons={data.forecast.horizons}
                    neighborhoodName={geo.neighborhoodName}
                    city={geo.city}
                    stateAbbr={geo.stateAbbr}
                    aiNarrative={isOutlier ? null : narrative}
                />
                </section>

                {/* ===== 6. COMPARABLES - Nearby context ===== */}
                <ComparablesTable
                    similar={data.comparables.similar}
                    higherUpside={data.comparables.higherUpside}
                    lowerRisk={data.comparables.lowerRisk}
                    currentTractGeoid={tractGeoid}
                    stateSlug={state}
                    citySlug={city}
                    aiComparableNarrative={isOutlier ? null : narrative?.comparable_narrative}
                />

                {/* ===== 7. MAP - Moved down from top, now contextual ===== */}
                <section className="space-y-3">
                    <h2 className="text-lg font-semibold text-foreground">Location</h2>
                    <ForecastMapEmbed
                        lat={mapCenter.lat}
                        lng={mapCenter.lng}
                        zoom={12}
                        bbox={tractBounds}
                        label={`${geo.neighborhoodName} on map`}
                        height={280}
                    />
                </section>

                {/* ===== 8. METHODOLOGY - Collapsed/secondary ===== */}
                <MethodCaveat
                    schemaVersion={SCHEMA}
                    originYear={data.forecast.originYear}
                    minForecastYear={minForecastYear}
                    maxForecastYear={maxForecastYear}
                />

                {/* ===== 9. EXPLORE MORE - Simplified footer ===== */}
                <nav className="pt-6 border-t border-border space-y-4">
                    <p className="text-sm font-medium text-foreground">Explore More</p>
                    <div className="flex flex-wrap gap-2">
                        <Link
                            href={`/forecasts/${state}/${city}`}
                            className="text-xs px-3 py-1.5 rounded-lg bg-secondary hover:bg-accent text-muted-foreground hover:text-foreground border border-border transition-all"
                        >
                            All {geo.city} neighborhoods
                        </Link>
                        <Link
                            href={`/forecasts/${state}`}
                            className="text-xs px-3 py-1.5 rounded-lg bg-secondary hover:bg-accent text-muted-foreground hover:text-foreground border border-border transition-all"
                        >
                            All {geo.stateName} cities
                        </Link>
                        <Link
                            href="/"
                            className="text-xs px-3 py-1.5 rounded-lg bg-primary/10 hover:bg-primary/20 text-primary border border-primary/20 transition-all"
                        >
                            Explore map
                        </Link>
                    </div>
                </nav>
            </div>
        </div>
    )
}
