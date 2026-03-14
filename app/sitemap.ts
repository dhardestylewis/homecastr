import type { MetadataRoute } from 'next'
import { getStatesWithData, parseTractGeoid } from '@/lib/publishing/geo-crosswalk'
import { getSupabaseAdmin } from '@/lib/supabase/admin'

const BASE_URL = 'https://www.homecastr.com'
const SCHEMA = process.env.FORECAST_SCHEMA || "forecast_queue"

/**
 * Tract segments: each fetches ~10k tracts to avoid Vercel function timeouts.
 * With ~66k tracts total, we need 7 tract segments (IDs 3–9).
 *
 * Segment layout:
 *   0 = core pages
 *   1 = state pages
 *   2 = county pages
 *   3–9 = tract pages (10k each)
 */
const TRACTS_PER_SEGMENT = 10000
const TRACT_SEGMENT_COUNT = 7 // ceil(66388 / 10000)

export async function generateSitemaps() {
    const segments = [
        { id: 0 }, // core
        { id: 1 }, // states
        { id: 2 }, // counties
    ]
    for (let i = 0; i < TRACT_SEGMENT_COUNT; i++) {
        segments.push({ id: 3 + i })
    }
    return segments
}

/** Paginated Supabase fetch for tract geoids */
async function fetchTractsPage(offset: number, limit: number) {
    const supabase = getSupabaseAdmin()
    const { data } = await supabase
        .schema(SCHEMA as any)
        .from("metrics_tract_forecast")
        .select("tract_geoid20")
        .eq("origin_year", 2025)
        .eq("horizon_m", 12)
        .eq("series_kind", "forecast")
        .not("p50", "is", null)
        .order("tract_geoid20", { ascending: true })
        .range(offset, offset + limit - 1)

    return data || []
}

/** Collect tracts from startOffset up to maxCount rows */
async function collectTracts(startOffset: number, maxCount: number) {
    const pageSize = 1000
    const results: string[] = []
    let offset = startOffset

    while (results.length < maxCount) {
        const fetchLimit = Math.min(pageSize, maxCount - results.length)
        const data = await fetchTractsPage(offset, fetchLimit)
        if (data.length === 0) break
        for (const row of data) results.push(row.tract_geoid20)
        if (data.length < fetchLimit) break
        offset += data.length
    }

    return results
}

export default async function sitemap(
    props: { id: Promise<string> }
): Promise<MetadataRoute.Sitemap> {
    const id = Number(await props.id)
    const entries: MetadataRoute.Sitemap = []

    // ── Segment 0: Core static pages ──
    if (id === 0) {
        entries.push(
            { url: `${BASE_URL}/`, lastModified: new Date(), changeFrequency: 'weekly', priority: 1.0 },
            { url: `${BASE_URL}/app`, lastModified: new Date(), changeFrequency: 'daily', priority: 0.9 },
            { url: `${BASE_URL}/forecasts`, lastModified: new Date(), changeFrequency: 'daily', priority: 0.9 },
            { url: `${BASE_URL}/methodology`, lastModified: new Date(), changeFrequency: 'monthly', priority: 0.8 },
            { url: `${BASE_URL}/faq`, lastModified: new Date(), changeFrequency: 'monthly', priority: 0.8 },
            { url: `${BASE_URL}/support`, lastModified: new Date(), changeFrequency: 'monthly', priority: 0.7 },
            { url: `${BASE_URL}/api-docs`, lastModified: new Date(), changeFrequency: 'monthly', priority: 0.7 },
        )
    }

    // ── Segment 1: State pages ──
    if (id === 1) {
        const states = await getStatesWithData(SCHEMA)
        for (const state of states) {
            entries.push({
                url: `${BASE_URL}/forecasts/${state.stateSlug}`,
                lastModified: new Date(),
                changeFrequency: 'weekly',
                priority: 0.8,
            })
        }
    }

    // ── Segment 2: County / city pages ──
    if (id === 2) {
        const allCountyPaths = new Set<string>()
        const tracts = await collectTracts(0, 100000)
        for (const geoid of tracts) {
            const geo = parseTractGeoid(geoid)
            allCountyPaths.add(`/forecasts/${geo.stateSlug}/${geo.citySlug}`)
        }
        for (const path of allCountyPaths) {
            entries.push({
                url: `${BASE_URL}${path}`,
                lastModified: new Date(),
                changeFrequency: 'weekly',
                priority: 0.7,
            })
        }
    }

    // ── Segments 3–9: Tract pages (~10k each) ──
    if (id >= 3 && id <= 3 + TRACT_SEGMENT_COUNT - 1) {
        const segIdx = id - 3
        const startOffset = segIdx * TRACTS_PER_SEGMENT
        const tracts = await collectTracts(startOffset, TRACTS_PER_SEGMENT)
        for (const geoid of tracts) {
            const geo = parseTractGeoid(geoid)
            entries.push({
                url: `${BASE_URL}/forecasts/${geo.stateSlug}/${geo.citySlug}/${geo.neighborhoodSlug}/home-price-forecast`,
                lastModified: new Date(),
                changeFrequency: 'monthly',
                priority: 0.6,
            })
        }
    }

    return entries
}
