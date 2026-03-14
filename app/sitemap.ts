import type { MetadataRoute } from 'next'
import { getStatesWithData, parseTractGeoid } from '@/lib/publishing/geo-crosswalk'
import { getSupabaseAdmin } from '@/lib/supabase/admin'

const BASE_URL = 'https://www.homecastr.com'
const SCHEMA = process.env.FORECAST_SCHEMA || "forecast_queue"

/**
 * Next.js 16 generateSitemaps — returns an array of { id } objects.
 * Next.js will automatically create a sitemap index at /sitemap.xml
 * and individual sitemaps at /sitemap/0.xml, /sitemap/1.xml, etc.
 */
export async function generateSitemaps() {
    return [
        { id: 0 }, // core pages
        { id: 1 }, // states
        { id: 2 }, // counties
        { id: 3 }, // tracts (part 1: rows 0–39999)
        { id: 4 }, // tracts (part 2: rows 40000+)
    ]
}

/** Paginated Supabase fetch for tract geoids */
async function fetchTracts(offset: number, limit: number) {
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

/** Paginated helper to stream all tracts from offset..offset+maxCount */
async function collectTracts(startOffset: number, maxCount: number) {
    const pageSize = 1000
    const results: string[] = []
    let offset = startOffset

    while (results.length < maxCount) {
        const fetchLimit = Math.min(pageSize, maxCount - results.length)
        const data = await fetchTracts(offset, fetchLimit)
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

    // ── Segment 3: Tract pages (first 40,000) ──
    if (id === 3) {
        const tracts = await collectTracts(0, 40000)
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

    // ── Segment 4: Tract pages (40,000+) ──
    if (id === 4) {
        const tracts = await collectTracts(40000, 40000)
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
