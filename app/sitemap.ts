import type { MetadataRoute } from "next"

const BASE_URL = process.env.NEXT_PUBLIC_SITE_URL || "https://www.homecastr.com"

export default async function sitemap(): Promise<MetadataRoute.Sitemap> {
    const entries: MetadataRoute.Sitemap = []

    // Static Routes (always included regardless of DB availability)
    entries.push({
        url: `${BASE_URL}`,
        lastModified: new Date(),
        changeFrequency: "weekly",
        priority: 1.0,
    })
    entries.push({
        url: `${BASE_URL}/app`,
        lastModified: new Date(),
        changeFrequency: "daily",
        priority: 0.9,
    })
    const staticBaseRoutes = ['/methodology', '/coverage/houston', '/faq', '/for-investors', '/for-agents']
    for (const route of staticBaseRoutes) {
        entries.push({
            url: `${BASE_URL}${route}`,
            lastModified: new Date(),
            changeFrequency: "monthly",
            priority: 0.8,
        })
    }

    // Forecasts index
    entries.push({
        url: `${BASE_URL}/forecasts`,
        lastModified: new Date(),
        changeFrequency: "daily",
        priority: 0.9,
    })

    // Dynamic Routes — wrapped in resilient try/catch with dynamic import
    // so that if geo-crosswalk or its dependencies fail, the sitemap still
    // returns the static entries above.
    try {
        const SCHEMA = process.env.FORECAST_SCHEMA || "forecast_queue"
        const { getStatesWithData, getCitiesForState } = await import("@/lib/publishing/geo-crosswalk")

        const states = await getStatesWithData(SCHEMA)

        for (const state of states) {
            // State hub
            entries.push({
                url: `${BASE_URL}/forecasts/${state.stateSlug}`,
                lastModified: new Date(),
                changeFrequency: "weekly",
                priority: 0.8,
            })

            const cities = await getCitiesForState(state.stateSlug, SCHEMA)

            for (const city of cities) {
                // City hub
                entries.push({
                    url: `${BASE_URL}/forecasts/${state.stateSlug}/${city.citySlug}`,
                    lastModified: new Date(),
                    changeFrequency: "weekly",
                    priority: 0.7,
                })
            }
        }
    } catch (err) {
        console.error("[SITEMAP] Error generating dynamic sitemap entries (static entries still included):", err)
    }

    return entries
}
