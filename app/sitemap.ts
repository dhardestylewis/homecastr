import type { MetadataRoute } from "next"
import { getStatesWithData, getCitiesForState, getTractsForCity } from "@/lib/publishing/geo-crosswalk"

const SCHEMA = process.env.FORECAST_SCHEMA || "forecast_queue"
const BASE_URL = process.env.NEXT_PUBLIC_SITE_URL || "https://homecastr.com"

export default async function sitemap(): Promise<MetadataRoute.Sitemap> {
    const entries: MetadataRoute.Sitemap = []

    // Static Routes
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

    try {
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
        console.error("[SITEMAP] Error generating sitemap:", err)
    }

    return entries
}
