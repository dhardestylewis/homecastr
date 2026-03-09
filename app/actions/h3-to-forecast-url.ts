"use server"

/**
 * h3-to-forecast-url.ts
 *
 * Given the lat/lng of an H3 cell (from DetailsResponse.coordinates),
 * resolve the corresponding /forecasts/[state]/[city] URL by reverse-geocoding
 * via Nominatim and mapping to our slug conventions.
 *
 * Returns null if the geography cannot be resolved or is not in our forecast data.
 */

import { getStatesWithData, getCitiesForState } from "@/lib/publishing/geo-crosswalk"

// State name → slug mapping (ISO 3166-2 abbreviation → lowercase)
const STATE_NAME_TO_SLUG: Record<string, string> = {
    "Alabama": "al", "Alaska": "ak", "Arizona": "az", "Arkansas": "ar", "California": "ca",
    "Colorado": "co", "Connecticut": "ct", "Delaware": "de", "District of Columbia": "dc",
    "Florida": "fl", "Georgia": "ga", "Hawaii": "hi", "Idaho": "id", "Illinois": "il",
    "Indiana": "in", "Iowa": "ia", "Kansas": "ks", "Kentucky": "ky", "Louisiana": "la",
    "Maine": "me", "Maryland": "md", "Massachusetts": "ma", "Michigan": "mi",
    "Minnesota": "mn", "Mississippi": "ms", "Missouri": "mo", "Montana": "mt",
    "Nebraska": "ne", "Nevada": "nv", "New Hampshire": "nh", "New Jersey": "nj",
    "New Mexico": "nm", "New York": "ny", "North Carolina": "nc", "North Dakota": "nd",
    "Ohio": "oh", "Oklahoma": "ok", "Oregon": "or", "Pennsylvania": "pa",
    "Rhode Island": "ri", "South Carolina": "sc", "South Dakota": "sd", "Tennessee": "tn",
    "Texas": "tx", "Utah": "ut", "Vermont": "vt", "Virginia": "va", "Washington": "wa",
    "West Virginia": "wv", "Wisconsin": "wi", "Wyoming": "wy", "Puerto Rico": "pr",
}

// County name → first matching city slug (populated from geo-crosswalk on demand)
// We use a soft fuzzy match against the city list to stay robust.
function slugify(name: string): string {
    return name
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/^-+|-+$/g, "")
}

function scoreCityMatch(countyName: string, citySlug: string): number {
    const countySlug = slugify(countyName)
    if (countySlug === citySlug) return 100
    if (citySlug.includes(countySlug) || countySlug.includes(citySlug)) return 50
    // Partial token overlap
    const countyTokens = countySlug.split("-")
    const cityTokens = citySlug.split("-")
    const overlap = countyTokens.filter(t => cityTokens.includes(t)).length
    if (overlap > 0) return overlap * 10
    return 0
}

const SCHEMA = process.env.FORECAST_SCHEMA || "forecast_queue"

export async function h3ToForecastUrl(
    lat: number,
    lng: number
): Promise<{ href: string; stateSlug: string; citySlug: string } | null> {
    try {
        // 1. Reverse-geocode via Nominatim (same API as h3-details.ts)
        const url = `https://nominatim.openstreetmap.org/reverse?lat=${lat.toFixed(6)}&lon=${lng.toFixed(6)}&format=json&zoom=10&addressdetails=1`
        const res = await fetch(url, {
            headers: { "User-Agent": "Homecastr/1.0 (contact@homecastr.com)" },
            next: { revalidate: 3600 }, // Cache for 1 hour
        })
        if (!res.ok) return null

        const data = await res.json()
        const addr = data?.address ?? {}

        // 2. Extract state and county/city from Nominatim address
        const stateName: string = addr.state ?? ""
        const countyName: string = (addr.county ?? addr.city_district ?? addr.city ?? "")
            .replace(/ County$/, "")
            .replace(/ Parish$/, "")
            .replace(/ Borough$/, "")
            .trim()

        if (!stateName) return null

        const stateSlug = STATE_NAME_TO_SLUG[stateName]
        if (!stateSlug) return null

        // 3. Fetch available cities for this state from our forecast data
        const cities = await getCitiesForState(stateSlug, SCHEMA)
        if (cities.length === 0) return null

        // 4. Find best-matching city
        let bestSlug: string | null = null
        let bestScore = 0

        for (const c of cities) {
            const score = scoreCityMatch(countyName, c.citySlug)
            if (score > bestScore) {
                bestScore = score
                bestSlug = c.citySlug
            }
        }

        // Require at least a partial match
        if (!bestSlug || bestScore < 10) {
            // Fallback: just link to the state page
            return { href: `/forecasts/${stateSlug}`, stateSlug, citySlug: "" }
        }

        return {
            href: `/forecasts/${stateSlug}/${bestSlug}`,
            stateSlug,
            citySlug: bestSlug,
        }
    } catch (err) {
        console.error("[h3ToForecastUrl] Error:", err)
        return null
    }
}
