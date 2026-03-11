"use server"

import { STATE_FIPS, COUNTY_CITY, ZIP_CITY_NATIONAL, slugify } from "@/lib/publishing/geo-crosswalk"
import { COUNTY_NAMES } from "@/lib/publishing/county-fips"

export interface SearchGeoResult {
    type: "state" | "county" | "city" | "neighborhood" | "zip"
    name: string
    stateAbbr: string
    url: string
}

export async function searchGeographies(query: string): Promise<SearchGeoResult[]> {
    if (!query || query.length < 2) return []

    const normalized = query.toLowerCase().trim()
    const results: SearchGeoResult[] = []

    // 1. Search States
    for (const [fips, state] of Object.entries(STATE_FIPS)) {
        if (state.name.toLowerCase().includes(normalized) || state.abbr.toLowerCase() === normalized) {
            results.push({
                type: "state",
                name: state.name,
                stateAbbr: state.abbr,
                url: `/forecasts/${state.abbr.toLowerCase()}`,
            })
        }
    }

    // 2. Search Cities (from COUNTY_CITY)
    const matchedCities = new Set<string>()
    for (const [fips, city] of Object.entries(COUNTY_CITY)) {
        if (city.toLowerCase().includes(normalized)) {
            const stateFips = fips.substring(0, 2)
            const state = STATE_FIPS[stateFips]
            if (state) {
                const cityKey = `${city.toLowerCase()}-${state.abbr.toLowerCase()}`
                if (!matchedCities.has(cityKey)) {
                    matchedCities.add(cityKey)
                    results.push({
                        type: "city",
                        name: city,
                        stateAbbr: state.abbr,
                        url: `/forecasts/${state.abbr.toLowerCase()}/${slugify(city)}`,
                    })
                }
            }
        }
    }

    // 3. Search Counties
    for (const [fips, countyName] of Object.entries(COUNTY_NAMES)) {
        // Skip if exact same name as a city we just matched to avoid duplicate-looking MSAs
        if (countyName.toLowerCase().includes(normalized)) {
            const stateFips = fips.substring(0, 2)
            const state = STATE_FIPS[stateFips]
            if (state) {
                // Determine if this county maps to a known city page, or a synthetic "county-XXXXX" page
                // The geo-crosswalk links to the generic city slug if it matches COUNTY_CITY or COUNTY_NAMES.
                const mappedCity = COUNTY_CITY[fips] || countyName
                results.push({
                    type: "county",
                    name: `${countyName} County`,
                    stateAbbr: state.abbr,
                    url: `/forecasts/${state.abbr.toLowerCase()}/${slugify(mappedCity)}`,
                })
            }
        }
    }

    // 4. Search ZIPs / Neighborhoods (from ZIP_CITY_NATIONAL which has 30k+ entries)
    // To keep it fast and prevent massive lists, we'll slice if it's too big, or just search until we hit a limit
    let zipCount = 0
    for (const [zip, name] of Object.entries(ZIP_CITY_NATIONAL)) {
        if (zip === normalized || name.toLowerCase().includes(normalized)) {
            const type = /^\d{5}$/.test(normalized) ? "zip" : "neighborhood"
            // To figure out the URL, we technically need the state. 
            // We usually can't get the state directly from just ZIP easily without a ZCTA shapefile,
            // but we can query `parcel_ladder_v1` on the fly for exact ZIP matches to get the URL, OR 
            // we could construct a URL that gets redirected. 
            // Actually, we DO NOT have a direct state mapping for ZIP_CITY_NATIONAL natively in memory.
            // Let's rely on a global search page or use a Supabase query for ZIPs/Neighborhoods.
        }
    }

    return results.slice(0, 20)
}
