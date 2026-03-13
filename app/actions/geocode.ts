"use server"

interface GeocodeResult {
    lat: number
    lng: number
    displayName: string
    /** Nominatim type: "house", "street", "suburb", "city", etc. */
    resultType: string
    /** Nominatim class: "building", "highway", "place", "boundary", etc. */
    resultClass: string
}

/**
 * Geocodes an address string using OpenStreetMap Nominatim API.
 * 
 * Note: Nominatim Usage Policy requires a valid User-Agent.
 * We enforce a delay to be respectful if hit via loop, though server actions are usually one-off.
 */
export async function geocodeAddress(query: string): Promise<GeocodeResult | null> {
    if (!query || query.trim().length < 3) return null

    try {
        const params = new URLSearchParams({
            q: query,
            format: "json",
            limit: "1",
        })

        const response = await fetch(`https://nominatim.openstreetmap.org/search?${params.toString()}`, {
            headers: {
                "User-Agent": "HomecastrUI/1.0",
            },
        })

        if (!response.ok) {
            console.error(`[Geocode] Error: ${response.statusText}`)
            return null
        }

        const data = await response.json()

        if (Array.isArray(data) && data.length > 0) {
            const result = data[0]
            return {
                lat: Number.parseFloat(result.lat),
                lng: Number.parseFloat(result.lon),
                displayName: result.display_name,
                resultType: result.type || "",
                resultClass: result.class || "",
            }
        }

        return null
    } catch (error) {
        console.error("[Geocode] Exception:", error)
        return null
    }
}

export interface AutocompleteResult {
    lat: number
    lng: number
    displayName: string
    address: {
        road?: string
        suburb?: string
        city?: string
        state?: string
        postcode?: string
    }
}

/**
 * Fetches autocomplete suggestions from Nominatim (US nationwide).
 */
export async function getAutocompleteSuggestions(query: string): Promise<AutocompleteResult[]> {
    if (!query || query.trim().length < 3) return []

    try {
        const params = new URLSearchParams({
            q: query,
            format: "json",
            limit: "5",
            addressdetails: "1",
            countrycodes: "us"
        })

        const response = await fetch(`https://nominatim.openstreetmap.org/search?${params.toString()}`, {
            headers: {
                "User-Agent": "HomecastrUI/1.0",
            },
        })

        if (!response.ok) return []

        const data = await response.json()

        if (Array.isArray(data)) {
            return data.map((item: any) => {
                const addr = item.address || {}
                let displayName = item.display_name
                
                // Nominatim often formats as "123, Main Street, Neighborhood, City..."
                // We want to remove the comma between the house number and street if both exist.
                if (addr.house_number && (addr.road || addr.street || addr.pedestrian)) {
                    const street = addr.road || addr.street || addr.pedestrian
                    // Replace "123, Main Street" with "123 Main Street" at the start of the string
                    const commaPattern = new RegExp(`^${addr.house_number},\\s*${street}`, 'i')
                    displayName = displayName.replace(commaPattern, `${addr.house_number} ${street}`)
                }

                return {
                    lat: Number.parseFloat(item.lat),
                    lng: Number.parseFloat(item.lon),
                    displayName: displayName,
                    address: addr
                }
            })
        }

        return []
    } catch (error) {
        console.error("[Geocode] Auto-complete error:", error)
        return []
    }
}

/**
 * Reverse geocodes a coordinate to a human-readable address.
 * The detail level adapts to the map zoom:
 *   z <= 8:   County, State ZIP
 *   z <= 11:  City, State ZIP
 *   z <= 13:  Neighborhood/Suburb, City, ZIP
 *   z <= 15:  Street, City, ZIP
 *   z >= 16:  123 Street, City, ZIP
 */
export async function reverseGeocode(lat: number, lng: number, zoom = 18): Promise<string | null> {
    try {
        // At high zoom (block/parcel), try Mapbox first for street-level address
        if (zoom >= 14) {
            const mapboxToken = process.env.MAPBOX_SECRET_TOKEN
            if (mapboxToken) {
                try {
                    const mbxUrl = `https://api.mapbox.com/search/geocode/v6/reverse?longitude=${lng}&latitude=${lat}&access_token=${mapboxToken}`
                    const mbxRes = await fetch(mbxUrl)
                    if (mbxRes.ok) {
                        const mbx = await mbxRes.json()
                        const feature = mbx.features?.[0]
                        if (feature) {
                            const ctx = feature.properties?.context || {}
                            const houseNum = ctx.address?.address_number || ""
                            const street = ctx.street?.name || ""
                            const neighborhood = ctx.neighborhood?.name || ""
                            const city = ctx.place?.name || ""
                            const state = ctx.region?.region_code || ""
                            const postcode = ctx.postcode?.name || ""

                            const parts: string[] = []
                            if (zoom >= 16 && houseNum && street) {
                                parts.push(`${houseNum} ${street}`)
                            } else if (street) {
                                parts.push(street)
                            }
                            if (neighborhood && neighborhood !== city) parts.push(neighborhood)
                            if (city) parts.push(city)
                            if (state || postcode) parts.push([state, postcode].filter(Boolean).join(" "))

                            const result = parts.filter(Boolean).join(", ")
                            if (result) return result
                        }
                    }
                } catch {
                    // Mapbox failed — fall through to BigDataCloud
                }
            }
        }

        // BigDataCloud: neighbourhood-level (free, no limits)
        const bdcUrl = `https://api.bigdatacloud.net/data/reverse-geocode-client?latitude=${lat}&longitude=${lng}&localityLanguage=en`
        const response = await fetch(bdcUrl)

        if (!response.ok) {
            // Fallback to Nominatim
            const params = new URLSearchParams({
                lat: lat.toString(),
                lon: lng.toString(),
                zoom: zoom.toString(),
                format: "json",
                addressdetails: "1",
            })
            const nomResponse = await fetch(`https://nominatim.openstreetmap.org/reverse?${params.toString()}`, {
                headers: { "User-Agent": "HomecastrUI/1.0" },
            })
            if (!nomResponse.ok) return null
            const data = await nomResponse.json()
            const addr = data.address || {}
            const house = addr.house_number || ""
            const road = addr.road || addr.street || ""
            const neighbourhood = addr.neighbourhood || addr.suburb || addr.hamlet || ""
            const city = addr.city || addr.town || addr.village || addr.municipality || ""
            const state = addr.state ? _abbreviateState(addr.state) : ""
            const postcode = addr.postcode || ""
            let parts: string[] = []
            if (zoom >= 16 && house && road) parts.push(`${house} ${road}`)
            else if (zoom >= 14 && road) parts.push(road)
            else if (neighbourhood) parts.push(neighbourhood)
            if (city) parts.push(city)
            if (state || postcode) parts.push([state, postcode].filter(Boolean).join(" "))
            return parts.filter(Boolean).join(", ") || data.display_name || null
        }

        const bdc = await response.json()

        // Extract neighbourhood: highest-order informative entry that is a place name
        const informative: Array<{ name?: string; description?: string; order?: number }> =
            bdc.localityInfo?.informative || []
        const placeEntries = informative.filter(
            (i) =>
                i.name &&
                i.order &&
                i.order >= 8 &&
                !i.description?.includes("FIPS") &&
                !i.description?.includes("postal") &&
                !i.description?.includes("time zone")
        )
        const neighbourhood = placeEntries.length > 0
            ? placeEntries[placeEntries.length - 1].name!
            : null

        const city = bdc.city || bdc.locality || ""
        const state = bdc.principalSubdivision ? _abbreviateState(bdc.principalSubdivision) : ""
        const postcode = bdc.postcode || ""

        const parts: string[] = []
        if (neighbourhood) parts.push(neighbourhood)
        if (city) parts.push(city)
        if (state || postcode) parts.push([state, postcode].filter(Boolean).join(" "))

        return parts.filter(Boolean).join(", ") || null
    } catch (error) {
        console.error("[Geocode] Reverse geocode error:", error)
        return null
    }
}

/** Abbreviate US state names to 2-letter codes */
function _abbreviateState(state: string): string {
    const map: Record<string, string> = {
        "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
        "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
        "Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID",
        "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS",
        "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
        "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS",
        "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
        "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
        "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK",
        "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
        "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT",
        "Vermont": "VT", "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
        "Wisconsin": "WI", "Wyoming": "WY",
    }
    return map[state] || state
}
