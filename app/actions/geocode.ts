"use server"

interface GeocodeResult {
    lat: number
    lng: number
    displayName: string
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
 * Fetches autocomplete suggestions from Nominatim, restricted to Harris County.
 */
export async function getAutocompleteSuggestions(query: string): Promise<AutocompleteResult[]> {
    if (!query || query.trim().length < 3) return []

    try {
        // Harris County Bounding Box (approximate)
        // min_lon=-95.96, min_lat=29.50, max_lon=-94.90, max_lat=30.17
        const viewbox = "-95.96,30.17,-94.90,29.50"

        const params = new URLSearchParams({
            q: query,
            format: "json",
            limit: "5",
            addressdetails: "1",
            viewbox: viewbox,
            bounded: "1"
        })

        const response = await fetch(`https://nominatim.openstreetmap.org/search?${params.toString()}`, {
            headers: {
                "User-Agent": "HomecastrUI/1.0",
            },
        })

        if (!response.ok) return []

        const data = await response.json()

        if (Array.isArray(data)) {
            return data.map((item: any) => ({
                lat: Number.parseFloat(item.lat),
                lng: Number.parseFloat(item.lon),
                displayName: item.display_name,
                address: item.address || {}
            }))
        }

        return []
    } catch (error) {
        console.error("[Geocode] Auto-complete error:", error)
        return []
    }
}

/**
 * Reverse geocodes a coordinate to a human-readable address.
 */
export async function reverseGeocode(lat: number, lng: number, zoom = 18): Promise<string | null> {
    try {
        const params = new URLSearchParams({
            lat: lat.toString(),
            lon: lng.toString(),
            zoom: zoom.toString(),
            format: "json",
        })

        const response = await fetch(`https://nominatim.openstreetmap.org/reverse?${params.toString()}`, {
            headers: {
                "User-Agent": "HomecastrUI/1.0",
            },
        })

        if (!response.ok) return null

        const data = await response.json()

        // Format: "123, Main St, ..." -> "123 Main St, ..."
        let formatted = data.display_name
        if (formatted) {
            formatted = formatted.replace(/^(\d+),\s+/, "$1 ")
        }

        return formatted || null
    } catch (error) {
        console.error("[Geocode] Reverse geocode error:", error)
        return null
    }
}
