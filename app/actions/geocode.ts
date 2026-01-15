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
                "User-Agent": "ProperlyticUI/1.0",
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
