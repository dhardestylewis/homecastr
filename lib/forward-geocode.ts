/**
 * Server-side forward geocoding: address string → { lat, lng }.
 * Uses Nominatim (free, 1 req/sec) with Mapbox fallback if MAPBOX_SECRET_TOKEN is set.
 */

// ── Nominatim throttle (shared across invocations in same process) ──
let lastNominatimTime = 0

async function nominatimThrottle() {
    const now = Date.now()
    const elapsed = now - lastNominatimTime
    if (elapsed < 1100) {
        await new Promise((r) => setTimeout(r, 1100 - elapsed))
    }
    lastNominatimTime = Date.now()
}

export interface ForwardGeocodeResult {
    lat: number
    lng: number
    displayName: string
}

/**
 * Forward-geocode an address string to coordinates.
 * Tries Nominatim first, then Mapbox if available.
 */
export async function forwardGeocode(address: string): Promise<ForwardGeocodeResult | null> {
    if (!address || address.trim().length < 3) return null

    // 1) Nominatim (free)
    try {
        await nominatimThrottle()
        const params = new URLSearchParams({
            q: address,
            format: "json",
            limit: "1",
            countrycodes: "us",
        })
        const res = await fetch(
            `https://nominatim.openstreetmap.org/search?${params.toString()}`,
            { headers: { "User-Agent": "HomecastrAPI/1.0 (homecastr.ai)" } }
        )
        if (res.ok) {
            const data = await res.json()
            if (Array.isArray(data) && data.length > 0) {
                return {
                    lat: parseFloat(data[0].lat),
                    lng: parseFloat(data[0].lon),
                    displayName: data[0].display_name,
                }
            }
        }
    } catch (e) {
        console.error("[ForwardGeocode] Nominatim error:", e)
    }

    // 2) Mapbox fallback (paid, more reliable)
    const mapboxToken = process.env.MAPBOX_SECRET_TOKEN
    if (mapboxToken) {
        try {
            const encoded = encodeURIComponent(address)
            const url = `https://api.mapbox.com/search/geocode/v6/forward?q=${encoded}&country=us&limit=1&access_token=${mapboxToken}`
            const res = await fetch(url)
            if (res.ok) {
                const data = await res.json()
                const feature = data.features?.[0]
                if (feature?.geometry?.coordinates) {
                    const [lng, lat] = feature.geometry.coordinates
                    return {
                        lat,
                        lng,
                        displayName: feature.properties?.full_address || feature.properties?.name || address,
                    }
                }
            }
        } catch (e) {
            console.error("[ForwardGeocode] Mapbox error:", e)
        }
    }

    return null
}
