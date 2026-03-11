import { NextRequest, NextResponse } from "next/server"
import { enrichWithNeighborhood } from "@/lib/publishing/geo-crosswalk"

// ── Throttle for Nominatim (free, 1 req/sec) ──
let lastNominatimTime = 0

async function nominatimThrottle() {
    const now = Date.now()
    const elapsed = now - lastNominatimTime
    if (elapsed < 1100) {
        await new Promise((r) => setTimeout(r, 1100 - elapsed))
    }
    lastNominatimTime = Date.now()
}

// ── Provider: Nominatim (free, street-level) ──
async function fetchNominatim(lat: string, lng: string, zoom: string) {
    await nominatimThrottle()
    const url = `https://nominatim.openstreetmap.org/reverse?lat=${lat}&lon=${lng}&zoom=${zoom}&format=json&addressdetails=1`
    const res = await fetch(url, {
        headers: { "User-Agent": "HomecastrUI/1.0 (properlytic.com)" },
    })
    if (res.status === 429) return { rateLimited: true, data: null }
    if (!res.ok) return { rateLimited: false, data: null }
    return { rateLimited: false, data: await res.json() }
}

// ── Provider: Mapbox (paid, street-level) ──
async function fetchMapbox(lat: string, lng: string) {
    const token = process.env.MAPBOX_SECRET_TOKEN
    if (!token) return null

    try {
        const url = `https://api.mapbox.com/search/geocode/v6/reverse?longitude=${lng}&latitude=${lat}&access_token=${token}`
        const res = await fetch(url)
        if (!res.ok) return null

        const data = await res.json()
        const feature = data.features?.[0]
        if (!feature) return null

        const ctx = feature.properties?.context || {}
        const addr = feature.properties?.full_address || feature.properties?.name || ""

        // Map Mapbox response → Nominatim-compatible shape
        return {
            address: {
                house_number: ctx.address?.address_number || null,
                road: ctx.street?.name || null,
                suburb: ctx.neighborhood?.name || ctx.locality?.name || null,
                neighbourhood: ctx.neighborhood?.name || null,
                city: ctx.place?.name || null,
            },
            display_name: addr,
        }
    } catch {
        return null
    }
}

// ── Provider: BigDataCloud (free, neighbourhood only) ──
async function fetchBigDataCloud(lat: string, lng: string) {
    try {
        const url = `https://api.bigdatacloud.net/data/reverse-geocode-client?latitude=${lat}&longitude=${lng}&localityLanguage=en`
        const res = await fetch(url)
        if (!res.ok) return null

        const bdc = await res.json()
        const city = bdc.city || bdc.locality || null

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

        return {
            address: {
                house_number: null,
                road: null,
                suburb: neighbourhood,
                neighbourhood: neighbourhood,
                city: city,
            },
            display_name: [neighbourhood, city].filter(Boolean).join(", "),
        }
    } catch {
        return null
    }
}

export async function GET(req: NextRequest) {
    const lat = req.nextUrl.searchParams.get("lat")
    const lng = req.nextUrl.searchParams.get("lng")
    const level = req.nextUrl.searchParams.get("level") || "tract"
    const geoid = req.nextUrl.searchParams.get("geoid")

    if (!lat || !lng) {
        return NextResponse.json({ error: "lat and lng required" }, { status: 400 })
    }

    const cacheHeaders = { "Cache-Control": "public, max-age=86400" }

    try {
        // ═══════════════════════════════════════════════════
        // TRACT: Internal Crosswalk (geo-crosswalk.ts)
        // ═══════════════════════════════════════════════════
        if (level === "tract" && geoid) {
            try {
                const geo = await enrichWithNeighborhood({ tractGeoid: geoid } as any)
                if (geo?.neighborhoodName) {
                    return NextResponse.json({
                        address: {
                            suburb: geo.neighborhoodName,
                            neighbourhood: geo.neighborhoodName
                        },
                        display_name: geo.neighborhoodName
                    }, { headers: cacheHeaders })
                }
            } catch (err) {
                console.error("[GEOCODE PROXY] enrichWithNeighborhood failed:", err)
            }
        }

        // ═══════════════════════════════════════════════════
        // ZCTA / FALLBACK: BigDataCloud
        //   → fallback: Nominatim → empty
        // ═══════════════════════════════════════════════════
        if (level === "tract" || level === "zcta") {
            const bdc = await fetchBigDataCloud(lat, lng)
            if (bdc?.address?.suburb) {
                return NextResponse.json(bdc, { headers: cacheHeaders })
            }
            // Fallback to Nominatim for tract
            const nom = await fetchNominatim(lat, lng, "14")
            if (nom.data) {
                return NextResponse.json(nom.data, { headers: cacheHeaders })
            }
            // Return whatever BigDataCloud had (even without suburb)
            if (bdc) return NextResponse.json(bdc, { headers: cacheHeaders })
            return NextResponse.json({ address: {}, display_name: "" }, { headers: cacheHeaders })
        }

        // ═══════════════════════════════════════════════════
        // BLOCK / PARCEL: Nominatim → Mapbox → BigDataCloud
        //   Cascade with graceful fallback at each tier
        // ═══════════════════════════════════════════════════
        const zoom = level === "parcel" ? "18" : "17"

        // 1) Nominatim (free, best street detail)
        const nom = await fetchNominatim(lat, lng, zoom)
        if (nom.data?.address?.road) {
            return NextResponse.json(nom.data, { headers: cacheHeaders })
        }
        if (nom.rateLimited) {
            console.log("[GEOCODE PROXY] Nominatim 429 — trying Mapbox")
        }

        // 2) Mapbox (paid, reliable street detail)
        const mbx = await fetchMapbox(lat, lng)
        if (mbx?.address?.road) {
            return NextResponse.json(mbx, { headers: cacheHeaders })
        }

        // 3) BigDataCloud (free, neighbourhood only)
        const bdc = await fetchBigDataCloud(lat, lng)
        if (bdc?.address?.suburb) {
            return NextResponse.json(bdc, { headers: cacheHeaders })
        }

        // 4) Return best available (even if incomplete → client shows raw ID)
        return NextResponse.json(
            nom.data || mbx || bdc || { address: {}, display_name: "" },
            { headers: cacheHeaders }
        )
    } catch (err) {
        console.error("[GEOCODE PROXY] Error:", err)
        return NextResponse.json({ error: "Geocode failed" }, { status: 500 })
    }
}
