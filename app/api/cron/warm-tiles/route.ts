import { NextResponse } from "next/server"

/**
 * Cache-warming cron endpoint.
 *
 * Pre-fetches forecast tiles for common landing viewports so the first
 * real visitor gets Redis cache HITs instead of cold PostgreSQL RPCs.
 *
 * Runs every 3 hours via Vercel Cron (matching the 4h Redis tile TTL
 * so tiles never go cold between warmings).
 *
 * Coverage:
 *   - US-wide view (z=4-5)   → ~6 tiles
 *   - Default landing (z=10) → NYC area → ~16 tiles
 *   - Popular metros at z=8  → Houston, LA, Chicago, Dallas, Miami, SF → ~6 tiles
 */

// Lat/Lng → tile coordinates at zoom z
function latLngToTile(lat: number, lng: number, z: number): { x: number; y: number } {
    const n = Math.pow(2, z)
    const x = Math.floor((lng + 180) / 360 * n)
    const latRad = lat * Math.PI / 180
    const y = Math.floor((1 - Math.log(Math.tan(latRad) + 1 / Math.cos(latRad)) / Math.PI) / 2 * n)
    return { x, y }
}

// Generate tile coordinates covering a viewport centered on (lat, lng) at zoom z
function viewportTiles(lat: number, lng: number, z: number, radius = 2): Array<{ z: number; x: number; y: number }> {
    const center = latLngToTile(lat, lng, z)
    const tiles: Array<{ z: number; x: number; y: number }> = []
    for (let dx = -radius; dx <= radius; dx++) {
        for (let dy = -radius; dy <= radius; dy++) {
            tiles.push({ z, x: center.x + dx, y: center.y + dy })
        }
    }
    return tiles
}

// Common viewports to warm
const VIEWPORTS = [
    // US-wide (low zoom, few tiles) — what Google bots and overview visitors see
    { lat: 39.8, lng: -98.5, z: 4, radius: 1 },   // ~9 tiles
    { lat: 39.8, lng: -98.5, z: 5, radius: 1 },   // ~9 tiles

    // Default landing viewport — NYC at z=10
    { lat: 40.7484, lng: -73.9857, z: 10, radius: 2 },  // ~25 tiles

    // Popular metros at z=8 (zoomed-out city view)
    { lat: 29.76, lng: -95.37, z: 8, radius: 1 },     // Houston
    { lat: 34.05, lng: -118.24, z: 8, radius: 1 },    // Los Angeles
    { lat: 41.88, lng: -87.62, z: 8, radius: 1 },     // Chicago
    { lat: 32.78, lng: -96.80, z: 8, radius: 1 },     // Dallas
    { lat: 25.76, lng: -80.19, z: 8, radius: 1 },     // Miami
    { lat: 37.77, lng: -122.42, z: 8, radius: 1 },    // San Francisco
    { lat: 47.60, lng: -122.33, z: 8, radius: 1 },    // Seattle
    { lat: 33.45, lng: -112.07, z: 8, radius: 1 },    // Phoenix
    { lat: 39.74, lng: -104.99, z: 8, radius: 1 },    // Denver
    { lat: 42.36, lng: -71.06, z: 8, radius: 1 },     // Boston
]

const ORIGIN_YEAR = 2024
const HORIZON_M = 24  // 2026 (default year)

export async function GET(request: Request) {
    // Verify cron secret in production
    const authHeader = request.headers.get("authorization")
    if (process.env.CRON_SECRET && authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
        return NextResponse.json({ error: "Unauthorized" }, { status: 401 })
    }

    const baseUrl = process.env.NEXT_PUBLIC_SITE_URL
        || process.env.VERCEL_URL
        || "http://localhost:3000"

    const protocol = baseUrl.startsWith("http") ? "" : "https://"
    const origin = `${protocol}${baseUrl}`

    // Collect all tiles to warm
    const allTiles: Array<{ z: number; x: number; y: number }> = []
    for (const vp of VIEWPORTS) {
        allTiles.push(...viewportTiles(vp.lat, vp.lng, vp.z, vp.radius))
    }

    // Deduplicate
    const seen = new Set<string>()
    const uniqueTiles = allTiles.filter(t => {
        const key = `${t.z}/${t.x}/${t.y}`
        if (seen.has(key)) return false
        seen.add(key)
        return true
    })

    console.log(`[WARM-TILES] Warming ${uniqueTiles.length} tiles...`)

    // Fetch in batches of 10 to avoid overwhelming the server
    const BATCH = 10
    let warmed = 0
    let errors = 0

    for (let i = 0; i < uniqueTiles.length; i += BATCH) {
        const batch = uniqueTiles.slice(i, i + BATCH)
        const results = await Promise.allSettled(
            batch.map(t =>
                fetch(
                    `${origin}/api/forecast-tiles/${t.z}/${t.x}/${t.y}?originYear=${ORIGIN_YEAR}&horizonM=${HORIZON_M}`,
                    { cache: "no-store" }  // Bypass browser/CDN cache to ensure Redis gets populated
                )
            )
        )
        for (const r of results) {
            if (r.status === "fulfilled" && (r.value.status === 200 || r.value.status === 204)) {
                warmed++
            } else {
                errors++
            }
        }
    }

    console.log(`[WARM-TILES] Done: ${warmed} warmed, ${errors} errors out of ${uniqueTiles.length} tiles`)

    return NextResponse.json({
        warmed,
        errors,
        total: uniqueTiles.length,
        timestamp: new Date().toISOString(),
    })
}
