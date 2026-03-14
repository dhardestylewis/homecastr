
import { NextResponse } from "next/server"
import { getSupabaseServerClient } from "@/lib/supabase/server"
import { withRedisBinaryCache } from "@/lib/redis"

// Type for route params
interface RouteParams {
    params: {
        z: string
        x: string
        y: string
    }
}

// Helper: Convert Tile Z/X/Y to Web Mercator Bounding Box (EPSG:3857)
function tileToEnvelope(z: number, x: number, y: number) {
    const worldSize = 20037508.3427892
    const tileCount = Math.pow(2, z)
    const tileSize = (worldSize * 2) / tileCount

    const minX = -worldSize + x * tileSize
    const maxX = -worldSize + (x + 1) * tileSize
    const maxY = worldSize - y * tileSize
    const minY = worldSize - (y + 1) * tileSize

    return { minX, minY, maxX, maxY }
}

// Helper: Get H3 Resolution from Zoom (matching map-view.tsx)
function getH3Res(zoom: number) {
    if (zoom < 10.5) return 7
    if (zoom < 12.0) return 8
    if (zoom < 13.5) return 9
    if (zoom < 15.0) return 10
    return 11
}

export async function GET(
    request: Request,
    { params }: { params: Promise<{ z: string; x: string; y: string }> }
) {
    const { z: zStr, x: xStr, y: yStr } = await params

    const z = parseInt(zStr)
    const x = parseInt(xStr)
    const y = parseInt(yStr)

    // Get query params
    const { searchParams } = new URL(request.url)
    const year = parseInt(searchParams.get("year") || "2026")

    // Determine H3 Resolution
    const h3Res = getH3Res(z)

    // Redis cache key
    const cacheKey = `tile:h3:${z}:${x}:${y}:${year}:${h3Res}`

    try {
        console.log(`[TILE-API] z:${z} x:${x} y:${y} year:${year} res:${h3Res}`)

        const { data: cachedBuffer, fromCache } = await withRedisBinaryCache(
            cacheKey,
            async () => {
                const supabase = await getSupabaseServerClient()

                const { data, error } = await supabase.rpc('get_h3_tile_mvt', {
                    z,
                    x,
                    y,
                    query_year: year,
                    query_res: h3Res
                })

                if (error) {
                    console.error("[TILE-API] RPC Error:", {
                        message: error.message,
                        details: error.details,
                        hint: error.hint,
                        code: error.code,
                        params: { z, x, y, year, h3Res }
                    })
                    throw error
                }

                if (!data) {
                    return null
                }

                let buffer: Buffer
                if (typeof data === 'string') {
                    if (data.startsWith('\\x')) {
                        buffer = Buffer.from(data.substring(2), 'hex')
                    } else {
                        buffer = Buffer.from(data, 'base64')
                    }
                } else {
                    buffer = Buffer.from(data)
                }

                if (buffer.length === 0) {
                    return null
                }

                return buffer
            },
            14400,  // 4 hour TTL
        )

        if (fromCache) {
            console.log(`[TILE-CACHE] HIT ${z}/${x}/${y}`)
        }

        if (!cachedBuffer) {
            return new NextResponse(null, { status: 204 })
        }

        return new NextResponse(new Uint8Array(cachedBuffer), {
            status: 200,
            headers: {
                "Content-Type": "application/vnd.mapbox-vector-tile",
                "Cache-Control": "public, max-age=3600",
                "Access-Control-Allow-Origin": "*"
            }
        })

    } catch (e: any) {
        console.error("Tile Endpoint Error:", e)
        return NextResponse.json({ error: e.message }, { status: 500 })
    }
}

