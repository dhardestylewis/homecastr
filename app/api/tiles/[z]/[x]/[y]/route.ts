
import { NextResponse } from "next/server"
import { getSupabaseServerClient } from "@/lib/supabase/server"

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
    // Note: We used to use 'scale' but here we have 'z'. 
    // We should strictly map z to res.
    const h3Res = getH3Res(z)

    // Bounds for ST_MakeEnvelope (EPSG:3857)
    const bbox = tileToEnvelope(z, x, y) // { minX, minY, maxX, maxY }

    // SQL Logic:
    // 1. Select grid cells in this tile envelope.
    // 2. Join with details/rows for the requested year.
    // 3. Convert geometry to MVT using ST_AsMVTGeom
    // 4. Wrap result in ST_AsMVT

    const supabase = await getSupabaseServerClient()

    try {
        console.log(`[TILE-API] z:${z} x:${x} y:${y} year:${year} res:${h3Res}`)

        // Call the RPC function we just created
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
            return NextResponse.json({ error: error.message }, { status: 500 })
        }

        if (!data) {
            return new NextResponse(null, { status: 204 })
        }

        // Supabase bytea returns a base64 string or hex string depending on config,
        // but often it's just handled as binary if using the right client settings.
        // If it's a string, we might need to convert it.
        let buffer: Buffer
        if (typeof data === 'string') {
            // Check if it's hex (common for bytea) or base64
            if (data.startsWith('\\x')) {
                buffer = Buffer.from(data.substring(2), 'hex')
            } else {
                buffer = Buffer.from(data, 'base64')
            }
        } else {
            buffer = Buffer.from(data)
        }

        return new NextResponse(new Uint8Array(buffer), {
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

