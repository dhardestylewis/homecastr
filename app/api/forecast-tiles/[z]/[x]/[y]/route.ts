import { NextResponse } from "next/server"
import { getSupabaseAdmin } from "@/lib/supabase/admin"
import { withRedisBinaryCache } from "@/lib/redis"
import crypto from "crypto"

const TILE_HEADERS = {
    "Content-Type": "application/vnd.mapbox-vector-tile",
    "Cache-Control": "public, max-age=3600, s-maxage=86400, stale-while-revalidate=86400",  // 1 hr browser cache, 24 hr CDN cache
    "Access-Control-Allow-Origin": "*",
} as const

/** Return an empty MVT tile (204 No Content) which is the MapLibre standard for empty tiles */
function emptyTile() {
    return new NextResponse(null, {
        status: 204,
        headers: {
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "public, max-age=3600",  // 1 hr browser cache for empty regions
        },
    })
}

/**
 * Forecast choropleth MVT tile endpoint.
 *
 * Calls forecast_queue.mvt_choropleth_forecast(z,x,y,origin_year,horizon_m)
 * which auto-routes by zoom:
 *   z <= 4  → State (falls back to ZCTA if no state data)
 *   z <= 7  → ZCTA
 *   z <= 11 → Tract
 *   z <= 16 → Tabblock
 *   z >= 17 → Parcel (capped at 3500)
 *
 * Tiles are cached in Redis (4h TTL) to avoid repeated PostgreSQL RPC calls.
 *
 * Query params:
 *   originYear  (default 2025)
 *   horizonM    (default 12)
 *   seriesKind  (default 'forecast')
 *   variantId   (default '__forecast__')
 *   level       (optional override: state/zcta/tract/tabblock/parcel/unsd/neighborhood)
 */
export async function GET(
    request: Request,
    { params }: { params: Promise<{ z: string; x: string; y: string }> }
) {
    const { z: zStr, x: xStr, y: yStr } = await params

    const z = parseInt(zStr)
    const x = parseInt(xStr)
    const y = parseInt(yStr)

    const { searchParams } = new URL(request.url)
    const originYear = parseInt(searchParams.get("originYear") || "2025")
    const horizonM = parseInt(searchParams.get("horizonM") || "12")
    const seriesKind = searchParams.get("seriesKind") || "forecast"
    const variantId = searchParams.get("variantId") || "__forecast__"
    const levelOverride = searchParams.get("level") || null
    const schemaName = searchParams.get("schema") || "forecast_queue"

    // Redis cache key encodes all parameters that affect tile content
    const cacheKey = `tile:forecast:${schemaName}:${z}:${x}:${y}:${originYear}:${horizonM}:${seriesKind}:${variantId}:${levelOverride || '_'}`

    const { data: cachedBuffer, fromCache } = await withRedisBinaryCache(
        cacheKey,
        async () => {
            const rpcParams = {
                z,
                x,
                y,
                p_origin_year: originYear,
                p_horizon_m: horizonM,
                p_level_override: levelOverride,
                p_series_kind: seriesKind,
                p_variant_id: variantId,
                p_run_id: null,
                p_backtest_id: null,
                p_parcel_limit: 3500,
            }

            // Retry up to 3 times with exponential backoff on transient errors
            for (let attempt = 0; attempt < 3; attempt++) {
                try {
                    const supabase = getSupabaseAdmin()

                    const { data, error } = await supabase
                        .schema(schemaName as any)
                        .rpc("mvt_choropleth_forecast", rpcParams)

                    if (error) {
                        console.error(`[FORECAST-TILE] RPC error (attempt ${attempt + 1}/3):`, {
                            message: error.message,
                            code: error.code,
                            tile: `${z}/${x}/${y}`,
                        })
                        if (attempt < 2) {
                            await new Promise((r) => setTimeout(r, 200 * Math.pow(3, attempt)))
                            continue
                        }
                        return null
                    }

                    if (!data) {
                        return null
                    }

                    // Supabase bytea → Buffer
                    let buffer: Buffer
                    if (typeof data === "string") {
                        if (data.startsWith("\\x")) {
                            buffer = Buffer.from(data.substring(2), "hex")
                        } else {
                            buffer = Buffer.from(data, "base64")
                        }
                    } else {
                        buffer = Buffer.from(data)
                    }

                    if (buffer.length === 0) {
                        return null
                    }

                    return buffer
                } catch (e: any) {
                    console.error(`[FORECAST-TILE] Exception (attempt ${attempt + 1}/3):`, e.message, `tile=${z}/${x}/${y}`)
                    if (attempt < 2) {
                        await new Promise((r) => setTimeout(r, 200 * Math.pow(3, attempt)))
                        continue
                    }
                    return null
                }
            }

            return null
        },
        14400,  // 4 hour TTL
    )

    if (!cachedBuffer) {
        return emptyTile()
    }

    // Generate ETag from the cache key to allow 304 Not Modified responses
    // if the user pans away and back
    const etag = `"${crypto.createHash('md5').update(cacheKey).digest('hex')}"`;
    
    // Check if the client already has this version
    const ifNoneMatch = request.headers.get("if-none-match");
    if (ifNoneMatch === etag) {
        return new NextResponse(null, {
            status: 304,
            headers: {
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": TILE_HEADERS["Cache-Control"],
                "ETag": etag,
            }
        });
    }

    return new NextResponse(new Uint8Array(cachedBuffer), {
        status: 200,
        headers: {
            ...TILE_HEADERS,
            "ETag": etag,
        },
    })
}

