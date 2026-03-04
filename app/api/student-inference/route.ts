import { NextResponse } from "next/server"

/**
 * Student viewport inference proxy — forwards bbox requests to Modal endpoint.
 * 
 * POST /api/student-inference
 *   Body: { bbox: [minLat, minLng, maxLat, maxLng], year?: number }
 *   Response: GeoJSON FeatureCollection with p10/p50/p90 per building
 */

// Modal web endpoint URL (deployed via `modal deploy`)
const MODAL_ENDPOINT_URL = process.env.STUDENT_INFERENCE_URL || "https://homecastr--student-viewport-inference-predict.modal.run"

// Cache for recent results (in-memory, per serverless instance)
const resultCache = new Map<string, { data: any; timestamp: number }>()
const CACHE_TTL_MS = 60 * 60 * 1000 // 1 hour

function getCacheKey(bbox: number[], year: number): string {
    // Round bbox to ~100m precision to enable cache hits on nearby viewports
    const rounded = bbox.map(v => Math.round(v * 100) / 100)
    return `${rounded.join(",")}_${year}`
}

export async function POST(request: Request) {
    try {
        const body = await request.json()
        const { bbox, year = 2026, include_forecast = false } = body

        if (!bbox || bbox.length !== 4) {
            return NextResponse.json(
                { error: "bbox must be [minLat, minLng, maxLat, maxLng]" },
                { status: 400 }
            )
        }

        // Validate bbox
        const [minLat, minLng, maxLat, maxLng] = bbox
        if (minLat >= maxLat || minLng >= maxLng) {
            return NextResponse.json(
                { error: "Invalid bbox: min must be less than max" },
                { status: 400 }
            )
        }

        // Limit bbox size (prevent huge requests)
        const latSpan = maxLat - minLat
        const lngSpan = maxLng - minLng
        if (latSpan > 0.5 || lngSpan > 0.5) {
            return NextResponse.json(
                { error: "Bbox too large. Max span is 0.5 degrees (~55km)." },
                { status: 400 }
            )
        }

        // Only cache Phase 2 (forecast) results
        const cacheKey = getCacheKey(bbox, year)
        if (include_forecast) {
            const cached = resultCache.get(cacheKey)
            if (cached && Date.now() - cached.timestamp < CACHE_TTL_MS) {
                console.log(`[STUDENT-INFERENCE] Cache hit (Phase 2): ${cacheKey}`)
                return NextResponse.json(cached.data, {
                    headers: {
                        "X-Cache": "HIT",
                        "Cache-Control": "public, max-age=3600",
                    }
                })
            }
        }

        if (!MODAL_ENDPOINT_URL) {
            return NextResponse.json(
                { error: "Student inference endpoint not configured. Set STUDENT_INFERENCE_URL." },
                { status: 503 }
            )
        }

        const phase = include_forecast ? "Phase 2 (forecast)" : "Phase 1 (buildings)"
        console.log(`[STUDENT-INFERENCE] ${phase}: bbox=${bbox} year=${year}`)

        // Forward to Modal endpoint, using request.signal so we drop the connection
        // immediately if the user moves the map quickly and the client aborts.
        const response = await fetch(MODAL_ENDPOINT_URL, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ bbox, year, include_forecast }),
            signal: request.signal,
        })

        if (!response.ok) {
            const errorText = await response.text()
            console.error(`[STUDENT-INFERENCE] Modal error: ${response.status} ${errorText}`)
            return NextResponse.json(
                { error: `Inference failed: ${response.status}` },
                { status: 502 }
            )
        }

        const result = await response.json()

        // Cache result
        resultCache.set(cacheKey, { data: result, timestamp: Date.now() })

        // Evict old cache entries
        if (resultCache.size > 100) {
            const oldest = [...resultCache.entries()]
                .sort((a, b) => a[1].timestamp - b[1].timestamp)
                .slice(0, 50)
            oldest.forEach(([key]) => resultCache.delete(key))
        }

        console.log(`[STUDENT-INFERENCE] Success: ${result.features?.length || 0} buildings in ${result.metadata?.latency_s || '?'}s`)

        return NextResponse.json(result, {
            headers: {
                "X-Cache": "MISS",
                "Cache-Control": "public, max-age=3600",
            }
        })

    } catch (error: any) {
        // Suppress logging noise if the user just panned the map and aborted the request
        if (error.name === "AbortError" || request.signal.aborted) {
            console.log("[STUDENT-INFERENCE] Request aborted by client map movement.")
            return NextResponse.json(
                { error: "Aborted by client" },
                { status: 499 } // 499 Client Closed Request
            )
        }

        console.error("[STUDENT-INFERENCE] Error:", error.message)

        if (error.name === "TimeoutError") {
            return NextResponse.json(
                { error: "Inference timed out. Try a smaller viewport." },
                { status: 504 }
            )
        }

        return NextResponse.json(
            { error: "Internal server error" },
            { status: 500 }
        )
    }
}

export async function GET() {
    return NextResponse.json({
        status: "ok",
        endpoint: "student-viewport-inference",
        configured: !!MODAL_ENDPOINT_URL,
    })
}
