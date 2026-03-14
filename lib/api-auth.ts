import { NextRequest, NextResponse } from "next/server"
import { getSupabaseAdmin } from "@/lib/supabase/admin"
import { logApiUsage, type ApiUsageEntry } from "@/lib/api-usage"

// ── Demo key: allows zero-signup API testing ──
const DEMO_KEY = "hc_demo_public_readonly"
const DEMO_LIMIT = 50 // requests per hour per IP
const demoRateLimit = new Map<string, { count: number; resetAt: number }>()

// ── RapidAPI proxy: verify requests coming through RapidAPI marketplace ──
const RAPIDAPI_PROXY_SECRET = process.env.RAPIDAPI_PROXY_SECRET || ""

/**
 * Validate an API key from the request headers.
 * Returns the key row if valid, null if invalid.
 */
export async function validateApiKey(req: NextRequest) {
    const apiKey = req.headers.get("x-api-key")

    if (!apiKey) return null

    // Demo key — skip DB lookup
    if (apiKey === DEMO_KEY) {
        return { id: "demo", email: "demo@homecastr.ai", key: DEMO_KEY, created_at: null, revoked_at: null }
    }

    try {
        const supabase = getSupabaseAdmin()
        const { data, error } = await supabase
            .from("api_keys")
            .select("id, email, key, created_at, revoked_at")
            .eq("key", apiKey)
            .single()

        if (error || !data) return null

        // Check if key has been revoked
        if (data.revoked_at) return null

        return data
    } catch {
        return null
    }
}

/**
 * Middleware helper — returns a 401 response if the key is invalid.
 * Use in any API route: const auth = await requireApiKey(req); if (auth) return auth;
 */
export async function requireApiKey(req: NextRequest) {
    // RapidAPI proxy — if request came through RapidAPI marketplace, allow it
    if (RAPIDAPI_PROXY_SECRET) {
        const proxySecret = req.headers.get("x-rapidapi-proxy-secret")
        if (proxySecret === RAPIDAPI_PROXY_SECRET) {
            return null // Verified RapidAPI proxy request
        }
    }

    const apiKey = req.headers.get("x-api-key")

    // Demo key — rate-limited, no DB lookup
    if (apiKey === DEMO_KEY) {
        const ip = req.headers.get("x-forwarded-for") || req.headers.get("x-real-ip") || "unknown"
        const now = Date.now()
        const entry = demoRateLimit.get(ip) || { count: 0, resetAt: now + 3_600_000 }
        if (now > entry.resetAt) {
            entry.count = 0
            entry.resetAt = now + 3_600_000
        }
        entry.count++
        demoRateLimit.set(ip, entry)
        if (entry.count > DEMO_LIMIT) {
            return NextResponse.json(
                { error: "Demo key rate limit exceeded (50/hour). Get a free API key at /api-docs" },
                { status: 429 }
            )
        }
        return null // Demo key valid
    }

    const keyData = await validateApiKey(req)
    if (!keyData) {
        return NextResponse.json(
            { error: "Invalid or missing API key. Include a valid x-api-key header." },
            { status: 401 }
        )
    }
    return null // Valid — proceed
}

// ── Determine source + keyId from request headers ──
function resolveIdentity(req: NextRequest): { keyId: string | null; source: ApiUsageEntry["source"] } {
    if (RAPIDAPI_PROXY_SECRET && req.headers.get("x-rapidapi-proxy-secret") === RAPIDAPI_PROXY_SECRET) {
        return { keyId: "rapidapi", source: "rapidapi" }
    }
    const apiKey = req.headers.get("x-api-key")
    if (apiKey === DEMO_KEY) return { keyId: "demo", source: "demo" }
    return { keyId: apiKey, source: "direct" }
}

type RouteHandler = (req: NextRequest) => Promise<NextResponse>

/**
 * Higher-order wrapper that adds auth + usage logging to an API route.
 *
 *   export const GET = withApiLogging(async (req) => { ... })
 *
 * - Calls requireApiKey first; returns early on auth failure (still logged).
 * - Measures wall-clock latency.
 * - Fire-and-forget logs every request to the api_usage table.
 */
export function withApiLogging(handler: RouteHandler): RouteHandler {
    return async (req: NextRequest) => {
        const start = Date.now()
        const { pathname } = new URL(req.url)
        const ip = req.headers.get("x-forwarded-for") || req.headers.get("x-real-ip") || "unknown"
        const { keyId, source } = resolveIdentity(req)

        // Auth check
        const authError = await requireApiKey(req)
        if (authError) {
            logApiUsage({
                endpoint: pathname,
                method: req.method,
                keyId,
                status: authError.status,
                latencyMs: Date.now() - start,
                ip,
                source,
            })
            return authError
        }

        // Run the actual handler
        let response: NextResponse
        try {
            response = await handler(req)
        } catch (err: any) {
            response = NextResponse.json(
                { error: err.message || "Internal server error" },
                { status: 500 }
            )
        }

        // Fire-and-forget log
        logApiUsage({
            endpoint: pathname,
            method: req.method,
            keyId,
            status: response.status,
            latencyMs: Date.now() - start,
            ip,
            source,
        })

        return response
    }
}
