import { NextRequest, NextResponse } from "next/server"
import { getSupabaseAdmin } from "@/lib/supabase/admin"

// ── Demo key: allows zero-signup API testing ──
const DEMO_KEY = "hc_demo_public_readonly"
const DEMO_LIMIT = 50 // requests per hour per IP
const demoRateLimit = new Map<string, { count: number; resetAt: number }>()

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
