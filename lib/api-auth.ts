import { NextRequest, NextResponse } from "next/server"
import { getSupabaseAdmin } from "@/lib/supabase/admin"

/**
 * Validate an API key from the request headers.
 * Returns the key row if valid, null if invalid.
 */
export async function validateApiKey(req: NextRequest) {
    const apiKey = req.headers.get("x-api-key")

    if (!apiKey) return null

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
    const keyData = await validateApiKey(req)
    if (!keyData) {
        return NextResponse.json(
            { error: "Invalid or missing API key. Include a valid x-api-key header." },
            { status: 401 }
        )
    }
    return null // Valid — proceed
}
