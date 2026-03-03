import { createClient, SupabaseClient } from "@supabase/supabase-js"

/**
 * Server-only Supabase client using the service role key.
 * Bypasses RLS — use only in API routes, never in client code.
 * Singleton — reuses the same client across all requests to avoid
 * saturating the connection pooler with concurrent tile fetches.
 */
let _client: SupabaseClient | null = null

export function getSupabaseAdmin() {
    if (_client) return _client

    const url = process.env.NEXT_PUBLIC_SUPABASE_URL!
    const key = process.env.SUPABASE_SERVICE_ROLE_KEY!

    if (!url || !key) {
        throw new Error("Missing NEXT_PUBLIC_SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")
    }

    _client = createClient(url, key, {
        auth: { persistSession: false, autoRefreshToken: false },
    })

    return _client
}
