import { getSupabaseAdmin } from "@/lib/supabase/admin"

export interface ApiUsageEntry {
    endpoint: string
    method: string
    keyId: string | null
    status: number
    latencyMs: number
    ip: string
    source: "direct" | "rapidapi" | "demo"
}

/**
 * Fire-and-forget: log an API request to the `api_usage` table.
 * Never throws, never blocks the response.
 */
export function logApiUsage(entry: ApiUsageEntry): void {
    try {
        const supabase = getSupabaseAdmin()
        supabase
            .from("api_usage")
            .insert({
                endpoint: entry.endpoint,
                method: entry.method,
                key_id: entry.keyId,
                status: entry.status,
                latency_ms: entry.latencyMs,
                ip: entry.ip,
                source: entry.source,
            })
            .then(({ error }) => {
                if (error) console.error("[api-usage] insert failed:", error.message)
            })
            .catch((err: any) => {
                console.error("[api-usage] insert exception:", err?.message)
            })
    } catch {
        // Swallow — logging must never break a response
    }
}
