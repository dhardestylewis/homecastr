import { NextRequest, NextResponse } from "next/server"
import { requireApiKey, validateApiKey } from "@/lib/api-auth"
import { getSupabaseAdmin } from "@/lib/supabase/admin"

/**
 * GET /api/v1/usage — API usage stats (non-demo keys only).
 *
 * Returns request counts for 24h / 7d / 30d, broken down by
 * endpoint and source (direct / rapidapi / demo).
 */
export async function GET(req: NextRequest) {
    // Auth — block demo key from accessing stats
    const authError = await requireApiKey(req)
    if (authError) return authError

    const keyData = await validateApiKey(req)
    if (keyData?.id === "demo") {
        return NextResponse.json(
            { error: "Usage stats are not available with the demo key. Get a free API key at /api-docs." },
            { status: 403 }
        )
    }

    try {
        const supabase = getSupabaseAdmin()

        // ── Total counts by time window ──
        const windows = [
            { label: "24h", interval: "24 hours" },
            { label: "7d", interval: "7 days" },
            { label: "30d", interval: "30 days" },
        ]

        const totals: Record<string, number> = {}
        for (const w of windows) {
            const { count, error } = await supabase
                .from("api_usage")
                .select("*", { count: "exact", head: true })
                .gte("ts", new Date(Date.now() - parseDuration(w.interval)).toISOString())

            if (error) throw error
            totals[w.label] = count ?? 0
        }

        // ── Breakdown by endpoint (last 30d) ──
        const thirtyDaysAgo = new Date(Date.now() - 30 * 86_400_000).toISOString()
        const { data: rows, error: rowsErr } = await supabase
            .from("api_usage")
            .select("endpoint, source, status")
            .gte("ts", thirtyDaysAgo)
            .order("ts", { ascending: false })
            .limit(10_000)

        if (rowsErr) throw rowsErr

        const byEndpoint: Record<string, number> = {}
        const bySource: Record<string, number> = {}
        const byStatus: Record<string, number> = {}

        for (const r of rows ?? []) {
            byEndpoint[r.endpoint] = (byEndpoint[r.endpoint] || 0) + 1
            bySource[r.source] = (bySource[r.source] || 0) + 1
            const bucket = r.status < 300 ? "2xx" : r.status < 400 ? "3xx" : r.status < 500 ? "4xx" : "5xx"
            byStatus[bucket] = (byStatus[bucket] || 0) + 1
        }

        return NextResponse.json({
            totals,
            by_endpoint: byEndpoint,
            by_source: bySource,
            by_status: byStatus,
        })
    } catch (error: any) {
        console.error("[API] Usage stats error:", error)
        return NextResponse.json(
            { error: error.message || "Internal server error" },
            { status: 500 }
        )
    }
}

function parseDuration(s: string): number {
    if (s === "24 hours") return 86_400_000
    if (s === "7 days") return 7 * 86_400_000
    if (s === "30 days") return 30 * 86_400_000
    return 86_400_000
}
