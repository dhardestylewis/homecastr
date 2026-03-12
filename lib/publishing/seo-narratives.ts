import { getSupabaseAdmin } from "@/lib/supabase/admin"
import { withRedisCache } from "@/lib/redis"

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface SeoNarrative {
    market_summary: string
    trend_analysis: string
    uncertainty_interpretation: string
    comparable_narrative: string
}

// ---------------------------------------------------------------------------
// Fetcher
// ---------------------------------------------------------------------------

/**
 * Fetch the AI-generated narrative for a given geography.
 * Returns null if no narrative has been generated yet.
 * Cached in Redis for 1 hour.
 */
export async function fetchSeoNarrative(
    geoid: string,
    level: "state" | "county" | "city" | "tract" = "tract"
): Promise<SeoNarrative | null> {
    return withRedisCache(`seo_narrative:${geoid}:${level}`, async () => {
        const supabase = getSupabaseAdmin()

        const { data, error } = await supabase
            .from("seo_narratives")
            .select("narrative_json")
            .eq("geoid", geoid)
            .eq("level", level)
            .single()

        if (error || !data) return null
        return data.narrative_json as SeoNarrative
    }, 3600) // 1 hour — high-cardinality key (~85k tracts)
}
