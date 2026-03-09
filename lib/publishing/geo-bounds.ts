import { getSupabaseAdmin } from "@/lib/supabase/admin"

/**
 * Fetches the exact bounding box [minLng, minLat, maxLng, maxLat] for a geographic feature
 * by directly querying the PostGIS geometry extent via Supabase RPC.
 */
export async function getDynamicBounds(
    level: "state" | "county" | "tract" | "parcel" | "neighborhood",
    geoid: string,
    stateSlug?: string
): Promise<[number, number, number, number] | null> {
    const supabase = getSupabaseAdmin()

    try {
        const { data, error } = await supabase.rpc("get_feature_bounds", {
            p_level: level,
            p_geoid: geoid,
            p_state_slug: stateSlug || null
        })

        if (error) {
            console.error(`[geo-bounds] RPC error fetching bounds for ${level} ${geoid}:`, error.message)
            return null
        }

        if (Array.isArray(data) && data.length === 4) {
            return data as [number, number, number, number]
        }

        return null
    } catch (e) {
        console.error(`[geo-bounds] Error executing get_feature_bounds for ${level} ${geoid}:`, e)
        return null
    }
}
