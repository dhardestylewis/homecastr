import { getSupabaseAdmin } from "@/lib/supabase/admin"

/**
 * Fetches the exact bounding box [minLng, minLat, maxLng, maxLat] for a geographic feature
 * by directly querying the PostGIS geometry extent via Supabase RPC.
 *
 * Supported levels: state, county, zcta, tract, parcel, neighborhood
 */
export async function getDynamicBounds(
    level: "state" | "county" | "zcta" | "tract" | "parcel" | "neighborhood",
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

        // Supabase returns PostgreSQL float[] in different formats depending on client:
        // - As a JS array: [minLng, minLat, maxLng, maxLat]
        // - As a string:   "{-71.08,43.06,-66.95,47.46}"
        // Handle both cases.
        if (data == null) return null

        let bbox: number[]

        if (Array.isArray(data)) {
            bbox = data
        } else if (typeof data === "string") {
            // Parse PostgreSQL array string: "{-71.08,43.06,-66.95,47.46}"
            const cleaned = data.replace(/[{}]/g, "")
            bbox = cleaned.split(",").map(Number)
        } else {
            console.warn(`[geo-bounds] Unexpected RPC response type for ${level} ${geoid}:`, typeof data, data)
            return null
        }

        if (bbox.length === 4 && bbox.every(v => !isNaN(v))) {
            return bbox as [number, number, number, number]
        }

        return null
    } catch (e) {
        console.error(`[geo-bounds] Error executing get_feature_bounds for ${level} ${geoid}:`, e)
        return null
    }
}
