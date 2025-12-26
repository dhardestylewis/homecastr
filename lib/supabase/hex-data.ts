import { createBrowserClient } from "@supabase/ssr"

// Cache for loaded hex layers to minimize database queries
const hexDataCache = new Map<string, any>()

// Get or create Supabase client
function getSupabaseClient() {
  return createBrowserClient(process.env.NEXT_PUBLIC_SUPABASE_URL!, process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!)
}

// Fetch precomputed hex data for a specific H3 resolution
export async function getHexDataForResolution(h3Resolution: number, forecastYear = 2026) {
  const cacheKey = `h3_res_${h3Resolution}_year_${forecastYear}`

  // Return cached data if available
  if (hexDataCache.has(cacheKey)) {
    return hexDataCache.get(cacheKey)
  }

  try {
    const supabase = getSupabaseClient()

    // Fetch the precomputed bulk payload from h3_precomputed_json
    const { data, error } = await supabase
      .from("h3_precomputed_json")
      .select("payload")
      .eq("forecast_year", forecastYear)
      .eq("h3_res", h3Resolution)
      .single()

    if (error) {
      console.error(`[v0] Failed to fetch H3 resolution ${h3Resolution}:`, error.message)
      return null
    }

    if (!data?.payload) {
      console.warn(`[v0] No data found for H3 resolution ${h3Resolution} year ${forecastYear}`)
      return null
    }

    // Cache the result
    hexDataCache.set(cacheKey, data.payload)
    console.log(`[v0] Loaded ${data.payload.hexagons?.length} hexagons for H3 resolution ${h3Resolution}`)

    return data.payload
  } catch (err) {
    console.error("[v0] Error fetching hex data:", err)
    return null
  }
}

// Clear cache when needed
export function clearHexDataCache() {
  hexDataCache.clear()
}

// Convert precomputed hex data to the format expected by map-view
export function convertPrecomputedHexes(payload: any, h3Resolution: number) {
  if (!payload?.hexagons) return []

  return payload.hexagons.map((hex: any) => ({
    id: hex.h3_id,
    centerLng: hex.lng,
    centerLat: hex.lat,
    properties: {
      id: hex.h3_id,
      O: hex.opportunity, // Opportunity score
      R: hex.reliability, // Reliability score
      n_accts: hex.property_count,
      med_mean_ape_pct: 0, // Not in precomputed data, using default
      med_mean_pred_cv_pct: 0, // Not in precomputed data, using default
      stability_flag: hex.alert_pct > 0.2, // Flag if >20% of properties have alerts
      robustness_flag: hex.alert_pct < 0.1, // Flag if <10% have alerts (robust)
      alert_pct: hex.alert_pct,
    },
  }))
}
