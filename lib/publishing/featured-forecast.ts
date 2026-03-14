import { getSupabaseAdmin } from "@/lib/supabase/admin"
import { withRedisCache } from "@/lib/redis"

const SCHEMA = process.env.FORECAST_SCHEMA || "forecast_queue"

export interface FeaturedForecastData {
  tract: {
    geoid: string
    neighborhoodSlug: string
    citySlug: string
    stateSlug: string
  }
  location: {
    neighborhood: string
    city: string
    state: string
    zip: string
  }
  currentValue: number
  horizons: Array<{
    year: number
    p10: number
    p50: number
    p90: number
  }>
}

// Featured tracts for the homepage - high-signal markets with good data
const FEATURED_TRACTS = [
  // Austin, TX - Manor/Pflugerville area
  { geoid: "48453010810", fallbackZip: "78653" },
  // Houston, TX - Third Ward  
  { geoid: "48201312300", fallbackZip: "77003" },
  // Phoenix, AZ - Central Phoenix
  { geoid: "04013112202", fallbackZip: "85008" },
  // Miami, FL - Little Havana
  { geoid: "12086004200", fallbackZip: "33135" },
  // Denver, CO - Five Points
  { geoid: "08031003600", fallbackZip: "80205" },
]

/**
 * Fetch featured forecast data for the homepage.
 * Tries each featured tract until one returns valid data.
 * Uses Redis cache for performance.
 */
export async function fetchFeaturedForecast(): Promise<FeaturedForecastData | null> {
  return withRedisCache("homepage_featured_forecast", async () => {
    const supabase = getSupabaseAdmin()
    
    for (const featured of FEATURED_TRACTS) {
      try {
        // Get tract location info
        const { data: tractInfo } = await supabase
          .from("tract_slug_lookup")
          .select("tract_geoid, neighborhood_slug, city_slug, state_slug")
          .eq("tract_geoid", featured.geoid)
          .single()

        if (!tractInfo) continue

        // Get forecast data with all horizons
        const { data: forecastData } = await supabase
          .schema(SCHEMA as any)
          .from("metrics_tract_forecast")
          .select("horizon_m, p10, p25, p50, p75, p90, origin_year")
          .eq("tract_geoid20", featured.geoid)
          .eq("series_kind", "forecast")
          .order("horizon_m", { ascending: true })

        if (!forecastData || forecastData.length === 0) continue

        // Get baseline value (current/near-term P50)
        const baselineRow = forecastData.find((r: any) => r.horizon_m === 12) || forecastData[0]
        const currentValue = (baselineRow as any).p50 || 0
        
        if (currentValue < 50000 || currentValue > 5000000) continue // Skip outliers

        // Get origin year for calculating forecast years
        const originYear = (forecastData[0] as any).origin_year || 2025

        // Transform to horizons array
        const horizons = forecastData
          .filter((r: any) => r.p50 && r.p10 && r.p90)
          .map((r: any) => ({
            year: originYear + r.horizon_m / 12,
            p10: r.p10,
            p50: r.p50,
            p90: r.p90,
          }))

        if (horizons.length < 3) continue // Need enough data points

        // Format location names from slugs
        const formatName = (slug: string) => 
          slug.split("-").map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(" ")

        return {
          tract: {
            geoid: featured.geoid,
            neighborhoodSlug: tractInfo.neighborhood_slug,
            citySlug: tractInfo.city_slug,
            stateSlug: tractInfo.state_slug,
          },
          location: {
            neighborhood: formatName(tractInfo.neighborhood_slug),
            city: formatName(tractInfo.city_slug),
            state: tractInfo.state_slug.toUpperCase().slice(0, 2),
            zip: featured.fallbackZip,
          },
          currentValue,
          horizons,
        }
      } catch (e) {
        console.error(`Failed to fetch featured forecast for ${featured.geoid}:`, e)
        continue
      }
    }

    return null
  }, 3600) // Cache for 1 hour
}

/**
 * Get a random featured tract for variety on the homepage.
 * Falls back to first available tract if specified one has no data.
 */
export async function fetchRandomFeaturedForecast(): Promise<FeaturedForecastData | null> {
  // Shuffle the featured tracts
  const shuffled = [...FEATURED_TRACTS].sort(() => Math.random() - 0.5)
  
  const supabase = getSupabaseAdmin()
  
  for (const featured of shuffled) {
    try {
      const { data: tractInfo } = await supabase
        .from("tract_slug_lookup")
        .select("tract_geoid, neighborhood_slug, city_slug, state_slug")
        .eq("tract_geoid", featured.geoid)
        .single()

      if (!tractInfo) continue

      const { data: forecastData } = await supabase
        .schema(SCHEMA as any)
        .from("metrics_tract_forecast")
        .select("horizon_m, p10, p25, p50, p75, p90, origin_year")
        .eq("tract_geoid20", featured.geoid)
        .eq("series_kind", "forecast")
        .order("horizon_m", { ascending: true })

      if (!forecastData || forecastData.length === 0) continue

      const baselineRow = forecastData.find((r: any) => r.horizon_m === 12) || forecastData[0]
      const currentValue = (baselineRow as any).p50 || 0
      
      if (currentValue < 50000 || currentValue > 5000000) continue

      const originYear = (forecastData[0] as any).origin_year || 2025

      const horizons = forecastData
        .filter((r: any) => r.p50 && r.p10 && r.p90)
        .map((r: any) => ({
          year: originYear + r.horizon_m / 12,
          p10: r.p10,
          p50: r.p50,
          p90: r.p90,
        }))

      if (horizons.length < 3) continue

      const formatName = (slug: string) => 
        slug.split("-").map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(" ")

      return {
        tract: {
          geoid: featured.geoid,
          neighborhoodSlug: tractInfo.neighborhood_slug,
          citySlug: tractInfo.city_slug,
          stateSlug: tractInfo.state_slug,
        },
        location: {
          neighborhood: formatName(tractInfo.neighborhood_slug),
          city: formatName(tractInfo.city_slug),
          state: tractInfo.state_slug.toUpperCase().slice(0, 2),
          zip: featured.fallbackZip,
        },
        currentValue,
        horizons,
      }
    } catch (e) {
      continue
    }
  }

  return null
}
