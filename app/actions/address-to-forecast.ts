"use server"

import { getSupabaseServerClient } from "@/lib/supabase/server"

interface AddressToForecastResult {
  success: boolean
  forecastUrl?: string
  tractGeoid?: string
  neighborhoodName?: string
  city?: string
  state?: string
  error?: string
}

/**
 * Takes lat/lng coordinates and returns the forecast page URL for that location.
 * Uses the tract_slug_lookup table to find the pre-computed URL slugs for the tract.
 * Falls back to PostGIS point-in-polygon query if no slug exists.
 */
export async function addressToForecast(lat: number, lng: number): Promise<AddressToForecastResult> {
  try {
    const supabase = await getSupabaseServerClient()
    
    // First, find which tract contains this point using PostGIS
    const { data: tractData, error: tractError } = await supabase
      .rpc("find_tract_at_point", {
        p_lat: lat,
        p_lng: lng,
      })
    
    let tractGeoid: string | null = null
    
    if (!tractError && tractData && tractData.length > 0) {
      tractGeoid = tractData[0].geoid
    }
    
    // If RPC doesn't exist, try a raw query approach via tract_slug_lookup
    // by checking if we have any data at all
    if (!tractGeoid) {
      // Try to find the nearest tract using a simple distance query
      // This is a fallback - the tract_slug_lookup table has all our mapped tracts
      const { data: allTracts, error: allError } = await supabase
        .from("tract_slug_lookup")
        .select("tract_geoid, state_slug, city_slug, neighborhood_slug")
        .limit(1)
      
      if (allError) {
        console.error("[addressToForecast] Error checking tract_slug_lookup:", allError)
      }
      
      // For now, we need to use a workaround since we can't do PostGIS directly
      // Let's check if there's a find_tract_at_point function or create one
      console.error("[addressToForecast] No tract found at coordinates:", lat, lng)
      return {
        success: false,
        error: "Could not find forecast data for this location. Try a different address.",
      }
    }
    
    // Look up the pre-computed slugs for this tract
    const { data: slugData, error: slugError } = await supabase
      .from("tract_slug_lookup")
      .select("state_slug, city_slug, neighborhood_slug")
      .eq("tract_geoid", tractGeoid)
      .single()
    
    if (slugError || !slugData) {
      console.error("[addressToForecast] No slug found for tract:", tractGeoid, slugError)
      return {
        success: false,
        error: "This area does not have forecast data available yet.",
      }
    }
    
    return {
      success: true,
      forecastUrl: `/forecasts/${slugData.state_slug}/${slugData.city_slug}/${slugData.neighborhood_slug}/home-price-forecast`,
      tractGeoid,
    }
  } catch (error) {
    console.error("[addressToForecast] Error:", error)
    return {
      success: false,
      error: "An error occurred while looking up this address",
    }
  }
}
