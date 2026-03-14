"use server"

import { getSupabaseServerClient } from "@/lib/supabase/server"
import { parseTractGeoid, slugify, enrichWithNeighborhood } from "@/lib/publishing/geo-crosswalk"

const FORECAST_SCHEMA = "forecast_queue"

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
 * Uses the nearest_geography RPC to find the census tract, then builds the URL.
 */
export async function addressToForecast(lat: number, lng: number): Promise<AddressToForecastResult> {
  try {
    const supabase = await getSupabaseServerClient()
    
    // Find the nearest tract using the RPC
    const { data: nearest, error: rpcError } = await (supabase as any)
      .schema(FORECAST_SCHEMA)
      .rpc("nearest_geography", {
        p_lat: lat,
        p_lng: lng,
        p_level: "tract",
      })
    
    if (rpcError || !nearest || nearest.length === 0) {
      console.error("[addressToForecast] RPC failed or no results:", rpcError)
      return {
        success: false,
        error: "Could not find forecast data for this location. Try a different address.",
      }
    }
    
    const tractGeoid = nearest[0].id
    if (!tractGeoid || tractGeoid.length < 11) {
      return {
        success: false,
        error: "Invalid tract ID returned",
      }
    }
    
    // Parse the tract geoid to get geography info
    const baseGeo = parseTractGeoid(tractGeoid.substring(0, 11))
    
    // Enrich with neighborhood name from database
    const geoInfo = await enrichWithNeighborhood(baseGeo)
    
    // Build the forecast URL
    const stateSlug = slugify(geoInfo.stateAbbr)
    const citySlug = slugify(geoInfo.city)
    const neighborhoodSlug = slugify(geoInfo.neighborhoodName)
    
    return {
      success: true,
      forecastUrl: `/forecasts/${stateSlug}/${citySlug}/${neighborhoodSlug}/home-price-forecast`,
      tractGeoid,
      neighborhoodName: geoInfo.neighborhoodName,
      city: geoInfo.city,
      state: geoInfo.stateAbbr,
    }
  } catch (error) {
    console.error("[addressToForecast] Error:", error)
    return {
      success: false,
      error: "An error occurred while looking up this address",
    }
  }
}
