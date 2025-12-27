"use server"

import { getSupabaseServerClient } from "@/lib/supabase/server"
import { polygonToCells, latLngToCell, cellToLatLng } from "h3-js"

export interface H3HexagonData {
  h3_id: string
  lat: number
  lng: number
  opportunity: number | null
  reliability: number | null
  property_count: number
  sample_accuracy: number | null
  alert_pct: number | null
  has_data: boolean  // true if property_count > 0
}

export interface ViewportBounds {
  minLat: number
  maxLat: number
  minLng: number
  maxLng: number
}

/**
 * Fetches H3 hexagon data for map rendering within a bounding box.
 * Uses h3_precomputed_hex_details as the authoritative source.
 * 
 * @param h3Resolution - H3 resolution level (7-11)
 * @param forecastYear - Year to fetch data for
 * @param bounds - Optional viewport bounds to filter tiles spatially
 */
export async function getH3DataForResolution(
  h3Resolution: number,
  forecastYear = 2026,
  bounds?: ViewportBounds
): Promise<H3HexagonData[]> {
  try {
    const supabase = await getSupabaseServerClient()

    // Start query using RPC for spatial efficiency if bounds exist
    // This avoids sending 15k+ IDs in the URL which crashes the server/browser
    let rpcError = null
    let data = []

    if (bounds) {
      // RPC: get_h3_data_in_viewport(min_lat, min_lng, max_lat, max_lng, resolution, year)
      const { data: rpcData, error } = await supabase.rpc('get_h3_data_in_viewport', {
        min_lat: bounds.minLat,
        min_lng: bounds.minLng,
        max_lat: bounds.maxLat,
        max_lng: bounds.maxLng,
        resolution: h3Resolution,
        year: forecastYear
      })

      if (!error) {
        data = rpcData || []
        console.log(`[v0] RPC get_h3_data_in_viewport returned ${data.length} cells`)

        // Since we removed generation, what we get is what we render.
        return data.map((h: any) => ({
          ...h,
          has_data: (h.property_count ?? 0) > 0
        })) as H3HexagonData[]
      }

      console.warn("[v0] RPC failed, falling back to BBox select:", error.message)
      rpcError = error
    }

    // Fallback path: If RPC failed or no bounds (global view), use standard select
    if (!bounds || rpcError) {
      let query = supabase
        .from("h3_precomputed_hex_details")
        .select("h3_id, lat, lng, opportunity, reliability, property_count, sample_accuracy, alert_pct")
        .eq("h3_res", h3Resolution)
        .eq("forecast_year", forecastYear)

      if (bounds) {
        query = query
          .gte("lat", bounds.minLat)
          .lte("lat", bounds.maxLat)
          .gte("lng", bounds.minLng)
          .lte("lng", bounds.maxLng)
      }

      // Limit safety valve
      const limit = h3Resolution >= 10 ? 15000 : 50000
      query = query.limit(limit)

      const { data: dbData, error } = await query

      if (error) {
        console.error("[v0] Error fetching H3 data (Fallback):", error)
        return []
      }
      data = dbData || []
      console.log(`[v0] Fallback Query returned ${data.length} cells`)
    }

    return (data || []).map((h: any) => ({
      ...h,
      has_data: (h.property_count ?? 0) > 0
    })) as H3HexagonData[]
  } catch (err) {
    console.error("[v0] Exception fetching H3 data:", err)
    return []
  }
}

export async function getParcelGeometries(bounds: ViewportBounds): Promise<any[]> {
  // TODO: Implement parcel geometry fetching when parcel table is available in Supabase
  console.log("[v0] Parcel geometries not yet implemented")
  return []
}
