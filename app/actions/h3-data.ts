"use server"

import { getSupabaseServerClient } from "@/lib/supabase/server"

export interface H3HexagonData {
  h3_id: string
  lat: number
  lng: number
  opportunity: number
  reliability: number
  property_count: number
  sample_accuracy: number
  alert_pct: number
}

export async function getH3DataForResolution(h3Resolution: number, forecastYear = 2026): Promise<H3HexagonData[]> {
  try {
    const supabase = await getSupabaseServerClient()

    const { data, error } = await supabase
      .from("h3_precomputed_hex_rows")
      .select("h3_id, lat, lng, opportunity, reliability, property_count, sample_accuracy, alert_pct")
      .eq("h3_res", h3Resolution)
      .eq("forecast_year", forecastYear)

    if (error) {
      console.error("[v0] Error fetching H3 data:", error)
      return []
    }

    console.log(`[v0] Fetched ${data?.length || 0} REAL hexagons from Supabase at resolution ${h3Resolution}`)
    return (data || []) as H3HexagonData[]
  } catch (err) {
    console.error("[v0] Exception fetching H3 data:", err)
    return []
  }
}

export async function getParcelGeometries(bounds: {
  minLat: number
  maxLat: number
  minLng: number
  maxLng: number
}): Promise<any[]> {
  // TODO: Implement parcel geometry fetching when parcel table is available in Supabase
  // This would query a parcels table with PostGIS geometry data
  // At highest zoom (H3 res 10+), switch from hexagons to actual parcel polygons
  console.log("[v0] Parcel geometries not yet implemented - add parcels table to Supabase with geometry column")
  return []
}
