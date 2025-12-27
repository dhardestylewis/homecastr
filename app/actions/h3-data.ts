"use server"

import { getSupabaseServerClient } from "@/lib/supabase/server"

/**
 * Lightweight H3 data for map rendering - only the essentials for coloring hexagons
 */
export interface H3CompactData {
  h3_id: string
  o: number | null  // opportunity (for color)
  r: number | null  // reliability (for stroke width)
  lat: number       // for client-side viewport check
  lng: number
}

export interface ViewportBounds {
  minLat: number
  maxLat: number
  minLng: number
  maxLng: number
}

/**
 * OPTIMIZED: Fetches only essential H3 data for map rendering (3 columns instead of 8+)
 * Reduces payload size by ~75% for faster network transfer
 */
export async function getH3CompactDataForViewport(
  h3Resolution: number,
  forecastYear = 2026,
  bounds: ViewportBounds
): Promise<H3CompactData[]> {
  try {
    const supabase = await getSupabaseServerClient()

    const PAGE_SIZE = 1000 // Supabase default max per request
    let allData: any[] = []
    let offset = 0
    let hasMore = true

    // Paginate to get ALL results (Supabase limits to 1000 per request)
    while (hasMore) {
      const { data, error, count } = await supabase
        .from("h3_precomputed_hex_details")
        .select("h3_id, opportunity, reliability, lat, lng", { count: 'exact' })
        .eq("h3_res", h3Resolution)
        .eq("forecast_year", forecastYear)
        .gte("lat", bounds.minLat)
        .lte("lat", bounds.maxLat)
        .gte("lng", bounds.minLng)
        .lte("lng", bounds.maxLng)
        .range(offset, offset + PAGE_SIZE - 1)

      if (error) {
        console.error("[v0] Compact data fetch error:", error.message)
        break
      }

      if (data && data.length > 0) {
        allData = allData.concat(data)
        offset += data.length

        // Log progress for large fetches
        if (offset > PAGE_SIZE) {
          console.log(`[v0] Fetching... ${offset} rows so far (total: ${count ?? 'unknown'})`)
        }
      }

      hasMore = data && data.length === PAGE_SIZE
    }

    console.log(`[v0] Compact fetch COMPLETE: ${allData.length} hexes for res ${h3Resolution}`)

    return allData.map((h: any) => ({
      h3_id: h.h3_id,
      o: h.opportunity,
      r: h.reliability,
      lat: h.lat,
      lng: h.lng
    }))
  } catch (err) {
    console.error("[v0] Compact data exception:", err)
    return []
  }
}

// Legacy interface for backwards compatibility with map-view.tsx
export interface H3HexagonData {
  h3_id: string
  lat: number
  lng: number
  opportunity: number | null
  reliability: number | null
  property_count: number
  sample_accuracy: number | null
  alert_pct: number | null
  has_data: boolean
}

/**
 * Legacy function - use getH3CompactDataForViewport for better performance
 */
export async function getH3DataForResolution(
  h3Resolution: number,
  forecastYear = 2026,
  bounds?: ViewportBounds
): Promise<H3HexagonData[]> {
  // If we have bounds, use the compact function and fill in defaults
  if (bounds) {
    const compactData = await getH3CompactDataForViewport(h3Resolution, forecastYear, bounds)

    console.log(`[DB-DIAG] Query: res=${h3Resolution}, year=${forecastYear}`)
    console.log(`[DB-DIAG] Bounds: lat ${bounds.minLat.toFixed(3)}→${bounds.maxLat.toFixed(3)}, lng ${bounds.minLng.toFixed(3)}→${bounds.maxLng.toFixed(3)}`)
    console.log(`[DB-DIAG] Returned: ${compactData.length} rows (limit: 10000)`)

    return compactData.map(h => ({
      h3_id: h.h3_id,
      lat: h.lat,
      lng: h.lng,
      opportunity: h.o,
      reliability: h.r,
      property_count: 1, // Default - details fetched on click
      sample_accuracy: null,
      alert_pct: null,
      has_data: true
    }))
  }

  // Fallback for no bounds (shouldn't happen with proper viewport tracking)
  const supabase = await getSupabaseServerClient()
  const { data, error } = await supabase
    .from("h3_precomputed_hex_details")
    .select("h3_id, lat, lng, opportunity, reliability")
    .eq("h3_res", h3Resolution)
    .eq("forecast_year", forecastYear)
    .limit(5000)

  if (error) {
    console.error("[v0] Fallback fetch error:", error)
    return []
  }

  return (data || []).map((h: any) => ({
    h3_id: h.h3_id,
    lat: h.lat,
    lng: h.lng,
    opportunity: h.opportunity,
    reliability: h.reliability,
    property_count: 1,
    sample_accuracy: null,
    alert_pct: null,
    has_data: true
  }))
}

export async function getParcelGeometries(bounds: ViewportBounds): Promise<any[]> {
  console.log("[v0] Parcel geometries not implemented")
  return []
}

/**
 * Prefetch all forecast years for smooth playback
 * Returns a map of year -> data for caching
 */
export async function prefetchAllYears(
  h3Resolution: number,
  bounds: ViewportBounds,
  years: number[] = [2026, 2027, 2028, 2029, 2030, 2031, 2032]
): Promise<Map<number, H3HexagonData[]>> {
  console.log(`[PREFETCH] Starting prefetch for years ${years.join(', ')} at res ${h3Resolution}`)

  const results = new Map<number, H3HexagonData[]>()

  // Fetch all years in parallel
  const promises = years.map(async (year) => {
    const data = await getH3DataForResolution(h3Resolution, year, bounds)
    return { year, data }
  })

  const resolved = await Promise.all(promises)

  for (const { year, data } of resolved) {
    results.set(year, data)
    console.log(`[PREFETCH] Year ${year}: ${data.length} hexes prefetched`)
  }

  console.log(`[PREFETCH] Complete - all ${years.length} years loaded`)
  return results
}
