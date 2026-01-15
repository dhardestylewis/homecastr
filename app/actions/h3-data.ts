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

    // STEP 1: Spatial Query on Grid Table
    // Filter by AOI + Resolution + Bounds
    const { data: gridData, error: gridError } = await supabase
      .from("h3_aoi_grid")
      .select("h3_id, lat, lng")
      .eq("h3_res", h3Resolution)
      .eq("aoi_id", "harris_county") // Ensure we check the correct partition
      .gte("lat", bounds.minLat)
      .lte("lat", bounds.maxLat)
      .gte("lng", bounds.minLng)
      .lte("lng", bounds.maxLng)
      .limit(30000) // Increased for Res 11 at 1080p (can be ~25k hexes)

    if (gridError) {
      console.error("[v0] Grid fetch error:", gridError.message)
      return []
    }

    if (!gridData || gridData.length === 0) {
      return []
    }

    const ids = gridData.map((g: any) => g.h3_id)

    // STEP 2: Data Query on Details Table
    // Fetch attributes for the visible IDs
    const { data: detailsData, error: detailsError } = await supabase
      .from("h3_precomputed_hex_details")
      .select("h3_id, opportunity, reliability")
      .eq("forecast_year", forecastYear)
      .eq("h3_res", h3Resolution)
      .in("h3_id", ids)

    if (detailsError) {
      console.error("[v0] Details fetch error:", detailsError.message)
      return []
    }

    // STEP 3: Merge in Memory
    // Map for O(1) lookup
    const detailsMap = new Map(detailsData?.map((d: any) => [d.h3_id, d]) || [])

    return gridData.map((g: any) => {
      const d = detailsMap.get(g.h3_id)
      return {
        h3_id: g.h3_id,
        o: d?.opportunity ?? null,
        r: d?.reliability ?? null,
        lat: g.lat,
        lng: g.lng
      }
    })

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

// Drop-in instrumentation for your server action.
// Goal: distinguish "DB returned 0" from "client filtered to 0" and surface silent Supabase errors.

function asNum(x: unknown): number {
  const v = typeof x === "string" ? Number(x) : (x as number)
  if (!Number.isFinite(v)) throw new Error(`Non-finite number: ${String(x)}`)
  return v
}

/**
 * Legacy function - use getH3CompactDataForViewport for better performance
 */
export async function getH3DataForResolution(
  h3Resolution: number,
  forecastYear = 2026,
  bounds?: ViewportBounds
): Promise<H3HexagonData[]> {
  const supabase = await getSupabaseServerClient()

  console.log("[SERVER-V2] getH3DataForResolution called") // DEBUG MARKER

  const aoi_id = "harris_county"
  const res = Math.trunc(asNum(h3Resolution))
  const year = Math.trunc(asNum(forecastYear))

  const b = bounds
    ? {
      minLat: asNum(bounds.minLat),
      maxLat: asNum(bounds.maxLat),
      minLng: asNum(bounds.minLng),
      maxLng: asNum(bounds.maxLng),
    }
    : null

  const gridCountQ = supabase
    .from("h3_aoi_grid")
    .select("h3_id", { count: "exact", head: true })
    .eq("aoi_id", aoi_id)
    .eq("h3_res", res)

  const gridCountQ2 = b
    ? gridCountQ
      .gte("lat", b.minLat)
      .lte("lat", b.maxLat)
      .gte("lng", b.minLng)
      .lte("lng", b.maxLng)
    : gridCountQ

  const { count: grid_count, error: grid_count_err } = await gridCountQ2
  console.log("[DBG v2] grid_count", { aoi_id, res, year, bounds: b, grid_count, grid_count_err })

  const gridRowsQ = supabase
    .from("h3_aoi_grid")
    .select("h3_id, lat, lng")
    .eq("aoi_id", aoi_id)
    .eq("h3_res", res)

  const gridRowsQ2 = b
    ? gridRowsQ
      .gte("lat", b.minLat)
      .lte("lat", b.maxLat)
      .gte("lng", b.minLng)
      .lte("lng", b.maxLng)
    : gridRowsQ

  const { data: grid_rows, error: grid_rows_err } = await gridRowsQ2.limit(30000) // Increased limit
  if (grid_rows_err) {
    console.error("[DBG] grid_rows error", grid_rows_err)
    throw grid_rows_err
  }

  const ids = (grid_rows ?? []).map((r) => r.h3_id)
  console.log("[DBG] grid_rows", { n: ids.length })

  // IMPORTANT: chunk the IN list to avoid request and URL limits at higher resolutions.
  const chunkSize = 500
  const chunks: string[][] = []
  for (let i = 0; i < ids.length; i += chunkSize) chunks.push(ids.slice(i, i + chunkSize))

  const details_all: any[] = []
  for (const idChunk of chunks) {
    const { data, error } = await supabase
      .from("h3_precomputed_hex_details")
      .select(
        "h3_id, property_count, predicted_value, current_value, opportunity, reliability, sample_accuracy, alert_pct, risk_score, score"
      )
      .eq("forecast_year", year)
      .eq("h3_res", res)
      .in("h3_id", idChunk)

    if (error) {
      console.error("[DBG] details chunk error", error)
      throw error
    }
    if (data) details_all.push(...data)
  }

  console.log("[DBG] details_rows", {
    n: details_all.length,
    non_null_opportunity: details_all.filter((d) => d.opportunity !== null).length,
    non_null_reliability: details_all.filter((d) => d.reliability !== null).length,
  })

  // Reconstruct H3HexagonData[]
  const detailsMap = new Map(details_all.map((d: any) => [d.h3_id, d]));

  return (grid_rows ?? []).map((g: any) => {
    const d = detailsMap.get(g.h3_id);
    return {
      h3_id: g.h3_id,
      lat: g.lat,
      lng: g.lng,
      opportunity: d?.opportunity ?? null,
      reliability: d?.reliability ?? null,
      property_count: d?.property_count ?? 0,
      sample_accuracy: d?.sample_accuracy ?? null,
      alert_pct: d?.alert_pct ?? null,
      has_data: !!d // If details exist, we have data
    };
  });
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
