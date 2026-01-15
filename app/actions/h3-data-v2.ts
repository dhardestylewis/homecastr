"use server"

import { getSupabaseServerClient } from "@/lib/supabase/server"

export interface H3HexagonDataV2 {
    h3_id: string
    lat: number
    lng: number
    opportunity: number | null
    reliability: number | null
    property_count: number
    sample_accuracy: number | null
    alert_pct: number | null
    med_years: number | null
    has_data: boolean
}

export interface ViewportBounds {
    minLat: number
    maxLat: number
    minLng: number
    maxLng: number
}

function asNum(x: unknown): number {
    const v = typeof x === "string" ? Number(x) : (x as number)
    if (!Number.isFinite(v)) throw new Error(`Non-finite number: ${String(x)}`)
    return v
}

export async function getH3DataV2(
    h3Resolution: number,
    forecastYear = 2026,
    bounds?: ViewportBounds
): Promise<H3HexagonDataV2[]> {
    const supabase = await getSupabaseServerClient()
    console.log("[SERVER-V2] getH3DataV2 called (Fresh Endpoint)")

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

    // 1. Grid Query
    let gridQuery = supabase
        .from("h3_aoi_grid")
        .select("h3_id, lat, lng")
        .eq("aoi_id", aoi_id)
        .eq("h3_res", res)

    if (b) {
        gridQuery = gridQuery
            .gte("lat", b.minLat)
            .lte("lat", b.maxLat)
            .gte("lng", b.minLng)
            .lte("lng", b.maxLng)
    }

    // LIMIT 30,000 to ensure we get all tiles
    // Use .range() instead of .limit() to override Supabase default 1000 row limit
    const { data: grid_rows, error: grid_rows_err } = await gridQuery.range(0, 29999)

    if (grid_rows_err) {
        console.error("[DBG] grid_rows error", grid_rows_err)
        throw grid_rows_err
    }

    const ids = (grid_rows ?? []).map((r) => r.h3_id)
    console.log("[DBG-V2] grid_rows fetched:", ids.length)

    if (ids.length === 0) return []

    // 2. Details Query (Chunked)
    const chunkSize = 500
    const chunks: string[][] = []
    for (let i = 0; i < ids.length; i += chunkSize) chunks.push(ids.slice(i, i + chunkSize))

    const details_all: any[] = []

    for (const idChunk of chunks) {
        const { data, error } = await supabase
            .from("h3_precomputed_hex_rows")  // Changed from hex_details - that table is empty!
            .select(
                "h3_id, property_count, opportunity, reliability, sample_accuracy, alert_pct"
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

    console.log("[DBG-V2] details_rows fetched:", details_all.length)

    // 3. Merge
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
            med_years: d?.med_years ?? null,
            has_data: !!d
        };
    });
}
