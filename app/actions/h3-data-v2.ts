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

    // 2. Details Query (Chunked) - Try hex_rows first, then hex_details as fallback
    const chunkSize = 500
    const chunks: string[][] = []
    for (let i = 0; i < ids.length; i += chunkSize) chunks.push(ids.slice(i, i + chunkSize))

    const rows_all: any[] = []
    const details_all: any[] = []

    for (const idChunk of chunks) {
        // Primary source: hex_rows (has reliable data)
        const { data: rowsData, error: rowsError } = await supabase
            .from("h3_precomputed_hex_rows")
            .select(
                "h3_id, property_count, opportunity, reliability, sample_accuracy, alert_pct"
            )
            .eq("forecast_year", year)
            .eq("h3_res", res)
            .in("h3_id", idChunk)

        if (rowsError) {
            console.error("[DBG] hex_rows chunk error", rowsError)
        }
        if (rowsData) rows_all.push(...rowsData)

        // Secondary source: hex_details (for any additional data)
        const { data: detailsData, error: detailsError } = await supabase
            .from("h3_precomputed_hex_details")
            .select(
                "h3_id, property_count, opportunity, reliability, sample_accuracy, alert_pct"
            )
            .eq("forecast_year", year)
            .eq("h3_res", res)
            .in("h3_id", idChunk)

        if (!detailsError && detailsData) details_all.push(...detailsData)
    }

    console.log("[DBG-V2] hex_rows fetched:", rows_all.length)
    console.log("[DBG-V2] hex_details fetched:", details_all.length)

    // 3. Merge - prefer hex_details (new canonical), fallback to hex_rows (legacy)
    const rowsMap = new Map(rows_all.map((d: any) => [d.h3_id, d]));
    const detailsMap = new Map(details_all.map((d: any) => [d.h3_id, d]));

    return (grid_rows ?? []).map((g: any) => {
        // Field-level merge: prefer hex_details value if non-null, fallback to hex_rows
        const detail = detailsMap.get(g.h3_id);
        const row = rowsMap.get(g.h3_id);

        return {
            h3_id: g.h3_id,
            lat: g.lat,
            lng: g.lng,
            opportunity: detail?.opportunity ?? row?.opportunity ?? null,
            reliability: detail?.reliability ?? row?.reliability ?? null,
            property_count: detail?.property_count ?? row?.property_count ?? 0,
            sample_accuracy: detail?.sample_accuracy ?? row?.sample_accuracy ?? null,
            alert_pct: detail?.alert_pct ?? row?.alert_pct ?? null,
            med_years: detail?.med_years ?? row?.med_years ?? null,
            has_data: !!(detail || row)
        };
    });
}
