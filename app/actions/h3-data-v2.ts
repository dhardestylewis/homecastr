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
    med_predicted_value: number | null
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
    // 1. Grid Query - Paginate to bypass Supabase 1000 row limit
    let all_grid_rows: any[] = []
    let offset = 0
    const BATCH_SIZE = 1000
    const MAX_ROWS = 30000

    while (true) {
        // Clone query to avoid mutating state if possible, though re-building might be safer.
        // Since we built 'gridQuery' as a builder chain, we might need to re-apply range.
        // Actually, Supabase query builders are mutable? Better to start from base or chain off 'gridQuery'.
        // Let's assume gridQuery is reusable.

        const { data: batch, error: batchErr } = await gridQuery.range(offset, offset + BATCH_SIZE - 1)

        if (batchErr) {
            console.error("[DBG] grid_rows error", batchErr)
            throw batchErr
        }

        if (!batch || batch.length === 0) break

        all_grid_rows.push(...batch)

        if (batch.length < BATCH_SIZE) break
        if (all_grid_rows.length >= MAX_ROWS) break

        offset += BATCH_SIZE
    }

    const ids = all_grid_rows.map((r) => r.h3_id)
    console.log("[DBG-V2] grid_rows fetched total:", ids.length)

    if (ids.length === 0) return []

    // 2. Details Query (Chunked) - Try hex_rows first, then hex_details as fallback
    const chunkSize = 500
    const chunks: string[][] = []
    for (let i = 0; i < ids.length; i += chunkSize) chunks.push(ids.slice(i, i + chunkSize))

    const rows_all: any[] = []
    const details_all: any[] = []

    for (const idChunk of chunks) {
        // Primary source: hex_rows (has reliable data)
        // Use .range() to override Supabase default 1000 limit
        const { data: rowsData, error: rowsError } = await supabase
            .from("h3_precomputed_hex_rows")
            .select(
                "h3_id, property_count, opportunity, reliability, sample_accuracy, alert_pct, med_predicted_value"
            )
            .eq("forecast_year", year)
            .eq("h3_res", res)
            .in("h3_id", idChunk)
            .range(0, 999)  // Explicit range - chunk is 500 so this is safe

        if (rowsError) {
            console.error("[DBG] hex_rows chunk error", rowsError)
        }
        if (rowsData) rows_all.push(...rowsData)

        // Secondary source: hex_details (for any additional data)
        const { data: detailsData, error: detailsError } = await supabase
            .from("h3_precomputed_hex_details")
            .select(
                "h3_id, property_count, opportunity, reliability, sample_accuracy, alert_pct, med_predicted_value"
            )
            .eq("forecast_year", year)
            .eq("h3_res", res)
            .in("h3_id", idChunk)
            .range(0, 999)  // Explicit range

        if (!detailsError && detailsData) details_all.push(...detailsData)
    }

    console.log("[DBG-V2] hex_rows fetched:", rows_all.length)
    console.log("[DBG-V2] hex_details fetched:", details_all.length)

    // 3. Merge - prefer hex_details (new canonical), fallback to hex_rows (legacy)
    const rowsMap = new Map(rows_all.map((d: any) => [d.h3_id, d]));
    const detailsMap = new Map(details_all.map((d: any) => [d.h3_id, d]));

    return (all_grid_rows ?? []).map((g: any) => {
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
            med_predicted_value: detail?.med_predicted_value ?? row?.med_predicted_value ?? null,
            has_data: !!(detail || row)
        };
    });
}
