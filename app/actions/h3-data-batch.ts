"use server"

import { getSupabaseServerClient } from "@/lib/supabase/server"
import type { H3HexagonDataV2, ViewportBounds } from "./h3-data-v2"

// Re-use types from v2
export type { H3HexagonDataV2, ViewportBounds }

function asNum(x: unknown): number {
    const v = typeof x === "string" ? Number(x) : (x as number)
    if (!Number.isFinite(v)) throw new Error(`Non-finite number: ${String(x)}`)
    return v
}

export async function getH3DataBatch(
    h3Resolution: number,
    years: number[],
    bounds?: ViewportBounds,
): Promise<Record<number, H3HexagonDataV2[]>> {
    const supabase = await getSupabaseServerClient()
    console.log(`[SERVER-BATCH] getH3DataBatch called for ${years.length} years: ${years.join(", ")}`)

    if (years.length === 0) return {}

    const aoi_id = "harris_county"
    const res = Math.trunc(asNum(h3Resolution))

    // Bounds check
    const b = bounds
        ? {
            minLat: asNum(bounds.minLat),
            maxLat: asNum(bounds.maxLat),
            minLng: asNum(bounds.minLng),
            maxLng: asNum(bounds.maxLng),
        }
        : null

    // 1. Grid Query - The grid is static across years, so we only query it ONCE.
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

    // Paginate Grid (similar to v2)
    let all_grid_rows: any[] = []
    let offset = 0
    const BATCH_SIZE = 1000
    const MAX_ROWS = 30000

    while (true) {
        const { data: batch, error: batchErr } = await gridQuery.range(offset, offset + BATCH_SIZE - 1)
        if (batchErr) throw batchErr
        if (!batch || batch.length === 0) break
        all_grid_rows.push(...batch)
        if (batch.length < BATCH_SIZE) break
        if (all_grid_rows.length >= MAX_ROWS) break
        offset += BATCH_SIZE
    }

    const ids = all_grid_rows.map((r) => r.h3_id)
    if (ids.length === 0) return {}

    // 2. Details Query - Fetch for ALL requested years in one go using IN()
    // We fetch details for all these IDs across all the requested years.
    // Chunk size must account for year multiplier: each ID returns up to `years.length` rows.
    // Supabase default limit is 1000 rows per request. With 13 years, max ~75 IDs per chunk.
    const chunkSize = Math.max(50, Math.floor(900 / years.length))
    const chunks: string[][] = []
    for (let i = 0; i < ids.length; i += chunkSize) chunks.push(ids.slice(i, i + chunkSize))

    // We will collect ALL detail rows here
    let details_all: any[] = []
    let rows_all: any[] = []

    // Process chunks with limited concurrency to avoid overwhelming Supabase
    const CONCURRENCY = 4
    for (let ci = 0; ci < chunks.length; ci += CONCURRENCY) {
        const batch = chunks.slice(ci, ci + CONCURRENCY)
        await Promise.all(
            batch.map(async (idChunk) => {
            // Parallel fetch: Hex Rows + Hex Details
            // Note: We use .in("forecast_year", years) to get all years at once
            const [rowsResult, detailsResult] = await Promise.all([
                supabase
                    .from("h3_precomputed_hex_rows")
                    .select("h3_id, property_count, opportunity, reliability, med_predicted_value, forecast_year")
                    // NOTE: sample_accuracy and alert_pct do NOT exist in hex_rows, only in hex_details
                    .in("forecast_year", years) // Batch Years
                    .eq("h3_res", res)
                    .in("h3_id", idChunk),
                // Removed .limit() - Supabase has its own max (1MB response size)
                // With 13 years × ~2000 cells, we need ~26k rows, 10k limit was dropping years

                supabase
                    .from("h3_precomputed_hex_details")
                    .select("h3_id, property_count, opportunity, reliability, sample_accuracy, alert_pct, predicted_value, forecast_year")
                    .in("forecast_year", years) // Batch Years
                    .eq("h3_res", res)
                    .in("h3_id", idChunk)
                // Removed .limit() - same reasoning as above
            ])

            if (rowsResult.error) {
                console.error("[DBG-BATCH] hex_rows chunk error", rowsResult.error.message)
            }
            if (detailsResult.error) {
                console.error("[DBG-BATCH] hex_details chunk error", detailsResult.error.message)
            }
            if (rowsResult.data) rows_all.push(...rowsResult.data)
            if (detailsResult.data) details_all.push(...detailsResult.data)
        })
        )
    }

    console.log(`[DBG-BATCH] Fetched total: ${rows_all.length} rows, ${details_all.length} details`)

    // 3. Group by Year and Merge
    const result: Record<number, H3HexagonDataV2[]> = {}

    // Create Maps for fast lookup: Map<"year-h3id", Row>
    const rowsMap = new Map(rows_all.map((d: any) => [`${d.forecast_year}-${d.h3_id}`, d]))
    const detailsMap = new Map(details_all.map((d: any) => [`${d.forecast_year}-${d.h3_id}`, d]))

    // Iterate over years, then over grid IDs to rebuild the complete dataset per year
    for (const y of years) {
        const yearData = all_grid_rows.map((g: any) => {
            const key = `${y}-${g.h3_id}`
            const detail = detailsMap.get(key)
            const row = rowsMap.get(key)

            // Merge logic (v2 standard)
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
                med_predicted_value: detail?.predicted_value ?? row?.med_predicted_value ?? null,
                has_data: !!(detail || row)
            }
        })

        // Filter valid only (same as v2)
        result[y] = yearData.filter(h => !isNaN(h.lat) && !isNaN(h.lng) && h.h3_id)
    }

    return result
}
