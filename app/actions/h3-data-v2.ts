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
    console.log("[SERVER-V2] getH3DataV2 called (Fresh Endpoint)")
    try {
        const supabase = await getSupabaseServerClient()

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

        // Parallelize chunk processing to prevent timeouts
        const results = await Promise.all(
            chunks.map(async (idChunk) => {
                const chunkRows: any[] = []
                const chunkDetails: any[] = []

                // Parallel fetch for this chunk
                const [rowsResult, detailsResult] = await Promise.all([
                    supabase
                        .from("h3_precomputed_hex_rows")
                        .select(
                            "h3_id, property_count, opportunity, reliability, alert_pct, med_predicted_value"
                            // NOTE: sample_accuracy does NOT exist in hex_rows, only in hex_details
                        )
                        .eq("forecast_year", year)
                        .eq("h3_res", res)
                        .in("h3_id", idChunk)
                        .range(0, 999), // Explicit range

                    supabase
                        .from("h3_precomputed_hex_details")
                        .select(
                            "h3_id, property_count, opportunity, reliability, sample_accuracy, alert_pct, predicted_value"
                        )
                        .eq("forecast_year", year)
                        .eq("h3_res", res)
                        .in("h3_id", idChunk)
                        .range(0, 999) // Explicit range
                ])

                if (rowsResult.error) {
                    console.error("[DBG] hex_rows chunk error", rowsResult.error)
                }
                if (rowsResult.data) chunkRows.push(...rowsResult.data)

                if (!detailsResult.error && detailsResult.data) {
                    chunkDetails.push(...detailsResult.data)
                }

                return { rows: chunkRows, details: chunkDetails }
            })
        )


        // Flatten results
        const rows_all = results.flatMap(r => r.rows)
        const details_all = results.flatMap(r => r.details)

        console.log(`[DBG-V2] Year ${year} Res ${res}: hex_rows=${rows_all.length}, hex_details=${details_all.length}`)

        // Sample some data to debug opportunity values
        if (details_all.length > 0) {
            const sample = details_all.slice(0, 3)
            console.log(`[DBG-V2] Sample details for year ${year}:`, sample.map((d: any) => ({
                h3_id: d.h3_id?.slice(0, 10),
                opportunity: d.opportunity,
                predicted_value: d.predicted_value
            })))
        } else if (rows_all.length > 0) {
            const sample = rows_all.slice(0, 3)
            console.log(`[DBG-V2] Sample rows for year ${year}:`, sample.map((d: any) => ({
                h3_id: d.h3_id?.slice(0, 10),
                opportunity: d.opportunity,
                med_predicted_value: d.med_predicted_value
            })))
        } else {
            console.log(`[DBG-V2] NO DATA found for year ${year} at res ${res}`)
        }

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
                // FIX: Map predicted_value from details to med_predicted_value
                med_predicted_value: detail?.predicted_value ?? row?.med_predicted_value ?? null,
                has_data: !!(detail || row)
            };
        });
    } catch (err) {
        console.error("[SERVER-V2] Critical error in getH3DataV2", err)
        return []
    }
}
