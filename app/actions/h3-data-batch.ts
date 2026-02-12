"use server"

import { getSupabaseServerClient } from "@/lib/supabase/server"
import type { H3HexagonDataV2, ViewportBounds } from "./h3-data-v2"

export type { H3HexagonDataV2, ViewportBounds }

/* --- Helpers --- */
function asNum(x: unknown): number {
    const v = typeof x === "string" ? Number(x) : (x as number)
    if (!Number.isFinite(v)) throw new Error(`Non-finite number: ${String(x)}`)
    return v
}

/* ------------------------------------------------------------------ */
/*  getH3DataBatch - multi-year prefetch                              */
/* ------------------------------------------------------------------ */
export async function getH3DataBatch(
    h3Resolution: number,
    years: number[],
    bounds?: ViewportBounds,
): Promise<Record<number, H3HexagonDataV2[]>> {
    console.log(`[v0] getH3DataBatch called – ${years.length} years: ${years.join(", ")}`)

    if (years.length === 0) return {}

    const supabase = await getSupabaseServerClient()
    const aoi_id = "harris_county"
    const res = Math.trunc(asNum(h3Resolution))

    const b = bounds
        ? {
              minLat: asNum(bounds.minLat),
              maxLat: asNum(bounds.maxLat),
              minLng: asNum(bounds.minLng),
              maxLng: asNum(bounds.maxLng),
          }
        : null

    /* ---------- 1. Grid (static across years, query once) ---------- */
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

    const all_grid_rows: any[] = []
    let offset = 0
    const PAGE = 1000
    const MAX = 30000

    while (true) {
        const { data: page, error } = await gridQuery.range(offset, offset + PAGE - 1)
        if (error) throw error
        if (!page || page.length === 0) break
        all_grid_rows.push(...page)
        if (page.length < PAGE || all_grid_rows.length >= MAX) break
        offset += PAGE
    }

    const ids = all_grid_rows.map((r) => r.h3_id)
    console.log(`[v0] batch grid rows: ${ids.length}`)
    if (ids.length === 0) return {}

    /* ---------- 2. Data (chunked with year-aware sizing) ---------- */
    // Each ID x year = 1 row. Supabase caps at 1000 rows default.
    const CHUNK = Math.max(50, Math.floor(900 / years.length))
    const chunks: string[][] = []
    for (let i = 0; i < ids.length; i += CHUNK) chunks.push(ids.slice(i, i + CHUNK))

    const rows_all: any[] = []
    const details_all: any[] = []

    // Limited concurrency to avoid overwhelming Supabase
    const CONCURRENCY = 4
    for (let ci = 0; ci < chunks.length; ci += CONCURRENCY) {
        const batch = chunks.slice(ci, ci + CONCURRENCY)
        await Promise.all(
            batch.map(async (idChunk) => {
                const [rowsRes, detailsRes] = await Promise.all([
                    // hex_rows does NOT have alert_pct or sample_accuracy
                    supabase
                        .from("h3_precomputed_hex_rows")
                        .select("h3_id, property_count, opportunity, reliability, med_predicted_value, forecast_year")
                        .in("forecast_year", years)
                        .eq("h3_res", res)
                        .in("h3_id", idChunk),

                    // hex_details has alert_pct and sample_accuracy
                    supabase
                        .from("h3_precomputed_hex_details")
                        .select("h3_id, property_count, opportunity, reliability, sample_accuracy, alert_pct, predicted_value, forecast_year")
                        .in("forecast_year", years)
                        .eq("h3_res", res)
                        .in("h3_id", idChunk),
                ])

                if (rowsRes.error) console.error("[v0] batch hex_rows err:", rowsRes.error.message)
                if (detailsRes.error) console.error("[v0] batch hex_details err:", detailsRes.error.message)

                if (rowsRes.data) rows_all.push(...rowsRes.data)
                if (detailsRes.data) details_all.push(...detailsRes.data)
            }),
        )
    }

    console.log(`[v0] batch totals: ${rows_all.length} rows, ${details_all.length} details`)

    /* ---------- 3. Group by year & merge ---------- */
    const rowsMap = new Map(rows_all.map((d: any) => [`${d.forecast_year}-${d.h3_id}`, d]))
    const detailsMap = new Map(details_all.map((d: any) => [`${d.forecast_year}-${d.h3_id}`, d]))

    const result: Record<number, H3HexagonDataV2[]> = {}

    for (const y of years) {
        result[y] = all_grid_rows
            .map((g: any) => {
                const key = `${y}-${g.h3_id}`
                const detail = detailsMap.get(key)
                const row = rowsMap.get(key)

                return {
                    h3_id: g.h3_id,
                    lat: g.lat,
                    lng: g.lng,
                    opportunity: detail?.opportunity ?? row?.opportunity ?? null,
                    reliability: detail?.reliability ?? row?.reliability ?? null,
                    property_count: detail?.property_count ?? row?.property_count ?? 0,
                    sample_accuracy: detail?.sample_accuracy ?? null,
                    alert_pct: detail?.alert_pct ?? null,
                    med_years: detail?.med_years ?? null,
                    med_predicted_value: detail?.predicted_value ?? row?.med_predicted_value ?? null,
                    has_data: !!(detail || row),
                }
            })
            .filter((h) => !isNaN(h.lat) && !isNaN(h.lng) && h.h3_id)
    }

    return result
}
