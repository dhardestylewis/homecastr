"use server"

import { getSupabaseServerClient } from "@/lib/supabase/server"

/* --- Types --- */
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

/* --- Helpers --- */
function asNum(x: unknown): number {
    const v = typeof x === "string" ? Number(x) : (x as number)
    if (!Number.isFinite(v)) throw new Error(`Non-finite number: ${String(x)}`)
    return v
}

/* ------------------------------------------------------------------ */
/*  getH3DataV2 - single year                                         */
/* ------------------------------------------------------------------ */
export async function getH3DataV2(
    h3Resolution: number,
    forecastYear = 2026,
    bounds?: ViewportBounds,
): Promise<H3HexagonDataV2[]> {
    console.log(`[v0] getH3DataV2 called – year=${forecastYear} res=${h3Resolution}`)
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

        /* ---------- 1. Grid (static across years) ---------- */
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
            if (error) { console.error("[v0] grid error", error); throw error }
            if (!page || page.length === 0) break
            all_grid_rows.push(...page)
            if (page.length < PAGE || all_grid_rows.length >= MAX) break
            offset += PAGE
        }

        const ids = all_grid_rows.map((r) => r.h3_id)
        console.log(`[v0] grid rows: ${ids.length}`)
        if (ids.length === 0) return []

        /* ---------- 2. Data (chunked) ---------- */
        const CHUNK = 500
        const chunks: string[][] = []
        for (let i = 0; i < ids.length; i += CHUNK) chunks.push(ids.slice(i, i + CHUNK))

        // Uses Promise.allSettled so a single chunk failure doesn't discard all results
        const settled = await Promise.allSettled(
            chunks.map(async (idChunk) => {
                const [rowsRes, detailsRes] = await Promise.all([
                    // hex_rows does NOT have alert_pct or sample_accuracy
                    supabase
                        .from("h3_precomputed_hex_rows")
                        .select("h3_id, property_count, opportunity, reliability, med_predicted_value")
                        .eq("forecast_year", year)
                        .eq("h3_res", res)
                        .in("h3_id", idChunk)
                        .range(0, 999),

                    // hex_details has alert_pct and sample_accuracy
                    supabase
                        .from("h3_precomputed_hex_details")
                        .select("h3_id, property_count, opportunity, reliability, sample_accuracy, alert_pct, predicted_value")
                        .eq("forecast_year", year)
                        .eq("h3_res", res)
                        .in("h3_id", idChunk)
                        .range(0, 999),
                ])

                if (rowsRes.error) console.error("[v0] hex_rows chunk err:", rowsRes.error.message)
                if (detailsRes.error) console.error("[v0] hex_details chunk err:", detailsRes.error.message)

                return {
                    rows: rowsRes.data ?? [],
                    details: detailsRes.data ?? [],
                }
            }),
        )

        // Collect successful results, log failures
        const successfulResults = settled
            .filter((s): s is PromiseFulfilledResult<{ rows: any[]; details: any[] }> => {
                if (s.status === "rejected") {
                    console.error("[v0] chunk failed (partial results preserved):", s.reason)
                    return false
                }
                return true
            })
            .map((s) => s.value)

        const rows_all = successfulResults.flatMap((r) => r.rows)
        const details_all = successfulResults.flatMap((r) => r.details)

        console.log(`[v0] year=${year} res=${res}: hex_rows=${rows_all.length}, hex_details=${details_all.length}`)

        /* ---------- 3. Merge ---------- */
        const rowsMap = new Map(rows_all.map((d: any) => [d.h3_id, d]))
        const detailsMap = new Map(details_all.map((d: any) => [d.h3_id, d]))

        return all_grid_rows.map((g: any) => {
            const detail = detailsMap.get(g.h3_id)
            const row = rowsMap.get(g.h3_id)

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
    } catch (err) {
        console.error("[v0] Critical error in getH3DataV2", err)
        return []
    }
}
