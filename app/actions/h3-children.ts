"use server"

import { getSupabaseServerClient } from "@/lib/supabase/server"
import { cellToChildren, getResolution } from "h3-js"

export interface ChildTimeline {
    id: string
    values: ({ year: number, value: number } | null)[]
}

/**
 * Fetches timelines for all direct children of a hex cell.
 * Used for "spaghetti plot" visualization in tooltips.
 */
export async function getH3ChildTimelines(parentId: string, yearStart = 2019, yearEnd = 2030): Promise<number[][]> {
    const supabase = await getSupabaseServerClient()

    // 1. Calculate children IDs (Resolution + 1)
    const currentRes = getResolution(parentId)
    // Limit: Don't go deeper than Res 10/11 to prevent massive queries if used improperly
    if (currentRes >= 11) return []

    const children = cellToChildren(parentId, currentRes + 1)

    // 2. Fetch data for all children
    const { data, error } = await supabase
        .from("h3_precomputed_hex_details")
        .select("h3_id, forecast_year, predicted_value")
        .in("h3_id", children)
        .gte("forecast_year", yearStart)
        .lte("forecast_year", yearEnd)
        .order("forecast_year", { ascending: true })

    if (error || !data) {
        console.error("Failed to fetch child timelines", error)
        return []
    }

    // 3. Group by H3 ID
    const timelineMap = new Map<string, Map<number, number>>()

    data.forEach(row => {
        if (!timelineMap.has(row.h3_id)) {
            timelineMap.set(row.h3_id, new Map())
        }
        if (row.predicted_value != null) {
            timelineMap.get(row.h3_id)!.set(row.forecast_year, row.predicted_value)
        }
    })

    // 4. Format as uniform arrays aligned with [yearStart...yearEnd]
    const years = Array.from({ length: yearEnd - yearStart + 1 }, (_, i) => yearStart + i)
    const result: number[][] = []

    for (const [id, yearMap] of timelineMap.entries()) {
        const line: number[] = []
        let hasData = false
        for (const y of years) {
            const val = yearMap.get(y)
            if (val !== undefined) {
                line.push(val)
                hasData = true
            } else {
                // Return 0 or NaN/null? FanChart filters !Number.isFinite.
                // Using NaN to create gaps is safer.
                line.push(NaN)
            }
        }
        if (hasData) {
            result.push(line)
        }
    }

    return result
}
