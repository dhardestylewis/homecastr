"use server"

import { getSupabaseServerClient } from "@/lib/supabase/server"
import * as h3 from "h3-js"
import { initLogger, traced, currentSpan, flush } from "braintrust"

if (typeof process !== "undefined" && process.env.BRAINTRUST_API_KEY) {
  initLogger({
    projectName: "Homecastr",
    apiKey: process.env.BRAINTRUST_API_KEY,
  })
}

/**
 * Internal helper to calculate metrics consistently across all tools.
 * Handles the 2026 baseline (0% change) and fallback to raw opportunity if current_value is missing.
 */
function calculateMetrics(data: any, currentValue: number | null, yearsOut: number) {
    if (!data) return null
    const predictedValue = data.predicted_value || 0
    let annualChange = 0

    if (currentValue && currentValue > 0 && yearsOut > 0) {
        const totalGrowth = (predictedValue - currentValue) / currentValue
        annualChange = Math.round((totalGrowth / yearsOut) * 10000) / 100
    } else if (yearsOut === 0) {
        annualChange = 0 // Forced 0% for baseline year
    } else {
        // Fallback to precomputed opportunity if we don't have a 2026 baseline for this specific hex
        annualChange = data.opportunity != null ? Math.round(data.opportunity * 10000) / 100 : 0
    }

    return {
        annual_change_pct: annualChange,
        current_value: currentValue,
        predicted_value: predictedValue,
        property_count: data.property_count,
    }
}

/**
 * Execute a tool call against real data sources (Supabase + Nominatim + h3-js).
 * Reusable by both Chat API and Tavus Frontend.
 */
export async function executeTopLevelTool(toolName: string, args: Record<string, any>): Promise<string> {
    console.log(`[Server Tool] Executing ${toolName}`, args)

    const result = await traced(async (span) => {
      span.log({ input: { tool: toolName, args } })
      const output = await _executeToolInner(toolName, args)
      span.log({ output: JSON.parse(output) })
      return output
    }, { name: `Tool: ${toolName}` })

    if (process.env.BRAINTRUST_API_KEY) {
      await flush().catch(() => {})
    }

    return result
}

async function _executeToolInner(toolName: string, args: Record<string, any>): Promise<string> {
    switch (toolName) {
        case "resolve_place": {
            try {
                // Biasing towards Houston metro area without forcing city limits
                const querySuffix = (args.query.toLowerCase().includes("houston") || args.query.toLowerCase().includes("tx")) ? "" : ", TX"
                const params = new URLSearchParams({
                    q: args.query + querySuffix,
                    format: "json",
                    limit: String(args.max_candidates || 3),
                    addressdetails: "1",
                    viewbox: "-95.96,30.17,-94.90,29.50",
                    bounded: "1",
                })
                const res = await fetch(`https://nominatim.openstreetmap.org/search?${params}`, {
                    headers: { "User-Agent": "HomecastrUI/1.0" },
                })
                if (!res.ok) return JSON.stringify({ candidates: [], error: "Geocode failed" })
                const data = await res.json()
                const candidates = (data || []).map((item: any) => ({
                    label: item.display_name,
                    lat: parseFloat(item.lat),
                    lng: parseFloat(item.lon),
                    confidence: 0.9,
                    kind: item.type || "place",
                    bbox: item.boundingbox || null,
                }))
                return JSON.stringify({ candidates })
            } catch (e: any) {
                console.error("[Chat Tool] resolve_place error:", e.message)
                return JSON.stringify({ candidates: [], error: e.message })
            }
        }

        case "point_to_hex": {
            try {
                const res = args.h3_res ?? 9
                const hexId = h3.latLngToCell(args.lat, args.lng, res)
                return JSON.stringify({ h3_id: hexId, h3_res: res })
            } catch (e: any) {
                return JSON.stringify({ error: e.message })
            }
        }

        case "get_h3_hex": {
            try {
                const supabase = await getSupabaseServerClient()
                const forecastYear = args.forecast_year || 2029
                const yearsOut = forecastYear - 2026

                const fetchMetrics = async (targetId: string) => {
                    const [fRes, cRes] = await Promise.all([
                        supabase
                            .from("h3_precomputed_hex_details")
                            .select("h3_id, h3_res, forecast_year, opportunity, predicted_value, property_count, lat, lng")
                            .eq("h3_id", targetId)
                            .eq("forecast_year", forecastYear)
                            .single(),
                        supabase
                            .from("h3_precomputed_hex_details")
                            .select("predicted_value")
                            .eq("h3_id", targetId)
                            .eq("forecast_year", 2026)
                            .single(),
                    ])
                    return { forecast: fRes.data, current: cRes.data?.predicted_value ?? null }
                }

                // Attempt primary hex
                let targetHexId = args.h3_id
                let result = await fetchMetrics(targetHexId)
                let neighborhoodContext = false

                if (!result.forecast || result.forecast.property_count === 0) {
                    // Neighborhood Context Fallback
                    const neighbors = h3.gridDisk(targetHexId, 1)
                    const { data: nearbyHexes } = await supabase
                        .from("h3_precomputed_hex_details")
                        .select("h3_id, property_count")
                        .eq("forecast_year", forecastYear)
                        .in("h3_id", neighbors)
                        .gt("property_count", 0)
                        .order("property_count", { ascending: false })
                        .limit(1)

                    if (nearbyHexes && nearbyHexes.length > 0) {
                        targetHexId = nearbyHexes[0].h3_id
                        result = await fetchMetrics(targetHexId)
                        neighborhoodContext = true
                    }
                }

                const data = result.forecast
                if (!data) return JSON.stringify({ hex: null, error: "Hex data not found" })

                const metrics = calculateMetrics(data, result.current, yearsOut)

                let locationName = `H3 Cell`
                try {
                    const revRes = await fetch(
                        `https://nominatim.openstreetmap.org/reverse?lat=${data.lat}&lon=${data.lng}&zoom=16&format=json`,
                        { headers: { "User-Agent": "HomecastrUI/1.0" } }
                    )
                    if (revRes.ok) {
                        const revData = await revRes.json()
                        const addr = revData.address || {}
                        locationName = addr.suburb || addr.neighbourhood || addr.road || revData.display_name?.split(",")[0] || locationName
                    }
                } catch { }

                return JSON.stringify({
                    hex: {
                        h3_id: data.h3_id,
                        h3_res: data.h3_res,
                        forecast_year: data.forecast_year,
                        years_out: yearsOut,
                        location: { name: locationName, lat: data.lat, lng: data.lng },
                        metrics,
                        context: neighborhoodContext ? "neighborhood_average" : "exact_location"
                    },
                })
            } catch (e: any) {
                return JSON.stringify({ hex: null, error: e.message })
            }
        }

        case "location_to_hex": {
            return await resolveLocationToHex(args, "location_to_hex")
        }

        case "add_location_to_selection": {
            // Support direct ID addition (single or multiple)
            if (args.h3_ids && args.h3_ids.length > 0) {
                return JSON.stringify({
                    action: "add_location_to_selection",
                    chosen: { label: `${args.h3_ids.length} locations`, kind: "collection" },
                    h3: { h3_ids: args.h3_ids, context: "collection" }
                })
            }
            if (args.h3_id) {
                return JSON.stringify({
                    action: "add_location_to_selection",
                    chosen: { label: "Selected Location", kind: "hex" },
                    h3: { h3_id: args.h3_id, context: "hex" }
                })
            }
            return await resolveLocationToHex(args, "add_location_to_selection")
        }

        case "clear_selection": {
            return JSON.stringify({
                success: true,
                action: "clear_selection"
            })
        }

        case "search_h3_hexes":
        case "rank_h3_hexes": {
            try {
                const supabase = await getSupabaseServerClient()
                const forecastYear = args.forecast_year || 2029
                const yearsOut = forecastYear - 2026
                const limit = Math.min(args.limit || 5, 10)
                const sortBy = args.objective || args.sort || "opportunity"

                const sortColumn = sortBy === "reliability" ? "reliability"
                    : sortBy === "predicted_value" || sortBy === "value" ? "predicted_value"
                        : "opportunity"

                let query = supabase
                    .from("h3_precomputed_hex_details")
                    .select("h3_id, h3_res, forecast_year, opportunity, predicted_value, property_count")
                    .eq("forecast_year", forecastYear)
                    .eq("h3_res", args.h3_res ?? 9)
                    .not(sortColumn, "is", null)

                if (args.bbox) {
                    const [minLat, maxLat, minLng, maxLng] = args.bbox
                    const polygon = [
                        [minLat, minLng], [maxLat, minLng],
                        [maxLat, maxLng], [minLat, maxLng], [minLat, minLng]
                    ]
                    const hexIds = h3.polygonToCells(polygon, args.h3_res ?? 9)
                    if (hexIds.length > 5000) query = query.in("h3_id", hexIds.slice(0, 2000))
                    else if (hexIds.length > 0) query = query.in("h3_id", hexIds)
                    else return JSON.stringify({ hexes: [] })
                }

                query = query.order(sortColumn, { ascending: false }).limit(limit)
                const { data, error } = await query
                if (error) return JSON.stringify({ hexes: [], error: error.message })

                const foundHexIds = (data || []).map((row: any) => row.h3_id)
                let currentValues: Record<string, number> = {}

                if (foundHexIds.length > 0) {
                    const { data: currentData } = await supabase
                        .from("h3_precomputed_hex_details")
                        .select("h3_id, predicted_value")
                        .eq("forecast_year", 2026)
                        .in("h3_id", foundHexIds)
                    currentData?.forEach(row => {
                        currentValues[row.h3_id] = row.predicted_value
                    })
                }

                const getName = async (lat: number, lng: number) => {
                    try {
                        const res = await fetch(`https://nominatim.openstreetmap.org/reverse?lat=${lat}&lon=${lng}&zoom=16&format=json`, {
                            headers: { "User-Agent": "HomecastrUI/1.0" },
                        })
                        if (res.ok) {
                            const d = await res.json()
                            const addr = d.address || {}
                            return addr.suburb || addr.neighbourhood || d.display_name?.split(",")[0] || "H3 Cell"
                        }
                    } catch { }
                    return "H3 Cell"
                }

                const hexes = await Promise.all((data || []).map(async (row: any, i: number) => {
                    const [lat, lng] = h3.cellToLatLng(row.h3_id)
                    const locationName = await getName(lat, lng)
                    const currentValue = currentValues[row.h3_id] || null
                    const metrics = calculateMetrics(row, currentValue, yearsOut)

                    return {
                        h3_id: row.h3_id,
                        h3_res: row.h3_res,
                        forecast_year: row.forecast_year,
                        location: { name: locationName, lat, lng },
                        metrics,
                        reasons: [`Rank #${i + 1} by ${sortBy}`],
                    }
                }))

                return JSON.stringify({ hexes, cursor: null })
            } catch (e: any) {
                return JSON.stringify({ hexes: [], error: e.message })
            }
        }

        case "compare_h3_hexes": {
            try {
                const supabase = await getSupabaseServerClient()
                const forecastYear = args.forecast_year || 2029
                const yearsOut = forecastYear - 2026

                const [leftRes, rightRes, leftCurrentRes, rightCurrentRes] = await Promise.all([
                    supabase
                        .from("h3_precomputed_hex_details")
                        .select("h3_id, opportunity, reliability, predicted_value, property_count, lat, lng")
                        .eq("h3_id", args.left_h3_id)
                        .eq("forecast_year", forecastYear)
                        .single(),
                    supabase
                        .from("h3_precomputed_hex_details")
                        .select("h3_id, opportunity, reliability, predicted_value, property_count, lat, lng")
                        .eq("h3_id", args.right_h3_id)
                        .eq("forecast_year", forecastYear)
                        .single(),
                    supabase
                        .from("h3_precomputed_hex_details")
                        .select("predicted_value")
                        .eq("h3_id", args.left_h3_id)
                        .eq("forecast_year", 2026)
                        .single(),
                    supabase
                        .from("h3_precomputed_hex_details")
                        .select("predicted_value")
                        .eq("h3_id", args.right_h3_id)
                        .eq("forecast_year", 2026)
                        .single(),
                ])

                const left = leftRes.data
                const right = rightRes.data
                const leftCurrentValue = leftCurrentRes.data?.predicted_value ?? null
                const rightCurrentValue = rightCurrentRes.data?.predicted_value ?? null

                const getName = async (lat: number, lng: number) => {
                    try {
                        const res = await fetch(`https://nominatim.openstreetmap.org/reverse?lat=${lat}&lon=${lng}&zoom=16&format=json`, {
                            headers: { "User-Agent": "HomecastrUI/1.0" },
                        })
                        if (res.ok) {
                            const d = await res.json()
                            const addr = d.address || {}
                            return addr.suburb || addr.neighbourhood || d.display_name?.split(",")[0] || "Unknown"
                        }
                    } catch { }
                    return "Unknown"
                }

                const [leftName, rightName] = await Promise.all([
                    left ? getName(left.lat, left.lng) : "Unknown",
                    right ? getName(right.lat, right.lng) : "Unknown",
                ])

                const highlights: string[] = []
                if (left && right) {
                    if ((left.opportunity ?? 0) > (right.opportunity ?? 0)) highlights.push(`${leftName} has a higher expected annual change`)
                    else if ((right.opportunity ?? 0) > (left.opportunity ?? 0)) highlights.push(`${rightName} has a higher expected annual change`)
                }

                return JSON.stringify({
                    left: { h3_id: args.left_h3_id, name: leftName, metrics: calculateMetrics(left, leftCurrentValue, yearsOut) },
                    right: { h3_id: args.right_h3_id, name: rightName, metrics: calculateMetrics(right, rightCurrentValue, yearsOut) },
                    highlights,
                })
            } catch (e: any) {
                return JSON.stringify({ error: e.message })
            }
        }

        case "explain_metric": {
            const explanations: Record<string, string> = {
                opportunity: "Expected annual change shows how much property values are projected to change each year over the next 4 years.",
                predicted_value: "The predicted median home value in this area by the forecast year.",
                property_count: "The number of properties in this hex cell used to calculate the forecast.",
                sample_accuracy: "How accurately our model predicted past values in this area.",
            }
            return JSON.stringify({
                short: explanations[args.metric] || `${args.metric} is a key real estate metric.`,
                long: explanations[args.metric] || `${args.metric} is used to evaluate real estate investment potential.`,
            })
        }

        case "inspect_location": {
            return JSON.stringify({
                success: true,
                action: "inspecting specific property",
                h3_id: args.h3_id,
                lat: args.lat,
                lng: args.lng,
                zoom: args.zoom
            })
        }

        case "inspect_neighborhood": {
            return JSON.stringify({
                success: true,
                action: "inspecting neighborhood",
                h3_ids: args.h3_ids,
                lat: args.lat,
                lng: args.lng,
                zoom: args.zoom
            })
        }

        case "record_feedback": {
            const feedbackId = "fb_" + Date.now()
            try {
              const score = typeof args.score === "number" ? args.score
                : args.helpful === true ? 1
                : args.helpful === false ? 0
                : undefined
              const span = currentSpan()
              span.log({
                input: { feedback_type: args.type || "general", comment: args.comment },
                output: { recorded: true, id: feedbackId },
                scores: {
                  ...(score !== undefined ? { helpfulness: score } : {}),
                  ...(args.relevance !== undefined ? { relevance: args.relevance } : {}),
                },
                metadata: { feedback_id: feedbackId },
              })
            } catch {}
            return JSON.stringify({ recorded: true, id: feedbackId })
        }

        default:
            return JSON.stringify({ error: "Tool not found" })
    }
}

function suffixForLabel(addr: any) {
    if (addr.city) return `, ${addr.city}, TX`
    if (addr.town) return `, ${addr.town}, TX`
    if (addr.village) return `, ${addr.village}, TX`
    return ", Houston, TX"
}

// Extracted logic for reusability between location_to_hex and add_location_to_selection
async function resolveLocationToHex(args: any, actionName: string) {
    try {
        // Better geocoding logic: don't force "Houston city" if neighborhood or other city is specified
        const querySuffix = (args.query.toLowerCase().includes("houston") || args.query.toLowerCase().includes("tx")) ? "" : ", TX"
        const params = new URLSearchParams({
            q: args.query + querySuffix,
            format: "json",
            limit: "1",
            addressdetails: "1",
            viewbox: "-95.96,30.17,-94.90,29.50", // Houston metro bounding box
            bounded: "1",
        })
        const geoRes = await fetch(`https://nominatim.openstreetmap.org/search?${params}`, {
            headers: { "User-Agent": "HomecastrUI/1.0" },
        })
        if (!geoRes.ok) return JSON.stringify({ error: "Geocode failed" })
        const geoData = await geoRes.json()
        if (!geoData || geoData.length === 0) return JSON.stringify({ error: "Location not found" })

        const place = geoData[0]
        const lat = parseFloat(place.lat)
        const lng = parseFloat(place.lon)
        const res = args.h3_res ?? 9
        const hexId = h3.latLngToCell(lat, lng, res)

        let metrics = null
        let neighborhoodContext = false

        if (args.include_metrics !== false) {
            const supabase = await getSupabaseServerClient()
            const forecastYear = args.forecast_year || 2029
            const yearsOut = forecastYear - 2026

            const fetchMetrics = async (targetId: string) => {
                const [fRes, cRes] = await Promise.all([
                    supabase
                        .from("h3_precomputed_hex_details")
                        .select("opportunity, predicted_value, property_count")
                        .eq("h3_id", targetId)
                        .eq("forecast_year", forecastYear)
                        .single(),
                    supabase
                        .from("h3_precomputed_hex_details")
                        .select("predicted_value")
                        .eq("h3_id", targetId)
                        .eq("forecast_year", 2026)
                        .single(),
                ])
                return { forecast: fRes.data, current: cRes.data?.predicted_value ?? null }
            }

            // Attempt primary hex
            const primary = await fetchMetrics(hexId)
            if (primary.forecast && primary.forecast.property_count > 0) {
                metrics = calculateMetrics(primary.forecast, primary.current, yearsOut)
            } else {
                // Neighborhood Context: If center point is non-residential (0 properties), search neighbors
                // Increase radius to 3 (~2km) to handle city centers
                const neighbors = h3.gridDisk(hexId, 3)
                const { data: nearbyHexes } = await supabase
                    .from("h3_precomputed_hex_details")
                    .select("h3_id, property_count")
                    .eq("forecast_year", forecastYear)
                    .in("h3_id", neighbors)
                    .gt("property_count", 0)
                    .order("property_count", { ascending: false })
                    .limit(1)

                if (nearbyHexes && nearbyHexes.length > 0) {
                    const bestHexId = nearbyHexes[0].h3_id
                    const fallbackResult = await fetchMetrics(bestHexId)
                    if (fallbackResult.forecast) {
                        metrics = calculateMetrics(fallbackResult.forecast, fallbackResult.current, yearsOut)
                        neighborhoodContext = true
                    }
                }
            }
        }

        const addr = place.address || {}
        const locationLabel = addr.suburb || addr.neighbourhood || addr.city_district || place.display_name.split(",")[0]

        // For neighborhood context, return the surrounding hexes so the UI can highlight them
        const neighborhoodIds = neighborhoodContext || (place.type === 'suburb' || place.type === 'neighbourhood')
            ? h3.gridDisk(hexId, 2) // Return 2-ring neighborhood (19 hexes)
            : [hexId]

        return JSON.stringify({
            action: actionName, // Include action name for frontend to distinguish
            chosen: {
                label: locationLabel + suffixForLabel(addr),
                lat,
                lng,
                kind: place.type || "place",
            },
            h3: {
                h3_id: hexId,
                h3_res: res,
                metrics,
                context: neighborhoodContext ? "neighborhood_average" : "exact_location",
                neighbors: neighborhoodIds
            },
        })
    } catch (e: any) {
        return JSON.stringify({ error: e.message })
    }
}
