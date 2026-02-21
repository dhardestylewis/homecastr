"use server"

import { getSupabaseServerClient } from "@/lib/supabase/server"

const FORECAST_SCHEMA = "forecast_20260220_7f31c6e4"

/** Geography-level table metadata */
const LEVEL_META: Record<string, { table: string; key: string }> = {
    zcta: { table: "metrics_zcta_forecast", key: "zcta5" },
    tract: { table: "metrics_tract_forecast", key: "tract_geoid20" },
    tabblock: { table: "metrics_tabblock_forecast", key: "tabblock_geoid20" },
    parcel: { table: "metrics_parcel_forecast", key: "acct" },
}

/**
 * Execute a tool call against the lot-level forecast tables
 * (zcta / tract / tabblock / parcel) instead of H3 precomputed hex details.
 * Reusable by both Chat API and Tavus Frontend.
 */
export async function executeTopLevelForecastTool(
    toolName: string,
    args: Record<string, any>
): Promise<string> {
    console.log(`[Forecast Tool] Executing ${toolName}`, args)

    switch (toolName) {

        // ── resolve_place: same as the H3 version ─────────────────
        case "resolve_place": {
            try {
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
                return JSON.stringify({ candidates: [], error: e.message })
            }
        }

        // ── get_forecast_area ────────────────────────────────────
        // Replaces get_h3_hex: fetch forecast metrics for a geography unit
        case "get_forecast_area": {
            try {
                const supabase = await getSupabaseServerClient()
                const level = args.level || "zcta"
                const meta = LEVEL_META[level]
                if (!meta) return JSON.stringify({ error: `Unknown level: ${level}` })

                const forecastYear = args.forecast_year || 2029
                const originYear = 2025
                const horizonM = (forecastYear - originYear) * 12

                const { data, error } = await (supabase as any)
                    .schema(FORECAST_SCHEMA)
                    .from(meta.table)
                    .select("p10, p25, p50, p75, p90, horizon_m, origin_year")
                    .eq(meta.key, args.id)
                    .eq("origin_year", originYear)
                    .order("horizon_m", { ascending: true })

                if (error) return JSON.stringify({ error: error.message })
                if (!data || data.length === 0) return JSON.stringify({ area: null, error: "No forecast data found" })

                // Find the closest horizon to the requested forecast year
                const target = data.reduce((best: any, row: any) =>
                    Math.abs(row.horizon_m - horizonM) < Math.abs(best.horizon_m - horizonM) ? row : best
                )

                // Reverse-geocode to get a friendly name
                let locationName = args.id
                if (args.lat && args.lng) {
                    try {
                        const revRes = await fetch(
                            `https://nominatim.openstreetmap.org/reverse?lat=${args.lat}&lon=${args.lng}&zoom=16&format=json`,
                            { headers: { "User-Agent": "HomecastrUI/1.0" } }
                        )
                        if (revRes.ok) {
                            const revData = await revRes.json()
                            const addr = revData.address || {}
                            locationName = addr.suburb || addr.neighbourhood || addr.road || revData.display_name?.split(",")[0] || locationName
                        }
                    } catch { }
                }

                return JSON.stringify({
                    area: {
                        level,
                        id: args.id,
                        origin_year: originYear,
                        forecast_year: forecastYear,
                        location: { name: locationName, lat: args.lat, lng: args.lng },
                        metrics: {
                            predicted_value: target.p50,
                            p10: target.p10,
                            p25: target.p25,
                            p50: target.p50,
                            p75: target.p75,
                            p90: target.p90,
                            horizon_m: target.horizon_m,
                        },
                        all_horizons: data.map((r: any) => ({
                            horizon_m: r.horizon_m,
                            p50: r.p50,
                        })),
                    },
                })
            } catch (e: any) {
                return JSON.stringify({ area: null, error: e.message })
            }
        }

        // ── location_to_area ─────────────────────────────────────
        // Replaces location_to_hex: geocode → determine geography → fetch forecast
        case "location_to_area":
        case "add_location_to_selection": {
            try {
                const querySuffix = (args.query.toLowerCase().includes("houston") || args.query.toLowerCase().includes("tx")) ? "" : ", TX"
                const params = new URLSearchParams({
                    q: args.query + querySuffix,
                    format: "json",
                    limit: "1",
                    addressdetails: "1",
                    viewbox: "-95.96,30.17,-94.90,29.50",
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
                const addr = place.address || {}
                const locationLabel = addr.suburb || addr.neighbourhood || addr.city_district || place.display_name?.split(",")[0] || args.query

                // Determine the geography level based on zoom or type
                const level = args.level || "zcta"
                const forecastYear = args.forecast_year || 2029

                // Build the tool result
                const result: any = {
                    action: toolName,
                    chosen: {
                        label: locationLabel,
                        lat,
                        lng,
                        kind: place.type || "place",
                    },
                    location: { lat, lng, level },
                }

                // If we have a specific area_id, fetch metrics
                if (args.area_id) {
                    const metricsResult = await executeTopLevelForecastTool("get_forecast_area", {
                        level,
                        id: args.area_id,
                        forecast_year: forecastYear,
                        lat,
                        lng,
                    })
                    const parsed = JSON.parse(metricsResult)
                    if (parsed.area) {
                        result.metrics = parsed.area.metrics
                    }
                }

                return JSON.stringify(result)
            } catch (e: any) {
                return JSON.stringify({ error: e.message })
            }
        }

        // ── rank_forecast_areas ──────────────────────────────────
        // Replaces rank_h3_hexes: rank geography units by p50 or growth
        case "rank_forecast_areas": {
            try {
                const supabase = await getSupabaseServerClient()
                const level = args.level || "zcta"
                const meta = LEVEL_META[level]
                if (!meta) return JSON.stringify({ error: `Unknown level: ${level}` })

                const forecastYear = args.forecast_year || 2029
                const originYear = 2025
                const horizonM = (forecastYear - originYear) * 12
                const limit = Math.min(args.limit || 5, 10)
                const objective = args.objective || "value"

                const sortColumn = objective === "value" ? "p50" : "p50" // both sort by p50 for now

                const { data, error } = await (supabase as any)
                    .schema(FORECAST_SCHEMA)
                    .from(meta.table)
                    .select(`${meta.key}, p50, p10, p90, horizon_m`)
                    .eq("origin_year", originYear)
                    .eq("horizon_m", horizonM)
                    .not("p50", "is", null)
                    .order(sortColumn, { ascending: false })
                    .limit(limit)

                if (error) return JSON.stringify({ areas: [], error: error.message })

                const areas = (data || []).map((row: any, i: number) => ({
                    level,
                    id: row[meta.key],
                    metrics: {
                        p50: row.p50,
                        p10: row.p10,
                        p90: row.p90,
                        horizon_m: row.horizon_m,
                    },
                    rank: i + 1,
                    reason: `Rank #${i + 1} by ${objective}`,
                }))

                return JSON.stringify({ areas })
            } catch (e: any) {
                return JSON.stringify({ areas: [], error: e.message })
            }
        }

        // ── fly_to_location ──────────────────────────────────────
        // Same as H3 version but with area_id + level instead of select_hex_id
        case "fly_to_location": {
            return JSON.stringify({
                success: true,
                action: "fly_to_location",
                lat: args.lat,
                lng: args.lng,
                zoom: args.zoom,
                area_id: args.area_id || null,
                level: args.level || null,
            })
        }

        // ── clear_selection ──────────────────────────────────────
        case "clear_selection": {
            return JSON.stringify({
                success: true,
                action: "clear_selection",
            })
        }

        // ── explain_metric ───────────────────────────────────────
        case "explain_metric": {
            const explanations: Record<string, string> = {
                p50: "The median predicted home value — 50% chance the actual value falls above or below this.",
                p10: "The 10th percentile — a pessimistic scenario. 90% chance the value will be above this.",
                p90: "The 90th percentile — an optimistic scenario. Only 10% chance the value exceeds this.",
                growth: "Annual growth rate: how much property values in this area are projected to change each year.",
                predicted_value: "The predicted median home value in this area by the forecast year.",
            }
            return JSON.stringify({
                short: explanations[args.metric] || `${args.metric} is a key real estate metric.`,
                long: explanations[args.metric] || `${args.metric} is used to evaluate real estate investment potential.`,
            })
        }

        case "record_feedback":
            return JSON.stringify({ recorded: true, id: "fb_" + Date.now() })

        default:
            return JSON.stringify({ error: `Tool not found: ${toolName}` })
    }
}
