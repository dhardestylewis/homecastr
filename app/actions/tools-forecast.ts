"use server"

import { getSupabaseServerClient } from "@/lib/supabase/server"

const FORECAST_SCHEMA = "forecast_queue"

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
                const params = new URLSearchParams({
                    q: args.query,
                    format: "json",
                    limit: String(args.max_candidates || 3),
                    addressdetails: "1",
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
                const forecastYear = args.forecast_year || 2029
                const originYear = 2025
                const horizonM = (forecastYear - originYear) * 12

                // Level fallback order: requested → tract → zcta
                const requestedLevel = args.level || "zcta"
                const levelOrder = [requestedLevel, ...["tabblock", "tract", "zcta"].filter(l => l !== requestedLevel)]

                let resolvedId = args.id
                let resolvedLevel = requestedLevel

                for (const level of levelOrder) {
                    const meta = LEVEL_META[level]
                    if (!meta) continue

                    let featureId = (level === requestedLevel) ? resolvedId : null

                    // If no id, try to find nearest geometry by lat/lng
                    if ((!featureId || featureId === "") && args.lat && args.lng) {
                        try {
                            const { data: nearest } = await (supabase as any)
                                .schema(FORECAST_SCHEMA)
                                .rpc("nearest_geography", {
                                    p_lat: args.lat,
                                    p_lng: args.lng,
                                    p_level: level,
                                })
                            if (nearest && nearest.length > 0) {
                                featureId = nearest[0].id
                            }
                        } catch {
                            // RPC might not exist — skip this level
                            continue
                        }
                    }

                    if (!featureId || featureId === "") continue

                    const { data, error } = await (supabase as any)
                        .schema(FORECAST_SCHEMA)
                        .from(meta.table)
                        .select("p10, p25, p50, p75, p90, horizon_m, origin_year")
                        .eq(meta.key, featureId)
                        .eq("origin_year", originYear)
                        .order("horizon_m", { ascending: true })

                    if (error || !data || data.length === 0) continue

                    resolvedId = featureId
                    resolvedLevel = level

                    // Find the closest horizon to the requested forecast year
                    const target = data.reduce((best: any, row: any) =>
                        Math.abs(row.horizon_m - horizonM) < Math.abs(best.horizon_m - horizonM) ? row : best
                    )

                    // Reverse-geocode to get a friendly name
                    let locationName = resolvedId
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
                            level: resolvedLevel,
                            id: resolvedId,
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
                }

                return JSON.stringify({ area: null, error: "No forecast data found for any geography level" })
            } catch (e: any) {
                return JSON.stringify({ area: null, error: e.message })
            }
        }

        // ── location_to_area ─────────────────────────────────────
        // Replaces location_to_hex: geocode → determine geography → fetch forecast
        case "location_to_area":
        case "add_location_to_selection": {
            try {
                const level = args.level || "zcta"
                const forecastYear = args.forecast_year || 2029

                // If area_id is given directly (e.g. from rank_forecast_areas), skip geocoding
                if (args.area_id && !args.query) {
                    const metricsResult = await executeTopLevelForecastTool("get_forecast_area", {
                        level,
                        id: args.area_id,
                        forecast_year: forecastYear,
                    })
                    const parsed = JSON.parse(metricsResult)
                    const result: any = {
                        action: toolName,
                        chosen: {
                            label: parsed.area?.label || `Area ${args.area_id}`,
                            lat: parsed.area?.lat,
                            lng: parsed.area?.lng,
                            kind: level,
                        },
                        location: { lat: parsed.area?.lat, lng: parsed.area?.lng, level },
                    }
                    if (parsed.area) {
                        result.metrics = parsed.area.metrics
                    }
                    return JSON.stringify(result)
                }

                const query = args.query || ""
                const params = new URLSearchParams({
                    q: query,
                    format: "json",
                    limit: "1",
                    addressdetails: "1",
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
                const locationLabel = addr.suburb || addr.neighbourhood || addr.city_district || place.display_name?.split(",")[0] || query

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

                // Always attempt to fetch metrics (using id or lat/lng fallback)
                const metricsResult = await executeTopLevelForecastTool("get_forecast_area", {
                    level,
                    id: args.area_id || "",
                    forecast_year: forecastYear,
                    lat,
                    lng,
                })
                const parsed = JSON.parse(metricsResult)
                if (parsed.area) {
                    result.metrics = parsed.area.metrics
                    if (parsed.area.id) result.location.area_id = parsed.area.id
                } else if (!parsed.area && parsed.error) {
                    // Explicitly tell the AI that the place was found, but data is missing
                    result.coverage_error = "Place resolved correctly, but NO FORECAST DATA was found at this location. Inform the user per Rule 11."
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
                // Parcel-level ranking is too slow (millions of rows) — fall back to tract
                let level = args.level || "zcta"
                let fallbackNote = ""
                if (level === "parcel" || level === "tabblock") {
                    fallbackNote = ` (auto-elevated from ${level} to tract for performance)`
                    level = "tract"
                }
                const meta = LEVEL_META[level]
                if (!meta) return JSON.stringify({ error: `Unknown level: ${level}` })

                const forecastYear = args.forecast_year || 2029
                const originYear = 2025
                const horizonM = (forecastYear - originYear) * 12
                const limit = Math.min(args.limit || 5, 10)
                const objective = args.objective || "value"

                if (objective === "growth") {
                    // Growth ranking: fetch forecast + current p50, compute growth_pct
                    const currentHorizonM = 12 // 2026 = origin + 1 year

                    // 1. Get a pool of areas at the forecast horizon
                    const fetchLimit = args.query ? 3000 : 100
                    const { data: futureData, error: futureErr } = await (supabase as any)
                        .schema(FORECAST_SCHEMA)
                        .from(meta.table)
                        .select(`${meta.key}, p50, p10, p90, horizon_m`)
                        .eq("origin_year", originYear)
                        .eq("horizon_m", horizonM)
                        .not("p50", "is", null)
                        .order("p50", { ascending: false })
                        .limit(fetchLimit)

                    if (futureErr || !futureData || futureData.length === 0) {
                        return JSON.stringify({ areas: [], error: futureErr?.message || "No forecast data" })
                    }

                    // 2. Get the current p50 for the same areas
                    const areaIds = futureData.map((r: any) => r[meta.key])
                    const { data: currentData } = await (supabase as any)
                        .schema(FORECAST_SCHEMA)
                        .from(meta.table)
                        .select(`${meta.key}, p50`)
                        .eq("origin_year", originYear)
                        .eq("horizon_m", currentHorizonM)
                        .in(meta.key, areaIds)

                    // 3. Build lookup of current values
                    const currentMap = new Map<string, number>()
                    if (currentData) {
                        for (const row of currentData) {
                            currentMap.set(row[meta.key], row.p50)
                        }
                    }

                    // 4. Compute growth_pct and sort
                    const withGrowth = futureData
                        .filter((row: any) => currentMap.has(row[meta.key]) && currentMap.get(row[meta.key])! > 0)
                        .map((row: any) => {
                            const currentP50 = currentMap.get(row[meta.key])!
                            const growthPct = ((row.p50 - currentP50) / currentP50) * 100
                            return { ...row, currentP50, growthPct }
                        })
                        .filter((row: any) => row.growthPct <= 100 && row.growthPct >= -50) // Strip out extreme anomalies (e.g. empty lots -> skyscrapers)
                        .sort((a: any, b: any) => b.growthPct - a.growthPct)

                    let filteredGrowth = withGrowth
                    if (args.query) {
                        const queryStr = args.query.toLowerCase()
                        const { parseTractGeoid, ZIP_NAMES } = await import("@/lib/publishing/geo-crosswalk")
                        filteredGrowth = withGrowth.filter((row: any) => {
                            const id = row[meta.key]
                            if (level === "zcta") {
                                const name = (ZIP_NAMES[id] || "").toLowerCase()
                                return name.includes(queryStr) || queryStr.includes(name) || id.includes(queryStr)
                            } else {
                                try {
                                    const info = parseTractGeoid(id.substring(0, 11))
                                    const matchCity = info.city.toLowerCase().includes(queryStr) || queryStr.includes(info.city.toLowerCase())
                                    const matchState = info.stateName.toLowerCase() === queryStr || info.stateAbbr.toLowerCase() === queryStr
                                    const isNYCQuery = queryStr === "nyc" || queryStr === "new york" || queryStr === "new york city"
                                    const isNYCBorough = ["manhattan", "brooklyn", "queens", "bronx", "staten island"].includes(info.city.toLowerCase())
                                    return matchCity || matchState || (isNYCQuery && isNYCBorough)
                                } catch { return false }
                            }
                        })
                    }

                    const areas = filteredGrowth.slice(0, limit).map((row: any, i: number) => ({
                        level,
                        id: row[meta.key],
                        metrics: {
                            p50: row.p50,
                            p10: row.p10,
                            p90: row.p90,
                            current_value: row.currentP50,
                            growth_pct: Math.round(row.growthPct * 10) / 10,
                            horizon_m: row.horizon_m,
                        },
                        rank: i + 1,
                        reason: `Rank #${i + 1} by growth (${Math.round(row.growthPct * 10) / 10}%)${args.query ? ' in ' + args.query : ''}${fallbackNote}`,
                    }))

                    return JSON.stringify({ areas })
                }

                // Value ranking: straightforward sort by p50
                const fetchLimit = args.query ? 3000 : limit
                const { data, error } = await (supabase as any)
                    .schema(FORECAST_SCHEMA)
                    .from(meta.table)
                    .select(`${meta.key}, p50, p10, p90, horizon_m`)
                    .eq("origin_year", originYear)
                    .eq("horizon_m", horizonM)
                    .not("p50", "is", null)
                    .order("p50", { ascending: false })
                    .limit(fetchLimit)

                if (error) return JSON.stringify({ areas: [], error: error.message })

                let filteredData = data || []
                if (args.query) {
                    const queryStr = args.query.toLowerCase()
                    const { parseTractGeoid, ZIP_NAMES } = await import("@/lib/publishing/geo-crosswalk")
                    filteredData = filteredData.filter((row: any) => {
                        const id = row[meta.key]
                        if (level === "zcta") {
                            const name = (ZIP_NAMES[id] || "").toLowerCase()
                            return name.includes(queryStr) || queryStr.includes(name) || id.includes(queryStr)
                        } else {
                            try {
                                const info = parseTractGeoid(id.substring(0, 11))
                                const matchCity = info.city.toLowerCase().includes(queryStr) || queryStr.includes(info.city.toLowerCase())
                                const matchState = info.stateName.toLowerCase() === queryStr || info.stateAbbr.toLowerCase() === queryStr
                                const isNYCQuery = queryStr === "nyc" || queryStr === "new york" || queryStr === "new york city"
                                const isNYCBorough = ["manhattan", "brooklyn", "queens", "bronx", "staten island"].includes(info.city.toLowerCase())
                                return matchCity || matchState || (isNYCQuery && isNYCBorough)
                            } catch { return false }
                        }
                    })
                }

                const areas = filteredData.slice(0, limit).map((row: any, i: number) => ({
                    level,
                    id: row[meta.key],
                    metrics: {
                        p50: row.p50,
                        p10: row.p10,
                        p90: row.p90,
                        horizon_m: row.horizon_m,
                    },
                    rank: i + 1,
                    reason: `Rank #${i + 1} by ${objective}${args.query ? ' in ' + args.query : ''}${fallbackNote}`,
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
