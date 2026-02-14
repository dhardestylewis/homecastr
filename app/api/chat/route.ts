import { NextRequest, NextResponse } from "next/server"
import OpenAI from "openai"

// Tool definitions from tavus-tool-definitions.json (inlined for the API route)
const TOOL_DEFINITIONS: OpenAI.ChatCompletionTool[] = [
    {
        type: "function",
        function: {
            name: "resolve_place",
            description:
                "Resolve a free-text location (address/neighborhood/zip/landmark) into candidate lat/lng results. Use first when the user provides a location string.",
            parameters: {
                type: "object",
                properties: {
                    query: { type: "string", description: "User-entered location string." },
                    city_hint: { type: "string", description: "Disambiguation context; pass empty string if unknown." },
                    max_candidates: { type: "integer", description: "Number of candidates to return (1-10)." },
                },
                required: ["query", "city_hint", "max_candidates"],
            },
        },
    },
    {
        type: "function",
        function: {
            name: "point_to_tile",
            description: "Convert lat/lng into Web Mercator slippy tile coordinates (z/x/y) for the requested zoom levels.",
            parameters: {
                type: "object",
                properties: {
                    lat: { type: "number", description: "Latitude." },
                    lng: { type: "number", description: "Longitude." },
                    zooms: { type: "array", description: "Zoom levels to compute tiles for (0-22).", items: { type: "integer" } },
                },
                required: ["lat", "lng", "zooms"],
            },
        },
    },
    {
        type: "function",
        function: {
            name: "point_to_hex",
            description:
                "Map a lat/lng point to the containing H3 hex (h3_id) at a given h3_res using the authoritative h3_aoi_grid geometry layer.",
            parameters: {
                type: "object",
                properties: {
                    lat: { type: "number", description: "Latitude." },
                    lng: { type: "number", description: "Longitude." },
                    h3_res: { type: "integer", description: "H3 resolution (0-15)." },
                    include_geometry: { type: "boolean", description: "Whether to include hex polygon geometry." },
                },
                required: ["lat", "lng", "h3_res", "include_geometry"],
            },
        },
    },
    {
        type: "function",
        function: {
            name: "location_to_hex",
            description:
                "One-shot convenience: resolve text location -> choose best candidate -> map to h3_id -> optionally return metrics and tile xyz for UI centering.",
            parameters: {
                type: "object",
                properties: {
                    query: { type: "string", description: "User-entered location string." },
                    city_hint: { type: "string", description: "Disambiguation context; pass empty string if unknown." },
                    max_candidates: { type: "integer", description: "Number of candidates to consider (1-10)." },
                    h3_res: { type: "integer", description: "H3 resolution (0-15)." },
                    forecast_year: { type: "integer", description: "Forecast year to fetch metrics for." },
                    include_metrics: { type: "boolean", description: "Whether to include metrics for the chosen hex." },
                    zooms: { type: "array", description: "Zoom levels for tile xyz (0-22).", items: { type: "integer" } },
                    include_geometry: { type: "boolean", description: "Whether to include hex polygon geometry." },
                },
                required: ["query", "city_hint", "max_candidates", "h3_res", "forecast_year", "include_metrics", "zooms", "include_geometry"],
            },
        },
    },
    {
        type: "function",
        function: {
            name: "get_h3_hex",
            description: "Fetch the metrics for a single H3 hex at (forecast_year, h3_res), optionally including geometry.",
            parameters: {
                type: "object",
                properties: {
                    h3_id: { type: "string", description: "H3 cell id." },
                    h3_res: { type: "integer", description: "H3 resolution (0-15)." },
                    forecast_year: { type: "integer", description: "Forecast year." },
                    include_geometry: { type: "boolean", description: "Whether to include geometry." },
                },
                required: ["h3_id", "h3_res", "forecast_year", "include_geometry"],
            },
        },
    },
    {
        type: "function",
        function: {
            name: "search_h3_hexes",
            description: "Search and rank H3 hexes in an area (bbox or radius) with metric filters and sorting.",
            parameters: {
                type: "object",
                properties: {
                    forecast_year: { type: "integer", description: "Forecast year." },
                    h3_res: { type: "integer", description: "H3 resolution (0-15)." },
                    area: { type: "object", description: "AreaSelector object (bbox or radius)." },
                    filters: { type: "object", description: "Metric thresholds (min_opportunity, min_reliability, etc.)." },
                    sort: { type: "string", description: "Sort key for ranking results." },
                    limit: { type: "integer", description: "Max results (1-50)." },
                    cursor: { type: "string", description: "Pagination cursor; empty string for first page." },
                    include_geometry: { type: "boolean", description: "Whether to include geometry." },
                },
                required: ["forecast_year", "h3_res", "area", "filters", "sort", "limit", "cursor", "include_geometry"],
            },
        },
    },
    {
        type: "function",
        function: {
            name: "rank_h3_hexes",
            description:
                "Rank H3 hexes by objective (higher_growth, more_predictable, best_risk_adjusted) within an area and constraints.",
            parameters: {
                type: "object",
                properties: {
                    forecast_year: { type: "integer", description: "Forecast year." },
                    h3_res: { type: "integer", description: "H3 resolution (0-15)." },
                    area: { type: "object", description: "AreaSelector object (bbox or radius)." },
                    objective: { type: "string", description: "Ranking objective." },
                    constraints: { type: "object", description: "Ranking constraints (min_property_count, min_sample_accuracy)." },
                    limit: { type: "integer", description: "Max results (1-50)." },
                    cursor: { type: "string", description: "Pagination cursor; empty string for first page." },
                    include_geometry: { type: "boolean", description: "Whether to include geometry." },
                },
                required: ["forecast_year", "h3_res", "area", "objective", "constraints", "limit", "cursor", "include_geometry"],
            },
        },
    },
    {
        type: "function",
        function: {
            name: "compare_h3_hexes",
            description: "Compare two H3 hexes at the same (forecast_year, h3_res) and return side-by-side highlights.",
            parameters: {
                type: "object",
                properties: {
                    forecast_year: { type: "integer", description: "Forecast year." },
                    h3_res: { type: "integer", description: "H3 resolution (0-15)." },
                    left_h3_id: { type: "string", description: "Left hex id." },
                    right_h3_id: { type: "string", description: "Right hex id." },
                    include_geometry: { type: "boolean", description: "Whether to include geometry." },
                },
                required: ["forecast_year", "h3_res", "left_h3_id", "right_h3_id", "include_geometry"],
            },
        },
    },
    {
        type: "function",
        function: {
            name: "explain_metric",
            description:
                "Explain a metric (opportunity, reliability, predicted_value, property_count, sample_accuracy) in homeowner/broker language. No database access.",
            parameters: {
                type: "object",
                properties: {
                    metric: { type: "string", description: "Metric name." },
                    audience: { type: "string", description: "Audience: homeowner, broker, analyst." },
                },
                required: ["metric", "audience"],
            },
        },
    },
    {
        type: "function",
        function: {
            name: "record_feedback",
            description: "Record user feedback about whether an answer was helpful and what to improve.",
            parameters: {
                type: "object",
                properties: {
                    session_id: { type: "string", description: "Conversation/session id from your app." },
                    question: { type: "string", description: "User question text." },
                    tool_used: { type: "string", description: "Which tool produced the answer." },
                    helpful: { type: "boolean", description: "Whether the answer was helpful." },
                    notes: { type: "string", description: "Optional notes (pass empty string if none)." },
                },
                required: ["session_id", "question", "tool_used", "helpful", "notes"],
            },
        },
    },
    // --- UI control tools (handled client-side, not server) ---
    {
        type: "function",
        function: {
            name: "fly_to_location",
            description:
                "Smoothly pan and zoom the map to a specific location. Use this after resolving a place or when discussing a specific area. The map will animate to the target.",
            parameters: {
                type: "object",
                properties: {
                    lat: { type: "number", description: "Target latitude." },
                    lng: { type: "number", description: "Target longitude." },
                    zoom: { type: "integer", description: "Target zoom level (9-18). Use 14 for neighborhoods, 16 for specific addresses." },
                    select_hex_id: { type: "string", description: "Optional H3 hex id to select/highlight on the map after flying." },
                },
                required: ["lat", "lng", "zoom"],
            },
        },
    },
]

const SYSTEM_PROMPT = `You are Homecastr, a real estate data assistant for Houston, TX (Harris County). You look up property value forecasts and neighborhood metrics from our database.

RULES:
1. For EVERY query: call fly_to_location in the SAME tool call batch as data lookups. Never wait for data results before flying.
2. For COMPARISONS (e.g. "compare Heights vs Montrose"): call location_to_hex for EACH neighborhood AND ONE fly_to_location centered between both (zoom 13). The location_to_hex results already include all metrics — do NOT also call compare_h3_hexes.
3. For SUGGESTIONS/RECOMMENDATIONS (e.g. "any suggestions in Montrose?"): use rank_h3_hexes sorted by "opportunity" descending with a bounding box around the neighborhood to find the top 3-5 sub-areas. Name each one by its nearest cross-street or landmark.
4. NEVER give generic real estate advice, neighborhood descriptions, or lifestyle info. You are a DATA tool. If tools fail, say "I couldn't pull that data — want me to try again?" and stop.
5. Only report numbers from tool results. No editorializing or parenthetical explanations.
6. Do NOT mention "confidence" or "reliability".
6. Default forecast_year: 2029, h3_res: 9.
7. Use real Houston place names. Never generic labels.
8. FORMAT — keep it tight:
   - "annual_change_pct" is already a percentage. Display as: "Expected Change: X% each year over the next N years" where N = forecast_year - 2026.
   - Show BOTH current value AND predicted value. Example output:
     Heights (2029 forecast):
     Current Value: $580,000
     Predicted Value: $683,251
     Expected Change: -26% each year over the next 3 years
   - Do NOT editorialize about property count in summaries. It's metadata, not insight.
   - Summary: 1-2 sentences comparing the key difference (value or expected change). No fluff.
   - Total response: 6-10 lines, never more than 15

You query real data from the Homecastr database. Always use tools — never guess or make up numbers.`

export async function POST(req: NextRequest) {
    try {
        const apiKey = process.env.OPENAI_API_KEY
        if (!apiKey) {
            return NextResponse.json(
                { error: "OPENAI_API_KEY not configured. Add it to .env.local" },
                { status: 500 }
            )
        }

        const openai = new OpenAI({ apiKey })
        const { messages } = await req.json()

        if (!messages || !Array.isArray(messages)) {
            return NextResponse.json({ error: "messages array required" }, { status: 400 })
        }

        // Prepend system prompt
        const conversationMessages: OpenAI.ChatCompletionMessageParam[] = [
            { role: "system" as const, content: SYSTEM_PROMPT },
            ...messages,
        ]

        const allMapActions: any[] = []
        const allToolsUsed: string[] = []
        const MAX_ROUNDS = 5

        for (let round = 0; round < MAX_ROUNDS; round++) {
            console.log(`[Chat API] Round ${round + 1}, messages: ${conversationMessages.length}`)

            const response = await openai.chat.completions.create({
                model: "gpt-4o-mini",
                messages: conversationMessages,
                tools: TOOL_DEFINITIONS,
                tool_choice: round < MAX_ROUNDS - 1 ? "auto" : "none", // Force text on last round
                temperature: 0.7,
                max_tokens: 1024,
            })

            const assistantMessage = response.choices[0].message
            console.log(`[Chat API] Round ${round + 1} response:`, {
                hasContent: !!assistantMessage.content,
                contentPreview: assistantMessage.content?.substring(0, 100),
                toolCalls: assistantMessage.tool_calls?.map(tc => tc.function.name) || [],
            })

            // No tool calls — we have our final text response
            if (!assistantMessage.tool_calls || assistantMessage.tool_calls.length === 0) {
                console.log(`[Chat API] Final response (round ${round + 1}):`, assistantMessage.content?.substring(0, 100))
                return NextResponse.json({
                    message: {
                        role: "assistant",
                        content: assistantMessage.content || "(No response generated)",
                    },
                    mapActions: allMapActions,
                    toolCallsUsed: allToolsUsed,
                })
            }

            // Process tool calls
            // Add assistant message with tool_calls to conversation
            conversationMessages.push(assistantMessage)

            for (const tc of assistantMessage.tool_calls) {
                if (tc.function.name === "fly_to_location") {
                    // UI tool — extract for client-side, return dummy result to LLM
                    try {
                        const args = JSON.parse(tc.function.arguments)
                        allMapActions.push(args)
                        console.log(`[Chat API] fly_to_location:`, args)
                    } catch (e) {
                        console.error(`[Chat API] Failed to parse fly_to_location args:`, tc.function.arguments)
                    }
                    conversationMessages.push({
                        role: "tool",
                        tool_call_id: tc.id,
                        content: JSON.stringify({ status: "ok", message: "Map is now showing the requested location." }),
                    })
                } else {
                    // API tool — query real data
                    allToolsUsed.push(tc.function.name)
                    try {
                        const args = JSON.parse(tc.function.arguments)
                        const result = await executeToolCall(tc.function.name, args)
                        console.log(`[Chat API] Tool ${tc.function.name} result:`, result.substring(0, 200))
                        conversationMessages.push({
                            role: "tool",
                            tool_call_id: tc.id,
                            content: result,
                        })
                    } catch (e) {
                        console.error(`[Chat API] Failed to process tool ${tc.function.name}:`, e)
                        conversationMessages.push({
                            role: "tool",
                            tool_call_id: tc.id,
                            content: JSON.stringify({ error: "Failed to process tool call" }),
                        })
                    }
                }
            }
        }

        // If we exhausted rounds, return whatever we have
        console.log(`[Chat API] Exhausted ${MAX_ROUNDS} rounds, returning last state`)
        return NextResponse.json({
            message: {
                role: "assistant",
                content: "I found some information for you. Let me know if you'd like more details!",
            },
            mapActions: allMapActions,
            toolCallsUsed: allToolsUsed,
        })
    } catch (error: any) {
        console.error("[Chat API] Error:", error)
        return NextResponse.json(
            { error: error.message || "Chat request failed" },
            { status: 500 }
        )
    }
}

/**
 * Execute a tool call against real data sources (Supabase + Nominatim + h3-js).
 */
async function executeToolCall(toolName: string, args: Record<string, any>): Promise<string> {
    const { getSupabaseServerClient } = await import("@/lib/supabase/server")
    const h3 = await import("h3-js")

    switch (toolName) {
        case "resolve_place": {
            // Use Nominatim to geocode the query (same as existing geocode action)
            try {
                const params = new URLSearchParams({
                    q: args.query + (args.city_hint ? `, ${args.city_hint}` : ", Houston, TX"),
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
                console.log(`[Chat Tool] resolve_place "${args.query}": ${candidates.length} results`)
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

        case "point_to_tile": {
            // Simple tile math
            const tiles = (args.zooms || []).map((z: number) => {
                const x = Math.floor(((args.lng + 180) / 360) * Math.pow(2, z))
                const latRad = (args.lat * Math.PI) / 180
                const y = Math.floor((1 - Math.log(Math.tan(latRad) + 1 / Math.cos(latRad)) / Math.PI) / 2 * Math.pow(2, z))
                return { z, x, y }
            })
            return JSON.stringify({ tiles })
        }

        case "get_h3_hex": {
            try {
                const supabase = await getSupabaseServerClient()
                const forecastYear = args.forecast_year || 2029
                const yearsOut = forecastYear - 2026
                const [forecastResult, currentResult] = await Promise.all([
                    supabase
                        .from("h3_precomputed_hex_details")
                        .select("h3_id, h3_res, forecast_year, opportunity, predicted_value, property_count, lat, lng")
                        .eq("h3_id", args.h3_id)
                        .eq("forecast_year", forecastYear)
                        .single(),
                    supabase
                        .from("h3_precomputed_hex_details")
                        .select("predicted_value")
                        .eq("h3_id", args.h3_id)
                        .eq("forecast_year", 2026)
                        .single(),
                ])

                const data = forecastResult.data
                const error = forecastResult.error
                const currentValue = currentResult.data?.predicted_value ?? null

                if (error || !data) {
                    console.error("[Chat Tool] get_h3_hex error:", error?.message)
                    return JSON.stringify({ hex: null, error: error?.message || "Not found" })
                }

                // Reverse geocode the hex center for a location name
                let locationName = `H3 Cell ${args.h3_id}`
                try {
                    const revRes = await fetch(`https://nominatim.openstreetmap.org/reverse?lat=${data.lat}&lon=${data.lng}&zoom=16&format=json`, {
                        headers: { "User-Agent": "HomecastrUI/1.0" },
                    })
                    if (revRes.ok) {
                        const revData = await revRes.json()
                        const addr = revData.address || {}
                        locationName = addr.suburb || addr.neighbourhood || addr.road || revData.display_name?.split(",")[0] || locationName
                    }
                } catch { /* ignore reverse geocode failure */ }

                console.log(`[Chat Tool] get_h3_hex ${args.h3_id}: value=$${data.predicted_value}, opp=${data.opportunity}`)
                return JSON.stringify({
                    hex: {
                        h3_id: data.h3_id,
                        h3_res: data.h3_res,
                        forecast_year: data.forecast_year,
                        years_out: yearsOut,
                        location: { name: locationName, lat: data.lat, lng: data.lng },
                        metrics: {
                            annual_change_pct: data.opportunity != null ? Math.round(data.opportunity * 10000) / 100 : null,
                            current_value: currentValue,
                            predicted_value: data.predicted_value,
                            property_count: data.property_count,
                        },
                    },
                })
            } catch (e: any) {
                console.error("[Chat Tool] get_h3_hex exception:", e.message)
                return JSON.stringify({ hex: null, error: e.message })
            }
        }

        case "location_to_hex": {
            try {
                // Step 1: Geocode the query
                const params = new URLSearchParams({
                    q: args.query + ", Houston, TX",
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
                const res = args.h3_res ?? 9

                // Step 2: Convert to H3 hex
                const hexId = h3.latLngToCell(lat, lng, res)

                // Step 3: Query metrics from Supabase
                let metrics = null
                if (args.include_metrics !== false) {
                    const supabase = await getSupabaseServerClient()
                    const forecastYear = args.forecast_year || 2029
                    const yearsOut = forecastYear - 2026
                    const [forecastResult, currentResult] = await Promise.all([
                        supabase
                            .from("h3_precomputed_hex_details")
                            .select("opportunity, predicted_value, property_count")
                            .eq("h3_id", hexId)
                            .eq("forecast_year", forecastYear)
                            .single(),
                        supabase
                            .from("h3_precomputed_hex_details")
                            .select("predicted_value")
                            .eq("h3_id", hexId)
                            .eq("forecast_year", 2026)
                            .single(),
                    ])
                    const data = forecastResult.data
                    if (data) {
                        metrics = {
                            annual_change_pct: data.opportunity != null ? Math.round(data.opportunity * 10000) / 100 : null,
                            current_value: currentResult.data?.predicted_value ?? null,
                            predicted_value: data.predicted_value,
                            property_count: data.property_count,
                            years_out: yearsOut,
                        }
                    }
                }

                const addr = place.address || {}
                const locationLabel = addr.suburb || addr.neighbourhood || addr.city_district || place.display_name.split(",")[0]

                console.log(`[Chat Tool] location_to_hex "${args.query}" → ${hexId} at (${lat}, ${lng}), metrics:`, metrics)
                return JSON.stringify({
                    chosen: {
                        label: locationLabel + ", Houston, TX",
                        lat,
                        lng,
                        kind: place.type || "place",
                    },
                    h3: {
                        h3_id: hexId,
                        h3_res: res,
                        metrics,
                    },
                })
            } catch (e: any) {
                console.error("[Chat Tool] location_to_hex error:", e.message)
                return JSON.stringify({ error: e.message })
            }
        }

        case "search_h3_hexes":
        case "rank_h3_hexes": {
            try {
                const supabase = await getSupabaseServerClient()
                const forecastYear = args.forecast_year || 2029
                const limit = Math.min(args.limit || 5, 10)
                const sortBy = args.objective || args.sort || "opportunity"

                // Map sort field to column name
                const sortColumn = sortBy === "reliability" ? "reliability"
                    : sortBy === "predicted_value" || sortBy === "value" ? "predicted_value"
                        : "opportunity"

                // Build query with optional bbox filter
                let query = supabase
                    .from("h3_precomputed_hex_details")
                    .select("h3_id, h3_res, forecast_year, opportunity, reliability, predicted_value, property_count, lat, lng")
                    .eq("forecast_year", forecastYear)
                    .eq("h3_res", args.h3_res ?? 9)
                    .not(sortColumn, "is", null)
                    .order(sortColumn, { ascending: false })
                    .limit(limit)

                // If bbox provided, filter by bounds
                if (args.bbox) {
                    const [minLat, maxLat, minLng, maxLng] = args.bbox
                    query = query
                        .gte("lat", minLat).lte("lat", maxLat)
                        .gte("lng", minLng).lte("lng", maxLng)
                }

                const { data, error } = await query

                if (error) {
                    console.error("[Chat Tool] search/rank error:", error.message)
                    return JSON.stringify({ hexes: [], error: error.message })
                }

                // Reverse geocode each hex to get a location name (batch, with rate limiting)
                const hexes = await Promise.all((data || []).map(async (row: any, i: number) => {
                    let locationName = `H3 Cell`
                    try {
                        // Stagger requests to respect Nominatim rate limits
                        await new Promise(r => setTimeout(r, i * 200))
                        const revRes = await fetch(
                            `https://nominatim.openstreetmap.org/reverse?lat=${row.lat}&lon=${row.lng}&zoom=16&format=json`,
                            { headers: { "User-Agent": "HomecastrUI/1.0" } }
                        )
                        if (revRes.ok) {
                            const revData = await revRes.json()
                            const addr = revData.address || {}
                            locationName = addr.suburb || addr.neighbourhood || addr.road || revData.display_name?.split(",")[0] || locationName
                        }
                    } catch { /* ignore */ }

                    return {
                        h3_id: row.h3_id,
                        h3_res: row.h3_res,
                        forecast_year: row.forecast_year,
                        location: { name: locationName, lat: row.lat, lng: row.lng },
                        metrics: {
                            annual_change_pct: row.opportunity != null ? Math.round(row.opportunity * 10000) / 100 : null,
                            predicted_value: row.predicted_value,
                            property_count: row.property_count,
                        },
                        reasons: [`Rank #${i + 1} by ${sortBy}`],
                    }
                }))

                console.log(`[Chat Tool] ${toolName}: ${hexes.length} results sorted by ${sortColumn}`)
                return JSON.stringify({ hexes, cursor: null })
            } catch (e: any) {
                console.error(`[Chat Tool] ${toolName} error:`, e.message)
                return JSON.stringify({ hexes: [], error: e.message })
            }
        }

        case "compare_h3_hexes": {
            try {
                const supabase = await getSupabaseServerClient()
                const forecastYear = args.forecast_year || 2029

                const [leftRes, rightRes] = await Promise.all([
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
                ])

                const left = leftRes.data
                const right = rightRes.data

                // Reverse geocode both
                const getName = async (lat: number, lng: number) => {
                    try {
                        const res = await fetch(`https://nominatim.openstreetmap.org/reverse?lat=${lat}&lon=${lng}&zoom=16&format=json`, {
                            headers: { "User-Agent": "HomecastrUI/1.0" },
                        })
                        if (res.ok) {
                            const d = await res.json()
                            return d.address?.suburb || d.address?.neighbourhood || d.display_name?.split(",")[0] || "Unknown"
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
                    if ((left.predicted_value ?? 0) > (right.predicted_value ?? 0)) highlights.push(`${leftName} has higher predicted value`)
                    else if ((right.predicted_value ?? 0) > (left.predicted_value ?? 0)) highlights.push(`${rightName} has higher predicted value`)
                }

                const toMetrics = (d: any) => d ? {
                    annual_change_pct: d.opportunity != null ? Math.round(d.opportunity * 10000) / 100 : null,
                    predicted_value: d.predicted_value,
                    property_count: d.property_count,
                } : null

                console.log(`[Chat Tool] compare: ${leftName} vs ${rightName}`)
                return JSON.stringify({
                    left: { h3_id: args.left_h3_id, name: leftName, metrics: toMetrics(left) },
                    right: { h3_id: args.right_h3_id, name: rightName, metrics: toMetrics(right) },
                    highlights,
                })
            } catch (e: any) {
                console.error("[Chat Tool] compare error:", e.message)
                return JSON.stringify({ error: e.message })
            }
        }

        case "explain_metric": {
            const explanations: Record<string, string> = {
                opportunity: "Expected annual change shows how much property values are projected to change each year over the next 4 years. For example, +5% means values are expected to rise about 5% per year.",
                predicted_value: "The predicted median home value in this area by the forecast year, based on historical trends and market conditions.",
                property_count: "The number of properties in this hex cell used to calculate the forecast. More properties generally means a more reliable prediction.",
                sample_accuracy: "How accurately our model predicted past values in this area. Higher accuracy means the model performs well here.",
            }
            return JSON.stringify({
                short: explanations[args.metric] || `${args.metric} is a key real estate metric.`,
                long: explanations[args.metric] || `${args.metric} is used to evaluate real estate investment potential.`,
            })
        }

        case "record_feedback":
            return JSON.stringify({ recorded: true, id: "fb_" + Date.now() })

        default:
            return JSON.stringify({ status: "ok", note: `Tool ${toolName} executed` })
    }
}

