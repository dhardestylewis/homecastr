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

const SYSTEM_PROMPT = `You are Homecastr, a friendly and knowledgeable real estate analytics assistant for Houston, Texas (Harris County). You help homeowners and buyers understand property value forecasts, neighborhood trends, and investment metrics.

Your tools let you:
- Resolve locations (addresses, neighborhoods, zip codes) to coordinates
- Look up H3 hex metrics (opportunity/growth, reliability/confidence, predicted values)
- Search and rank areas by investment criteria
- Compare neighborhoods side-by-side
- Explain metrics in plain English
- Control the map (fly to locations, highlight hexes)

IMPORTANT BEHAVIOR:
1. When you identify a location the user is asking about, ALWAYS call fly_to_location to pan the map there so they can see it.
2. Keep responses concise and conversational — you're talking to homeowners, not analysts.
3. Use plain language for metrics: "growth potential" instead of "opportunity score", "confidence" instead of "reliability".
4. When you don't have real tool results yet (tools are in development), provide helpful mock/example responses and still call fly_to_location to demonstrate map interaction.
5. Default to forecast_year 2029 and h3_res 9 unless the user specifies otherwise.

You are currently in DEMO/TEST mode — tool endpoints are not yet connected to a live backend. When tool calls would normally hit the API, simulate realistic responses based on Houston real estate knowledge while still calling fly_to_location to move the map.`

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
        const MAX_ROUNDS = 3

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
                    // API tool — generate mock result
                    allToolsUsed.push(tc.function.name)
                    try {
                        const args = JSON.parse(tc.function.arguments)
                        const mockResult = generateMockToolResult(tc.function.name, args)
                        console.log(`[Chat API] Tool ${tc.function.name} mock result:`, mockResult.substring(0, 100))
                        conversationMessages.push({
                            role: "tool",
                            tool_call_id: tc.id,
                            content: mockResult,
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
 * Generate realistic mock tool results while backend is not connected.
 */
function generateMockToolResult(toolName: string, args: Record<string, any>): string {
    switch (toolName) {
        case "resolve_place":
            return JSON.stringify({
                candidates: [
                    {
                        label: args.query + ", Houston, TX",
                        lat: 29.743 + Math.random() * 0.05,
                        lng: -95.391 + Math.random() * 0.05,
                        confidence: 0.92,
                        kind: "neighborhood",
                        bbox: null,
                    },
                ],
            })

        case "get_h3_hex":
            return JSON.stringify({
                hex: {
                    h3_id: args.h3_id,
                    h3_res: args.h3_res,
                    forecast_year: args.forecast_year,
                    metrics: {
                        opportunity: 0.08 + Math.random() * 0.1,
                        reliability: 0.65 + Math.random() * 0.25,
                        predicted_value: 280000 + Math.random() * 120000,
                        property_count: Math.floor(50 + Math.random() * 200),
                        sample_accuracy: 0.7 + Math.random() * 0.2,
                    },
                    geometry_geojson: null,
                    reasons: ["Strong historical appreciation", "High property density"],
                },
            })

        case "search_h3_hexes":
        case "rank_h3_hexes":
            return JSON.stringify({
                hexes: Array.from({ length: Math.min(args.limit || 5, 5) }, (_, i) => ({
                    h3_id: `8928308280${i}ffff`,
                    h3_res: args.h3_res,
                    forecast_year: args.forecast_year,
                    metrics: {
                        opportunity: 0.12 - i * 0.02,
                        reliability: 0.85 - i * 0.05,
                        predicted_value: 350000 - i * 20000,
                        property_count: 150 - i * 20,
                    },
                    reasons: [`Rank #${i + 1} by ${args.objective || args.sort || "score"}`],
                })),
                cursor: null,
            })

        case "compare_h3_hexes":
            return JSON.stringify({
                left: {
                    h3_id: args.left_h3_id,
                    metrics: { opportunity: 0.09, reliability: 0.78, predicted_value: 320000 },
                },
                right: {
                    h3_id: args.right_h3_id,
                    metrics: { opportunity: 0.12, reliability: 0.71, predicted_value: 295000 },
                },
                highlights: [
                    "Right hex has higher growth potential (+3%)",
                    "Left hex has better confidence (78% vs 71%)",
                ],
            })

        case "explain_metric":
            const explanations: Record<string, string> = {
                opportunity: "Growth potential measures how much we expect property values to increase. A 10% score means values may rise about 10% by the forecast year.",
                reliability: "Confidence tells you how certain our prediction is. 80% confidence means the forecast is well-supported by data.",
                predicted_value: "The predicted median home value in this area by the forecast year, based on historical trends and market conditions.",
                property_count: "The number of properties in this hex cell used to calculate the forecast. More properties generally means a more reliable prediction.",
                sample_accuracy: "How accurately our model predicted past values in this area. Higher accuracy means the model performs well here.",
            }
            return JSON.stringify({
                short: explanations[args.metric] || `${args.metric} is a key real estate metric.`,
                long: explanations[args.metric] || `${args.metric} is used to evaluate real estate investment potential.`,
            })

        case "record_feedback":
            return JSON.stringify({ recorded: true, id: "fb_" + Date.now() })

        case "location_to_hex":
            return JSON.stringify({
                chosen: {
                    label: args.query + ", Houston, TX",
                    lat: 29.74 + Math.random() * 0.05,
                    lng: -95.39 + Math.random() * 0.05,
                    confidence: 0.9,
                    kind: "neighborhood",
                },
                tiles: [{ z: 11, x: 468, y: 844 }],
                h3: {
                    h3_id: "8928308280fffff",
                    h3_res: args.h3_res,
                    metrics: args.include_metrics
                        ? {
                            opportunity: 0.085,
                            reliability: 0.76,
                            predicted_value: 295000,
                            property_count: 142,
                            sample_accuracy: 0.82,
                        }
                        : null,
                },
            })

        default:
            return JSON.stringify({ status: "ok", note: `Mock result for ${toolName}` })
    }
}
