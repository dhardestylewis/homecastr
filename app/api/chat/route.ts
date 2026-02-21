import { NextRequest, NextResponse } from "next/server"
import OpenAI from "openai"
import { initLogger, wrapOpenAI, flush } from "braintrust"
import * as h3 from "h3-js"
import { executeTopLevelTool } from "@/app/actions/tools"

// Braintrust: init once at module load so wrapOpenAI has a current logger when tracing
if (typeof process !== "undefined" && process.env.BRAINTRUST_API_KEY) {
  initLogger({
    projectName: "Homecastr",
    apiKey: process.env.BRAINTRUST_API_KEY,
  })
}

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
                    forecast_year: { type: "integer", description: "Forecast year (e.g., 2030 for a 4-year trend). Use 2026 ONLY if asking for current market value." },
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
                    bbox: { type: "array", description: "Bounding box [minLat, maxLat, minLng, maxLng].", items: { type: "number" } },
                    filters: { type: "object", description: "Metric thresholds (min_opportunity, min_reliability, etc.)." },
                    sort: { type: "string", description: "Sort key for ranking results." },
                    limit: { type: "integer", description: "Max results (1-50)." },
                    cursor: { type: "string", description: "Pagination cursor; empty string for first page." },
                    include_geometry: { type: "boolean", description: "Whether to include geometry." },
                },
                required: ["forecast_year", "h3_res", "filters", "sort", "limit", "cursor", "include_geometry"],
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
                    bbox: { type: "array", description: "Bounding box [minLat, maxLat, minLng, maxLng].", items: { type: "number" } },
                    objective: { type: "string", description: "Ranking objective." },
                    constraints: { type: "object", description: "Ranking constraints (min_property_count, min_sample_accuracy)." },
                    limit: { type: "integer", description: "Max results (1-50)." },
                    cursor: { type: "string", description: "Pagination cursor; empty string for first page." },
                    include_geometry: { type: "boolean", description: "Whether to include geometry." },
                },
                required: ["forecast_year", "h3_res", "objective", "constraints", "limit", "cursor", "include_geometry"],
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

const SYSTEM_PROMPT = `You are Homecastr, a real estate analyst for Houston, TX.

RULES:
1. PROACTIVITY: Report results immediately once a tool returns. Never say "I'm waiting" or "Give me a second."
2. BASELINE REPORTING (2026): If the forecast_year is 2026, ONLY report the "Current Market Value (2026)". Do NOT show "Predicted Value" or "Annual Change" as they are identical to the current state.
3. TREND REPORTING (Post-2026): For future years, always provide:
   - Current Market Value (2026)
   - Predicted Value ([Year])
   - Avg. Annual Change (%)
4. TOOL EFFICIENCY: Use 'location_to_hex' for one-shot lookups. Avoid redundant tool calls.
5. FLY FIRST: Batch 'fly_to_location' with data tools.
6. ANALYTICAL TONE: Be concise. Mention specific dollar values. Speak like a professional analyst, not a JSON parser. Use Markdown.`

export async function POST(req: NextRequest) {
    try {
        const apiKey = process.env.OPENAI_API_KEY
        if (!apiKey) {
            return NextResponse.json(
                { error: "OPENAI_API_KEY not configured. Add it to .env.local" },
                { status: 500 }
            )
        }

        // Use Braintrust-wrapped client if key is set (logger already inited at module load)
        const baseClient = new OpenAI({ apiKey })
        const openai = process.env.BRAINTRUST_API_KEY ? wrapOpenAI(baseClient) : baseClient
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
                toolCalls: assistantMessage.tool_calls?.map(tc => tc.type === 'function' ? (tc as any).function.name : 'unknown') || [],
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
                if (tc.type !== 'function') continue
                const toolFn = (tc as any).function
                if (toolFn.name === "fly_to_location") {
                    // UI tool — extract for client-side, return dummy result to LLM
                    try {
                        const args = JSON.parse(toolFn.arguments)
                        allMapActions.push(args)
                        console.log(`[Chat API] fly_to_location:`, args)
                    } catch (e) {
                        console.error(`[Chat API] Failed to parse fly_to_location args:`, toolFn.arguments)
                    }
                    conversationMessages.push({
                        role: "tool",
                        tool_call_id: tc.id,
                        content: JSON.stringify({ status: "ok", message: "Map is now showing the requested location." }),
                    })
                } else {
                    // API tool — query real data
                    allToolsUsed.push(toolFn.name)
                    try {
                        const args = JSON.parse(toolFn.arguments)
                        const result = await executeTopLevelTool(toolFn.name, args)
                        if (toolFn.name === "rank_h3_hexes") {
                            try {
                                const resultObj = JSON.parse(result)
                                if (resultObj.hexes && resultObj.hexes.length > 0) {
                                    // Create map action to fly to top result and highlight all
                                    const topHex = resultObj.hexes[0]
                                    const highlights = resultObj.hexes.map((h: any) => h.h3_id)

                                    if (topHex.location && topHex.location.lat && topHex.location.lng) {
                                        allMapActions.push({
                                            lat: topHex.location.lat,
                                            lng: topHex.location.lng,
                                            zoom: 14,
                                            select_hex_id: topHex.h3_id, // Lock top hex
                                            highlighted_hex_ids: highlights // Highlight all
                                        })
                                    }
                                }
                            } catch (e) {
                                console.error("[Chat API] Failed to parse rank_h3_hexes result for map action:", e)
                            }
                        } else if (toolFn.name === "location_to_hex") {
                            // existing logic for single location if needed, or just let fly_to handle it
                            try {
                                const resultObj = JSON.parse(result)
                                if (resultObj.hex && resultObj.hex.location) {
                                    // We don't necessarily want to force select it if fly_to was also called
                                    // But if the user asked "show me X", selecting it is nice.
                                }
                            } catch (e) { }
                        }

                        conversationMessages.push({
                            role: "tool",
                            tool_call_id: tc.id,
                            content: result,
                        })
                    } catch (e) {
                        console.error(`[Chat API] Failed to process tool ${toolFn.name}:`, e)
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
    } finally {
        if (process.env.BRAINTRUST_API_KEY) {
            await flush().catch(() => {})
        }
    }
}


