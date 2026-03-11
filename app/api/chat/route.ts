import { NextRequest, NextResponse } from "next/server"
import OpenAI from "openai"
import * as h3 from "h3-js"
import { executeTopLevelTool } from "@/app/actions/tools"
import { executeTopLevelForecastTool } from "@/app/actions/tools-forecast"
import { initLogger, wrapOpenAI, flush } from "braintrust"

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
    {
        type: "function",
        function: {
            name: "clear_selection",
            description:
                "Clear the current map selection, dismissing any open tooltip and deselecting any highlighted geometry. Use when the user asks to clear, reset, or dismiss the current selection.",
            parameters: {
                type: "object",
                properties: {},
                required: [],
            },
        },
    },
    {
        type: "function",
        function: {
            name: "end_session",
            description: "End the current chat session or video call. Use when the user says goodbye, thanks you and wants to leave, or explicitly asks to close the chat.",
            parameters: { type: "object", properties: {}, required: [] }
        }
    }
]

// Forecast-mode tool definitions (geography-level, matching tavus.ts)
const FORECAST_TOOL_DEFINITIONS: OpenAI.ChatCompletionTool[] = [
    {
        type: "function",
        function: {
            name: "resolve_place",
            description: "Resolve a free-text location into lat/lng results.",
            parameters: {
                type: "object",
                properties: {
                    query: { type: "string" },
                    city_hint: { type: "string" },
                    max_candidates: { type: "integer" }
                },
                required: ["query", "city_hint", "max_candidates"]
            }
        }
    },
    {
        type: "function",
        function: {
            name: "location_to_area",
            description: "Resolve text location -> determine geography -> return forecast metrics for that area. Use for initial one-shot lookups.",
            parameters: {
                type: "object",
                properties: {
                    query: { type: "string", description: "Location name or address" },
                    level: { type: "string", enum: ["zcta", "tract", "tabblock", "parcel"], description: "Geography level" },
                    area_id: { type: "string", description: "Specific area ID if known" },
                    forecast_year: { type: "integer", description: "Forecast year (2026 for current, 2030 for trend)" },
                    include_metrics: { type: "boolean" }
                },
                required: ["query", "forecast_year"]
            }
        }
    },
    {
        type: "function",
        function: {
            name: "get_forecast_area",
            description: "Fetch forecast metrics for a specific geography unit (zip code, tract, block, or parcel).",
            parameters: {
                type: "object",
                properties: {
                    level: { type: "string", enum: ["zcta", "tract", "tabblock", "parcel"] },
                    id: { type: "string", description: "Geography ID (e.g. 77079 for zcta)" },
                    forecast_year: { type: "integer" },
                    lat: { type: "number" },
                    lng: { type: "number" }
                },
                required: ["level", "id", "forecast_year"]
            }
        }
    },
    {
        type: "function",
        function: {
            name: "rank_forecast_areas",
            description: "Rank geography units by forecasted value or growth.",
            parameters: {
                type: "object",
                properties: {
                    level: { type: "string", enum: ["zcta", "tract", "tabblock", "parcel"] },
                    forecast_year: { type: "integer" },
                    objective: { type: "string", enum: ["value", "growth"] },
                    limit: { type: "integer" }
                },
                required: ["objective"]
            }
        }
    },
    {
        type: "function",
        function: {
            name: "add_location_to_selection",
            description: "Add a location to the current map selection for comparison. Use after initial search to compare areas.",
            parameters: {
                type: "object",
                properties: {
                    query: { type: "string", description: "Location name or address" },
                    area_id: { type: "string" },
                    level: { type: "string", enum: ["zcta", "tract", "tabblock", "parcel"] },
                    forecast_year: { type: "integer" },
                    include_metrics: { type: "boolean" }
                },
                required: ["query"]
            }
        }
    },
    {
        type: "function",
        function: {
            name: "fly_to_location",
            description: "Pan and zoom the map to a specific location.",
            parameters: {
                type: "object",
                properties: {
                    lat: { type: "number" },
                    lng: { type: "number" },
                    zoom: { type: "integer" },
                    area_id: { type: "string" },
                    level: { type: "string" }
                },
                required: ["lat", "lng", "zoom"]
            }
        }
    },
    {
        type: "function",
        function: {
            name: "explain_metric",
            description: "Explain a forecast metric in plain language.",
            parameters: {
                type: "object",
                properties: {
                    metric: { type: "string" },
                    audience: { type: "string" }
                },
                required: ["metric"]
            }
        }
    },
    {
        type: "function",
        function: {
            name: "clear_selection",
            description: "Clear the current map selection, dismissing the tooltip, fan chart, and deselecting any highlighted geometry. Use when the user asks to clear, reset, dismiss, or close the current selection or tooltip.",
            parameters: { type: "object", properties: {}, required: [] }
        }
    },
    {
        type: "function",
        function: {
            name: "clear_comparison",
            description: "Clear only the comparison overlay while keeping the primary selection and tooltip visible. Use when the user asks to remove or clear just the comparison.",
            parameters: { type: "object", properties: {}, required: [] }
        }
    },
    {
        type: "function",
        function: {
            name: "set_forecast_year",
            description: "Change the forecast timeline year displayed on the map and charts. Valid years: 2019 through 2030. Use when the user asks to change the year, set the timeline, or view a different forecast horizon.",
            parameters: {
                type: "object",
                properties: {
                    year: { type: "integer", minimum: 2019, maximum: 2030, description: "The forecast year to display (2019-2030)" }
                },
                required: ["year"]
            }
        }
    },
    {
        type: "function",
        function: {
            name: "set_color_mode",
            description: "Switch the map coloring between 'value' (absolute predicted home values) and 'growth' (percentage growth rate). Use when the user asks to see growth, value, switch view, or toggle the map display.",
            parameters: {
                type: "object",
                properties: {
                    mode: { type: "string", enum: ["value", "growth"], description: "The color mode to display" }
                },
                required: ["mode"]
            }
        }
    },
    {
        type: "function",
        function: {
            name: "end_session",
            description: "End the current chat session or video call. Use when the user says goodbye, thanks you and wants to leave, or explicitly asks to close the chat.",
            parameters: { type: "object", properties: {}, required: [] }
        }
    }
]

const SYSTEM_PROMPT = `You are Homecastr, a real estate analyst providing nationwide home price forecasts across the United States.

RULES:
1. PROACTIVITY: Report results immediately once a tool returns. Never say "I'm waiting" or "Give me a second."
2. BASELINE REPORTING (2026): If the forecast_year is 2026, ONLY report the "Current Market Value (2026)". Do NOT show "Predicted Value" or "Annual Change" as they are identical to the current state.
3. TREND REPORTING (Post-2026): For future years, always provide:
   - Current Market Value (2026)
   - Predicted Value ([Year])
   - Avg. Annual Change (%)
4. TOOL EFFICIENCY: Use 'location_to_hex' for one-shot lookups. Avoid redundant tool calls.
5. FLY FIRST: Batch 'fly_to_location' with data tools.
6. ANALYTICAL TONE: Be concise. Mention specific dollar values. Speak like a professional analyst, not a JSON parser. Use Markdown.
7. COVERAGE: Our Schrödinger Bridge v12 model covers residential properties across the United States, including Texas, New York, Florida, and more. If a specific area is not yet available, inform the user and suggest browsing the forecast directory at homecastr.com/forecasts.`

const FORECAST_SYSTEM_PROMPT = `You are Homecastr, a real estate forecast analyst covering the United States.

RULES:
1. PROACTIVITY: Report results immediately once a tool returns. Never say "I'm waiting."
2. GEOGRAPHY: Data is organized at zip code (zcta), census tract, block, and parcel levels. Use 'location_to_area' for initial lookups.
3. YEAR TERMINOLOGY: The current year is 2026. Years 2019-2026 are HISTORICAL — call them "historical" or "past", never "forecast". Years 2027-2030 are FORECAST years. When the user asks about a specific year, ALWAYS call 'set_forecast_year' to update the map slider.
4. BASELINE (2026): If the year is 2026, ONLY report the "Current Market Value (2026)".
5. TREND (Post-2026): Always provide Current Value (2026), Predicted Value, and Avg Annual Change as a PERCENTAGE first, then dollar amount.
6. GROWTH: When discussing growth, appreciation, or comparing which area grows faster:
   - ALWAYS call 'set_color_mode' with mode 'growth' to switch the map to growth view.
   - ALWAYS present growth as a PERCENTAGE first (e.g. "+22.1%"), then optionally dollar amounts.
   - If the user specifies a future year, also call 'set_forecast_year' for that year.
   - When switching back to value discussion, call 'set_color_mode' with mode 'value'.
7. TOOL EFFICIENCY: Use 'location_to_area' for one-shot lookups. Use 'add_location_to_selection' to compare areas.
8. AUTO-FLY: 'location_to_area' and 'add_location_to_selection' automatically pan the map and select the feature. Do NOT also call 'fly_to_location' when using these tools — it causes conflicts. Only use 'fly_to_location' alone for simple pan/zoom without data lookup.
9. NEVER mention technical IDs. Say "this zip code" or "this neighborhood" instead.
10. Use Markdown formatting. Be concise and analytical.
11. COVERAGE: Our Schrödinger Bridge v12 model covers residential properties across the United States, including Texas, New York, Florida, and more. If a specific area is not yet available, inform the user and suggest browsing the forecast directory at homecastr.com/forecasts.
12. MANDATORY TOOL USE: You MUST call the appropriate tool for ANY map interaction. NEVER claim you performed an action without calling the tool. If the user asks to clear selections, MUST call 'clear_selection'. If asked to clear just the comparison, MUST call 'clear_comparison'. If asked to zoom or pan, MUST call 'fly_to_location'. NEVER say "I can't" for actions you have tools for.
13. HUMAN-READABLE LOCATIONS: NEVER show raw coordinates (lat/lng) to the user. When describing the user's current view, use 'resolve_place' to identify the nearest neighborhood, landmark, or street name. Say "You're looking at the Heights area" not "You're at (29.79, -95.41)".`

export async function POST(req: NextRequest) {
    try {
        const apiKey = process.env.OPENAI_API_KEY
        if (!apiKey) {
            return NextResponse.json(
                { error: "OPENAI_API_KEY not configured. Add it to .env.local" },
                { status: 500 }
            )
        }

        const rawOpenai = new OpenAI({ apiKey })
        const openai = process.env.BRAINTRUST_API_KEY ? wrapOpenAI(rawOpenai) : rawOpenai
        const { messages, forecastMode, mapViewport } = await req.json()
        console.log(`[Chat API] forecastMode=${forecastMode}, using ${forecastMode ? 'FORECAST' : 'H3'} tools`)

        if (!messages || !Array.isArray(messages)) {
            return NextResponse.json({ error: "messages array required" }, { status: 400 })
        }

        // Build viewport context string
        let viewportContext = ""
        if (mapViewport) {
            const [lng, lat] = mapViewport.center || []
            viewportContext = `\n\nCURRENT MAP STATE (internal, do NOT show coordinates to user — use resolve_place to identify neighborhood names):\n- Center: (${lat?.toFixed(5)}, ${lng?.toFixed(5)})\n- Zoom: ${mapViewport.zoom}\n- Selected area: ${mapViewport.selectedId || "none"}`
        }

        // Prepend system prompt with viewport context
        const systemPrompt = (forecastMode ? FORECAST_SYSTEM_PROMPT : SYSTEM_PROMPT) + viewportContext
        const conversationMessages: OpenAI.ChatCompletionMessageParam[] = [
            { role: "system" as const, content: systemPrompt },
            ...messages,
        ]

        const activeToolDefs = forecastMode ? FORECAST_TOOL_DEFINITIONS : TOOL_DEFINITIONS
        const executeTool = forecastMode ? executeTopLevelForecastTool : executeTopLevelTool

        const allMapActions: any[] = []
        const allToolsUsed: string[] = []
        const MAX_ROUNDS = 5

        for (let round = 0; round < MAX_ROUNDS; round++) {
            console.log(`[Chat API] Round ${round + 1}, messages: ${conversationMessages.length}`)

            const response = await openai.chat.completions.create({
                model: "gpt-4o-mini",
                messages: conversationMessages,
                tools: activeToolDefs,
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
                } else if (toolFn.name === "clear_selection") {
                    // UI tool — clear the current selection
                    allMapActions.push({ action: "clear_selection" })
                    console.log(`[Chat API] clear_selection`)
                    conversationMessages.push({
                        role: "tool",
                        tool_call_id: tc.id,
                        content: JSON.stringify({ status: "ok", message: "Selection cleared. The tooltip, fan chart, and map highlight have been dismissed." }),
                    })
                } else if (toolFn.name === "clear_comparison") {
                    // UI tool — clear only the comparison overlay
                    allMapActions.push({ action: "clear_comparison" })
                    console.log(`[Chat API] clear_comparison`)
                    conversationMessages.push({
                        role: "tool",
                        tool_call_id: tc.id,
                        content: JSON.stringify({ status: "ok", message: "Comparison overlay cleared. Primary selection remains." }),
                    })
                } else if (toolFn.name === "set_forecast_year") {
                    // UI tool — change the forecast timeline year
                    try {
                        const args = JSON.parse(toolFn.arguments)
                        const yr = args.year || 2029
                        allMapActions.push({ action: "set_forecast_year", year: yr })
                        console.log(`[Chat API] set_forecast_year: ${yr}`)
                    } catch (e) {
                        console.error(`[Chat API] Failed to parse set_forecast_year args:`, toolFn.arguments)
                    }
                    conversationMessages.push({
                        role: "tool",
                        tool_call_id: tc.id,
                        content: JSON.stringify({ status: "ok", message: `Timeline changed to forecast year ${JSON.parse(toolFn.arguments).year || 2029}.` }),
                    })
                } else if (toolFn.name === "set_color_mode") {
                    // UI tool — switch map between value and growth views
                    try {
                        const args = JSON.parse(toolFn.arguments)
                        const mode = args.mode === "growth" ? "growth" : "value"
                        allMapActions.push({ action: "set_color_mode", mode })
                        console.log(`[Chat API] set_color_mode: ${mode}`)
                    } catch (e) {
                        console.error(`[Chat API] Failed to parse set_color_mode args:`, toolFn.arguments)
                    }
                    conversationMessages.push({
                        role: "tool",
                        tool_call_id: tc.id,
                        content: JSON.stringify({ status: "ok", message: `Map view switched to ${JSON.parse(toolFn.arguments).mode || "value"} mode.` }),
                    })
                } else if (toolFn.name === "end_session") {
                    // UI tool — end the chat
                    allMapActions.push({ action: "end_session" })
                    console.log(`[Chat API] end_session`)
                    conversationMessages.push({
                        role: "tool",
                        tool_call_id: tc.id,
                        content: JSON.stringify({ status: "ok", message: "Ending session." }),
                    })
                } else {
                    // API tool — query real data
                    allToolsUsed.push(toolFn.name)
                    try {
                        const args = JSON.parse(toolFn.arguments)
                        const result = await executeTool(toolFn.name, args)

                        // Auto-create map actions from location results
                        if (forecastMode && toolFn.name === "location_to_area") {
                            try {
                                const resultObj = JSON.parse(result)
                                if (resultObj.chosen?.lat) {
                                    allMapActions.push({
                                        lat: resultObj.chosen.lat,
                                        lng: resultObj.chosen.lng,
                                        zoom: 13,
                                        area_id: resultObj.area?.id || args.area_id,
                                        level: args.level || "zcta",
                                    })
                                }
                            } catch (e) { }
                        } else if (forecastMode && toolFn.name === "add_location_to_selection") {
                            try {
                                const resultObj = JSON.parse(result)
                                if (resultObj.chosen?.lat) {
                                    allMapActions.push({
                                        action: "add_location_to_selection",
                                        lat: resultObj.chosen.lat,
                                        lng: resultObj.chosen.lng,
                                        zoom: 13,
                                        area_id: resultObj.area?.id || args.area_id,
                                        level: args.level || "zcta",
                                    })
                                }
                            } catch (e) { }
                        } else if (forecastMode && toolFn.name === "rank_forecast_areas") {
                            try {
                                const resultObj = JSON.parse(result)
                                if (resultObj.areas?.length > 0) {
                                    // TODO: fly to top area when we have lat/lng for ranked areas
                                }
                            } catch (e) { }
                        } else if (toolFn.name === "rank_h3_hexes") {
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
            await flush().catch(() => { })
        }
    }
}


