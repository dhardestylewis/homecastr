"use server"

import { z } from "zod"
import { initLogger, traced, flush } from "braintrust"

if (typeof process !== "undefined" && process.env.BRAINTRUST_API_KEY) {
  initLogger({
    projectName: "Homecastr",
    apiKey: process.env.BRAINTRUST_API_KEY,
  })
}

/**
 * Server action to create a Tavus Conversational Video Agent session.
 * POSTs to the Tavus v2 conversations API with property context.
 */

interface CreateTavusConversationParams {
  predictedValue: number | null
  opportunityScore: number | null
  capRate: number | null
  address?: string | null
}

interface TavusConversationResponse {
  conversation_id?: string
  conversation_url?: string
  error?: string
}

export async function createTavusConversation({
  predictedValue,
  opportunityScore,
  capRate,
  address,
}: CreateTavusConversationParams): Promise<TavusConversationResponse> {
  try {
    const apiKey = process.env.TAVUS_API_KEY
    const personaId = process.env.TAVUS_PERSONA_ID
    const replicaId = process.env.TAVUS_REPLICA_ID

    if (!apiKey) {
      console.error("[TAVUS] Missing API Key")
      return { error: "TAVUS_API_KEY is not configured on the server." }
    }

    if (!personaId && !replicaId) {
      console.error("[TAVUS] Missing Persona/Replica ID")
      return { error: "Both TAVUS_PERSONA_ID and TAVUS_REPLICA_ID are missing. Please configure at least one." }
    }

    // 1. Context Sanitization: Use tools for real data, don't pass pre-computed numbers.
    const locationContext = address && !address.includes("neighborhood") ? `at ${address}` : "in Houston";

    const conversational_context = `The user is exploring properties ${locationContext} in Houston, TX. 
    IMPORTANT: You have zero initial metrics. You MUST call 'location_to_hex' or 'get_h3_hex' immediately to find the real market data for the current selection.
    
    SYSTEM RULES:
    1. Use tools for EVERYTHING. Do not guess.
    2. When discussing a neighborhood or cluster, use 'fly_to_location' with 'selected_hex_ids' containing multiple relevant IDs to visually indicate the area.
    3. This triggers the Visual Inspector (drawer) automatically for the user.
    4. Speak naturally like a human. Report 1-2 metrics max per turn.
    5. Do NOT mention hex IDs or technical jargon. Do not say "H3 Cell".
    6. To compare locations, use 'add_location_to_selection'. To reset, use 'clear_selection'.
    7. CHAINING: If you 'rank_h3_hexes', you can immediately 'add_location_to_selection' using the 'h3_ids' list you just found to visualize them.`;

    const custom_greeting = `Hi! I see you're checking out properties ${locationContext}. How can I help you understand the market trajectory here?`

    // 2. Persona Initialization
    // We force a transient persona for now to ensure our new "Human Speech" rules are active.
    let activePersonaId = null;

    if (!activePersonaId) {
      try {
        const toolDefinitions = [
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
              name: "location_to_hex",
              description: "Resolve text location -> map to h3_id -> return metrics.",
              parameters: {
                type: "object",
                properties: {
                  query: { type: "string" },
                  forecast_year: { type: "integer" },
                  include_metrics: { type: "boolean" }
                },
                required: ["query", "forecast_year", "include_metrics"]
              }
            }
          },
          {
            type: "function",
            function: {
              name: "get_h3_hex",
              description: "Fetch metrics for a single H3 hex.",
              parameters: {
                type: "object",
                properties: {
                  h3_id: { type: "string" },
                  forecast_year: { type: "integer" }
                },
                required: ["h3_id", "forecast_year"]
              }
            }
          },
          {
            type: "function",
            function: {
              name: "rank_h3_hexes",
              description: "Rank H3 hexes in an area for growth or value.",
              parameters: {
                type: "object",
                properties: {
                  forecast_year: { type: "integer" },
                  h3_res: { type: "integer" },
                  objective: { type: "string" },
                  limit: { type: "integer" }
                },
                required: ["objective"]
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
                  select_hex_id: { type: "string" },
                  selected_hex_ids: {
                    type: "array",
                    items: { type: "string" },
                    description: "Optional list of hex IDs to highlight."
                  }
                },
                required: ["lat", "lng", "zoom"]
              }
            }
          },

          // DEPRECATED: inspect_location and inspect_neighborhood are now handled by location_to_hex and add_location_to_selection
          // allowing the agent to be more efficient.
          /*
          {
            type: "function",
            function: {
              name: "inspect_location",
              ...
            }
          },
          {
            type: "function",
            function: {
              name: "inspect_neighborhood",
              ...
            }
          }
          */
          {
            type: "function",
            function: {
              name: "add_location_to_selection",
              description: "Add a location (or specific hexes) to the current map selection for comparison.",
              parameters: {
                type: "object",
                properties: {
                  query: { type: "string", description: "City or neighborhood name (optional if h3_id provided)" },
                  h3_id: { type: "string", description: "Specific Hex ID to add (from search results)" },
                  h3_ids: { type: "array", items: { type: "string" }, description: "List of Hex IDs to add (e.g. top ranked)" },
                  forecast_year: { type: "integer" },
                  include_metrics: { type: "boolean" }
                },
                anyOf: [
                  { required: ["query"] },
                  { required: ["h3_id"] },
                  { required: ["h3_ids"] }
                ]
              }
            }
          },
          {
            type: "function",
            function: {
              name: "clear_selection",
              description: "Clear all current map selections and reset the view.",
              parameters: {
                type: "object",
                properties: {},
                required: []
              }
            }
          }
        ];

        const personaBody = {
          persona_name: `Homecastr Voice ${Date.now()}`,
          pipeline_mode: "full",
          system_prompt: `You are Homecastr, a real estate analyst. You are TALKING to a user.

VOICE ETIQUETTE (CRITICAL):
1. SPEAK LIKE A HUMAN: Do NOT read tool results verbatim. Do NOT read JSON blocks.
2. NEVER mention IDs: Never say "H3 Cell", "Hex ID", or long strings of letters/numbers. Just say "this area" or the neighborhood name.
3. CONCISE: Report only 1 or 2 metrics at a time (e.g., just the Value and the Growth). Wait for the user to ask for more.
4. PROACTIVITY: As soon as a tool returns metrics, translate them into a natural sentence. "I found that Midtown is looking at an estimated value of $370k by 2030."
5. NO LOOPS: If you have data, share it. Do not tell the user you are waiting if the result is already in your tool history.

RULES:
- TREND ANALYSIS: Use 'forecast_year: 2030' by default for trends.
- BASELINE (2026): If the year is 2026, ONLY report the "Current Market Value (2026)". Do NOT show "Predicted Value" or "Annual Change" as they are identical to the current state.
- Post-2026: For future years, always provide BOTH Current (2026) and Predicted ([Year]).
- DOLLARS: Mention specific dollar values clearly.
- NO REDUNDANCY: If you successfully call 'location_to_hex', you DO NOT need to call 'resolve_place' or 'fly_to_location' afterwards. The map will update automatically.
- NO FILLER: No conversational filler like "reaching out to my modules." Just get the data.

COMPARISON WORKFLOW (CRITICAL):
1. INITIAL SEARCH: Use 'location_to_hex' to find the first location.
2. COMPARISON: When the user asks to compare with another location, use 'add_location_to_selection'. DO NOT use 'location_to_hex' again (it resets the view). DO NOT use 'clear_selection'.
3. ACCUMULATE: 'add_location_to_selection' adds the new location to the map so the user can see both.
4. TARGETING: If you are adding a list of locations (e.g. top 3), the map will highlight them all.`,
          layers: {
            llm: {
              model: "tavus-gpt-oss",
              tools: toolDefinitions
            }
          }
        };

        const personaResult = await traced(async (span) => {
          const personaRes = await fetch("https://tavusapi.com/v2/personas", {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "X-Api-Key": apiKey,
            },
            body: JSON.stringify(personaBody),
          });

          if (!personaRes.ok) {
            const errText = await personaRes.text();
            span.log({ input: { persona_name: personaBody.persona_name }, output: { error: errText }, metadata: { status: personaRes.status } });
            return { error: errText, status: personaRes.status };
          }

          const data = await personaRes.json();
          span.log({ input: { persona_name: personaBody.persona_name, model: "tavus-gpt-oss" }, output: { persona_id: data.persona_id }, metadata: { status: personaRes.status } });
          return { persona_id: data.persona_id };
        }, { name: "Tavus Persona Creation" });

        if (personaResult.error) {
          console.error(`[TAVUS] Persona Creation Failed: ${personaResult.status} - ${personaResult.error}`);
          return { error: `Failed to create persona: ${personaResult.error}` };
        }

        activePersonaId = personaResult.persona_id;
        console.log(`[TAVUS] Created transient persona: ${activePersonaId}`);
      } catch (e) {
        console.error("[TAVUS] Error creating persona:", e);
        return { error: "Failed to initialize agent persona." };
      }
    }

    // 2. Create the conversation using that Persona
    const body: Record<string, unknown> = {
      persona_id: activePersonaId,
      conversational_context,
      custom_greeting,
      conversation_name: "Homecastr Live Agent",
      max_participants: 2,
      properties: {
        max_call_duration: 1800,
        participant_left_timeout: 30,
        participant_absent_timeout: 120,
        enable_recording: true,
        language: "english",
      },
    }

    if (replicaId) {
      body.replica_id = replicaId
    }

    const result = await traced(async (span) => {
      const response = await fetch("https://tavusapi.com/v2/conversations", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-api-key": apiKey,
        },
        body: JSON.stringify(body),
      })

      if (!response.ok) {
        const errorText = await response.text()
        span.log({ input: { persona_id: activePersonaId, address }, output: { error: errorText }, metadata: { status: response.status } })
        console.error("[TAVUS] API error:", response.status, errorText)
        return { error: `Tavus API error: ${response.status} - ${errorText}` }
      }

      const data = await response.json()
      span.log({
        input: { persona_id: activePersonaId, address },
        output: { conversation_id: data.conversation_id },
        metadata: { status: response.status },
      })
      console.log("[TAVUS] Conversation created:", data.conversation_id)

      return {
        conversation_id: data.conversation_id,
        conversation_url: data.conversation_url,
      }
    }, { name: "Tavus Conversation Creation" })

    if (process.env.BRAINTRUST_API_KEY) {
      await flush().catch(() => {})
    }

    return result
  } catch (err) {
    console.error("[TAVUS] Unexpected error:", err)
    return { error: `Unexpected error: ${err instanceof Error ? err.message : String(err)}` }
  }
}

