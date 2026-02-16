"use server"

import { z } from "zod"


/**
 * Server action to create a Tavus Conversational Video Agent session.
 * POSTs to the Tavus v2 conversations API with property context.
 */

interface CreateTavusConversationParams {
  predictedValue: number | null
  opportunityScore: number | null
  capRate: number | null
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

    // Format values for natural language
    const prediction = predictedValue != null
      ? `$${predictedValue.toLocaleString("en-US", { maximumFractionDigits: 0 })}`
      : "an estimated value"
    const opportunity = opportunityScore != null
      ? `${opportunityScore.toFixed(1)}%`
      : "N/A"
    const capRateStr = capRate != null
      ? `${(capRate * 100).toFixed(2)}%`
      : "N/A"

    const conversational_context = `The user is looking at a property in Houston with an Opportunity Score of ${opportunity} and a Cap Rate of ${capRateStr}. Our models predict a value of ${prediction} by 2030.`

    const custom_greeting = `Hi! I see you're checking out this property in Houston. How can I help you understand its future value?`

    const body: Record<string, unknown> = {
      system_prompt: `You are Homecastr, a real estate data assistant for Houston, TX (Harris County) being driven by an AI video avatar.
Your goal is to explain this specific property's investment potential using the provided metrics (Opportunity Score, Cap Rate, Predicted Value).

TONE & PERSONA:
1. You are professional, concise, and data-driven.
2. Only report numbers from the context provided. Never guess or make up numbers.
3. Do NOT mention "confidence" or "reliability" unless asked directly.
4. Use real estate terminology but explain it simply if needed.
5. If asked about other properties/neighborhoods, politely explain you only have data for this specific location right now.

CONTEXT:
The user is looking at a specific map location with the data provided in the prompt.`,
      tools: [
        {
          type: "function",
          function: {
            name: "fly_to_location",
            description: "Smoothly pan and zoom the map to a specific location. Use this after resolving a place or when discussing a specific area.",
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
            name: "rank_h3_hexes",
            description: "Find and rank H3 hexes in an area. Use this to find 'top areas' for growth or value.",
            parameters: {
              type: "object",
              properties: {
                forecast_year: { type: "integer", description: "Forecast year (default 2029)." },
                h3_res: { type: "integer", description: "H3 resolution (default 9)." },
                objective: { type: "string", description: "Ranking objective: higher_growth, more_predictable, best_risk_adjusted." },
                limit: { type: "integer", description: "Max results." }
              },
              required: ["objective"]
            }
          }
        }
      ],
      conversational_context,
      custom_greeting,
      conversation_name: "Homecastr Live Agent",
      // Lock to two-person view: just the user and the replica (no multi-party)
      max_participants: 2,
      properties: {
        max_call_duration: 1800, // 30 minutes
        participant_left_timeout: 30,
        participant_absent_timeout: 120,
        language: "english",
      },
    }

    if (personaId) {
      body.persona_id = personaId
    }
    if (replicaId) {
      body.replica_id = replicaId
    }

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
      console.error("[TAVUS] API error:", response.status, errorText)
      return { error: `Tavus API error: ${response.status} - ${errorText}` }
    }

    const data = await response.json()
    console.log("[TAVUS] Conversation created:", data.conversation_id)

    return {
      conversation_id: data.conversation_id,
      conversation_url: data.conversation_url,
    }
  } catch (err) {
    console.error("[TAVUS] Unexpected error:", err)
    return { error: `Unexpected error: ${err instanceof Error ? err.message : String(err)}` }
  }
}

