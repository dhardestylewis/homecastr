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

