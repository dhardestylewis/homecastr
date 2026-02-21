import { NextRequest, NextResponse } from "next/server"
import { analyzeConversation, detectDeepfake } from "@/lib/modulate"
import { getSupabaseServerClient } from "@/lib/supabase/server"
import { initLogger, traced, flush } from "braintrust"

if (typeof process !== "undefined" && process.env.BRAINTRUST_API_KEY) {
  initLogger({
    projectName: "Homecastr",
    apiKey: process.env.BRAINTRUST_API_KEY,
  })
}

export async function POST(req: NextRequest) {
  try {
    const data = await req.json()
    const eventType: string = data.event_type ?? ""
    const conversationId: string = data.conversation_id ?? ""

    console.log(`[Tavus Webhook] ${eventType} for ${conversationId}`)

    switch (eventType) {
      case "system.replica_joined":
        console.log(`[Tavus Webhook] Replica joined conversation ${conversationId}`)
        await storeEvent(conversationId, "replica_joined", data.properties)
        break

      case "system.shutdown": {
        const reason = data.properties?.shutdown_reason ?? "unknown"
        console.log(`[Tavus Webhook] Conversation ${conversationId} ended: ${reason}`)
        await storeEvent(conversationId, "shutdown", { reason })
        break
      }

      case "application.transcription_ready": {
        const transcript = data.properties?.transcript ?? []
        console.log(
          `[Tavus Webhook] Transcript ready for ${conversationId} (${transcript.length} messages)`
        )

        const result = await traced(
          async (span) => {
            const analysis = await analyzeConversation({
              conversation_id: conversationId,
              transcript,
              metadata: { source: "tavus_webhook" },
            })

            span.log({
              input: {
                conversation_id: conversationId,
                message_count: transcript.length,
              },
              output: {
                safety_score: analysis.overall_safety_score,
                compliance_score: analysis.compliance_score,
                flags_count: analysis.safety_flags.length,
                off_script: analysis.off_script_events.detected,
                deepfake: analysis.deepfake_detection.is_synthetic,
              },
              scores: {
                safety: analysis.overall_safety_score,
                compliance: analysis.compliance_score,
              },
              metadata: { source: "modulate_analysis" },
            })

            return analysis
          },
          { name: "Modulate: Conversation Analysis" }
        )

        await storeAnalysis(conversationId, transcript, result)
        break
      }

      case "application.recording_ready": {
        const s3Key = data.properties?.s3_key
        const bucketName = data.properties?.bucket_name
        const duration = data.properties?.duration

        console.log(
          `[Tavus Webhook] Recording ready for ${conversationId}: ${s3Key} (${duration}s)`
        )

        if (s3Key && bucketName) {
          const audioUrl = `https://${bucketName}.s3.amazonaws.com/${s3Key}`

          const deepfakeResult = await traced(
            async (span) => {
              const result = await detectDeepfake(audioUrl)
              span.log({
                input: { conversation_id: conversationId, audio_url: audioUrl },
                output: result,
                metadata: { duration },
              })
              return result
            },
            { name: "Modulate: Deepfake Detection" }
          )

          await storeEvent(conversationId, "recording_analyzed", {
            s3_key: s3Key,
            duration,
            deepfake: deepfakeResult,
          })
        }
        break
      }

      case "application.perception_analysis": {
        const analysis = data.properties?.analysis
        console.log(`[Tavus Webhook] Perception analysis for ${conversationId}`)
        await storeEvent(conversationId, "perception_analysis", {
          analysis: typeof analysis === "string" ? analysis.slice(0, 2000) : analysis,
        })
        break
      }

      default:
        console.log(`[Tavus Webhook] Unhandled event: ${eventType}`)
    }

    if (process.env.BRAINTRUST_API_KEY) {
      await flush().catch(() => {})
    }

    return NextResponse.json({ status: "ok" })
  } catch (err) {
    console.error("[Tavus Webhook] Error:", err)
    return NextResponse.json(
      { error: err instanceof Error ? err.message : "Webhook processing failed" },
      { status: 500 }
    )
  }
}

// ---------------------------------------------------------------------------
// Supabase persistence helpers
// ---------------------------------------------------------------------------

async function storeEvent(
  conversationId: string,
  eventType: string,
  properties: Record<string, unknown>
) {
  try {
    const supabase = await getSupabaseServerClient()
    await supabase.from("modulate_events").insert({
      conversation_id: conversationId,
      event_type: eventType,
      properties,
      created_at: new Date().toISOString(),
    })
  } catch (err) {
    console.warn("[Tavus Webhook] Could not store event (table may not exist yet):", err)
  }
}

async function storeAnalysis(
  conversationId: string,
  transcript: { role: string; content: string }[],
  analysis: Awaited<ReturnType<typeof analyzeConversation>>
) {
  try {
    const supabase = await getSupabaseServerClient()
    await supabase.from("modulate_analyses").insert({
      conversation_id: conversationId,
      transcript,
      safety_score: analysis.overall_safety_score,
      compliance_score: analysis.compliance_score,
      emotion_timeline: analysis.emotion_timeline,
      safety_flags: analysis.safety_flags,
      deepfake_synthetic: analysis.deepfake_detection.is_synthetic,
      deepfake_confidence: analysis.deepfake_detection.confidence,
      off_script_detected: analysis.off_script_events.detected,
      off_script_instances: analysis.off_script_events.instances,
      summary: analysis.summary,
      created_at: new Date().toISOString(),
    })
  } catch (err) {
    console.warn("[Tavus Webhook] Could not store analysis (table may not exist yet):", err)
  }
}
