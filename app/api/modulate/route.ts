import { NextRequest, NextResponse } from "next/server"
import { getSupabaseServerClient } from "@/lib/supabase/server"

/**
 * GET /api/modulate?conversation_id=xxx
 * Returns the Modulate analysis for a given conversation, or the latest analyses.
 */
export async function GET(req: NextRequest) {
  try {
    const conversationId = req.nextUrl.searchParams.get("conversation_id")
    const limit = parseInt(req.nextUrl.searchParams.get("limit") ?? "10", 10)
    const supabase = await getSupabaseServerClient()

    if (conversationId) {
      const { data, error } = await supabase
        .from("modulate_analyses")
        .select("*")
        .eq("conversation_id", conversationId)
        .single()

      if (error) {
        return NextResponse.json(
          { error: "Analysis not found", details: error.message },
          { status: 404 }
        )
      }

      const { data: events } = await supabase
        .from("modulate_events")
        .select("*")
        .eq("conversation_id", conversationId)
        .order("created_at", { ascending: true })

      return NextResponse.json({ analysis: data, events: events ?? [] })
    }

    const { data, error } = await supabase
      .from("modulate_analyses")
      .select(
        "id, conversation_id, safety_score, compliance_score, deepfake_synthetic, off_script_detected, summary, created_at"
      )
      .order("created_at", { ascending: false })
      .limit(limit)

    if (error) {
      return NextResponse.json(
        { error: "Could not fetch analyses", details: error.message },
        { status: 500 }
      )
    }

    return NextResponse.json({ analyses: data })
  } catch (err) {
    return NextResponse.json(
      { error: err instanceof Error ? err.message : "Unknown error" },
      { status: 500 }
    )
  }
}
