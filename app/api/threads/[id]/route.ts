import { getSupabaseServerClient } from "@/lib/supabase/server"
import { NextResponse } from "next/server"

// GET /api/threads/[id] - Load a thread by ID
export async function GET(
  request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params
    
    const supabase = await getSupabaseServerClient()
    
    const { data, error } = await supabase
      .from("chat_threads")
      .select("*")
      .eq("id", id)
      .single()
    
    if (error || !data) {
      return NextResponse.json({ error: "Thread not found" }, { status: 404 })
    }
    
    return NextResponse.json({
      id: data.id,
      messages: data.messages,
      forecastContext: {
        tractGeoid: data.tract_geoid,
        neighborhoodName: data.neighborhood_name,
        city: data.city,
        state: data.state,
        currentUrl: data.current_url,
      },
      createdAt: data.created_at,
      updatedAt: data.updated_at,
    })
  } catch (error) {
    console.error("Thread API error:", error)
    return NextResponse.json({ error: "Internal server error" }, { status: 500 })
  }
}
