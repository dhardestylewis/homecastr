import { getSupabaseServerClient } from "@/lib/supabase/server"
import { NextResponse } from "next/server"

// POST /api/threads - Create or update a chat thread
export async function POST(request: Request) {
  try {
    const { threadId, messages, forecastContext } = await request.json()
    
    const supabase = await getSupabaseServerClient()
    
    if (threadId) {
      // Update existing thread
      const { error } = await supabase
        .from("chat_threads")
        .update({
          messages,
          tract_geoid: forecastContext?.tractGeoid,
          neighborhood_name: forecastContext?.neighborhoodName,
          city: forecastContext?.city,
          state: forecastContext?.state,
          current_url: forecastContext?.currentUrl,
          updated_at: new Date().toISOString(),
        })
        .eq("id", threadId)
      
      if (error) {
        console.error("Error updating thread:", error)
        return NextResponse.json({ error: "Failed to update thread" }, { status: 500 })
      }
      
      return NextResponse.json({ id: threadId })
    } else {
      // Create new thread
      const { data, error } = await supabase
        .from("chat_threads")
        .insert({
          messages,
          tract_geoid: forecastContext?.tractGeoid,
          neighborhood_name: forecastContext?.neighborhoodName,
          city: forecastContext?.city,
          state: forecastContext?.state,
          current_url: forecastContext?.currentUrl,
        })
        .select("id")
        .single()
      
      if (error) {
        console.error("Error creating thread:", error)
        return NextResponse.json({ error: "Failed to create thread" }, { status: 500 })
      }
      
      return NextResponse.json({ id: data.id })
    }
  } catch (error) {
    console.error("Thread API error:", error)
    return NextResponse.json({ error: "Internal server error" }, { status: 500 })
  }
}
