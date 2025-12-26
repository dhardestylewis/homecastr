"use server"

import { createServerClient } from "@supabase/ssr"
import { cookies } from "next/headers"

interface H3Hexagon {
  h3_id: string
  lat: number
  lng: number
  opportunity: number
  reliability: number
  property_count: number
  alert_pct: number
}

interface H3Payload {
  hexagons: H3Hexagon[]
}

export async function getH3DataForResolution(h3Res: number, forecastYear = 2026): Promise<H3Hexagon[]> {
  const cookieStore = await cookies()

  const supabase = createServerClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL!,
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!,
    {
      cookies: {
        getAll() {
          return cookieStore.getAll()
        },
        setAll(cookiesToSet) {
          try {
            cookiesToSet.forEach(({ name, value, options }) => cookieStore.set(name, value, options))
          } catch {
            // Ignore errors in Server Components
          }
        },
      },
    },
  )

  const { data, error } = await supabase
    .from("h3_precomputed_json")
    .select("payload")
    .eq("forecast_year", forecastYear)
    .eq("h3_res", h3Res)
    .single()

  if (error) {
    console.error(`[v0] Error fetching H3 data for res ${h3Res}:`, error)
    return []
  }

  const payload = data?.payload as H3Payload
  return payload?.hexagons ?? []
}

// Fetch all resolutions at once for caching
export async function getAllH3Data(forecastYear = 2026): Promise<Record<number, H3Hexagon[]>> {
  const cookieStore = await cookies()

  const supabase = createServerClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL!,
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!,
    {
      cookies: {
        getAll() {
          return cookieStore.getAll()
        },
        setAll(cookiesToSet) {
          try {
            cookiesToSet.forEach(({ name, value, options }) => cookieStore.set(name, value, options))
          } catch {
            // Ignore errors in Server Components
          }
        },
      },
    },
  )

  const { data, error } = await supabase
    .from("h3_precomputed_json")
    .select("h3_res, payload")
    .eq("forecast_year", forecastYear)

  if (error) {
    console.error("[v0] Error fetching all H3 data:", error)
    return {}
  }

  const result: Record<number, H3Hexagon[]> = {}
  for (const row of data ?? []) {
    const payload = row.payload as H3Payload
    result[row.h3_res] = payload?.hexagons ?? []
  }

  return result
}
