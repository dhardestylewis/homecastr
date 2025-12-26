"use server"

import { getSupabaseServerClient } from "@/lib/supabase/server"

export interface PropertyForecast {
  id: number
  acct: string
  yr: number
  valuation: number
  is_imputed: boolean
}

export interface ForecastConfidence {
  id: number
  forecast_year: number
  horizon_years: number
  mape_median: number
  mape_tail_risk: number
}

export async function getPropertyForecast(acct: string): Promise<PropertyForecast[]> {
  const supabase = await getSupabaseServerClient()

  const { data, error } = await supabase
    .from("property_forecasts")
    .select("*")
    .eq("acct", acct)
    .order("yr", { ascending: true })

  if (error) {
    console.error("Error fetching property forecast:", error)
    throw new Error(`Failed to fetch forecast for account ${acct}: ${error.message}`)
  }

  return data || []
}

export async function searchPropertyByAccount(acct: string): Promise<PropertyForecast | null> {
  const supabase = await getSupabaseServerClient()

  const { data, error } = await supabase
    .from("property_forecasts")
    .select("*")
    .eq("acct", acct)
    .order("yr", { ascending: false })
    .limit(1)
    .single()

  if (error) {
    if (error.code === "PGRST116") {
      // No rows returned
      return null
    }
    console.error("Error searching property:", error)
    throw new Error(`Failed to search property: ${error.message}`)
  }

  return data
}

export async function getForecastConfidence(): Promise<ForecastConfidence[]> {
  const supabase = await getSupabaseServerClient()

  const { data, error } = await supabase
    .from("forecast_confidence")
    .select("*")
    .order("forecast_year", { ascending: true })
    .order("horizon_years", { ascending: true })

  if (error) {
    console.error("Error fetching forecast confidence:", error)
    throw new Error(`Failed to fetch forecast confidence: ${error.message}`)
  }

  return data || []
}

export async function getAllPropertyAccounts(limit = 100): Promise<string[]> {
  const supabase = await getSupabaseServerClient()

  const { data, error } = await supabase.from("property_forecasts").select("acct").limit(limit)

  if (error) {
    console.error("Error fetching property accounts:", error)
    return []
  }

  // Get unique account IDs
  const uniqueAccts = [...new Set(data.map((row) => row.acct))]
  return uniqueAccts
}
