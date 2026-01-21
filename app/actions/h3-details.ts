"use server"

import { getSupabaseServerClient } from "@/lib/supabase/server"
import type { DetailsResponse, FanChartData } from "@/lib/types"

/**
 * Fetches cell details from h3_precomputed_hex_details table.
 * This is the authoritative source for the inspector panel.
 */
export async function getH3CellDetails(h3Id: string, forecastYear = 2026): Promise<DetailsResponse | null> {
  console.log(`[SERVER] getH3CellDetails called: h3_id=${h3Id}, year=${forecastYear}`)
  try {
    const supabase = await getSupabaseServerClient()

    // Run queries in parallel: 
    // 1. Details for the requested forecast year
    // 2. Historical values (2019-2025)
    // 3. Grid coordinates
    // 4. Base details for FanChart (2026) if needed
    const [detailsResult, historyResult, gridResult, baseResult] = await Promise.all([
      supabase
        .from("h3_precomputed_hex_details")
        .select("*")
        .eq("h3_id", h3Id)
        .eq("forecast_year", forecastYear)
        .single(),
      supabase
        .from("h3_precomputed_hex_details")
        .select("forecast_year, predicted_value")
        .eq("h3_id", h3Id)
        .lte("forecast_year", 2025)
        .order("forecast_year", { ascending: true }),
      supabase
        .from("h3_aoi_grid")
        .select("lat, lng")
        .eq("h3_id", h3Id)
        .eq("aoi_id", "harris_county")
        .single(),
      forecastYear !== 2026
        ? supabase
          .from("h3_precomputed_hex_details")
          .select("*")
          .eq("h3_id", h3Id)
          .eq("forecast_year", 2026)
          .single()
        : Promise.resolve({ data: null, error: null })
    ])

    const { data: detailsData, error: detailsError } = detailsResult
    const { data: historyData } = historyResult
    const { data: gridData } = gridResult
    // Use base data for FanChart if available (to keep chart stable), otherwise use current details
    const fanSourceData = baseResult.data ?? detailsData

    console.log(`[SERVER] Query result for ${h3Id}/${forecastYear}: predicted_value=${detailsData?.predicted_value}, opportunity=${detailsData?.opportunity}`)
    console.log(`[SERVER] History result for ${h3Id}: ${historyData?.length ?? 0} rows`)

    if (detailsError || !detailsData) {
      console.error(`[v0] Failed to fetch details for ${h3Id}:`, detailsError)

      // Fallback: try hex_rows for basic data
      const { data: hexData, error: hexError } = await supabase
        .from("h3_precomputed_hex_rows")
        .select("*")
        .eq("h3_id", h3Id)
        .eq("forecast_year", forecastYear)
        .single()

      if (hexError || !hexData) {
        console.error(`[v0] Fallback to hex_rows also failed for ${h3Id}:`, hexError)
        return null
      }

      // Return partial response from hex_rows (many fields will be null)
      return buildPartialResponse(h3Id, hexData)
    }

    // Extract historical values array [2019, ..., 2025]
    // Create mapping of year -> value
    const historyMap = new Map(historyData?.map(r => [r.forecast_year, r.predicted_value]) ?? [])
    const historicalValues = [2019, 2020, 2021, 2022, 2023, 2024, 2025].map(y => historyMap.get(y) ?? 0)

    return buildFullResponse(h3Id, detailsData, historicalValues, gridData, fanSourceData)
  } catch (err) {
    console.error(`[SERVER] Critical error in getH3CellDetails for ${h3Id}:`, err)
    return null
  }
}

/**
 * Builds full response from h3_precomputed_hex_details
 * Column mapping verified against actual Supabase schema:
 * - All terms exist: accuracy_term, confidence_term, stability_term, robustness_term, support_term
 * - Risk metrics: risk_score, tail_gap_z, medae_z (lowercase), inv_dscr_z, score, hard_fail, alert_pct
 * - Proforma: predicted_value, noi, monthly_rent, dscr, cap_rate, breakeven_occ, liquidity
 * - Fan chart: fan_p10_y1-5, fan_p50_y1-5, fan_p90_y1-5
 * - Additional: ape, pred_cv, med_years
 * - V14: Coordinates from h3_aoi_grid
 */
function buildFullResponse(h3Id: string, d: any, historicalValues?: number[], grid?: { lat: number; lng: number } | null, fanData?: any): DetailsResponse {
  return {
    id: h3Id,
    locationLabel: `H3 Cell (Res ${d.h3_res})`,
    coordinates: grid ? { lat: grid.lat, lng: grid.lng } : undefined,
    historicalValues, // Pass through
    opportunity: {
      value: d.opportunity_pct ?? (d.opportunity != null ? d.opportunity * 100 : null),
      unit: "%",
      trend: d.trend ?? (d.opportunity > 0.05 ? "up" : d.opportunity < -0.02 ? "down" : "stable"),
    },
    reliability: {
      value: d.reliability,
      components: {
        accuracy_term: d.accuracy_term ?? null,
        confidence_term: d.confidence_term ?? null,
        stability_term: d.stability_term ?? null,
        robustness_term: d.robustness_term ?? null, // EXISTS in Supabase
        support_term: d.support_term ?? null,
      },
    },
    metrics: {
      n_accts: d.property_count,
      med_n_years: d.med_years ?? null, // EXISTS in Supabase (nullable)
      med_mean_ape_pct: d.ape ?? (d.sample_accuracy != null ? d.sample_accuracy * 100 : null), // Use ape column
      med_mean_pred_cv_pct: d.pred_cv ?? null, // EXISTS in Supabase
    },
    proforma: {
      predicted_value: d.predicted_value ?? null,
      noi: d.noi ?? null,
      monthly_rent: d.monthly_rent ?? null,
      dscr: d.dscr ?? null,
      cap_rate: d.cap_rate ?? null,
      breakeven_occ: d.breakeven_occ ?? null,
      liquidity_rank: d.liquidity ?? null, // EXISTS in Supabase
    },
    riskScoring: {
      R: d.risk_score ?? null,
      score: d.score ?? null,
      alert_triggered: d.hard_fail === 't' || (d.alert_pct ?? 0) > 0.25,
      tail_gap_z: d.tail_gap_z ?? null, // EXISTS in Supabase
      medAE_z: d.medae_z ?? null, // EXISTS - note lowercase 'medae_z' in DB
      inv_dscr_z: d.inv_dscr_z ?? null, // EXISTS in Supabase
    },
    fanChart: buildFanChart(fanData ?? d),
    stressTests: undefined,
  }
}

/**
 * Builds partial response from h3_precomputed_hex_rows (fallback)
 */
function buildPartialResponse(h3Id: string, d: any): DetailsResponse {
  return {
    id: h3Id,
    locationLabel: `H3 Cell (Res ${d.h3_res})`,
    opportunity: {
      value: d.opportunity != null ? d.opportunity * 100 : null,
      unit: "%",
      trend: d.opportunity > 0.05 ? "up" : d.opportunity < -0.02 ? "down" : "stable",
    },
    reliability: {
      value: d.reliability,
      components: {
        accuracy_term: null,
        confidence_term: null,
        stability_term: null,
        robustness_term: null,
        support_term: null,
      },
    },
    metrics: {
      n_accts: d.property_count,
      med_n_years: null,
      med_mean_ape_pct: d.sample_accuracy != null ? d.sample_accuracy * 100 : null,
      med_mean_pred_cv_pct: null,
    },
    proforma: undefined, // Not available in hex_rows
    riskScoring: {
      R: null,
      score: null,
      alert_triggered: d.hard_fail === 't' || (d.alert_pct ?? 0) > 0.25,
      tail_gap_z: null,
      medAE_z: null,
      inv_dscr_z: null,
    },
    fanChart: undefined,
    stressTests: undefined,
  }
}

/**
 * Builds fan chart data from details if fields exist
 */
function buildFanChart(d: any): FanChartData | undefined {
  // Check if fan chart fields exist
  const hasP50 = d.fan_p50_y1 != null && d.fan_p50_y2 != null && d.fan_p50_y3 != null && d.fan_p50_y4 != null && d.fan_p50_y5 != null

  if (!hasP50) return undefined

  return {
    years: [1, 2, 3, 4, 5],
    p10: [d.fan_p10_y1, d.fan_p10_y2, d.fan_p10_y3, d.fan_p10_y4, d.fan_p10_y5].map(v => v ?? 0),
    p50: [d.fan_p50_y1, d.fan_p50_y2, d.fan_p50_y3, d.fan_p50_y4, d.fan_p50_y5],
    p90: [d.fan_p90_y1, d.fan_p90_y2, d.fan_p90_y3, d.fan_p90_y4, d.fan_p90_y5].map(v => v ?? 0),
    y_med: [d.fan_p50_y1, d.fan_p50_y2, d.fan_p50_y3, d.fan_p50_y4, d.fan_p50_y5], // Use p50 as y_med
  }
}
