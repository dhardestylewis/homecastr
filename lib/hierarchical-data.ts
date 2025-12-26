// Hierarchical mock data system that generates fine-grained data
// at H3 resolution 10 and aggregates upward to coarser resolutions

import type { FeatureProperties } from "./types"

// Base data storage - finest resolution (H3 res 10)
const baseDataCache = new Map<string, FeatureProperties>()

// Generate base data at finest resolution (simulating individual parcels/properties)
function generateBaseData(baseId: string): FeatureProperties {
  const seed = baseId.split("-").reduce((acc, val) => acc + val.charCodeAt(0), 0)
  const random = (offset: number) => (((seed + offset) * 9301 + 49297) % 233280) / 233280

  // Individual property/parcel level metrics
  return {
    id: baseId,
    O: random(1) * 14 - 2, // Opportunity: -2 to +12
    R: random(2) * 0.5 + 0.3, // Reliability: 0.3 to 0.8 (individual properties vary)
    n_accts: 1, // Single property
    med_mean_ape_pct: random(4) * 25, // Higher variance at property level
    med_mean_pred_cv_pct: random(5) * 30, // Higher prediction variance
    stability_flag: random(6) > 0.7, // More likely to have stability issues
    robustness_flag: random(7) > 0.8, // More likely to have robustness issues
  }
}

// Get or generate base data for a specific location
function getBaseData(row: number, col: number): FeatureProperties {
  const baseId = `base-${row}-${col}`

  if (!baseDataCache.has(baseId)) {
    baseDataCache.set(baseId, generateBaseData(baseId))
  }

  return baseDataCache.get(baseId)!
}

// Calculate which base cells (res 10) fall within a coarser cell
function getBaseCellsForCoarserCell(
  row: number,
  col: number,
  targetResolution: number,
): Array<{ row: number; col: number }> {
  // Each coarser resolution aggregates from finer resolutions
  // Resolution 10 → 9: aggregate 7 cells (hexagon + 6 neighbors)
  // Resolution 9 → 8: aggregate 7 cells
  // etc.

  const aggregationFactor = Math.pow(7, 10 - targetResolution) // Approximately
  const baseCells: Array<{ row: number; col: number }> = []

  // For simplicity, map coarser cells to a grid of base cells
  const baseRow = row * Math.ceil(Math.sqrt(aggregationFactor))
  const baseCol = col * Math.ceil(Math.sqrt(aggregationFactor))
  const gridSize = Math.ceil(Math.sqrt(aggregationFactor))

  for (let r = 0; r < gridSize; r++) {
    for (let c = 0; c < gridSize; c++) {
      baseCells.push({
        row: baseRow + r,
        col: baseCol + c,
      })
    }
  }

  return baseCells
}

// DEPRECATED: Mock hierarchical data system
// This was used for development but is now replaced with real Supabase data

export function getDataForResolution(row: number, col: number, h3Resolution: number): FeatureProperties {
  console.warn("[v0] DEPRECATED: hierarchical-data.ts should not be called - use real Supabase data instead")

  // Return minimal mock data as fallback
  return {
    id: `deprecated-hex-r${h3Resolution}-${row}-${col}`,
    O: 0,
    R: 0,
    n_accts: 0,
    med_mean_ape_pct: 0,
    med_mean_pred_cv_pct: 0,
    stability_flag: false,
    robustness_flag: false,
  }
}

export function clearDataCache() {
  console.warn("[v0] DEPRECATED: clearDataCache called on deprecated mock data system")
}
