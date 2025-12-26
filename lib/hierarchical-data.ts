// Hierarchical mock data system that generates fine-grained data
// at H3 resolution 10 and aggregates upward to coarser resolutions

import type { FeatureProperties } from "./types"
import { getHexDataForResolution } from "./supabase/hex-data"

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

// Track whether we're using precomputed data
let usingPrecomputedData = false

// Get data from Supabase or generate mock data
export async function getPrecomputedOrMockData(row: number, col: number, h3Resolution: number) {
  // Try to get precomputed data from Supabase
  const payload = await getHexDataForResolution(h3Resolution)

  if (payload && payload.hexagons) {
    usingPrecomputedData = true
    // Return the converted data (will be filtered by row/col in map-view)
    return { payload, isPrecomputed: true }
  }

  // Fallback to mock data
  usingPrecomputedData = false
  return { isPrecomputed: false }
}

export function isUsingPrecomputedData() {
  return usingPrecomputedData
}

// Aggregate base data up to a target resolution
export function getDataForResolution(row: number, col: number, h3Resolution: number): FeatureProperties {
  const id = `hex-r${h3Resolution}-${row}-${col}`

  if (h3Resolution === 10) {
    // Finest resolution - return base data directly
    const baseData = getBaseData(row, col)
    return { ...baseData, id }
  }

  // Coarser resolution - aggregate from base cells
  const baseCells = getBaseCellsForCoarserCell(row, col, h3Resolution)
  const baseDataArray = baseCells.map(({ row: r, col: c }) => getBaseData(r, c))

  // Aggregate metrics
  const n_accts = baseDataArray.reduce((sum, d) => sum + d.n_accts, 0)
  const weightedO = baseDataArray.reduce((sum, d) => sum + d.O * d.n_accts, 0) / n_accts
  const weightedR = baseDataArray.reduce((sum, d) => sum + d.R * d.n_accts, 0) / n_accts

  // For error metrics, use median of base cells
  const sortedApe = baseDataArray.map((d) => d.med_mean_ape_pct).sort((a, b) => a - b)
  const sortedCv = baseDataArray.map((d) => d.med_mean_pred_cv_pct).sort((a, b) => a - b)
  const medianApe = sortedApe[Math.floor(sortedApe.length / 2)]
  const medianCv = sortedCv[Math.floor(sortedCv.length / 2)]

  // Flags: only true if majority of base cells have the flag
  const stabilityCount = baseDataArray.filter((d) => d.stability_flag).length
  const robustnessCount = baseDataArray.filter((d) => d.robustness_flag).length

  return {
    id,
    O: weightedO, // Aggregated Opportunity
    R: weightedR, // Aggregated Reliability (typically higher when aggregated)
    n_accts, // Sum of accounts
    med_mean_ape_pct: medianApe, // Median APE (typically lower when aggregated)
    med_mean_pred_cv_pct: medianCv, // Median CV (typically lower when aggregated)
    stability_flag: stabilityCount > baseCells.length / 2,
    robustness_flag: robustnessCount > baseCells.length / 2,
  }
}

// Clear cache if needed (e.g., when user changes data source)
export function clearDataCache() {
  baseDataCache.clear()
}
