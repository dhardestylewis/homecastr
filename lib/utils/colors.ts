// Color utilities for opportunity and reliability encoding

// Opportunity color scale (diverging: red for negative, green for positive)
export function getOpportunityColor(value: number): string {
  if (value < 0) {
    // Negative: red scale
    const intensity = Math.min(Math.abs(value) / 10, 1)
    return `oklch(${0.6 - intensity * 0.15} ${0.15 + intensity * 0.1} 25)`
  } else if (value < 3) {
    // Low positive: yellow
    return `oklch(0.75 0.15 85)`
  } else if (value < 6) {
    // Mid positive: yellow-green
    return `oklch(0.75 0.15 120)`
  } else if (value < 10) {
    // High positive: green
    return `oklch(0.70 0.18 145)`
  } else {
    // Very high: teal
    return `oklch(0.65 0.15 180)`
  }
}

// Reliability opacity bins
export function getReliabilityOpacity(value: number): number {
  if (value < 0.2) return 0.25
  if (value < 0.4) return 0.45
  if (value < 0.6) return 0.65
  if (value < 0.8) return 0.85
  return 1.0
}

// Get reliability bin label
export function getReliabilityBinLabel(value: number): string {
  if (value < 0.2) return "Very Low"
  if (value < 0.4) return "Low"
  if (value < 0.6) return "Moderate"
  if (value < 0.8) return "High"
  return "Very High"
}

// Format opportunity value
export function formatOpportunity(value: number, unit = "% CAGR"): string {
  const sign = value >= 0 ? "+" : ""
  return `${sign}${value.toFixed(1)}${unit}`
}

// Format reliability as percentage
export function formatReliability(value: number): string {
  return `${(value * 100).toFixed(0)}%`
}
