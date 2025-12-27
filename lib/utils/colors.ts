// Color utilities for opportunity and reliability encoding

// Opportunity color scale (diverging: red for negative, green for positive)
// Interpolate between two OKLCH colors
// range: 0 to 1
function interpolateOkLch(start: [number, number, number], end: [number, number, number], t: number): string {
  const l = start[0] + (end[0] - start[0]) * t
  const c = start[1] + (end[1] - start[1]) * t
  // Handle hue interpolation correctly (shortest path)
  let hStart = start[2]
  let hEnd = end[2]

  if (Math.abs(hEnd - hStart) > 180) {
    if (hEnd > hStart) hStart += 360
    else hEnd += 360
  }

  const h = hStart + (hEnd - hStart) * t
  return `oklch(${l} ${c} ${h})`
}

// Continuous Perceptually Uniform Bidirectional Color Scale
// 2 endpoints + neutral center
// Negative (< 0): Deep Purple to Neutral
// Positive (> 0): Neutral to Deep Blue/Cyan
export function getOpportunityColor(value: number): string {
  // Define keyframes in [Lightness, Chroma, Hue]
  const NEGATIVE_EXTREME: [number, number, number] = [0.55, 0.22, 300] // Deep Purple (Low Value)
  const NEUTRAL: [number, number, number] = [0.97, 0.00, 90]  // White/Grey (Neutral)
  const POSITIVE_EXTREME: [number, number, number] = [0.55, 0.22, 240] // Deep Blue (High Value)

  // Normalize value to t [0, 1] for each side
  // Cap at reasonable extremes to avoid washing out too early or late
  // Assuming relevant range is approx -10% to +20% based on data seen

  if (value < 0) {
    // Negative visual range: 0 to -50%
    // Start from Neutral with SAME hue as Negative Extreme to avoid rainbow effect
    const NEUTRAL_NEG: [number, number, number] = [0.97, 0, 300]
    const t = Math.min(Math.abs(value) / 50, 1)
    return interpolateOkLch(NEUTRAL_NEG, NEGATIVE_EXTREME, t)
  } else {
    // Positive visual range: 0 to +50%
    // Start from Neutral with SAME hue as Positive Extreme to avoid rainbow effect
    const NEUTRAL_POS: [number, number, number] = [0.97, 0, 240]
    const t = Math.min(value / 50, 1)
    return interpolateOkLch(NEUTRAL_POS, POSITIVE_EXTREME, t)
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
