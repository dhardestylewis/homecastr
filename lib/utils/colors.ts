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


// Continuous Sequential Color Scale for Property Values ($)
// Magma-like: Deep Purple -> Red -> Orange -> Yellow
export function getValueColor(value: number): string {
  // Domain: 0 to 1.5M (Soft cap)
  const MIN_VAL = 100_000
  const MAX_VAL = 1_500_000

  // Normalize t [0, 1]
  const t = Math.max(0, Math.min(1, (value - MIN_VAL) / (MAX_VAL - MIN_VAL)))

  // Keyframes [Lightness, Chroma, Hue]
  const START: [number, number, number] = [0.25, 0.10, 280] // Deep Purple
  const MID: [number, number, number] = [0.60, 0.20, 30]  // Red-Orange
  const END: [number, number, number] = [0.95, 0.15, 80]  // Yellow

  if (t < 0.5) {
    // Interpolate Start -> Mid
    const localT = t * 2
    return interpolateOkLch(START, MID, localT)
  } else {
    // Interpolate Mid -> End
    const localT = (t - 0.5) * 2
    return interpolateOkLch(MID, END, localT)
  }
}

// Format opportunity value
export function formatOpportunity(value: number, unit = "%"): string {
  const sign = value >= 0 ? "+" : ""
  return `${sign}${value.toFixed(1)}${unit}`
}

// Format currency value
export function formatCurrency(value: number): string {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 0,
    notation: "compact",
    compactDisplay: "short"
  }).format(value)
}

// Format reliability as percentage
export function formatReliability(value: number): string {
  return `${(value * 100).toFixed(0)}%`
}

