"use client"

import Link from "next/link"
import { ArrowRight } from "lucide-react"
import type { ForecastHorizon, HistoryPoint } from "@/lib/publishing/forecast-data"

interface ForecastPreviewProps {
  neighborhoodName: string
  city: string
  stateAbbr: string
  horizons: ForecastHorizon[]
  history: HistoryPoint[]
  originYear: number
  baselineP50: number
  forecastUrl: string
}

/**
 * Compact forecast preview for the homepage hero.
 * Shows current value, P10/P50/P90 scenarios, mini fan chart, and key takeaway.
 */
export function ForecastPreview({
  neighborhoodName,
  city,
  stateAbbr,
  horizons,
  history,
  originYear,
  baselineP50,
  forecastUrl,
}: ForecastPreviewProps) {
  const h5 = horizons.find(h => h.horizon_m === 60)
  if (!h5) return null

  const fmtVal = (v: number) => {
    if (v >= 1_000_000) return `$${(v / 1_000_000).toFixed(2)}M`
    if (v >= 1_000) return `$${(v / 1_000).toFixed(0)}K`
    return `$${v.toFixed(0)}`
  }

  const fmtPct = (v: number) => `${v >= 0 ? "+" : ""}${v.toFixed(0)}%`
  
  // Calculate key metrics
  const downsideChange = ((h5.p10 / baselineP50) - 1) * 100
  const baseCaseChange = h5.appreciation
  const upsideChange = ((h5.p90 / baselineP50) - 1) * 100
  const spreadPct = h5.p50 > 0 ? ((h5.spread / h5.p50) * 100) : 0
  
  // Determine range width descriptor
  let rangeWidth = "typical"
  if (spreadPct < 60) rangeWidth = "narrow"
  else if (spreadPct > 120) rangeWidth = "wide"

  return (
    <div className="rounded-xl border border-border bg-card overflow-hidden shadow-xl shadow-black/5">
      {/* Header */}
      <div className="px-6 py-4 border-b border-border bg-muted/30">
        <div className="flex items-center justify-between">
          <div>
            <h3 className="font-semibold text-foreground">{neighborhoodName}</h3>
            <p className="text-sm text-muted-foreground">{city}, {stateAbbr}</p>
          </div>
          <span className="text-xs text-muted-foreground font-mono">2025–2030 outlook</span>
        </div>
      </div>
      
      {/* Summary cards - 4 columns */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-px bg-border">
        {/* Current value */}
        <div className="p-4 bg-card">
          <p className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Current</p>
          <p className="text-xl font-bold tracking-tight">{fmtVal(baselineP50)}</p>
        </div>
        
        {/* Downside P10 */}
        <div className="p-4 bg-card">
          <p className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Downside</p>
          <p className="text-xl font-bold tracking-tight text-muted-foreground">{fmtVal(h5.p10)}</p>
          <p className="text-xs text-muted-foreground">{fmtPct(downsideChange)}</p>
        </div>
        
        {/* Base case P50 */}
        <div className="p-4 bg-primary/5">
          <p className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Base Case</p>
          <p className="text-xl font-bold tracking-tight">{fmtVal(h5.p50)}</p>
          <p className="text-xs text-primary">{fmtPct(baseCaseChange)}</p>
        </div>
        
        {/* Upside P90 */}
        <div className="p-4 bg-card">
          <p className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Upside</p>
          <p className="text-xl font-bold tracking-tight text-muted-foreground">{fmtVal(h5.p90)}</p>
          <p className="text-xs text-muted-foreground">{fmtPct(upsideChange)}</p>
        </div>
      </div>
      
      {/* Mini fan chart with history */}
      <div className="px-6 py-4 border-t border-border">
        <MiniFanChart horizons={horizons} history={history} originYear={originYear} />
      </div>
      
      {/* Key takeaway */}
      <div className="px-6 py-4 border-t border-border bg-primary/5">
        <p className="text-sm text-foreground leading-relaxed">
          <span className="font-medium">Base case: {fmtPct(baseCaseChange)} by 2030</span>, with a forecast range from {fmtPct(downsideChange)} to {fmtPct(upsideChange)}. Uncertainty is {rangeWidth} for {city} markets.
        </p>
      </div>
      
      {/* CTA */}
      <div className="px-6 py-4 border-t border-border bg-muted/30">
        <Link
          href={forecastUrl}
          className="inline-flex items-center gap-2 text-sm font-medium text-primary hover:underline underline-offset-4"
        >
          View full forecast for {neighborhoodName}
          <ArrowRight className="w-3.5 h-3.5" />
        </Link>
      </div>
    </div>
  )
}

/**
 * Compact fan chart showing historical data + P10/P50/P90 forecast bands.
 * Mirrors the structure of HistoryForecastChart but in a compact form.
 */
function MiniFanChart({ 
  horizons, 
  history, 
  originYear 
}: { 
  horizons: ForecastHorizon[]
  history: HistoryPoint[]
  originYear: number 
}) {
  const width = 400
  const height = 120
  const padding = { top: 20, right: 40, bottom: 25, left: 50 }
  
  const innerWidth = width - padding.left - padding.right
  const innerHeight = height - padding.top - padding.bottom
  
  // Build combined data like HistoryForecastChart does
  const bridgeYear = originYear + 1
  
  // Get last 5 years of history for a cleaner chart (guard against undefined)
  const safeHistory = history || []
  const recentHistory = safeHistory.filter(h => h.year >= originYear - 4).slice(-5)
  
  // Combine history + forecast into one timeline
  const allYears: number[] = []
  const historyMap = new Map<number, number>()
  const forecastMap = new Map<number, { p10: number; p50: number; p90: number }>()
  
  // Add historical data
  for (const h of recentHistory) {
    allYears.push(h.year)
    historyMap.set(h.year, h.value)
  }
  
  // Add bridge point (where history meets forecast)
  const h12 = horizons.find(h => h.horizon_m === 12)
  if (h12 && !allYears.includes(bridgeYear)) {
    allYears.push(bridgeYear)
    historyMap.set(bridgeYear, h12.p50)
  }
  
  // Add forecast years
  for (const h of horizons) {
    if (h.forecastYear <= bridgeYear) continue
    if (!allYears.includes(h.forecastYear)) {
      allYears.push(h.forecastYear)
    }
    forecastMap.set(h.forecastYear, { p10: h.p10, p50: h.p50, p90: h.p90 })
  }
  
  allYears.sort((a, b) => a - b)
  
  // Get min/max for y scale
  const allValues = [
    ...Array.from(historyMap.values()),
    ...horizons.flatMap(h => [h.p10, h.p50, h.p90])
  ]
  const minVal = Math.min(...allValues) * 0.95
  const maxVal = Math.max(...allValues) * 1.05
  
  const xScale = (yearIdx: number) => padding.left + (yearIdx / (allYears.length - 1)) * innerWidth
  const yScale = (v: number) => padding.top + innerHeight - ((v - minVal) / (maxVal - minVal)) * innerHeight
  
  // Build historical line points
  const historyPoints: string[] = []
  allYears.forEach((year, i) => {
    const val = historyMap.get(year)
    if (val !== undefined) {
      historyPoints.push(`${xScale(i)},${yScale(val)}`)
    }
  })
  
  // Build forecast band and lines
  const forecastStartIdx = allYears.findIndex(y => y === bridgeYear)
  const p10Points: string[] = []
  const p50Points: string[] = []
  const p90Points: string[] = []
  
  // Include bridge point in forecast
  if (h12) {
    const bridgeIdx = allYears.indexOf(bridgeYear)
    p10Points.push(`${xScale(bridgeIdx)},${yScale(h12.p10)}`)
    p50Points.push(`${xScale(bridgeIdx)},${yScale(h12.p50)}`)
    p90Points.push(`${xScale(bridgeIdx)},${yScale(h12.p90)}`)
  }
  
  allYears.forEach((year, i) => {
    const f = forecastMap.get(year)
    if (f) {
      p10Points.push(`${xScale(i)},${yScale(f.p10)}`)
      p50Points.push(`${xScale(i)},${yScale(f.p50)}`)
      p90Points.push(`${xScale(i)},${yScale(f.p90)}`)
    }
  })
  
  // Band path (P10 forward, P90 backward)
  const bandPath = p10Points.length > 1 
    ? `M ${p10Points.join(" L ")} L ${[...p90Points].reverse().join(" L ")} Z`
    : ""
  
  const fmtVal = (v: number) => {
    if (v >= 1_000_000) return `$${(v / 1_000_000).toFixed(1)}M`
    if (v >= 1_000) return `$${Math.round(v / 1_000)}K`
    return `$${v}`
  }
  
  // Show year labels for first, bridge, and last years only to reduce clutter
  const labelYears = [allYears[0], bridgeYear, allYears[allYears.length - 1]]
  
  return (
    <svg viewBox={`0 0 ${width} ${height}`} className="w-full h-auto">
      {/* Forecast band fill */}
      {bandPath && <path d={bandPath} fill="currentColor" className="text-primary/10" />}
      
      {/* P10 line (dashed) */}
      {p10Points.length > 1 && (
        <polyline
          points={p10Points.join(" ")}
          fill="none"
          stroke="currentColor"
          strokeWidth="1"
          strokeDasharray="2,2"
          className="text-muted-foreground/50"
        />
      )}
      
      {/* P90 line (dashed) */}
      {p90Points.length > 1 && (
        <polyline
          points={p90Points.join(" ")}
          fill="none"
          stroke="currentColor"
          strokeWidth="1"
          strokeDasharray="2,2"
          className="text-muted-foreground/50"
        />
      )}
      
      {/* Historical line (solid, thicker) */}
      {historyPoints.length > 1 && (
        <polyline
          points={historyPoints.join(" ")}
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          className="text-muted-foreground"
        />
      )}
      
      {/* Forecast P50 line (solid, primary color) */}
      {p50Points.length > 1 && (
        <polyline
          points={p50Points.join(" ")}
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          className="text-primary"
        />
      )}
      
      {/* Bridge point indicator */}
      {forecastStartIdx >= 0 && (
        <line
          x1={xScale(forecastStartIdx)}
          y1={padding.top}
          x2={xScale(forecastStartIdx)}
          y2={height - padding.bottom}
          stroke="currentColor"
          strokeWidth="1"
          strokeDasharray="4,4"
          className="text-border"
        />
      )}
      
      {/* Year labels */}
      {allYears.map((year, i) => (
        labelYears.includes(year) && (
          <text
            key={`year-${i}`}
            x={xScale(i)}
            y={height - 5}
            textAnchor="middle"
            className="text-[10px] fill-muted-foreground font-mono"
          >
            {year}
          </text>
        )
      ))}
      
      {/* Y-axis labels - min and max */}
      <text
        x={padding.left - 5}
        y={yScale(maxVal)}
        textAnchor="end"
        dominantBaseline="middle"
        className="text-[10px] fill-muted-foreground font-mono"
      >
        {fmtVal(maxVal)}
      </text>
      <text
        x={padding.left - 5}
        y={yScale(minVal)}
        textAnchor="end"
        dominantBaseline="middle"
        className="text-[10px] fill-muted-foreground font-mono"
      >
        {fmtVal(minVal)}
      </text>
    </svg>
  )
}
