"use client"

import Link from "next/link"
import { ArrowRight } from "lucide-react"
import type { ForecastHorizon } from "@/lib/publishing/forecast-data"

interface ForecastPreviewProps {
  neighborhoodName: string
  city: string
  stateAbbr: string
  horizons: ForecastHorizon[]
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
      
      {/* Mini fan chart */}
      <div className="px-6 py-4 border-t border-border">
        <MiniFanChart horizons={horizons} baselineP50={baselineP50} />
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
 * Compact fan chart showing P10/P50/P90 bands over time.
 */
function MiniFanChart({ horizons, baselineP50 }: { horizons: ForecastHorizon[]; baselineP50: number }) {
  const width = 400
  const height = 120
  const padding = { top: 20, right: 40, bottom: 25, left: 50 }
  
  const innerWidth = width - padding.left - padding.right
  const innerHeight = height - padding.top - padding.bottom
  
  // Get min/max for y scale
  const allValues = horizons.flatMap(h => [h.p10, h.p50, h.p90])
  const minVal = Math.min(...allValues) * 0.95
  const maxVal = Math.max(...allValues) * 1.05
  
  const xScale = (i: number) => padding.left + (i / (horizons.length - 1)) * innerWidth
  const yScale = (v: number) => padding.top + innerHeight - ((v - minVal) / (maxVal - minVal)) * innerHeight
  
  // Build paths
  const p10Points = horizons.map((h, i) => `${xScale(i)},${yScale(h.p10)}`).join(" ")
  const p50Points = horizons.map((h, i) => `${xScale(i)},${yScale(h.p50)}`).join(" ")
  const p90Points = horizons.map((h, i) => `${xScale(i)},${yScale(h.p90)}`).join(" ")
  
  // Band path (P10 forward, P90 backward)
  const bandPath = `M ${horizons.map((h, i) => `${xScale(i)},${yScale(h.p10)}`).join(" L ")} L ${horizons.map((h, i) => `${xScale(horizons.length - 1 - i)},${yScale(horizons[horizons.length - 1 - i].p90)}`).join(" L ")} Z`
  
  const years = horizons.map(h => h.forecastYear)
  
  const fmtVal = (v: number) => {
    if (v >= 1_000_000) return `$${(v / 1_000_000).toFixed(1)}M`
    if (v >= 1_000) return `$${Math.round(v / 1_000)}K`
    return `$${v}`
  }
  
  return (
    <svg viewBox={`0 0 ${width} ${height}`} className="w-full h-auto">
      {/* Band fill */}
      <path d={bandPath} fill="currentColor" className="text-primary/10" />
      
      {/* P10 line */}
      <polyline
        points={p10Points}
        fill="none"
        stroke="currentColor"
        strokeWidth="1"
        strokeDasharray="2,2"
        className="text-muted-foreground/50"
      />
      
      {/* P90 line */}
      <polyline
        points={p90Points}
        fill="none"
        stroke="currentColor"
        strokeWidth="1"
        strokeDasharray="2,2"
        className="text-muted-foreground/50"
      />
      
      {/* P50 line */}
      <polyline
        points={p50Points}
        fill="none"
        stroke="currentColor"
        strokeWidth="2"
        className="text-primary"
      />
      
      {/* P50 dots */}
      {horizons.map((h, i) => (
        <circle
          key={`dot-${i}`}
          cx={xScale(i)}
          cy={yScale(h.p50)}
          r="3"
          fill="currentColor"
          className="text-primary"
        />
      ))}
      
      {/* Year labels */}
      {years.map((year, i) => (
        <text
          key={`year-${i}`}
          x={xScale(i)}
          y={height - 5}
          textAnchor="middle"
          className="text-[10px] fill-muted-foreground font-mono"
        >
          {year}
        </text>
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
