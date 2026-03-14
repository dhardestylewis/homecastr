"use client"

import type { ForecastHorizon } from "@/lib/publishing/forecast-data"

interface Props {
  horizons: ForecastHorizon[]
  baselineP50: number
  neighborhoodName: string
  city: string
  stateAbbr: string
}

/**
 * Single most important insight at the top of the page.
 * Answers: "What is the base case, how bad could downside be, 
 * how large is upside, and why should I trust the range?"
 */
export function KeyTakeaway({ horizons, baselineP50, neighborhoodName, city, stateAbbr }: Props) {
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
  else if (spreadPct > 150) rangeWidth = "very wide"

  // Build the takeaway sentence
  let takeaway = ""
  if (baseCaseChange > 5) {
    takeaway = `The model expects ${neighborhoodName} to appreciate ${fmtPct(baseCaseChange)} by 2030 (base case), with upside potential to ${fmtPct(upsideChange)} and downside risk to ${fmtPct(downsideChange)}.`
  } else if (baseCaseChange < -5) {
    takeaway = `The model expects ${neighborhoodName} to decline ${fmtPct(Math.abs(baseCaseChange))} by 2030 (base case), with downside risk to ${fmtPct(downsideChange)} but potential recovery to ${fmtPct(upsideChange)}.`
  } else {
    takeaway = `The model expects ${neighborhoodName} to remain roughly flat through 2030 (${fmtPct(baseCaseChange)} base case), with a range from ${fmtPct(downsideChange)} downside to ${fmtPct(upsideChange)} upside.`
  }

  return (
    <section className="relative">
      {/* Hero summary grid */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
        {/* Current value */}
        <div className="p-5 rounded-xl bg-card border border-border">
          <p className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Current Value</p>
          <p className="text-2xl font-bold tracking-tight">{fmtVal(baselineP50)}</p>
          <p className="text-xs text-muted-foreground mt-1">Modeled estimate</p>
        </div>
        
        {/* Downside */}
        <div className="p-5 rounded-xl bg-muted/30 border border-border">
          <p className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Downside (P10)</p>
          <p className="text-2xl font-bold tracking-tight text-muted-foreground">{fmtVal(h5.p10)}</p>
          <p className="text-xs text-muted-foreground mt-1">{fmtPct(downsideChange)} by 2030</p>
        </div>
        
        {/* Base case */}
        <div className="p-5 rounded-xl bg-primary/5 border border-primary/20">
          <p className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Base Case (P50)</p>
          <p className="text-2xl font-bold tracking-tight">{fmtVal(h5.p50)}</p>
          <p className="text-xs text-primary mt-1">{fmtPct(baseCaseChange)} by 2030</p>
        </div>
        
        {/* Upside */}
        <div className="p-5 rounded-xl bg-muted/30 border border-border">
          <p className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Upside (P90)</p>
          <p className="text-2xl font-bold tracking-tight text-muted-foreground">{fmtVal(h5.p90)}</p>
          <p className="text-xs text-muted-foreground mt-1">{fmtPct(upsideChange)} by 2030</p>
        </div>
      </div>
      
      {/* Key takeaway paragraph */}
      <div className="p-5 rounded-xl bg-card border border-border">
        <div className="flex items-start gap-3">
          <div className="w-1 h-full min-h-[3rem] bg-primary/40 rounded-full shrink-0" />
          <div className="space-y-2">
            <p className="text-sm font-medium text-foreground">Key Takeaway</p>
            <p className="text-sm text-muted-foreground leading-relaxed">
              {takeaway} The forecast range is {rangeWidth} for this market.
            </p>
          </div>
        </div>
      </div>
    </section>
  )
}
