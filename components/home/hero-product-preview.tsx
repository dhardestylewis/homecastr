"use client"

import Link from "next/link"
import { ArrowUpRight } from "lucide-react"

// Demo fan chart data for the hero
const DEMO_YEARS = [2026, 2027, 2028, 2029, 2030]
const DEMO_P10 = [285000, 278000, 274000, 271000, 268000]
const DEMO_P50 = [295000, 305000, 318000, 330000, 345000]
const DEMO_P90 = [310000, 338000, 365000, 395000, 425000]

function MiniPercentileBars() {
  return (
    <div className="space-y-2">
      <div className="flex items-center gap-3">
        <span className="text-xs text-muted-foreground w-8 font-mono">P90</span>
        <div className="flex-1 h-2 bg-muted rounded-full overflow-hidden">
          <div className="h-full bg-accent/60 rounded-full" style={{ width: "85%" }} />
        </div>
        <span className="text-xs font-mono text-muted-foreground">$425K</span>
      </div>
      <div className="flex items-center gap-3">
        <span className="text-xs text-foreground w-8 font-mono font-semibold">P50</span>
        <div className="flex-1 h-2 bg-muted rounded-full overflow-hidden">
          <div className="h-full bg-accent rounded-full" style={{ width: "65%" }} />
        </div>
        <span className="text-xs font-mono font-semibold">$345K</span>
      </div>
      <div className="flex items-center gap-3">
        <span className="text-xs text-muted-foreground w-8 font-mono">P10</span>
        <div className="flex-1 h-2 bg-muted rounded-full overflow-hidden">
          <div className="h-full bg-accent/40 rounded-full" style={{ width: "45%" }} />
        </div>
        <span className="text-xs font-mono text-muted-foreground">$268K</span>
      </div>
    </div>
  )
}

function MiniFanChart() {
  const width = 280
  const height = 120
  const padding = { top: 15, right: 10, bottom: 20, left: 10 }
  const chartWidth = width - padding.left - padding.right
  const chartHeight = height - padding.top - padding.bottom

  const minY = Math.min(...DEMO_P10)
  const maxY = Math.max(...DEMO_P90)
  const yRange = maxY - minY

  const xScale = (i: number) => padding.left + (i / (DEMO_YEARS.length - 1)) * chartWidth
  const yScale = (v: number) => padding.top + chartHeight - ((v - minY) / yRange) * chartHeight

  // Build fan area path
  const p90Points = DEMO_P90.map((v, i) => `${i === 0 ? "M" : "L"} ${xScale(i)} ${yScale(v)}`).join(" ")
  const p10Points = [...DEMO_P10].reverse().map((v, i) => `L ${xScale(DEMO_YEARS.length - 1 - i)} ${yScale(v)}`).join(" ")
  const fanPath = `${p90Points} ${p10Points} Z`

  // P50 line
  const p50Path = DEMO_P50.map((v, i) => `${i === 0 ? "M" : "L"} ${xScale(i)} ${yScale(v)}`).join(" ")

  return (
    <svg viewBox={`0 0 ${width} ${height}`} className="w-full h-auto">
      {/* Fan area */}
      <path d={fanPath} fill="hsl(var(--accent))" fillOpacity={0.15} />
      
      {/* P50 line */}
      <path d={p50Path} fill="none" stroke="hsl(var(--accent))" strokeWidth={2} />
      
      {/* End points */}
      <circle cx={xScale(DEMO_YEARS.length - 1)} cy={yScale(DEMO_P50[DEMO_P50.length - 1])} r={4} fill="hsl(var(--accent))" />
      
      {/* Year labels */}
      {DEMO_YEARS.map((year, i) => (
        <text
          key={year}
          x={xScale(i)}
          y={height - 4}
          textAnchor="middle"
          className="text-[9px] fill-muted-foreground font-mono"
        >
          {year}
        </text>
      ))}
    </svg>
  )
}

export function HeroProductPreview() {
  return (
    <div className="max-w-6xl mx-auto px-6 pb-16">
      <div className="relative rounded-xl border border-border bg-card overflow-hidden shadow-2xl shadow-black/5">
        {/* Browser chrome */}
        <div className="flex items-center gap-2 px-4 py-3 border-b border-border bg-muted/30">
          <div className="flex gap-1.5">
            <div className="w-3 h-3 rounded-full bg-border" />
            <div className="w-3 h-3 rounded-full bg-border" />
            <div className="w-3 h-3 rounded-full bg-border" />
          </div>
          <div className="flex-1 flex justify-center">
            <div className="px-4 py-1 rounded-md bg-muted text-xs text-muted-foreground font-mono">
              homecastr.com/app
            </div>
          </div>
        </div>

        {/* Map + Panel Layout */}
        <div className="grid md:grid-cols-[1fr_320px] min-h-[400px] md:min-h-[480px]">
          {/* Map area */}
          <div className="relative bg-muted/20 min-h-[280px]">
            <Link href="/app" className="absolute inset-0 z-10">
              <span className="sr-only">Open forecast map</span>
            </Link>
            <iframe
              src="/app?yr=2028&embedded=true"
              className="w-full h-full border-0 pointer-events-none"
              title="Homecastr Interactive Map Preview"
            />
            {/* Overlay gradient for text legibility */}
            <div className="absolute inset-0 bg-gradient-to-t from-background/60 via-transparent to-transparent pointer-events-none md:hidden" />
            
            {/* Live indicator */}
            <div className="absolute top-4 left-4 z-20 flex items-center gap-2 px-3 py-1.5 rounded-full bg-background/90 backdrop-blur-sm border border-border text-xs font-medium">
              <div className="w-2 h-2 rounded-full bg-accent animate-pulse" />
              Live Forecasts
            </div>
          </div>

          {/* Detail panel */}
          <div className="border-l border-border bg-card p-5 flex flex-col gap-5">
            {/* Property header */}
            <div>
              <div className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Selected Property</div>
              <div className="font-semibold text-lg leading-tight">123 Main Street</div>
              <div className="text-sm text-muted-foreground">Houston, TX 77002</div>
            </div>

            {/* Current value */}
            <div className="flex items-baseline gap-2">
              <span className="text-3xl font-bold tracking-tight">$295,000</span>
              <span className="text-sm text-muted-foreground">current</span>
            </div>

            {/* Fan chart */}
            <div>
              <div className="text-xs text-muted-foreground uppercase tracking-wider mb-2">5-Year Forecast</div>
              <MiniFanChart />
            </div>

            {/* Percentile bands */}
            <div>
              <div className="text-xs text-muted-foreground uppercase tracking-wider mb-3">2030 Scenarios</div>
              <MiniPercentileBars />
            </div>

            {/* CTA */}
            <Link
              href="/app"
              className="mt-auto inline-flex items-center justify-center gap-2 px-4 py-2.5 text-sm font-medium bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
            >
              View Full Forecast
              <ArrowUpRight className="w-4 h-4" />
            </Link>
          </div>
        </div>
      </div>
    </div>
  )
}
