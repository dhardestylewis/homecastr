"use client"

import { useState } from "react"
import { useRouter } from "next/navigation"
import { Search, ArrowRight, TrendingUp, MapPin, BarChart3 } from "lucide-react"
import type { FeaturedForecastData } from "@/lib/publishing/featured-forecast"

const EXAMPLE_CHIPS = [
  { label: "What could my house be worth in 2030?", query: "What could my house be worth in 2030?" },
  { label: "Show downside vs upside", query: "Show downside vs upside" },
  { label: "How is my neighborhood expected to perform?", query: "How is my neighborhood expected to perform?" },
]

interface MiniFanChartProps {
  horizons: FeaturedForecastData["horizons"]
}

function MiniFanChart({ horizons }: MiniFanChartProps) {
  const width = 320
  const height = 140
  const padding = { top: 20, right: 15, bottom: 25, left: 15 }
  const chartWidth = width - padding.left - padding.right
  const chartHeight = height - padding.top - padding.bottom

  const years = horizons.map(h => h.year)
  const p10Values = horizons.map(h => h.p10)
  const p50Values = horizons.map(h => h.p50)
  const p90Values = horizons.map(h => h.p90)

  const minY = Math.min(...p10Values) * 0.95
  const maxY = Math.max(...p90Values) * 1.05
  const yRange = maxY - minY

  const xScale = (i: number) => padding.left + (i / (years.length - 1)) * chartWidth
  const yScale = (v: number) => padding.top + chartHeight - ((v - minY) / yRange) * chartHeight

  // Build fan area path
  const p90Points = p90Values.map((v, i) => `${i === 0 ? "M" : "L"} ${xScale(i)} ${yScale(v)}`).join(" ")
  const p10Points = [...p10Values].reverse().map((v, i) => `L ${xScale(years.length - 1 - i)} ${yScale(v)}`).join(" ")
  const fanPath = `${p90Points} ${p10Points} Z`

  // P50 line
  const p50Path = p50Values.map((v, i) => `${i === 0 ? "M" : "L"} ${xScale(i)} ${yScale(v)}`).join(" ")

  return (
    <svg viewBox={`0 0 ${width} ${height}`} className="w-full h-auto">
      {/* Fan area */}
      <path d={fanPath} fill="hsl(var(--accent))" fillOpacity={0.12} />
      
      {/* P10 dashed line */}
      <path 
        d={p10Values.map((v, i) => `${i === 0 ? "M" : "L"} ${xScale(i)} ${yScale(v)}`).join(" ")} 
        fill="none" 
        stroke="hsl(var(--accent))" 
        strokeWidth={1} 
        strokeDasharray="4 4"
        strokeOpacity={0.5}
      />
      
      {/* P90 dashed line */}
      <path 
        d={p90Values.map((v, i) => `${i === 0 ? "M" : "L"} ${xScale(i)} ${yScale(v)}`).join(" ")} 
        fill="none" 
        stroke="hsl(var(--accent))" 
        strokeWidth={1} 
        strokeDasharray="4 4"
        strokeOpacity={0.5}
      />
      
      {/* P50 line */}
      <path d={p50Path} fill="none" stroke="hsl(var(--accent))" strokeWidth={2.5} />
      
      {/* End point */}
      <circle cx={xScale(years.length - 1)} cy={yScale(p50Values[p50Values.length - 1])} r={5} fill="hsl(var(--accent))" />
      
      {/* Year labels */}
      {years.map((year, i) => (
        <text
          key={year}
          x={xScale(i)}
          y={height - 5}
          textAnchor="middle"
          className="text-[10px] fill-muted-foreground font-mono"
        >
          {year}
        </text>
      ))}
    </svg>
  )
}

interface MiniPercentileBarsProps {
  horizons: FeaturedForecastData["horizons"]
}

function MiniPercentileBars({ horizons }: MiniPercentileBarsProps) {
  const lastHorizon = horizons[horizons.length - 1]
  const p90 = lastHorizon.p90
  const p50 = lastHorizon.p50
  const p10 = lastHorizon.p10
  
  const formatK = (v: number) => `$${Math.round(v / 1000)}K`
  
  // Calculate relative widths based on values
  const maxVal = p90
  const p90Width = 90
  const p50Width = (p50 / maxVal) * 90
  const p10Width = (p10 / maxVal) * 90
  
  return (
    <div className="space-y-2.5">
      <div className="flex items-center gap-3">
        <span className="text-xs text-muted-foreground w-8 font-mono">P90</span>
        <div className="flex-1 h-2.5 bg-muted rounded-full overflow-hidden">
          <div className="h-full bg-accent/50 rounded-full" style={{ width: `${p90Width}%` }} />
        </div>
        <span className="text-xs font-mono text-muted-foreground">{formatK(p90)}</span>
      </div>
      <div className="flex items-center gap-3">
        <span className="text-xs text-foreground w-8 font-mono font-semibold">P50</span>
        <div className="flex-1 h-2.5 bg-muted rounded-full overflow-hidden">
          <div className="h-full bg-accent rounded-full" style={{ width: `${p50Width}%` }} />
        </div>
        <span className="text-xs font-mono font-semibold">{formatK(p50)}</span>
      </div>
      <div className="flex items-center gap-3">
        <span className="text-xs text-muted-foreground w-8 font-mono">P10</span>
        <div className="flex-1 h-2.5 bg-muted rounded-full overflow-hidden">
          <div className="h-full bg-accent/30 rounded-full" style={{ width: `${p10Width}%` }} />
        </div>
        <span className="text-xs font-mono text-muted-foreground">{formatK(p10)}</span>
      </div>
    </div>
  )
}

export function HeroForecastBar() {
  const [query, setQuery] = useState("")
  const router = useRouter()

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    // Route to the app with the query as a search param
    if (query.trim()) {
      router.push(`/app?q=${encodeURIComponent(query.trim())}`)
    } else {
      router.push("/app")
    }
  }

  const handleChipClick = (chipQuery: string) => {
    if (chipQuery) {
      setQuery(chipQuery)
    } else {
      // "Find my home forecast" chip - focus the input
      const input = document.getElementById("forecast-input")
      input?.focus()
    }
  }

  return (
    <div className="max-w-2xl mx-auto w-full">
      {/* Main input */}
      <form onSubmit={handleSubmit} className="relative">
        <div className="relative flex items-center">
          <Search className="absolute left-4 w-5 h-5 text-muted-foreground pointer-events-none" />
          <input
            id="forecast-input"
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Enter an address to get a forecast"
            className="w-full pl-12 pr-32 py-4 text-base bg-card border border-border rounded-xl focus:outline-none focus:ring-2 focus:ring-accent/50 focus:border-accent transition-all placeholder:text-muted-foreground"
          />
          <button
            type="submit"
            className="absolute right-2 inline-flex items-center gap-2 px-4 py-2 text-sm font-medium bg-primary text-primary-foreground rounded-lg hover:bg-primary/90 transition-colors"
          >
            Get Forecast
            <ArrowRight className="w-4 h-4" />
          </button>
        </div>
      </form>

      {/* Example chips */}
      <div className="flex flex-wrap items-center gap-2 mt-4 justify-center">
        <span className="text-xs text-muted-foreground">Try:</span>
        {EXAMPLE_CHIPS.map((chip) => (
          <button
            key={chip.label}
            onClick={() => handleChipClick(chip.query)}
            className="px-3 py-1.5 text-xs font-medium text-muted-foreground bg-muted/50 border border-border rounded-full hover:bg-muted hover:text-foreground transition-colors"
          >
            {chip.label}
          </button>
        ))}
      </div>
    </div>
  )
}

interface MockForecastCardProps {
  data: FeaturedForecastData
}

export function MockForecastCard({ data }: MockForecastCardProps) {
  const formatCurrency = (v: number) => `$${v.toLocaleString()}`
  
  // Guard against missing or empty data
  if (!data || !data.horizons || data.horizons.length === 0) {
    return null
  }
  
  const lastHorizon = data.horizons[data.horizons.length - 1]
  const horizonYear = lastHorizon.year
  const p90 = lastHorizon.p90
  const p50 = lastHorizon.p50
  const p10 = lastHorizon.p10
  
  // Calculate appreciation percentages
  const baseValue = data.currentValue
  const p50Appreciation = ((p50 / baseValue) - 1) * 100
  const p10Change = ((p10 / baseValue) - 1) * 100
  const p90Appreciation = ((p90 / baseValue) - 1) * 100
  
  // Build URL for this tract
  const tractUrl = `homecastr.com/forecasts/${data.tract.stateSlug}/${data.tract.citySlug}/${data.tract.neighborhoodSlug}`
  
  return (
    <div className="max-w-4xl mx-auto px-6 pb-16">
      <div className="rounded-xl border border-border bg-card overflow-hidden shadow-xl shadow-black/5">
        {/* Browser chrome */}
        <div className="flex items-center gap-2 px-4 py-3 border-b border-border bg-muted/30">
          <div className="flex gap-1.5">
            <div className="w-3 h-3 rounded-full bg-border" />
            <div className="w-3 h-3 rounded-full bg-border" />
            <div className="w-3 h-3 rounded-full bg-border" />
          </div>
          <div className="flex-1 flex justify-center">
            <div className="px-4 py-1 rounded-md bg-muted text-xs text-muted-foreground font-mono">
              {tractUrl}
            </div>
          </div>
        </div>

        {/* Forecast content */}
        <div className="p-6 md:p-8">
          {/* Property header */}
          <div className="flex flex-col md:flex-row md:items-start md:justify-between gap-4 mb-8">
            <div>
              <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1">
                <MapPin className="w-4 h-4" />
                {data.location.city}, {data.location.state} {data.location.zip}
              </div>
              <h2 className="text-2xl md:text-3xl font-bold tracking-tight">{data.location.neighborhood}</h2>
            </div>
            <div className="flex items-baseline gap-2 md:text-right">
              <span className="text-3xl md:text-4xl font-bold tracking-tight">{formatCurrency(data.currentValue)}</span>
              <span className="text-sm text-muted-foreground">current modeled value</span>
            </div>
          </div>

          {/* Forecast summary cards */}
          <div className="grid grid-cols-3 gap-4 mb-8">
            <div className="p-4 rounded-lg bg-muted/30 border border-border">
              <div className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Downside</div>
              <div className="text-xl font-bold text-muted-foreground">${Math.round(p10 / 1000)}K</div>
              <div className="text-xs text-muted-foreground mt-1">P10 by {horizonYear}</div>
            </div>
            <div className="p-4 rounded-lg bg-accent/10 border border-accent/20">
              <div className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Base Case</div>
              <div className="text-xl font-bold">${Math.round(p50 / 1000)}K</div>
              <div className="text-xs text-muted-foreground mt-1">P50 by {horizonYear}</div>
            </div>
            <div className="p-4 rounded-lg bg-muted/30 border border-border">
              <div className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Upside</div>
              <div className="text-xl font-bold text-muted-foreground">${Math.round(p90 / 1000)}K</div>
              <div className="text-xs text-muted-foreground mt-1">P90 by {horizonYear}</div>
            </div>
          </div>

          <div className="grid md:grid-cols-2 gap-8">
            {/* Fan chart */}
            <div>
              <div className="flex items-center gap-2 text-sm font-medium mb-4">
                <TrendingUp className="w-4 h-4 text-accent" />
                {data.horizons.length}-Year Value Forecast
              </div>
              <div className="p-4 rounded-lg bg-muted/20 border border-border">
                <MiniFanChart horizons={data.horizons} />
              </div>
            </div>

            {/* Percentile breakdown */}
            <div>
              <div className="flex items-center gap-2 text-sm font-medium mb-4">
                <BarChart3 className="w-4 h-4 text-accent" />
                {horizonYear} Scenario Breakdown
              </div>
              <div className="p-4 rounded-lg bg-muted/20 border border-border">
                <MiniPercentileBars horizons={data.horizons} />
                <div className="mt-4 pt-4 border-t border-border">
                  <p className="text-sm text-muted-foreground leading-relaxed">
                    <strong className="text-foreground">Base case:</strong> {p50Appreciation > 0 ? "+" : ""}{p50Appreciation.toFixed(0)}% appreciation driven by local demand and market conditions.
                  </p>
                </div>
              </div>
            </div>
          </div>

          {/* Plain English takeaway */}
          <div className="mt-8 p-4 rounded-lg bg-muted/30 border border-border">
            <div className="text-sm font-medium mb-2">Key Takeaway</div>
            <p className="text-sm text-muted-foreground leading-relaxed">
              This {data.location.neighborhood} forecast shows a base case of {p50Appreciation > 0 ? "+" : ""}{p50Appreciation.toFixed(0)}% appreciation through {horizonYear}.
              The P10-P90 range spans from {p10Change >= 0 ? "+" : ""}{p10Change.toFixed(0)}% to +{p90Appreciation.toFixed(0)}%, reflecting uncertainty in local employment conditions and rate sensitivity.
            </p>
          </div>
        </div>
      </div>
    </div>
  )
}

// Keep the old export for backwards compatibility but redirect to new components
export function HeroProductPreview() {
  // This is deprecated - use MockForecastCard with data prop instead
  return null
}
