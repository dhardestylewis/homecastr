"use client"

import { useState } from "react"
import { useRouter } from "next/navigation"
import { Search, ArrowRight, TrendingUp, MapPin, BarChart3 } from "lucide-react"

// Demo data for the mock forecast card
const DEMO_YEARS = [2026, 2027, 2028, 2029, 2030]
const DEMO_P10 = [285000, 278000, 274000, 271000, 268000]
const DEMO_P50 = [295000, 305000, 318000, 330000, 345000]
const DEMO_P90 = [310000, 338000, 365000, 395000, 425000]

const EXAMPLE_CHIPS = [
  { label: "What could my house be worth in 2030?", query: "What could my house be worth in 2030?" },
  { label: "Show downside vs upside", query: "Show downside vs upside" },
  { label: "How is my neighborhood expected to perform?", query: "How is my neighborhood expected to perform?" },
]

function MiniFanChart() {
  const width = 320
  const height = 140
  const padding = { top: 20, right: 15, bottom: 25, left: 15 }
  const chartWidth = width - padding.left - padding.right
  const chartHeight = height - padding.top - padding.bottom

  const minY = Math.min(...DEMO_P10) * 0.95
  const maxY = Math.max(...DEMO_P90) * 1.05
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
      <path d={fanPath} fill="hsl(var(--accent))" fillOpacity={0.12} />
      
      {/* P10 dashed line */}
      <path 
        d={DEMO_P10.map((v, i) => `${i === 0 ? "M" : "L"} ${xScale(i)} ${yScale(v)}`).join(" ")} 
        fill="none" 
        stroke="hsl(var(--accent))" 
        strokeWidth={1} 
        strokeDasharray="4 4"
        strokeOpacity={0.5}
      />
      
      {/* P90 dashed line */}
      <path 
        d={DEMO_P90.map((v, i) => `${i === 0 ? "M" : "L"} ${xScale(i)} ${yScale(v)}`).join(" ")} 
        fill="none" 
        stroke="hsl(var(--accent))" 
        strokeWidth={1} 
        strokeDasharray="4 4"
        strokeOpacity={0.5}
      />
      
      {/* P50 line */}
      <path d={p50Path} fill="none" stroke="hsl(var(--accent))" strokeWidth={2.5} />
      
      {/* End point */}
      <circle cx={xScale(DEMO_YEARS.length - 1)} cy={yScale(DEMO_P50[DEMO_P50.length - 1])} r={5} fill="hsl(var(--accent))" />
      
      {/* Year labels */}
      {DEMO_YEARS.map((year, i) => (
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

function MiniPercentileBars() {
  return (
    <div className="space-y-2.5">
      <div className="flex items-center gap-3">
        <span className="text-xs text-muted-foreground w-8 font-mono">P90</span>
        <div className="flex-1 h-2.5 bg-muted rounded-full overflow-hidden">
          <div className="h-full bg-accent/50 rounded-full" style={{ width: "85%" }} />
        </div>
        <span className="text-xs font-mono text-muted-foreground">$425K</span>
      </div>
      <div className="flex items-center gap-3">
        <span className="text-xs text-foreground w-8 font-mono font-semibold">P50</span>
        <div className="flex-1 h-2.5 bg-muted rounded-full overflow-hidden">
          <div className="h-full bg-accent rounded-full" style={{ width: "65%" }} />
        </div>
        <span className="text-xs font-mono font-semibold">$345K</span>
      </div>
      <div className="flex items-center gap-3">
        <span className="text-xs text-muted-foreground w-8 font-mono">P10</span>
        <div className="flex-1 h-2.5 bg-muted rounded-full overflow-hidden">
          <div className="h-full bg-accent/30 rounded-full" style={{ width: "45%" }} />
        </div>
        <span className="text-xs font-mono text-muted-foreground">$268K</span>
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

export function MockForecastCard() {
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
              homecastr.com/forecasts/tx/houston/123-main-st
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
                Houston, TX 77002
              </div>
              <h2 className="text-2xl md:text-3xl font-bold tracking-tight">123 Main Street</h2>
            </div>
            <div className="flex items-baseline gap-2 md:text-right">
              <span className="text-3xl md:text-4xl font-bold tracking-tight">$295,000</span>
              <span className="text-sm text-muted-foreground">current modeled value</span>
            </div>
          </div>

          {/* Forecast summary cards */}
          <div className="grid grid-cols-3 gap-4 mb-8">
            <div className="p-4 rounded-lg bg-muted/30 border border-border">
              <div className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Downside</div>
              <div className="text-xl font-bold text-muted-foreground">$268K</div>
              <div className="text-xs text-muted-foreground mt-1">P10 by 2030</div>
            </div>
            <div className="p-4 rounded-lg bg-accent/10 border border-accent/20">
              <div className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Base Case</div>
              <div className="text-xl font-bold">$345K</div>
              <div className="text-xs text-muted-foreground mt-1">P50 by 2030</div>
            </div>
            <div className="p-4 rounded-lg bg-muted/30 border border-border">
              <div className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Upside</div>
              <div className="text-xl font-bold text-muted-foreground">$425K</div>
              <div className="text-xs text-muted-foreground mt-1">P90 by 2030</div>
            </div>
          </div>

          <div className="grid md:grid-cols-2 gap-8">
            {/* Fan chart */}
            <div>
              <div className="flex items-center gap-2 text-sm font-medium mb-4">
                <TrendingUp className="w-4 h-4 text-accent" />
                5-Year Value Forecast
              </div>
              <div className="p-4 rounded-lg bg-muted/20 border border-border">
                <MiniFanChart />
              </div>
            </div>

            {/* Percentile breakdown */}
            <div>
              <div className="flex items-center gap-2 text-sm font-medium mb-4">
                <BarChart3 className="w-4 h-4 text-accent" />
                2030 Scenario Breakdown
              </div>
              <div className="p-4 rounded-lg bg-muted/20 border border-border">
                <MiniPercentileBars />
                <div className="mt-4 pt-4 border-t border-border">
                  <p className="text-sm text-muted-foreground leading-relaxed">
                    <strong className="text-foreground">Base case:</strong> Modest appreciation driven by stable local demand and moderate rate environment.
                  </p>
                </div>
              </div>
            </div>
          </div>

          {/* Plain English takeaway */}
          <div className="mt-8 p-4 rounded-lg bg-muted/30 border border-border">
            <div className="text-sm font-medium mb-2">Key Takeaway</div>
            <p className="text-sm text-muted-foreground leading-relaxed">
              This forecast range is wide because the property is sensitive to regional employment conditions and interest-rate shifts.
              The base case suggests 17% appreciation over five years, with outcomes ranging from a 9% decline to a 44% gain.
            </p>
          </div>
        </div>
      </div>
    </div>
  )
}

// Keep the old export for backwards compatibility but redirect to new components
export function HeroProductPreview() {
  return <MockForecastCard />
}
