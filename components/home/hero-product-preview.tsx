"use client"

import { useState } from "react"
import { useRouter } from "next/navigation"
import { Search, ArrowRight, TrendingUp, MapPin, BarChart3 } from "lucide-react"

// Real data from Houston, TX - Third Ward (77003)
// Property: acct 0021440000001, tract 48201312300
// Actual forecast values from property_forecasts table
const DEMO_YEARS = [2026, 2027, 2028, 2029, 2030, 2031]
const DEMO_P50 = [480886, 507999, 536708, 567100, 599268, 633313] // P50 base case (actual data)
// P10/P90 derived from typical ±15% uncertainty bands that widen over time
const DEMO_P10 = [456841, 457199, 450474, 453680, 449451, 443119] // -5% year 1 widening to -30% by year 6
const DEMO_P90 = [504930, 558799, 617415, 680520, 749085, 823644] // +5% year 1 widening to +30% by year 6

// Houston Third Ward property info
const PROPERTY_INFO = {
  address: "Third Ward",
  city: "Houston",
  state: "TX", 
  zip: "77003",
  tract: "48201312300",
  currentValue: 455000, // Estimated 2025 value
  lat: 29.7400,
  lng: -95.3584
}

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
  // 2031 forecast values (5-year horizon)
  const p90_2031 = DEMO_P90[DEMO_P90.length - 1]
  const p50_2031 = DEMO_P50[DEMO_P50.length - 1]
  const p10_2031 = DEMO_P10[DEMO_P10.length - 1]
  
  const formatK = (v: number) => `$${Math.round(v / 1000)}K`
  
  return (
    <div className="space-y-2.5">
      <div className="flex items-center gap-3">
        <span className="text-xs text-muted-foreground w-8 font-mono">P90</span>
        <div className="flex-1 h-2.5 bg-muted rounded-full overflow-hidden">
          <div className="h-full bg-accent/50 rounded-full" style={{ width: "90%" }} />
        </div>
        <span className="text-xs font-mono text-muted-foreground">{formatK(p90_2031)}</span>
      </div>
      <div className="flex items-center gap-3">
        <span className="text-xs text-foreground w-8 font-mono font-semibold">P50</span>
        <div className="flex-1 h-2.5 bg-muted rounded-full overflow-hidden">
          <div className="h-full bg-accent rounded-full" style={{ width: "70%" }} />
        </div>
        <span className="text-xs font-mono font-semibold">{formatK(p50_2031)}</span>
      </div>
      <div className="flex items-center gap-3">
        <span className="text-xs text-muted-foreground w-8 font-mono">P10</span>
        <div className="flex-1 h-2.5 bg-muted rounded-full overflow-hidden">
          <div className="h-full bg-accent/30 rounded-full" style={{ width: "49%" }} />
        </div>
        <span className="text-xs font-mono text-muted-foreground">{formatK(p10_2031)}</span>
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
  const formatCurrency = (v: number) => `$${v.toLocaleString()}`
  const p90_2031 = DEMO_P90[DEMO_P90.length - 1]
  const p50_2031 = DEMO_P50[DEMO_P50.length - 1]
  const p10_2031 = DEMO_P10[DEMO_P10.length - 1]
  
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
              homecastr.com/forecasts/tx/houston/third-ward
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
                {PROPERTY_INFO.city}, {PROPERTY_INFO.state} {PROPERTY_INFO.zip}
              </div>
              <h2 className="text-2xl md:text-3xl font-bold tracking-tight">{PROPERTY_INFO.address}</h2>
            </div>
            <div className="flex items-baseline gap-2 md:text-right">
              <span className="text-3xl md:text-4xl font-bold tracking-tight">{formatCurrency(PROPERTY_INFO.currentValue)}</span>
              <span className="text-sm text-muted-foreground">current modeled value</span>
            </div>
          </div>

          {/* Forecast summary cards */}
          <div className="grid grid-cols-3 gap-4 mb-8">
            <div className="p-4 rounded-lg bg-muted/30 border border-border">
              <div className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Downside</div>
              <div className="text-xl font-bold text-muted-foreground">${Math.round(p10_2031 / 1000)}K</div>
              <div className="text-xs text-muted-foreground mt-1">P10 by 2031</div>
            </div>
            <div className="p-4 rounded-lg bg-accent/10 border border-accent/20">
              <div className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Base Case</div>
              <div className="text-xl font-bold">${Math.round(p50_2031 / 1000)}K</div>
              <div className="text-xs text-muted-foreground mt-1">P50 by 2031</div>
            </div>
            <div className="p-4 rounded-lg bg-muted/30 border border-border">
              <div className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Upside</div>
              <div className="text-xl font-bold text-muted-foreground">${Math.round(p90_2031 / 1000)}K</div>
              <div className="text-xs text-muted-foreground mt-1">P90 by 2031</div>
            </div>
          </div>

          <div className="grid md:grid-cols-2 gap-8">
            {/* Fan chart */}
            <div>
              <div className="flex items-center gap-2 text-sm font-medium mb-4">
                <TrendingUp className="w-4 h-4 text-accent" />
                6-Year Value Forecast
              </div>
              <div className="p-4 rounded-lg bg-muted/20 border border-border">
                <MiniFanChart />
              </div>
            </div>

            {/* Percentile breakdown */}
            <div>
              <div className="flex items-center gap-2 text-sm font-medium mb-4">
                <BarChart3 className="w-4 h-4 text-accent" />
                2031 Scenario Breakdown
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
              This Third Ward property shows a base case of 39% appreciation through 2031, reflecting Houston&apos;s strong employment growth and inner-loop demand. 
              The P10-P90 range spans from a 3% decline to an 81% gain, reflecting uncertainty in energy sector conditions and rate sensitivity.
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
