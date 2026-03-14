"use client"

import { useState } from "react"
import { useRouter } from "next/navigation"
import { Search, ArrowRight } from "lucide-react"

// Chips route directly to the featured forecast page with assistant prompts
const FEATURED_FORECAST_PATH = "/forecasts/ny/queens/downtown-flushing-tr-086500/home-price-forecast"

const EXAMPLE_CHIPS = [
  { label: "What could my house be worth in 2030?", anchor: "key-takeaway" },
  { label: "Show downside vs upside", anchor: "uncertainty" },
  { label: "How is my neighborhood expected to perform?", anchor: "interpretation" },
]

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

  const handleChipClick = (anchor: string) => {
    // Route directly to the featured forecast page with the relevant section
    router.push(`${FEATURED_FORECAST_PATH}#${anchor}`)
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
            onClick={() => handleChipClick(chip.anchor)}
            className="px-3 py-1.5 text-xs font-medium text-muted-foreground bg-muted/50 border border-border rounded-full hover:bg-muted hover:text-foreground transition-colors"
          >
            {chip.label}
          </button>
        ))}
      </div>
    </div>
  )
}
