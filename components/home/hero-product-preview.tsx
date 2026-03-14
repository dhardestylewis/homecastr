"use client"

import { useState } from "react"
import { useRouter } from "next/navigation"
import { Search, ArrowRight } from "lucide-react"

const EXAMPLE_CHIPS = [
  { label: "What could my house be worth in 2030?", query: "What could my house be worth in 2030?" },
  { label: "Show downside vs upside", query: "Show downside vs upside" },
  { label: "How is my neighborhood expected to perform?", query: "How is my neighborhood expected to perform?" },
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
