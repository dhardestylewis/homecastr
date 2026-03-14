"use client"

import { useState, useEffect } from "react"
import { useRouter } from "next/navigation"
import { Search, ArrowRight } from "lucide-react"

// Animated placeholder examples - mix of addresses and questions
const EXAMPLE_PROMPTS = [
  "123 Main St, Austin TX",
  "What could my house be worth in 2030?",
  "456 Oak Ave, Brooklyn NY", 
  "Show me downside vs upside scenarios",
  "789 Pine St, Seattle WA",
]

// Quick action chips - full questions that prefill the input
const PROMPT_CHIPS = [
  "What could my house be worth in 2030?",
  "Show downside vs upside",
  "How is my neighborhood expected to perform?",
]

export function HeroForecastBar() {
  const [query, setQuery] = useState("")
  const [placeholderIndex, setPlaceholderIndex] = useState(0)
  const [isTyping, setIsTyping] = useState(false)
  const router = useRouter()

  // Rotate placeholder every 3 seconds when not typing
  useEffect(() => {
    if (isTyping || query) return
    
    const interval = setInterval(() => {
      setPlaceholderIndex((prev) => (prev + 1) % EXAMPLE_PROMPTS.length)
    }, 3000)
    
    return () => clearInterval(interval)
  }, [isTyping, query])

  const handleChipClick = (prompt: string) => {
    setQuery(prompt)
    const input = document.getElementById("forecast-input") as HTMLInputElement
    input?.focus()
  }

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    if (query.trim()) {
      router.push(`/app?q=${encodeURIComponent(query.trim())}`)
    } else {
      router.push("/app")
    }
  }

  const handleFocus = () => setIsTyping(true)
  const handleBlur = () => setIsTyping(false)

  return (
    <div className="max-w-2xl mx-auto w-full">
      {/* Main input with animated placeholder */}
      <form onSubmit={handleSubmit} className="relative">
        <div className="relative flex items-center">
          <Search className="absolute left-4 w-5 h-5 text-muted-foreground pointer-events-none" />
          
          {/* Custom placeholder that animates - shows both addresses and questions */}
          {!query && (
            <div className="absolute left-12 pointer-events-none">
              <span 
                key={placeholderIndex}
                className="animate-fade-in text-muted-foreground/50 italic"
              >
                {EXAMPLE_PROMPTS[placeholderIndex]}
              </span>
            </div>
          )}
          
          <input
            id="forecast-input"
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onFocus={handleFocus}
            onBlur={handleBlur}
            className="w-full pl-12 pr-32 py-4 text-base bg-card border border-border rounded-xl focus:outline-none focus:ring-2 focus:ring-accent/50 focus:border-accent transition-all"
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

      {/* Quick action chips */}
      <div className="flex flex-wrap items-center justify-center gap-2 mt-4">
        <span className="text-xs text-muted-foreground">Try:</span>
        {PROMPT_CHIPS.map((prompt) => (
          <button
            key={prompt}
            onClick={() => handleChipClick(prompt)}
            className="px-3 py-1.5 text-xs font-medium text-muted-foreground bg-muted/50 border border-border rounded-full hover:bg-muted hover:text-foreground transition-colors"
          >
            {prompt}
          </button>
        ))}
      </div>
    </div>
  )
}
