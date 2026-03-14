"use client"

import { useState, useEffect, useRef } from "react"
import { useRouter } from "next/navigation"
import { Search, ArrowRight, MapPin, MessageSquare } from "lucide-react"
import { getAutocompleteSuggestions, type AutocompleteResult, geocodeAddress } from "@/app/actions/geocode"
import { addressToForecast } from "@/app/actions/address-to-forecast"
import { useDebounce } from "@/hooks/use-debounce"
import { toast } from "sonner"

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

// Featured forecast page for demo purposes - queries route here with assistant
const FEATURED_FORECAST = "/forecasts/ny/queens/downtown-flushing-tr-086500/home-price-forecast"

// Detect if input looks like an address/location vs a question
function looksLikeLocation(text: string): boolean {
  // Contains "?" = definitely question
  // Starts with common question words = question
  if (text.includes("?")) return false
  if (/^(what|how|show|why|when|can|will|is|are|should)/i.test(text.trim())) return false
  
  // If it's not explicitly a question, treat it as a location lookup
  // This allows "Austin", "New York", "123 Main St", etc.
  return true
}

export function HeroForecastBar() {
  const [query, setQuery] = useState("")
  const [placeholderIndex, setPlaceholderIndex] = useState(0)
  const [isTyping, setIsTyping] = useState(false)
  const [suggestions, setSuggestions] = useState<AutocompleteResult[]>([])
  const [showSuggestions, setShowSuggestions] = useState(false)
  const [selectedIndex, setSelectedIndex] = useState(-1)
  const [isLoading, setIsLoading] = useState(false)
  const router = useRouter()
  const inputRef = useRef<HTMLInputElement>(null)
  const dropdownRef = useRef<HTMLFormElement>(null)
  
  const debouncedQuery = useDebounce(query, 300)

  // Rotate placeholder every 3 seconds when not typing
  useEffect(() => {
    if (isTyping || query) return
    
    const interval = setInterval(() => {
      setPlaceholderIndex((prev) => (prev + 1) % EXAMPLE_PROMPTS.length)
    }, 3000)
    
    return () => clearInterval(interval)
  }, [isTyping, query])

  // Fetch address suggestions when input looks like an address
  useEffect(() => {
    async function fetchSuggestions() {
      if (debouncedQuery.length < 3) {
        setSuggestions([])
        return
      }
      
      // Only fetch address suggestions if it looks like a location/address
      if (!looksLikeLocation(debouncedQuery)) {
        setSuggestions([])
        return
      }
      
      try {
        const results = await getAutocompleteSuggestions(debouncedQuery)
        setSuggestions(results)
        setShowSuggestions(results.length > 0)
      } catch (error) {
        console.error("Failed to fetch suggestions:", error)
      }
    }
    
    fetchSuggestions()
  }, [debouncedQuery])

  // Close dropdown when clicking outside
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setShowSuggestions(false)
      }
    }
    document.addEventListener("mousedown", handleClickOutside)
    return () => document.removeEventListener("mousedown", handleClickOutside)
  }, [])

  const handleChipClick = (prompt: string) => {
    setQuery(prompt)
    inputRef.current?.focus()
  }

  const handleSelectSuggestion = async (suggestion: AutocompleteResult) => {
    setQuery(suggestion.displayName)
    setShowSuggestions(false)
    setSuggestions([])
    setIsLoading(true)
    
    try {
      // Look up the tract from lat/lng and get the forecast URL
      const result = await addressToForecast(
        suggestion.lat,
        suggestion.lng,
        { resultType: suggestion.resultType, resultClass: suggestion.resultClass, displayName: suggestion.displayName }
      )
      
      if (result.success) {
        if (result.routeType === "city_hub" && result.cityHubUrl) {
          router.push(result.cityHubUrl)
        } else if (result.routeType === "tract_detail" && result.forecastUrl) {
          router.push(result.forecastUrl)
        } else {
          router.push(`/app?q=${encodeURIComponent(suggestion.displayName)}`)
        }
      } else {
        // Show error toast if area isn't covered yet
        toast.error(result.error || "This area does not have forecast data available yet.")
      }
    } catch (error) {
      console.error("[handleSelectSuggestion] Error:", error)
      toast.error("An error occurred while looking up this address")
    } finally {
      setIsLoading(false)
    }
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setShowSuggestions(false)
    
    // If there's a selected suggestion, use it
    if (selectedIndex >= 0 && suggestions[selectedIndex]) {
      handleSelectSuggestion(suggestions[selectedIndex])
      return
    }
    
    const trimmedQuery = query.trim()
    if (!trimmedQuery) {
      return
    }
    
    // If it looks like a location, try to geocode and find the forecast
    if (looksLikeLocation(trimmedQuery)) {
      setIsLoading(true)
      try {
        const geocodeResult = await geocodeAddress(trimmedQuery)
        if (geocodeResult) {
          const forecastResult = await addressToForecast(
            geocodeResult.lat, 
            geocodeResult.lng,
            { resultType: geocodeResult.resultType, resultClass: geocodeResult.resultClass, displayName: geocodeResult.displayName }
          )
          
          if (forecastResult.success) {
            if (forecastResult.routeType === "city_hub" && forecastResult.cityHubUrl) {
              router.push(forecastResult.cityHubUrl)
              return
            } else if (forecastResult.routeType === "tract_detail" && forecastResult.forecastUrl) {
              router.push(forecastResult.forecastUrl)
              return
            } else {
              router.push(`/app?q=${encodeURIComponent(trimmedQuery)}`)
              return
            }
          } else {
            router.push(`/app?q=${encodeURIComponent(trimmedQuery)}`)
            return
          }
        } else {
          toast.error("Could not find this address.")
          return
        }
      } catch (error) {
        console.error("[handleSubmit] Geocode error:", error)
        toast.error("An error occurred while looking up this address")
        return
      } finally {
        setIsLoading(false)
      }
    }
    
    // For questions or failed address lookups, route to the interactive map with query
    router.push(`/app?q=${encodeURIComponent(trimmedQuery)}`)
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (!showSuggestions || suggestions.length === 0) return
    
    if (e.key === "ArrowDown") {
      e.preventDefault()
      setSelectedIndex((prev) => (prev + 1) % suggestions.length)
    } else if (e.key === "ArrowUp") {
      e.preventDefault()
      setSelectedIndex((prev) => (prev - 1 + suggestions.length) % suggestions.length)
    } else if (e.key === "Enter" && selectedIndex >= 0) {
      e.preventDefault()
      handleSelectSuggestion(suggestions[selectedIndex])
    } else if (e.key === "Escape") {
      setShowSuggestions(false)
    }
  }

  const handleFocus = () => {
    setIsTyping(true)
    if (suggestions.length > 0) {
      setShowSuggestions(true)
    }
  }
  
  const handleBlur = () => {
    setIsTyping(false)
    // Delay hiding to allow click on suggestion
    setTimeout(() => setShowSuggestions(false), 200)
  }

  const isLocation = looksLikeLocation(query)

  return (
    <div className="max-w-2xl mx-auto w-full">
      {/* Main input with animated placeholder and autocomplete */}
      <form onSubmit={handleSubmit} className="relative" ref={dropdownRef}>
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
            ref={inputRef}
            id="forecast-input"
            type="text"
            value={query}
            onChange={(e) => {
              setQuery(e.target.value)
              setSelectedIndex(-1)
            }}
            onFocus={handleFocus}
            onBlur={handleBlur}
            onKeyDown={handleKeyDown}
            autoComplete="off"
            className="w-full pl-12 pr-32 py-4 text-base bg-card border border-border rounded-xl focus:outline-none focus:ring-2 focus:ring-accent/50 focus:border-accent transition-all"
          />
          <button
            type="submit"
            disabled={isLoading}
            className="absolute right-2 inline-flex items-center gap-2 px-4 py-2 text-sm font-medium bg-primary text-primary-foreground rounded-lg hover:bg-primary/90 transition-colors disabled:opacity-50"
          >
            {isLoading ? (
              <>
                <span className="w-4 h-4 border-2 border-primary-foreground/30 border-t-primary-foreground rounded-full animate-spin" />
                Loading...
              </>
            ) : (
              <>
                {isLocation ? "Get Forecast" : "Ask"}
                <ArrowRight className="w-4 h-4" />
              </>
            )}
          </button>
        </div>

        {/* Address autocomplete dropdown */}
        {showSuggestions && suggestions.length > 0 && (
          <div className="absolute top-full left-0 right-0 mt-2 bg-card border border-border rounded-xl shadow-lg z-50 overflow-hidden">
            {suggestions.map((suggestion, index) => (
              <button
                key={`${suggestion.lat}-${suggestion.lng}`}
                type="button"
                className={`w-full text-left px-4 py-3 flex items-start gap-3 transition-colors ${
                  index === selectedIndex 
                    ? "bg-muted" 
                    : "hover:bg-muted/50"
                }`}
                onMouseDown={(e) => {
                  e.preventDefault() // prevent input blur
                  handleSelectSuggestion(suggestion)
                }}
                onMouseEnter={() => setSelectedIndex(index)}
              >
                <MapPin className="w-4 h-4 mt-0.5 text-muted-foreground shrink-0" />
                <span className="text-sm line-clamp-2">{suggestion.displayName}</span>
              </button>
            ))}
          </div>
        )}
      </form>

      {/* Quick action chips */}
      <div className="flex flex-wrap items-center justify-center gap-2 mt-4">
        <span className="text-xs text-muted-foreground">Try:</span>
        {PROMPT_CHIPS.map((prompt) => (
          <button
            key={prompt}
            onClick={() => handleChipClick(prompt)}
            className="px-3 py-1.5 text-xs font-medium text-muted-foreground bg-muted/50 border border-border rounded-full hover:bg-muted hover:text-foreground transition-colors inline-flex items-center gap-1.5"
          >
            <MessageSquare className="w-3 h-3" />
            {prompt}
          </button>
        ))}
      </div>
    </div>
  )
}
