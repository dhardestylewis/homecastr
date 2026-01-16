"use client"

import type React from "react"

import { useState, useCallback, useEffect, useRef } from "react"
import { Search, X, MapPin, Building2 } from "lucide-react"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import { getAutocompleteSuggestions, type AutocompleteResult } from "@/app/actions/geocode"
import { useDebounce } from "@/hooks/use-debounce"

interface SearchBoxProps {
  onSearch: (query: string) => void
  placeholder?: string
  value?: string
}

export function SearchBox({ onSearch, placeholder = "Search address or ID...", value }: SearchBoxProps) {
  const [query, setQuery] = useState("")
  const [suggestions, setSuggestions] = useState<AutocompleteResult[]>([])
  const [isOpen, setIsOpen] = useState(false)
  const [isLoading, setIsLoading] = useState(false)

  // Custom Hook or just simplified debounce here
  // We can't import useDebounce if it doesn't exist, checking imports first.
  // Assuming we need to implement debounce.

  const dropdownRef = useRef<HTMLDivElement>(null)

  // Close dropdown when clicking outside
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setIsOpen(false)
      }
    }
    document.addEventListener("mousedown", handleClickOutside)
    return () => document.removeEventListener("mousedown", handleClickOutside)
  }, [])

  const debouncedQuery = useDebounce(query, 300)

  // Sync with external value (e.g. from map selection)
  useEffect(() => {
    if (value && value !== query) {
      setQuery(value)
    }
  }, [value])

  useEffect(() => {
    async function fetchSuggestions() {
      if (debouncedQuery.length < 3) {
        setSuggestions([])
        setIsOpen(false)
        return
      }

      setIsLoading(true)
      try {
        const results = await getAutocompleteSuggestions(debouncedQuery)
        setSuggestions(results)
        setIsOpen(results.length > 0)
      } catch (error) {
        console.error("Failed to fetch suggestions:", error)
      } finally {
        setIsLoading(false)
      }
    }

    fetchSuggestions()
  }, [debouncedQuery])

  const handleSubmit = useCallback(
    (e: React.FormEvent) => {
      e.preventDefault()
      setIsOpen(false)
      if (query.trim()) {
        onSearch(query.trim())
      }
    },
    [query, onSearch],
  )

  const handleClear = useCallback(() => {
    setQuery("")
    setSuggestions([])
    setIsOpen(false)
  }, [])

  const handleSelect = useCallback((suggestion: AutocompleteResult) => {
    setQuery(suggestion.displayName)
    setIsOpen(false)
    onSearch(suggestion.displayName)
  }, [onSearch])

  return (
    <div className="relative w-full" ref={dropdownRef}>
      <form onSubmit={handleSubmit} className="relative">
        {/* Main Glass Panel with Branding + Search */}
        <div className="glass-panel shadow-lg h-10 flex items-center px-3 gap-3 rounded-md w-full md:w-80 md:focus-within:w-[480px] transition-all duration-300 ease-in-out">
          {/* Branding */}
          <div className="flex items-center gap-2 text-primary shrink-0 border-r border-border pr-3">
            <Building2 className="w-4 h-4" />
            <span className="font-bold text-sm tracking-tight hidden sm:inline-block text-foreground">InvestMap</span>
          </div>

          {/* Search Input Area */}
          <div className="relative flex-1 flex items-center">
            <Search className="h-4 w-4 text-muted-foreground mr-2 shrink-0" />
            <Input
              id="search-box"
              name="search-box"
              type="search"
              value={query}
              onChange={(e) => {
                setQuery(e.target.value)
                if (!isOpen && e.target.value.length >= 3) setIsOpen(true)
              }}
              placeholder={placeholder}
              className="h-9 border-none bg-transparent shadow-none focus-visible:ring-0 px-0 text-sm placeholder:text-muted-foreground/70 [&::-webkit-search-cancel-button]:appearance-none [&::-webkit-search-cancel-button]:hidden [&::-webkit-search-decoration]:hidden"
              aria-label="Search"
              autoComplete="off"
            />

            {query && (
              <Button
                type="button"
                variant="ghost"
                size="icon"
                className="h-6 w-6 ml-1 hover:bg-muted/50 rounded-full"
                onClick={handleClear}
                aria-label="Clear search"
              >
                <X className="h-3 w-3" />
              </Button>
            )}
          </div>
        </div>
      </form>

      {isOpen && suggestions.length > 0 && (
        <div className="absolute top-full left-0 right-0 mt-1 bg-card/95 backdrop-blur-md border border-border rounded-md shadow-lg z-[100] max-h-60 overflow-y-auto">
          {suggestions.map((item, index) => (
            <button
              key={`${item.lat}-${item.lng}-${index}`}
              className="w-full text-left px-4 py-2 hover:bg-muted text-sm flex items-start gap-2"
              onClick={() => handleSelect(item)}
            >
              <MapPin className="h-4 w-4 mt-0.5 text-muted-foreground shrink-0" />
              <span className="line-clamp-2">{item.displayName}</span>
            </button>
          ))}
          <div className="px-2 py-1 flex items-center justify-end border-t border-border">
            <span className="text-[10px] text-muted-foreground">Search restricted to Harris County</span>
          </div>
        </div>
      )}
    </div>
  )
}
