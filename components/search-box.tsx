"use client"

import type React from "react"

import { useState, useCallback, useEffect, useRef } from "react"
import { Search, X, MapPin } from "lucide-react"
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
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
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
          className="pl-9 pr-9 bg-card/95 backdrop-blur-sm border-border shadow-lg h-10"
          aria-label="Search"
          autoComplete="off"
        />
        {query && (
          <Button
            type="button"
            variant="ghost"
            size="icon"
            className="absolute right-1 top-1/2 -translate-y-1/2 h-7 w-7"
            onClick={handleClear}
            aria-label="Clear search"
          >
            <X className="h-3.5 w-3.5" />
          </Button>
        )}
      </form>

      {isOpen && suggestions.length > 0 && (
        <div className="absolute top-full left-0 right-0 mt-1 bg-background border border-border rounded-md shadow-lg z-[100] max-h-60 overflow-y-auto">
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
