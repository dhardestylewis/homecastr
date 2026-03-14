"use client"

import type React from "react"

import { useState, useCallback, useEffect, useRef } from "react"
import Link from 'next/link'
import { Search, X, MapPin, MessageSquare, Mic } from "lucide-react"
import { HomecastrLogo } from "./homecastr-logo"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import { getAutocompleteSuggestions, type AutocompleteResult } from "@/app/actions/geocode"
import { useDebounce } from "@/hooks/use-debounce"
import { cn } from "@/lib/utils"

interface SearchBoxProps {
  onSearch: (query: string) => void
  placeholder?: string
  value?: string
  isChatOpen?: boolean
  onToggleChat?: () => void
  onMicClick?: () => void
  showMic?: boolean
}

export function SearchBox({ onSearch, placeholder = "Search address or ID...", value, isChatOpen, onToggleChat, onMicClick, showMic }: SearchBoxProps) {
  const [query, setQuery] = useState("")
  const [suggestions, setSuggestions] = useState<AutocompleteResult[]>([])
  const [isOpen, setIsOpen] = useState(false)
  const [isLoading, setIsLoading] = useState(false)

  // Custom Hook or just simplified debounce here
  // We can't import useDebounce if it doesn't exist, checking imports first.
  // Assuming we need to implement debounce.

  const inputRef = useRef<HTMLInputElement>(null)
  const dropdownRef = useRef<HTMLDivElement>(null)

  // Track if update is from user typing
  const shouldFetchRef = useRef(false)

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
      shouldFetchRef.current = false // Block fetch
      setQuery(value)
      setIsOpen(false)
    }
  }, [value])

  useEffect(() => {
    async function fetchSuggestions() {
      // Only fetch if initiated by user interaction
      if (!shouldFetchRef.current) return

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
    inputRef.current?.focus()
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
        <div className={cn("glass-panel shadow-lg h-10 flex items-center px-3 gap-3 rounded-md w-full md:focus-within:w-[480px] transition-all duration-300 ease-in-out border", isChatOpen ? "bg-muted/30 border-primary/30" : "border-transparent")}>
          {/* Branding */}
          <Link href="/app" className="flex items-center gap-2 text-primary shrink-0 border-r border-border pr-3 hover:opacity-80 transition-opacity">
            <HomecastrLogo variant="horizontal" size={20} />
          </Link>

          {/* Search Input Area */}
          <div className="relative flex-1 flex items-center gap-2">
            {isChatOpen ? (
              <MessageSquare className="h-4 w-4 text-primary shrink-0" />
            ) : (
              <Search className="h-4 w-4 text-muted-foreground shrink-0" />
            )}
            
            <Input
              ref={inputRef}
              id="search-box"
              name="search-box"
              type="text"
              value={query}
              onChange={(e) => {
                shouldFetchRef.current = true // Allow fetch
                setQuery(e.target.value)
                if (!isOpen && !isChatOpen && e.target.value.length >= 3) setIsOpen(true)
              }}
              onKeyDown={(e) => {
                if (e.key === "Enter" && isChatOpen) {
                  e.preventDefault()
                  if (query.trim()) {
                    onSearch(query.trim())
                    setQuery("")
                  }
                }
              }}
              placeholder={isChatOpen ? "Ask about this area..." : placeholder}
              className="h-9 border-none bg-transparent shadow-none focus-visible:ring-0 px-0 text-sm placeholder:text-muted-foreground/70 [&::-webkit-search-cancel-button]:appearance-none [&::-webkit-search-cancel-button]:hidden [&::-webkit-search-decoration]:hidden"
              aria-label="Search"
              autoComplete="off"
            />

            {showMic && !isChatOpen && onMicClick && (
              <button type="button" onClick={onMicClick} className="shrink-0 w-6 h-6 rounded-full flex items-center justify-center hover:bg-muted/50 active:scale-90 transition-transform text-muted-foreground" aria-label="Voice agent">
                <Mic size={14} />
              </button>
            )}

            {isChatOpen && onToggleChat && (
              <button type="button" onClick={onToggleChat} className="shrink-0 w-6 h-6 rounded-full flex items-center justify-center hover:bg-muted/50 text-muted-foreground" aria-label="Close Chat">
                <X size={14} />
              </button>
            )}

            {query && !isChatOpen && (
              <Button
                type="button"
                variant="ghost"
                size="icon"
                className="h-6 w-6 shrink-0 hover:bg-muted/50 rounded-full"
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
        <div className="absolute top-full left-0 right-0 mt-1 bg-card/95 backdrop-blur-md border border-border rounded-md shadow-lg z-[200] max-h-60 overflow-y-auto">
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
        </div>
      )}
    </div>
  )
}
