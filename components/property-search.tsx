"use client"

import type React from "react"

import { useState, useCallback } from "react"
import { Search, Loader2 } from "lucide-react"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import { getPropertyForecast, searchPropertyByAccount } from "@/app/actions/property-forecast"
import type { PropertyForecast } from "@/app/actions/property-forecast"

interface PropertySearchProps {
  onForecastLoaded?: (acct: string, forecast: PropertyForecast[]) => void
  onError?: (error: string) => void
}

export function PropertySearch({ onForecastLoaded, onError }: PropertySearchProps) {
  const [query, setQuery] = useState("")
  const [isSearching, setIsSearching] = useState(false)

  const handleSearch = useCallback(
    async (e: React.FormEvent) => {
      e.preventDefault()

      if (!query.trim()) {
        onError?.("Please enter a property account ID")
        return
      }

      setIsSearching(true)

      try {
        // First verify the account exists
        const property = await searchPropertyByAccount(query.trim())

        if (!property) {
          onError?.(`No property found with account ID: ${query.trim()}`)
          setIsSearching(false)
          return
        }

        // Fetch full forecast history
        const forecast = await getPropertyForecast(query.trim())

        if (forecast.length === 0) {
          onError?.(`No forecast data available for account: ${query.trim()}`)
        } else {
          onForecastLoaded?.(query.trim(), forecast)
        }
      } catch (err) {
        onError?.(err instanceof Error ? err.message : "Failed to search property")
      } finally {
        setIsSearching(false)
      }
    },
    [query, onForecastLoaded, onError],
  )

  return (
    <form onSubmit={handleSearch} className="relative w-full flex gap-2">
      <div className="relative flex-1">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
        <Input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Enter property account ID..."
          className="pl-9 bg-secondary/50 border-transparent focus:border-primary/50 h-9"
          disabled={isSearching}
          aria-label="Property account ID"
        />
      </div>
      <Button type="submit" size="sm" disabled={isSearching || !query.trim()} className="h-9">
        {isSearching ? (
          <>
            <Loader2 className="h-4 w-4 mr-1.5 animate-spin" />
            Searching
          </>
        ) : (
          "Search"
        )}
      </Button>
    </form>
  )
}
