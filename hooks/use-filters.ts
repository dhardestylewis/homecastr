"use client"

import { useState, useCallback, useEffect, useRef } from "react"
import { useSearchParams, useRouter } from "next/navigation"
import type { FilterState } from "@/lib/types"

const DEFAULT_FILTERS: FilterState = {
  reliabilityMin: 0,
  nAcctsMin: 0,
  medNYearsMin: 0,
  showUnderperformers: false,
  highlightWarnings: false,
  colorMode: "value",
  layerOverride: undefined,
  usePMTiles: false,
  useVectorMap: false,
  useForecastMap: false,
}

export function useFilters() {
  const router = useRouter()
  const searchParams = useSearchParams()
  const isFirstRender = useRef(true)

  const [filters, setFiltersState] = useState<FilterState>(() => {
    // Initialize from URL params
    const modeParam = searchParams.get("mode")
    return {
      reliabilityMin: Number.parseFloat(searchParams.get("rMin") || "0"),
      nAcctsMin: Number.parseInt(searchParams.get("nMin") || "0", 10),
      medNYearsMin: Number.parseFloat(searchParams.get("yMin") || "0"),
      showUnderperformers: searchParams.get("underperf") === "true",
      highlightWarnings: searchParams.get("warnings") === "true",
      colorMode: (modeParam === "growth" ? "growth" : "value"),
      layerOverride: searchParams.get("layer") ? Number.parseInt(searchParams.get("layer")!, 10) : undefined,
      usePMTiles: false, // PMTiles disabled — local tile file not available
      useVectorMap: searchParams.get("vector") === "true",
      useForecastMap: searchParams.get("forecast") === "true",
    }
  })

  // Sync filters to URL via useEffect (not during render)
  // IMPORTANT: We preserve existing URL params (lat, lng, zoom, id) to avoid
  // stripping map position params and causing a navigation cascade.
  useEffect(() => {
    // Skip URL update on first render (already initialized from URL)
    if (isFirstRender.current) {
      isFirstRender.current = false
      return
    }

    // Start from current URL params so we preserve lat/lng/zoom/id
    const params = new URLSearchParams(window.location.search)

    // Filter-owned keys: set or delete each one
    const setOrDelete = (key: string, value: string | undefined, condition: boolean) => {
      if (condition && value !== undefined) {
        params.set(key, value)
      } else {
        params.delete(key)
      }
    }

    setOrDelete("rMin", filters.reliabilityMin.toString(), filters.reliabilityMin > 0)
    setOrDelete("nMin", filters.nAcctsMin.toString(), filters.nAcctsMin > 0)
    setOrDelete("yMin", filters.medNYearsMin.toString(), filters.medNYearsMin > 0)
    setOrDelete("underperf", "false", !filters.showUnderperformers)
    setOrDelete("warnings", "false", !filters.highlightWarnings)
    setOrDelete("mode", filters.colorMode, filters.colorMode !== "growth")
    // PMTiles disabled locally — don't persist to URL
    params.delete("pmtiles")
    setOrDelete("vector", "true", !!filters.useVectorMap)
    setOrDelete("forecast", "true", !!filters.useForecastMap)
    setOrDelete("layer", filters.layerOverride?.toString(), filters.layerOverride !== undefined)

    const queryString = params.toString()
    router.replace(queryString ? `?${queryString}` : "/", { scroll: false })
  }, [filters, router])

  const setFilters = useCallback(
    (updates: Partial<FilterState>) => {
      setFiltersState((prev) => ({ ...prev, ...updates }))
    },
    [],
  )

  const resetFilters = useCallback(() => {
    setFiltersState(DEFAULT_FILTERS)
    router.replace("/", { scroll: false })
  }, [router])

  return {
    filters,
    setFilters,
    resetFilters,
  }
}
