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
  colorMode: "growth",
  layerOverride: undefined,
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
      highlightWarnings: searchParams.get("warnings") !== "false",
      colorMode: (modeParam === "value" ? "value" : "growth"),
      layerOverride: searchParams.get("layer") ? Number.parseInt(searchParams.get("layer")!, 10) : undefined,
    }
  })

  // Sync filters to URL via useEffect (not during render)
  useEffect(() => {
    // Skip URL update on first render (already initialized from URL)
    if (isFirstRender.current) {
      isFirstRender.current = false
      return
    }

    const params = new URLSearchParams()

    if (filters.reliabilityMin > 0) {
      params.set("rMin", filters.reliabilityMin.toString())
    }

    if (filters.nAcctsMin > 0) {
      params.set("nMin", filters.nAcctsMin.toString())
    }

    if (filters.medNYearsMin > 0) {
      params.set("yMin", filters.medNYearsMin.toString())
    }

    if (!filters.showUnderperformers) {
      params.set("underperf", "false")
    }

    if (!filters.highlightWarnings) {
      params.set("warnings", "false")
    }

    if (filters.colorMode !== "growth") {
      params.set("mode", filters.colorMode)
    }

    if (filters.layerOverride !== undefined) {
      params.set("layer", filters.layerOverride.toString())
    }

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
