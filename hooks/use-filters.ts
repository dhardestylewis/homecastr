"use client"

import { useState, useCallback } from "react"
import { useSearchParams, useRouter } from "next/navigation"
import type { FilterState } from "@/lib/types"

const DEFAULT_FILTERS: FilterState = {
  reliabilityMin: 0,
  nAcctsMin: 0,
  medNYearsMin: 0,
  showUnderperformers: true,
  highlightWarnings: true,
  layerOverride: undefined,
}

export function useFilters() {
  const router = useRouter()
  const searchParams = useSearchParams()

  const [filters, setFiltersState] = useState<FilterState>(() => {
    // Initialize from URL params
    return {
      reliabilityMin: Number.parseFloat(searchParams.get("rMin") || "0"),
      nAcctsMin: Number.parseInt(searchParams.get("nMin") || "0", 10),
      medNYearsMin: Number.parseFloat(searchParams.get("yMin") || "0"),
      showUnderperformers: searchParams.get("underperf") !== "false",
      highlightWarnings: searchParams.get("warnings") !== "false",
      layerOverride: searchParams.get("layer") ? Number.parseInt(searchParams.get("layer")!, 10) : undefined,
    }
  })

  // Sync filters to URL
  const updateUrl = useCallback(
    (newFilters: FilterState) => {
      const params = new URLSearchParams(searchParams.toString())

      if (newFilters.reliabilityMin > 0) {
        params.set("rMin", newFilters.reliabilityMin.toString())
      } else {
        params.delete("rMin")
      }

      if (newFilters.nAcctsMin > 0) {
        params.set("nMin", newFilters.nAcctsMin.toString())
      } else {
        params.delete("nMin")
      }

      if (newFilters.medNYearsMin > 0) {
        params.set("yMin", newFilters.medNYearsMin.toString())
      } else {
        params.delete("yMin")
      }

      if (!newFilters.showUnderperformers) {
        params.set("underperf", "false")
      } else {
        params.delete("underperf")
      }

      if (!newFilters.highlightWarnings) {
        params.set("warnings", "false")
      } else {
        params.delete("warnings")
      }

      if (newFilters.layerOverride !== undefined) {
        params.set("layer", newFilters.layerOverride.toString())
      } else {
        params.delete("layer")
      }

      router.replace(`?${params.toString()}`, { scroll: false })
    },
    [router, searchParams],
  )

  const setFilters = useCallback(
    (updates: Partial<FilterState>) => {
      setFiltersState((prev) => {
        const next = { ...prev, ...updates }
        updateUrl(next)
        return next
      })
    },
    [updateUrl],
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
