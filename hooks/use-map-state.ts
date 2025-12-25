"use client"

import { useState, useCallback } from "react"
import { useSearchParams, useRouter } from "next/navigation"
import type { MapState } from "@/lib/types"

const DEFAULT_MAP_STATE: MapState = {
  center: [-98.5795, 39.8283], // US center
  zoom: 4,
  selectedId: null,
  hoveredId: null,
}

export function useMapState() {
  const router = useRouter()
  const searchParams = useSearchParams()

  const [mapState, setMapStateInternal] = useState<MapState>(() => ({
    center: [
      Number.parseFloat(searchParams.get("lng") || DEFAULT_MAP_STATE.center[0].toString()),
      Number.parseFloat(searchParams.get("lat") || DEFAULT_MAP_STATE.center[1].toString()),
    ],
    zoom: Number.parseFloat(searchParams.get("zoom") || DEFAULT_MAP_STATE.zoom.toString()),
    selectedId: searchParams.get("id") || null,
    hoveredId: null,
  }))

  const setMapState = useCallback(
    (updates: Partial<MapState>) => {
      setMapStateInternal((prev) => {
        const next = { ...prev, ...updates }

        // Update URL for selection
        if (updates.selectedId !== undefined) {
          const params = new URLSearchParams(searchParams.toString())
          if (updates.selectedId) {
            params.set("id", updates.selectedId)
          } else {
            params.delete("id")
          }
          router.replace(`?${params.toString()}`, { scroll: false })
        }

        return next
      })
    },
    [router, searchParams],
  )

  const selectFeature = useCallback(
    (id: string | null) => {
      setMapState({ selectedId: id })
    },
    [setMapState],
  )

  const hoverFeature = useCallback(
    (id: string | null) => {
      setMapState({ hoveredId: id })
    },
    [setMapState],
  )

  return {
    mapState,
    setMapState,
    selectFeature,
    hoverFeature,
  }
}
