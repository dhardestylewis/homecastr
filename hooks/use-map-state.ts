"use client"

import { useState, useCallback, useEffect, useRef } from "react"
import { useSearchParams, useRouter } from "next/navigation"
import type { MapState } from "@/lib/types"

const DEFAULT_MAP_STATE: MapState = {
  center: [-95.3698, 29.7604], // Houston
  zoom: 11, // Good zoom for Harris County
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
    highlightedIds: searchParams.get("highlights") ? searchParams.get("highlights")!.split(",") : [],
  }))

  // Update URL when selectedId changes - use ref to prevent infinite loops
  const prevSelectedIdRef = useRef<string | null>(mapState.selectedId)

  useEffect(() => {
    if (typeof window === "undefined") return

    // Only update URL if selectedId actually changed
    if (prevSelectedIdRef.current === mapState.selectedId) return
    prevSelectedIdRef.current = mapState.selectedId

    const params = new URLSearchParams(window.location.search)
    if (mapState.selectedId) {
      params.set("id", mapState.selectedId)
    } else {
      params.delete("id")
    }

    if (mapState.highlightedIds && mapState.highlightedIds.length > 0) {
      params.set("highlights", mapState.highlightedIds.join(","))
    } else {
      params.delete("highlights")
    }
    router.replace(`?${params.toString()}`, { scroll: false })
  }, [mapState.selectedId, mapState.highlightedIds, router])

  const setMapState = useCallback((updates: Partial<MapState>) => {
    setMapStateInternal((prev) => ({ ...prev, ...updates }))
  }, [])

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
