"use client"

import React, { useEffect, useRef, useState, useCallback, useMemo } from "react"
import { createPortal } from "react-dom"
import maplibregl from "maplibre-gl"
import "maplibre-gl/dist/maplibre-gl.css"
import type { FilterState, MapState, FanChartData } from "@/lib/types"
import { cn } from "@/lib/utils"
import { useRouter, useSearchParams } from "next/navigation"
import { HomecastrLogo } from "@/components/homecastr-logo"
import { Bot, AlertCircle } from "lucide-react"
import { FanChart } from "@/components/fan-chart"
import { StreetViewCarousel } from "@/components/street-view-carousel"
import { useKeyboardOpen } from "@/hooks/use-keyboard-open"
import { usePostHog } from 'posthog-js/react'

// Tooltip positioning constants
const SIDEBAR_WIDTH = 390
const TOOLTIP_WIDTH = 320
const TOOLTIP_HEIGHT = 620

// Geography level definitions — zoom breakpoints must match the SQL router
const GEO_LEVELS = [
    { name: "state", minzoom: 0, maxzoom: 4.99, label: "State" },
    { name: "zcta", minzoom: 5, maxzoom: 7.99, label: "ZIP Code" },
    { name: "tract", minzoom: 8, maxzoom: 22, label: "Tract" },  // Extends to z22 as fallback underlay
    { name: "tabblock", minzoom: 12, maxzoom: 22, label: "Block" },
    { name: "parcel", minzoom: 17, maxzoom: 22, label: "Parcel" },
] as const

// US State FIPS → name lookup for state-level geocoding
const STATE_FIPS_NAMES: Record<string, string> = {
    "01": "Alabama", "02": "Alaska", "04": "Arizona", "05": "Arkansas",
    "06": "California", "08": "Colorado", "09": "Connecticut", "10": "Delaware",
    "11": "District of Columbia", "12": "Florida", "13": "Georgia", "15": "Hawaii",
    "16": "Idaho", "17": "Illinois", "18": "Indiana", "19": "Iowa",
    "20": "Kansas", "21": "Kentucky", "22": "Louisiana", "23": "Maine",
    "24": "Maryland", "25": "Massachusetts", "26": "Michigan", "27": "Minnesota",
    "28": "Mississippi", "29": "Missouri", "30": "Montana", "31": "Nebraska",
    "32": "Nevada", "33": "New Hampshire", "34": "New Jersey", "35": "New Mexico",
    "36": "New York", "37": "North Carolina", "38": "North Dakota", "39": "Ohio",
    "40": "Oklahoma", "41": "Oregon", "42": "Pennsylvania", "44": "Rhode Island",
    "45": "South Carolina", "46": "South Dakota", "47": "Tennessee", "48": "Texas",
    "49": "Utah", "50": "Vermont", "51": "Virginia", "53": "Washington",
    "54": "West Virginia", "55": "Wisconsin", "56": "Wyoming", "72": "Puerto Rico",
}

function getSmartTooltipPos(x: number, y: number, windowWidth: number, windowHeight: number) {
    if (typeof window === "undefined") return { x, y }

    const isMobileView = windowWidth < 768

    // ── Horizontal: default right of cursor, flip left if needed ──
    let left = x + 20
    if (left + TOOLTIP_WIDTH > windowWidth - 20) {
        const tryLeft = x - TOOLTIP_WIDTH - 20
        if (tryLeft < SIDEBAR_WIDTH + 10) {
            const spaceRight = windowWidth - x
            const spaceLeft = x - SIDEBAR_WIDTH
            if (spaceRight > spaceLeft && spaceRight > TOOLTIP_WIDTH) {
                left = x + 20
            } else if (spaceLeft > TOOLTIP_WIDTH) {
                left = tryLeft
            } else {
                left = Math.min(left, windowWidth - TOOLTIP_WIDTH - 10)
            }
        } else {
            left = tryLeft
        }
    }
    left = Math.max(10, left)

    // ── Vertical: anchor BOTTOM of tooltip near cursor, grows upward ──
    // This prevents scroll when clicking near the bottom of screen.
    const tooltipBottom = y + 10       // bottom edge sits just below cursor
    let top = tooltipBottom - TOOLTIP_HEIGHT
    top = Math.max(10, top)            // clamp so it never goes above viewport

    // Desktop: sidebar control panel occupies the top-left column.
    // Width = SIDEBAR_WIDTH (340px), height ≈ 260px (4 stacked rows).
    if (!isMobileView) {
        const CONTROL_PANEL_H = 300
        if (left < SIDEBAR_WIDTH + 10 && top < CONTROL_PANEL_H) {
            const pushedRight = SIDEBAR_WIDTH + 10
            if (pushedRight + TOOLTIP_WIDTH <= windowWidth - 10) {
                left = pushedRight
            } else {
                top = CONTROL_PANEL_H + 10
            }
        }
    }

    return { x: left, y: top }

}

// Format currency values
function formatValue(v: number | null | undefined): string {
    if (v == null) return "N/A"
    return "$" + v.toLocaleString("en-US", { maximumFractionDigits: 0 })
}

// Get label for current zoom — iterate REVERSE so most specific level wins
// (tract extends to z22 as a rendering underlay, but interaction should use
//  the finest-grained level that the SQL tile RPC actually returns)
function getLevelLabel(zoom: number): string {
    for (let i = GEO_LEVELS.length - 1; i >= 0; i--) {
        const lvl = GEO_LEVELS[i]
        if (zoom >= lvl.minzoom && zoom <= lvl.maxzoom) return lvl.label
    }
    return "Parcel"
}

// Get source-layer name for current zoom — iterate REVERSE so most specific level wins
// (tract extends to z22 as a rendering underlay, but the SQL tile RPC returns
//  tabblock at z12-16 and parcel at z17+, so interaction must match that routing)
function getSourceLayer(zoom: number): string {
    for (let i = GEO_LEVELS.length - 1; i >= 0; i--) {
        const lvl = GEO_LEVELS[i]
        if (zoom >= lvl.minzoom && zoom <= lvl.maxzoom) return lvl.name
    }
    return "parcel"
}

// Build all fill layer IDs for querying
function getAllFillLayerIds(suffix: string): string[] {
    return GEO_LEVELS.map((lvl) => `forecast-fill-${lvl.name}-${suffix}`)
}

interface ForecastMapProps {
    filters: FilterState
    mapState: MapState
    onFeatureSelect: (id: string | null) => void
    onFeatureHover: (id: string | null) => void
    onCoordsChange?: (coords: [number, number] | null) => void
    onGeocodedName?: (name: string | null) => void
    year: number
    className?: string
    onConsultAI?: (details: {
        predictedValue: number | null
        opportunityScore: number | null
        capRate: number | null
    }) => void
    predictedValue?: number | null
    opportunityScore?: number | null
    capRate?: number | null
    isChatOpen?: boolean
    isTavusOpen?: boolean
    compareMode?: boolean
    onPinnedCountChange?: (count: number) => void
    mobileBottomBar?: React.ReactNode
    mobileContentOverride?: React.ReactNode
    onMobileClose?: () => void
}

interface PinnedEntry {
    id: string
    data: FanChartData
    historicalValues?: number[]
    label?: string
    coords: [number, number]
    sourceLayer: string
    colorIdx: number // Stable color slot (1-4), assigned at pin time, never changes
}

export function ForecastMap({
    filters,
    mapState,
    year,
    onFeatureSelect,
    onFeatureHover,
    onCoordsChange,
    onGeocodedName,
    className,
    onConsultAI,
    isChatOpen = false,
    isTavusOpen = false,
    compareMode = false,
    onPinnedCountChange,
    mobileBottomBar,
    mobileContentOverride,
    onMobileClose,
}: ForecastMapProps) {
    const mapContainerRef = useRef<HTMLDivElement>(null)
    const mapRef = useRef<maplibregl.Map | null>(null)
    const [isLoaded, setIsLoaded] = useState(false)
    const posthog = usePostHog()

    // Pinned comparisons: persistent multi-area comparison (up to 4)
    const MAX_PINNED = 4
    const PINNED_COLORS = ["#a3e635", "#38bdf8", "#f472b6", "#facc15"] // lime, sky, pink, amber
    const [pinnedComparisons, setPinnedComparisons] = useState<PinnedEntry[]>([])
    const pinnedComparisonsRef = useRef<PinnedEntry[]>([])
    useEffect(() => { pinnedComparisonsRef.current = pinnedComparisons }, [pinnedComparisons])
    useEffect(() => { onPinnedCountChange?.(pinnedComparisons.length) }, [pinnedComparisons.length, onPinnedCountChange])

    // Find the lowest unused color slot (1-4) among current pins
    const getNextColorSlot = (entries: PinnedEntry[]): number => {
        const used = new Set(entries.map(e => e.colorIdx))
        for (let i = 1; i <= MAX_PINNED; i++) {
            if (!used.has(i)) return i
        }
        return 1 // fallback (should not happen if entries.length < MAX_PINNED)
    }

    // Apply feature states for all pinned entries using their stable colorIdx
    const applyPinnedFeatureStates = (map: maplibregl.Map, entries: PinnedEntry[]) => {
        entries.forEach((pc) => {
            ;["forecast-a", "forecast-b"].forEach((s) => {
                try {
                    map.setFeatureState(
                        { source: s, sourceLayer: pc.sourceLayer, id: pc.id },
                        { pinned: true, pinnedIdx: pc.colorIdx }
                    )
                } catch { }
            })
        })
    }

    // Centralized clear: robustly wipes all selected/hovered/pinned feature-state
    const clearAllLocalMapState = (map: maplibregl.Map) => {
        // Clear selected feature-state across ALL layers (zoom may have changed)
        if (selectedIdRef.current) {
            ;["forecast-a", "forecast-b"].forEach((s) => {
                for (const lvl of GEO_LEVELS) {
                    try {
                        map.setFeatureState(
                            { source: s, sourceLayer: lvl.name, id: selectedIdRef.current! },
                            { selected: false }
                        )
                    } catch { }
                }
            })
        }
        // Clear hovered feature-state across ALL layers
        if (hoveredIdRef.current) {
            ;["forecast-a", "forecast-b"].forEach((s) => {
                for (const lvl of GEO_LEVELS) {
                    try {
                        map.setFeatureState(
                            { source: s, sourceLayer: lvl.name, id: hoveredIdRef.current! },
                            { hover: false }
                        )
                    } catch { }
                }
            })
        }
        // Clear pinned feature-state per-feature using stored sourceLayer
        pinnedComparisonsRef.current.forEach((pc) => {
            ;["forecast-a", "forecast-b"].forEach((s) => {
                try {
                    map.setFeatureState(
                        { source: s, sourceLayer: pc.sourceLayer, id: pc.id },
                        { pinned: false, pinnedIdx: 0 }
                    )
                } catch { }
            })
        })
    }

    // Reset all local React state after clearing map state
    const resetLocalState = () => {
        selectedIdRef.current = null
        hoveredIdRef.current = null
        hoveredSourceLayerRef.current = null
        selectedSourceLayerRef.current = null
        setSelectedId(null)
        setSelectedProps(null)
        setTooltipData(null)
        setFixedTooltipPos(null)
        userDraggedRef.current = false
        setSelectedCoords(null)
        setFanChartData(null)
        setHistoricalValues(undefined)
        setComparisonData(null)
        setComparisonHistoricalValues(undefined)
        comparisonFetchRef.current = null
        detailFetchRef.current = null
        setPinnedComparisons([])
    }

    const router = useRouter()
    const searchParams = useSearchParams()
    const schema = searchParams.get("schema") || ""

    // Tooltip state
    const [tooltipData, setTooltipData] = useState<{
        globalX: number
        globalY: number
        properties: any
    } | null>(null)
    const [fixedTooltipPos, setFixedTooltipPos] = useState<{
        globalX: number
        globalY: number
    } | null>(null)
    const [selectedId, setSelectedId] = useState<string | null>(null)
    const selectedIdRef = useRef<string | null>(null)
    const selectedSourceLayerRef = useRef<string | null>(null) // Track which sourceLayer the selection was made on
    const hoveredIdRef = useRef<string | null>(null)
    const hoveredSourceLayerRef = useRef<string | null>(null) // Track which sourceLayer the hover was set on

    // Geographic coordinates for StreetView (from MapLibre events)
    const [tooltipCoords, setTooltipCoords] = useState<[number, number] | null>(null)
    // Coordinates locked to the selected area (for StreetView — doesn't follow hover)
    const [selectedCoords, setSelectedCoords] = useState<[number, number] | null>(null)
    const selectedCoordsRef = useRef<[number, number] | null>(null)
    useEffect(() => { selectedCoordsRef.current = selectedCoords }, [selectedCoords])

    // Notify parent of coordinate changes (for search bar geocoding)
    useEffect(() => { onCoordsChange?.(selectedCoords) }, [selectedCoords, onCoordsChange])

    // Sync external clear_selection (parent sets mapState.selectedId to null)
    useEffect(() => {
        if (mapState.selectedId === null && selectedId !== null) {
            const map = mapRef.current
            if (map) clearAllLocalMapState(map)
            resetLocalState()
            onFeatureSelect(null)
        }
    }, [mapState.selectedId])

    // Restore selection from URL param on initial map load
    const hasRestoredUrlSelection = useRef(false)



    useEffect(() => {
        if (hasRestoredUrlSelection.current) return
        if (!isLoaded || !mapRef.current || !mapState.selectedId) return
        hasRestoredUrlSelection.current = true

        const id = mapState.selectedId
        const map = mapRef.current
        const zoom = map.getZoom()
        const sourceLayer = getSourceLayer(zoom)

        // Apply visual selection state
        selectedIdRef.current = id
        selectedSourceLayerRef.current = sourceLayer
        setSelectedId(id)
        onFeatureSelect(id)

        // Set feature state for outline highlight (delayed to allow tiles to load)
        setTimeout(() => {
            ;["forecast-a", "forecast-b"].forEach((s) => {
                try {
                    map.setFeatureState(
                        { source: s, sourceLayer, id },
                        { selected: true }
                    )
                } catch { /* tiles may not be loaded yet */ }
            })

            // Query rendered features to populate tooltip props
            const activeSuffix = (map as any)._activeSuffix || "a"
            const fillLayerId = `forecast-fill-${sourceLayer}-${activeSuffix}`
            if (map.getLayer(fillLayerId)) {
                const features = map.querySourceFeatures(`forecast-${activeSuffix}`, { sourceLayer })
                const match = features.find(f => (f.properties?.id || f.id) === id)
                if (match?.properties) {
                    setSelectedProps(match.properties)
                } else {
                    setSelectedProps({ id })
                }
            } else {
                setSelectedProps({ id })
            }

            // Position tooltip at a sensible default (right-center of viewport)
            const container = mapContainerRef.current
            if (container) {
                const rect = container.getBoundingClientRect()
                const pos = getSmartTooltipPos(
                    rect.width / 2, rect.height / 2,
                    window.innerWidth, window.innerHeight
                )
                setFixedTooltipPos({ globalX: pos.x, globalY: pos.y })
            }
        }, 1500)

        // Fetch forecast detail data for the selected feature
        fetchForecastDetailRef.current(id, sourceLayer)

        // Set map center coords so geocoding works
        const center = map.getCenter()
        setSelectedCoords([center.lat, center.lng])
    }, [isLoaded, mapState.selectedId])

    // Re-fetch forecast detail when year changes (so fan chart P-values update)
    useEffect(() => {
        const selId = selectedIdRef.current
        const srcLayer = selectedSourceLayerRef.current
        if (selId && srcLayer) {
            fetchForecastDetailRef.current(selId, srcLayer)
        }
    }, [year, schema])

    // Mobile detection
    const [isMobile, setIsMobile] = useState(false)
    useEffect(() => {
        const check = () => setIsMobile(window.innerWidth < 768)
        check()
        window.addEventListener('resize', check)
        return () => window.removeEventListener('resize', check)
    }, [])

    const { isKeyboardOpen, keyboardHeight } = useKeyboardOpen()

    // Mobile swipe-to-minimize state
    const [mobileMinimized, setMobileMinimized] = useState(false)
    const [swipeTouchStart, setSwipeTouchStart] = useState<number | null>(null)
    const [swipeDragOffset, setSwipeDragOffset] = useState(0)
    // Mobile bottom sheet view toggle: 'chart' (default) or 'streetview'
    const [mobileBottomView, setMobileBottomView] = useState<'chart' | 'streetview'>('chart')
    // Mobile StreetView floating overlay
    const [mobileStreetViewOpen, setMobileStreetViewOpen] = useState(false)

    // Desktop drag-to-reposition state (locked tooltip)
    const dragRef = useRef<{ startX: number; startY: number; origX: number; origY: number } | null>(null)
    const userDraggedRef = useRef(false) // true once user has manually repositioned

    useEffect(() => {
        const onMouseMove = (e: MouseEvent) => {
            if (!dragRef.current) return
            const dx = e.clientX - dragRef.current.startX
            const dy = e.clientY - dragRef.current.startY
            const MARGIN = 10
            const rawX = dragRef.current.origX + dx
            const rawY = dragRef.current.origY + dy
            const clampedX = Math.max(MARGIN, Math.min(rawX, window.innerWidth - TOOLTIP_WIDTH - MARGIN))
            const clampedY = Math.max(MARGIN, Math.min(rawY, window.innerHeight - TOOLTIP_HEIGHT - MARGIN))
            setFixedTooltipPos({ globalX: clampedX, globalY: clampedY })
            userDraggedRef.current = true // user chose this position
        }
        const onMouseUp = () => { dragRef.current = null }
        window.addEventListener('mousemove', onMouseMove)
        window.addEventListener('mouseup', onMouseUp)
        return () => { window.removeEventListener('mousemove', onMouseMove); window.removeEventListener('mouseup', onMouseUp) }
    }, [])

    // Reset minimize when selection changes
    useEffect(() => {
        if (selectedId) setMobileMinimized(false)
    }, [selectedId])

    // Reverse geocode when selection changes — adapt to geography scale
    useEffect(() => {
        if (!selectedId || !selectedCoords) {
            setGeocodedName(null)
            return
        }
        const map = mapRef.current
        const zoom = map?.getZoom() || 10
        const geoLevel = selectedSourceLayerRef.current || getSourceLayer(zoom)

        // At State scale, look up name from FIPS code
        if (geoLevel === "state") {
            const fips = selectedId?.padStart(2, '0') || ''
            const name = STATE_FIPS_NAMES[fips]
            setGeocodedName(name || `State ${fips}`)
            return
        }

        // At ZIP Code scale, just show the ZIP code from the feature ID
        if (geoLevel === "zcta") {
            // ZCTA5 IDs are 5-digit ZIP codes
            const zip = selectedId?.length === 5 ? selectedId : selectedId?.slice(-5)
            setGeocodedName(`ZIP ${zip}`)
            return
        }

        const cacheKey = `${geoLevel}:${selectedId}`
        if (geocodeCacheRef.current[cacheKey]) {
            setGeocodedName(geocodeCacheRef.current[cacheKey])
            return
        }
        setGeocodedName(null) // Show loading
        const [lat, lng] = selectedCoords
        const url = `/api/geocode?lat=${lat}&lng=${lng}&level=${geoLevel}&geoid=${selectedId || ''}`
        console.log('[GEOCODE] Fetching primary:', url)
        // Proxy through our API route to avoid CORS issues with Nominatim
        fetch(url)
            .then(r => { console.log('[GEOCODE] Response status:', r.status); return r.ok ? r.json() : null })
            .then(data => {
                console.log('[GEOCODE] Data:', data?.address)
                if (!data) return
                const addr = data.address || {}
                let name: string | null = null
                if (geoLevel === "tract") {
                    // Try to use the address name directly from our enriched backend
                    if (addr.suburb || addr.neighbourhood) {
                        name = addr.suburb || addr.neighbourhood
                    } else {
                        // Fallback numeric parsing if no name found
                        const tractNum = selectedId ? selectedId.slice(-6) : ""
                        if (tractNum && tractNum.length === 6) {
                            const main = parseInt(tractNum.slice(0, 4), 10)
                            const suffix = tractNum.slice(4)
                            name = suffix === "00" ? `Tract ${main}` : `Tract ${main}.${suffix}`
                        } else {
                            name = null
                        }
                    }
                } else if (geoLevel === "parcel") {
                    // Parcel: show full address (e.g. "1747 West 25th Street")
                    const street = addr.road || null
                    const num = addr.house_number || null
                    name = (num && street) ? `${num} ${street}` : street || addr.suburb || addr.neighbourhood || null
                } else {
                    // Block: show street name
                    name = addr.road || addr.suburb || addr.neighbourhood || null
                }
                console.log('[GEOCODE] Resolved name:', name)
                if (name) {
                    geocodeCacheRef.current[cacheKey] = name
                    setGeocodedName(name)
                }
            })
            .catch((err) => { console.error('[GEOCODE] Error:', err) })
    }, [selectedId, selectedCoords])


    // Comparison hover coordinates (separate from tooltipCoords which stays pinned)
    const [comparisonCoords, setComparisonCoords] = useState<[number, number] | null>(null)

    // Reverse geocode comparison feature when hovering — keyed on feature ID, not coords
    useEffect(() => {
        const compId = tooltipData?.properties?.id
        if (!compId || compId === selectedId || !comparisonCoords) {
            setComparisonGeocodedName(null)
            return
        }
        const map = mapRef.current
        const zoom = map?.getZoom() || 10
        const geoLevel = hoveredSourceLayerRef.current || getSourceLayer(zoom)

        if (geoLevel === "state") {
            const fips = compId?.padStart(2, '0') || ''
            const name = STATE_FIPS_NAMES[fips]
            setComparisonGeocodedName(name || `State ${fips}`)
            return
        }

        if (geoLevel === "zcta") {
            const zip = compId?.length === 5 ? compId : compId?.slice(-5)
            setComparisonGeocodedName(`ZIP ${zip}`)
            return
        }

        // Cache by feature ID — hovering within same feature never re-fetches
        const cacheKey = `${geoLevel}:${compId}`
        if (geocodeCacheRef.current[cacheKey]) {
            setComparisonGeocodedName(geocodeCacheRef.current[cacheKey])
            return
        }
        setComparisonGeocodedName(null)
        const [lat, lng] = comparisonCoords
        fetch(`/api/geocode?lat=${lat}&lng=${lng}&level=${geoLevel}`)
            .then(r => r.ok ? r.json() : null)
            .then(data => {
                if (!data) return
                const addr = data.address || {}
                let name: string | null = null
                if (geoLevel === "tract") {
                    const tractNum = compId ? compId.slice(-6) : ""
                    if (tractNum && tractNum.length === 6) {
                        const main = parseInt(tractNum.slice(0, 4), 10)
                        const suffix = tractNum.slice(4)
                        name = suffix === "00" ? `Tract ${main}` : `Tract ${main}.${suffix}`
                    } else {
                        name = addr.suburb || addr.neighbourhood || null
                    }
                } else if (geoLevel === "parcel") {
                    const street = addr.road || null
                    const num = addr.house_number || null
                    name = (num && street) ? `${num} ${street}` : street || addr.suburb || addr.neighbourhood || null
                } else {
                    name = addr.road || addr.suburb || addr.neighbourhood || null
                }
                if (name) {
                    geocodeCacheRef.current[cacheKey] = name
                    setComparisonGeocodedName(name)
                }
            })
            .catch(() => { })
    }, [tooltipData?.properties?.id, selectedId])  // Only re-run when hovered feature ID changes

    // Fan chart detail state
    const [fanChartData, setFanChartData] = useState<FanChartData | null>(null)
    const [historicalValues, setHistoricalValues] = useState<number[] | undefined>(undefined)
    const [isLoadingDetail, setIsLoadingDetail] = useState(false)
    const detailFetchRef = useRef<string | null>(null)
    const detailTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)
    // LRU cache for forecast detail responses (key: "level:featureId", value: {fanChart, historicalValues})
    const detailCacheRef = useRef<Map<string, { fanChart: FanChartData | null; historicalValues: number[] | undefined }>>(new Map())
    const DETAIL_CACHE_MAX = 1000

    // Selected feature's properties (locked when clicked)
    const [selectedProps, setSelectedProps] = useState<any>(null)
    const selectedPropsRef = useRef<any>(null)
    useEffect(() => { selectedPropsRef.current = selectedProps }, [selectedProps])

    // Ref for compareMode so the MapLibre click handler (registered once) always sees the latest value
    const compareModeRef = useRef(compareMode)
    useEffect(() => { compareModeRef.current = compareMode }, [compareMode])

    // Reverse geocoded name for tooltip header
    const [geocodedName, setGeocodedName] = useState<string | null>(null)

    // Bubble geocoded name up to parent (for search bar)
    useEffect(() => {
        onGeocodedName?.(geocodedName)
    }, [geocodedName, onGeocodedName])
    const geocodeCacheRef = useRef<Record<string, string>>({})

    // Comparison state: hover overlay when a feature is selected
    const [comparisonData, setComparisonData] = useState<FanChartData | null>(null)
    const [comparisonHistoricalValues, setComparisonHistoricalValues] = useState<number[] | undefined>(undefined)
    const [comparisonGeocodedName, setComparisonGeocodedName] = useState<string | null>(null)
    const comparisonFetchRef = useRef<string | null>(null)
    const comparisonTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)
    const hoverDetailTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

    // Sync pinned comparisons to URL query params
    const prevPinnedIdsRef = useRef<string>("")
    useEffect(() => {
        const sortedIds = pinnedComparisons.map(p => `${p.sourceLayer}:${p.id}`).sort().join(",")
        if (prevPinnedIdsRef.current === sortedIds) return
        prevPinnedIdsRef.current = sortedIds

        const params = new URLSearchParams(window.location.search)
        if (pinnedComparisons.length > 0) {
            params.set("compare", pinnedComparisons.map(p => `${p.sourceLayer}:${p.id}`).join(","))
        } else {
            params.delete("compare")
        }

        // Use replace state to avoid polluting browser history with every comparison toggle
        const newUrl = `${window.location.pathname}${params.toString() ? `?${params.toString()}` : ""}${window.location.hash}`
        window.history.replaceState({}, "", newUrl)
    }, [pinnedComparisons])
    // Expose pinned data for PDF export and sharing
    useEffect(() => {
        ; (window as any).__getPinnedComparisons = () => pinnedComparisons.map(pc => ({
            label: pc.label || pc.id,
            historicalValues: pc.historicalValues,
            p50: pc.data?.p50,
            p10: pc.data?.p10,
            p90: pc.data?.p90,
            years: pc.data?.years,
        }))
            ; (window as any).__getPinnedIds = () => pinnedComparisons.map(pc => `${pc.sourceLayer}:${pc.id}`)
        return () => {
            delete (window as any).__getPinnedComparisons
            delete (window as any).__getPinnedIds
        }
    }, [pinnedComparisons])

    // Restore pinned comparisons from URL ?compare=layer:id1,layer:id2
    const compareRestoredRef = useRef(false)
    useEffect(() => {
        if (!isLoaded || compareRestoredRef.current) return
        const compareParam = searchParams.get("compare")
        if (!compareParam) return
        compareRestoredRef.current = true

        const map = mapRef.current
        const fallbackLayer = getSourceLayer(map?.getZoom() || 10)

        // Parse entries: "layer:id" or legacy "id" (backwards compat)
        const rawTokens = compareParam.split(",").filter(Boolean).slice(0, MAX_PINNED)
        const parsed = rawTokens.map(tok => {
            const colonIdx = tok.indexOf(":")
            if (colonIdx > 0) {
                return { id: tok.slice(colonIdx + 1), sourceLayer: tok.slice(0, colonIdx) }
            }
            return { id: tok, sourceLayer: fallbackLayer } // legacy format
        })
        if (!parsed.length) return

        // Fetch and pin each comparison
        const restoredEntries: PinnedEntry[] = []
        Promise.all(parsed.map(async ({ id, sourceLayer }) => {
            try {
                const res = await fetch(`/api/forecast-detail?level=${sourceLayer}&id=${encodeURIComponent(id)}&originYear=${originYear}${schema ? `&schema=${schema}` : ""}`)
                if (!res.ok) return
                const json = await res.json()
                const fanChart: FanChartData = {
                    p50: json.p50 || [],
                    p10: json.p10 || [],
                    p90: json.p90 || [],
                    years: json.years || [],
                }

                const entry: PinnedEntry = {
                    id,
                    data: fanChart,
                    historicalValues: json.historicalValues?.some((v: any) => v != null) ? json.historicalValues : undefined,
                    label: id,
                    coords: [0, 0],
                    sourceLayer,
                    colorIdx: restoredEntries.length + 1, // assign sequentially on restore
                }
                restoredEntries.push(entry)
            } catch { /* skip silently */ }
        })).then(() => {
            if (!restoredEntries.length) return
            setPinnedComparisons(prev => {
                const combined = [...prev]
                for (const entry of restoredEntries) {
                    if (combined.some(p => p.id === entry.id && p.sourceLayer === entry.sourceLayer)) continue
                    if (combined.length >= MAX_PINNED) break
                    // Assign colorIdx based on combined state
                    entry.colorIdx = getNextColorSlot(combined)
                    combined.push(entry)
                }
                // Apply feature-state to map after tiles have had time to load
                if (map) {
                    setTimeout(() => applyPinnedFeatureStates(map, combined), 1500)
                }
                return combined
            })
        })
    }, [isLoaded, searchParams, year, schema])
    const [hoverDwell, setHoverDwell] = useState(false)
    const hoverDwellTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

    // Viewport y-axis domain: fixed range from visible features
    const [viewportYDomain, setViewportYDomain] = useState<[number, number] | null>(null)

    // Shift-to-freeze comparison
    const [isShiftHeld, setIsShiftHeld] = useState(false)
    useEffect(() => {
        const down = (e: KeyboardEvent) => { if (e.key === 'Shift') setIsShiftHeld(true) }
        const up = (e: KeyboardEvent) => { if (e.key === 'Shift') setIsShiftHeld(false) }
        window.addEventListener('keydown', down)
        window.addEventListener('keyup', up)
        return () => { window.removeEventListener('keydown', down); window.removeEventListener('keyup', up) }
    }, [])

    // Dynamic origin_year based on Viewport Spatial checks
    // HCAD (Harris County) boundary: ~29.4 to 30.2 Lat, -95.9 to -94.9 Lng
    // HCAD temporarily disabled — using ACS-only (origin_year=2024) everywhere
    // When re-enabled, HCAD will use schema=forecast_hcad (separate DB schema)
    // instead of toggling origin_year, to prevent accidental data source mixing.
    const [mapCenter, setMapCenter] = useState<{ lat: number, lng: number } | null>(null)
    // const isHarrisCounty = mapCenter
    //     ? (mapCenter.lat >= 29.4 && mapCenter.lat <= 30.2 && mapCenter.lng >= -95.9 && mapCenter.lng <= -94.9)
    //     : false
    const isHarrisCounty = false // HCAD off — ACS only

    // ACS uses origin_year=2024 in forecast_queue schema
    // HCAD (when re-enabled) uses origin_year=2025 in forecast_hcad schema
    const originYear = 2024
    const horizonM = (year - originYear) * 12

    // Student inference state: triggers outside Harris County at z≥13
    // Feature flag: set NEXT_PUBLIC_STUDENT_INFERENCE=1 to enable (off by default in prod)
    const studentEnabled = typeof window !== 'undefined' &&
        process.env.NEXT_PUBLIC_STUDENT_INFERENCE === '1'
    // Debug flag: add ?debug=buildings to URL to enable building-level diagnostics
    // Invisible in production — no env var, no code path unless URL param is present
    const debugBuildings = typeof window !== 'undefined' &&
        new URLSearchParams(window.location.search).get('debug')?.includes('buildings')
    const studentFetchRef = useRef<ReturnType<typeof setTimeout> | null>(null)
    const studentAbortRef = useRef<AbortController | null>(null)
    const [studentLoading, setStudentLoading] = useState(false)
    // Debug: collected p50 trajectories from all visible student buildings for spaghetti plot
    const [studentChildLines, setStudentChildLines] = useState<number[][] | undefined>(undefined)
    // Fetch all horizons for a given feature to build FanChart data
    const fetchForecastDetail = useCallback(async (featureId: string, level: string) => {
        // Clear previous timeout
        if (detailTimerRef.current) {
            clearTimeout(detailTimerRef.current)
            detailTimerRef.current = null
        }

        const cacheKey = `${level}:${featureId}`
        // Check cache first — instant re-hover
        const cached = detailCacheRef.current.get(cacheKey)
        if (cached) {
            // Move to end for LRU freshness
            detailCacheRef.current.delete(cacheKey)
            detailCacheRef.current.set(cacheKey, cached)
            setFanChartData(cached.fanChart)
            setHistoricalValues(cached.historicalValues)
            detailFetchRef.current = cacheKey // Prevent duplicate fetches
            return
        }
        if (detailFetchRef.current === cacheKey) return // already fetching
        detailFetchRef.current = cacheKey
        setIsLoadingDetail(true)
        try {
            const schemaParam = schema ? `&schema=${schema}` : ""
            const url = `/api/forecast-detail?level=${level}&id=${encodeURIComponent(featureId)}&originYear=${originYear}${schemaParam}`
            let res = await fetch(url)
            // Retry once on transient server errors (Supabase cold-start / connection pool)
            if (res.status >= 500) {
                await new Promise(r => setTimeout(r, 500))
                res = await fetch(url)
            }
            if (!res.ok) {
                console.warn(`[FORECAST-DETAIL] ${res.status} for ${level}/${featureId} — skipping`)
                setFanChartData(null)
                return
            }
            const json = await res.json()
            const fanChart = json.years?.length > 0 ? (json as FanChartData) : null
            const histVals = json.historicalValues?.some((v: any) => v != null) ? json.historicalValues : undefined
            setFanChartData(fanChart)
            setHistoricalValues(histVals)
            // Store in cache with LRU eviction
            detailCacheRef.current.set(cacheKey, { fanChart, historicalValues: histVals })
            if (detailCacheRef.current.size > DETAIL_CACHE_MAX) {
                // Delete oldest entry (first key in Map iteration order)
                const oldest = detailCacheRef.current.keys().next().value
                if (oldest) detailCacheRef.current.delete(oldest)
            }
        } catch (err) {
            console.warn('[FORECAST-DETAIL] fetch error:', err)
            setFanChartData(null)
        } finally {
            setIsLoadingDetail(false)
        }
    }, [originYear, schema])

    // Ref so closures in the init [] effect always call the latest version
    const fetchForecastDetailRef = useRef(fetchForecastDetail)
    useEffect(() => { fetchForecastDetailRef.current = fetchForecastDetail }, [fetchForecastDetail])

    // VIEW SYNC: Update URL and Origin State when map moves
    // IMPORTANT: Read current params from window.location instead of searchParams
    // to avoid a dependency loop (searchParams changes when ANY param changes,
    // which would re-trigger this effect causing cascading navigations and NetworkErrors).
    const moveEndTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)
    useEffect(() => {
        if (!mapRef.current) return
        const map = mapRef.current

        const onMoveEnd = () => {
            const center = map.getCenter()
            const zoom = map.getZoom()

            // Spatial check for dynamic origin year
            // Only update state if center changed by more than 0.001 to avoid unnecessary re-renders
            setMapCenter(prev => {
                if (!prev) return { lat: center.lat, lng: center.lng }
                if (Math.abs(prev.lat - center.lat) < 0.001 && Math.abs(prev.lng - center.lng) < 0.001) return prev
                return { lat: center.lat, lng: center.lng }
            })

            // Debounce URL updates to coalesce rapid map moves
            if (moveEndTimerRef.current) clearTimeout(moveEndTimerRef.current)
            moveEndTimerRef.current = setTimeout(() => {
                const params = new URLSearchParams(window.location.search)
                params.set("lat", center.lat.toFixed(5))
                params.set("lng", center.lng.toFixed(5))
                params.set("zoom", zoom.toFixed(2))
                router.replace(`?${params.toString()}`, { scroll: false })
            }, 300)

            // ─── Student inference: fetch building predictions outside Harris County at z≥13 ───
            if (!studentEnabled) return  // feature flag off
            const outsideHarris = !(center.lat >= 29.4 && center.lat <= 30.2 && center.lng >= -95.9 && center.lng <= -94.9)
            if (outsideHarris && zoom >= 13) {
                // Debounce: wait 800ms after last move before fetching
                if (studentFetchRef.current) clearTimeout(studentFetchRef.current)
                studentFetchRef.current = setTimeout(async () => {
                    // Abort any in-flight Phase 1 request (Phase 2 is never aborted)
                    if (studentAbortRef.current) studentAbortRef.current.abort()
                    const ac = new AbortController()
                    studentAbortRef.current = ac

                    const bounds = map.getBounds()
                    const bbox = [
                        bounds.getSouth(),
                        bounds.getWest(),
                        bounds.getNorth(),
                        bounds.getEast(),
                    ]

                    setStudentLoading(true)
                    try {
                        // ─── Phase 1: Fetch building footprints only (fast ~17s) ───
                        const res1 = await fetch('/api/student-inference', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ bbox, year, include_forecast: false }),
                            signal: ac.signal,
                        })
                        if (!res1.ok) throw new Error(`HTTP ${res1.status}`)
                        const geojson1 = await res1.json()

                        // Show building outlines immediately (grey — p50=0 triggers fallback color)
                        const src = map.getSource('student-buildings') as maplibregl.GeoJSONSource
                        if (src && geojson1.features) {
                            src.setData(geojson1)
                            console.log(`[STUDENT] Phase 1: ${geojson1.features.length} building outlines loaded`)
                        }

                        // ─── Phase 2: Fetch real forecasts (slow ~4min) ───
                        // NO abort controller — Phase 2 runs to completion even if user pans
                        const phase2Bbox = [...bbox]  // capture bbox at this point
                        fetch('/api/student-inference', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ bbox: phase2Bbox, year, include_forecast: true }),
                            // No signal — intentionally non-cancellable
                        })
                            .then(res2 => {
                                if (!res2.ok) throw new Error(`HTTP ${res2.status}`)
                                return res2.json()
                            })
                            .then(geojson2 => {
                                const src2 = map.getSource('student-buildings') as maplibregl.GeoJSONSource
                                if (src2 && geojson2.features) {
                                    src2.setData(geojson2)
                                    console.log(`[STUDENT] Phase 2: ${geojson2.features.length} buildings with forecasts`)

                                    // ─── Debug diagnostics (only when ?debug=buildings) ───
                                    if (debugBuildings) {
                                        const sample = geojson2.features[0]?.properties
                                        console.log('[STUDENT-DEBUG] Sample feature properties:', JSON.stringify(sample, null, 2))
                                        console.log('[STUDENT-DEBUG] Available fields:', Object.keys(sample || {}))
                                        console.log('[STUDENT-DEBUG] Total features:', geojson2.features.length)
                                            // Expose to devtools console
                                            ; (window as any).__studentBuildings = geojson2.features
                                        console.log('[STUDENT-DEBUG] 💡 Access all buildings via: window.__studentBuildings')
                                    }
                                }
                                setStudentLoading(false)
                            })
                            .catch(err => {
                                console.error('[STUDENT] Phase 2 error:', err.message)
                                setStudentLoading(false)
                            })

                    } catch (err: any) {
                        if (err.name !== 'AbortError') {
                            console.error('[STUDENT] Inference error:', err.message)
                        }
                    } finally {
                        // Don't clear loading here — Phase 2 handles it
                    }
                }, 800)
            } else {
                // Inside Harris County or zoomed out — clear student layer
                const src = map.getSource('student-buildings') as maplibregl.GeoJSONSource
                if (src) {
                    src.setData({ type: 'FeatureCollection', features: [] })
                }
            }
        }

        map.on("moveend", onMoveEnd)
        return () => {
            map.off("moveend", onMoveEnd)
            if (moveEndTimerRef.current) clearTimeout(moveEndTimerRef.current)
        }
    }, [isLoaded, router, year, debugBuildings])

    // HORIZON OR ORIGIN YEAR CHANGED: refresh the HIDDEN source tile URL only.
    // The visible source keeps its current tiles; the swap effect flips visibility
    // once the hidden source finishes loading. This prevents duplicate fetching,
    // cache thrashing, and visible tile disappearance during transitions.
    const TILE_URL_VERSION = "8"
    const buildTileUrl = useCallback((oYear: number, tHorizonM: number, sch: string) => {
        const schemaParam = sch ? `&schema=${sch}` : ""
        return `${window.location.origin}/api/forecast-tiles/{z}/{x}/{y}` +
            `?originYear=${oYear}&horizonM=${tHorizonM}&v=${TILE_URL_VERSION}${schemaParam}`
    }, [])

    useEffect(() => {
        if (!isLoaded || !mapRef.current) return
        const map = mapRef.current

        // For tile rendering, clamp horizonM to >=24 (2026 baseline).
        // Historical mode SQL uses INNER JOIN on history tables that lack data at
        // ZCTA/tract level, producing empty tiles. By staying in forecast mode,
        // tiles always render. The slider year only affects tooltip/chart display.
        const tileHorizonM = Math.max(horizonM, 24)
        const url = buildTileUrl(originYear, tileHorizonM, schema)

        const isFirstMount = !(map as any)._activeSuffix
        if (isFirstMount) {
            // First mount: both sources have url="" — seed them both so the
            // visible source actually renders tiles, and the hidden one is ready
            // for the first swap.
            const srcA = map.getSource("forecast-a") as maplibregl.VectorTileSource | undefined
            const srcB = map.getSource("forecast-b") as maplibregl.VectorTileSource | undefined
            srcA?.setTiles([url])
            srcB?.setTiles([url])
            ;(map as any)._activeSuffix = "a"
        } else {
            // Subsequent changes: only repoint the hidden (next) source —
            // the visible one keeps its current tiles until the swap effect flips.
            const currentSuffix = (map as any)._activeSuffix
            const nextSuffix = currentSuffix === "a" ? "b" : "a"
            const nextSourceId = `forecast-${nextSuffix}`

            const nextSource = map.getSource(nextSourceId) as maplibregl.VectorTileSource | undefined
            if (nextSource) {
                nextSource.setTiles([url])
            }
        }
    }, [year, originYear, horizonM, isLoaded, schema, buildTileUrl])



    // Data-driven growth_pct breakpoints per geo level
    // Fetched once per horizon from /api/forecast-stats?mode=growth
    type GrowthLevelStats = { p5: number; p10: number; p25: number; p50: number; p75: number; p90: number; p95: number; count: number }
    const [growthStats, setGrowthStats] = useState<Record<string, GrowthLevelStats> | null>(null)
    const growthStatsRef = useRef<Record<string, GrowthLevelStats> | null>(null)
    useEffect(() => { growthStatsRef.current = growthStats }, [growthStats])
    const growthStatsFetchedHorizon = useRef<number | null>(null)

    // Fetch growth stats when horizon changes
    useEffect(() => {
        if (horizonM === 0 || horizonM === growthStatsFetchedHorizon.current) return
        growthStatsFetchedHorizon.current = horizonM
        const schemaParam = schema ? `&schema=${schema}` : ""
        fetch(`/api/forecast-stats?mode=growth&originYear=${originYear}&horizonM=${horizonM}${schemaParam}`)
            .then(r => r.ok ? r.json() : null)
            .then(json => {
                if (json?.levels) {
                    setGrowthStats(json.levels)
                    console.log('[GROWTH-STATS] Loaded for horizonM=', horizonM, json.levels)
                }
            })
            .catch(() => { /* fallback to hardcoded */ })
    }, [horizonM, originYear, schema])

    // Color ramp: growth mode uses growth_pct (% change from baseline).
    // Zero-centered: negative growth → blue, zero → neutral white, positive → amber/red.
    // Breakpoints are data-driven from actual growth_pct distributions per geo level.
    // Value mode uses absolute p50 with fixed percentile breakpoints.
    const buildFillColor = (colorMode?: string): any => {
        if (colorMode === "growth") {
            const presentYear = originYear + 2  // 2026 (origin 2024 + 2yr)
            if (year === presentYear || year === originYear) return "#e5e5e5" // No growth relative to self

            // Pick the right level stats based on zoom
            const zoom = mapRef.current?.getZoom() ?? 10
            const stats = growthStatsRef.current
            const levelStats = stats
                ? (zoom <= 7 ? stats.zcta : zoom <= 14 ? stats.tract : stats.tabblock) || stats.tract
                : null

            if (levelStats && levelStats.count > 10) {
                // Data-driven breakpoints: median (p50) is neutral, deviations show color
                // Nominal values mean median growth ≠ 0 (includes inflation)
                // Must enforce strictly ascending for MapLibre (percentiles can be equal
                // when distribution is tight, e.g. at state aggregation level)
                const eps = 0.01
                const raw = [
                    levelStats.p5 ?? -5,
                    levelStats.p25 ?? -2,
                    levelStats.p50 ?? 0,
                    levelStats.p75 ?? 5,
                    levelStats.p95 ?? 20
                ]
                // Walk forward ensuring each value is strictly greater than previous
                for (let i = 1; i < raw.length; i++) {
                    if (raw[i] <= raw[i - 1]) raw[i] = raw[i - 1] + eps
                }
                return [
                    "interpolate",
                    ["linear"],
                    ["coalesce", ["to-number", ["get", "growth_pct"], 0], 0],
                    raw[0], "#3b82f6",     // bottom 5% → deep blue
                    raw[1], "#93c5fd",     // below avg → light blue
                    raw[2], "#f8f8f8",     // median → neutral white
                    raw[3], "#f59e0b",     // above avg → amber
                    raw[4], "#ef4444",     // top 5% → deep red
                ]
            }

            // Fallback: hardcoded formula if stats haven't loaded yet
            const yrsFromPresent = Math.max(Math.abs(year - presentYear), 1)
            const zoomScale = zoom <= 7 ? 0.3 : zoom <= 11 ? 0.5 : zoom <= 14 ? 0.8 : 1.0
            const deepNeg = (-5 - 4 * yrsFromPresent) * zoomScale
            const slightNeg = -2 * yrsFromPresent * zoomScale
            const slightPos = 5 * yrsFromPresent * zoomScale
            const hotPos = 20 * yrsFromPresent * zoomScale
            return [
                "interpolate",
                ["linear"],
                ["coalesce", ["to-number", ["get", "growth_pct"], 0], 0],
                deepNeg, "#3b82f6",
                slightNeg, "#93c5fd",
                0, "#f8f8f8",
                slightPos, "#f59e0b",
                hotPos, "#ef4444",
            ]
        }
        if (colorMode === "growth_dollar") {
            const presentYear = originYear + 2
            const yrsFromPresent = Math.max(Math.abs(year - presentYear), 1)
            const diff = [
                "-",
                ["coalesce", ["get", "p50"], ["get", "value"], 0],
                ["coalesce", ["get", "baseline_value"], ["get", "value"], 0]
            ]
            return [
                "interpolate",
                ["linear"],
                diff,
                -10000 * yrsFromPresent, "#3b82f6", // Deep Blue
                0, "#f8f8f8",      // Whiteish
                30000 * yrsFromPresent, "#ef4444"  // Redish
            ]
        }
        return [
            "interpolate",
            ["linear"],
            ["coalesce", ["get", "p50"], ["get", "value"], 0],
            150000, "#1e1b4b",   // p5
            235000, "#4c1d95",   // p25
            335000, "#7c3aed",   // p50
            525000, "#db2777",   // p75
            1000000, "#fbbf24",  // p95
        ]
    }

    // INITIALIZE MAP
    useEffect(() => {
        if (!mapContainerRef.current) return

        const urlParams = new URLSearchParams(window.location.search)
        const initialLat = parseFloat(urlParams.get("lat") || "40.7484")
        const initialLng = parseFloat(urlParams.get("lng") || "-73.9857")
        const initialZoom = parseFloat(urlParams.get("zoom") || "10")

        const bboxParam = urlParams.get("bbox")
        let initialBounds: [number, number, number, number] | undefined
        if (bboxParam) {
            const parts = bboxParam.split(",").map(Number.parseFloat)
            if (parts.length === 4 && parts.every((p) => !isNaN(p))) {
                initialBounds = parts as [number, number, number, number]
            }
        }

        // DEBUG: patch MapLibre event registration BEFORE map construction.
        // This catches bad calls from plugins/controls/constructor-time code too.
        if (!(window as any).__MAPLIBRE_ON_DEBUG_PATCHED__) {
            ;(window as any).__MAPLIBRE_ON_DEBUG_PATCHED__ = true

            const proto = maplibregl.Map.prototype as any
            const originalOn = proto.on
            const originalOnce = proto.once

            proto.on = function (...args: any[]) {
                const [type, second] = args
                // If second arg is the actual listener (function) but extra args leaked in,
                // strip them to prevent MapLibre treating it as a delegated-layer call.
                if (args.length > 2 && typeof second === "function") {
                    return originalOn.call(this, type, second)
                }
                return originalOn.apply(this, args)
            }

            proto.once = function (...args: any[]) {
                const [type, second] = args
                if (args.length > 2 && typeof second === "function") {
                    return originalOnce.call(this, type, second)
                }
                return originalOnce.apply(this, args)
            }
        }

        const map = new maplibregl.Map({
            container: mapContainerRef.current,
            style: {
                version: 8,
                sources: {
                    osm: {
                        type: "raster",
                        tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
                        tileSize: 256,
                        attribution: "&copy; OpenStreetMap contributors",
                    },
                },
                layers: [
                    {
                        id: "osm-layer",
                        type: "raster",
                        source: "osm",
                    },
                ],
            },
            center: [initialLng, initialLat],
            zoom: initialZoom,
            ...(initialBounds ? { bounds: initialBounds, fitBoundsOptions: { padding: 50 } } : {}),
            maxZoom: 18,
            minZoom: 2,
            // Let MapLibre dynamically size per-source tile cache based on viewport.
            // A hard cap of 30 with multiple sources + A/B swapping caused thrashing.
            canvasContextAttributes: { preserveDrawingBuffer: true }, // Required for canvas.toDataURL() in PDF export
        })

        // Add Geolocate control for user-initiated location
        map.addControl(
            new maplibregl.GeolocateControl({
                positionOptions: {
                    enableHighAccuracy: true
                },
                trackUserLocation: true,
            }),
            'bottom-right'
        )

        map.on("load", () => {
            setIsLoaded(true)

            // Add a crosshatch pattern image synchronously using canvas pixel data
            const patternSize = 20;
            const patternCanvas = document.createElement('canvas');
            patternCanvas.width = patternSize;
            patternCanvas.height = patternSize;
            const ctx = patternCanvas.getContext('2d');
            if (ctx) {
                ctx.strokeStyle = 'rgba(255, 255, 255, 0.4)';
                ctx.lineWidth = 2;
                ctx.beginPath();
                ctx.moveTo(-5, 25);
                ctx.lineTo(25, -5);
                ctx.moveTo(-5, -5);
                ctx.lineTo(25, 25);
                ctx.stroke();
                const imageData = ctx.getImageData(0, 0, patternSize, patternSize);
                map.addImage('crosshatch-pattern', { width: patternSize, height: patternSize, data: new Uint8Array(imageData.data.buffer) });
            }

            const fillColor = buildFillColor()

            // Add A/B sources — seed with the real tile URL immediately.
            // Previously tiles:[] caused MapLibre to crash with
            // "Cannot read properties of undefined (reading 'length')"
            // when its internal parser tried to iterate the empty array.
            const tileHorizonM = Math.max(horizonM, 24)
            const schemaParam = schema ? `&schema=${schema}` : ""
            const initialTileUrl = `${window.location.origin}/api/forecast-tiles/{z}/{x}/{y}` +
                `?originYear=${originYear}&horizonM=${tileHorizonM}&v=${TILE_URL_VERSION}${schemaParam}`

            const addSource = (id: string) => {
                map.addSource(id, {
                    type: "vector",
                    tiles: [initialTileUrl],
                    minzoom: 0,
                    maxzoom: 18,
                    promoteId: "id",
                })
            }

            addSource("forecast-a")
            addSource("forecast-b")
            ;(map as any)._activeSuffix = "a" // Mark as initialized so URL useEffect skips first-mount path

            // For each A/B source, create fill + outline layers for EACH geography level
            // with proper minzoom/maxzoom so MapLibre automatically shows the right one
            const addLayersForSource = (sourceId: string, suffix: string, visible: boolean) => {
                for (const lvl of GEO_LEVELS) {
                    // Fill layer
                    map.addLayer({
                        id: `forecast-fill-${lvl.name}-${suffix}`,
                        type: "fill",
                        source: sourceId,
                        "source-layer": lvl.name,
                        minzoom: lvl.minzoom,
                        maxzoom: lvl.maxzoom + 0.01,
                        layout: { visibility: visible ? "visible" : "none" },
                        paint: {
                            "fill-color": fillColor,
                            "fill-opacity": 0.35,
                            "fill-outline-color": "rgba(255,255,255,0.2)",
                        },
                    })

                    // Crosshatch pattern overlay layer (only visible for selected/pinned features)
                    map.addLayer({
                        id: `forecast-pattern-${lvl.name}-${suffix}`,
                        type: "fill",
                        source: sourceId,
                        "source-layer": lvl.name,
                        minzoom: lvl.minzoom,
                        maxzoom: lvl.maxzoom + 0.01,
                        layout: { visibility: visible ? "visible" : "none" },
                        paint: {
                            "fill-pattern": "crosshatch-pattern",
                            "fill-opacity": [
                                "case",
                                ["boolean", ["feature-state", "selected"], false],
                                1,
                                [">", ["number", ["feature-state", "pinnedIdx"], 0], 0],
                                1,
                                0,
                            ],
                        },
                    })

                    // Outline layer for hover/selected
                    map.addLayer({
                        id: `forecast-outline-${lvl.name}-${suffix}`,
                        type: "line",
                        source: sourceId,
                        "source-layer": lvl.name,
                        minzoom: lvl.minzoom,
                        maxzoom: lvl.maxzoom + 0.01,
                        layout: { visibility: visible ? "visible" : "none" },
                        paint: {
                            "line-color": [
                                "case",
                                ["boolean", ["feature-state", "selected"], false],
                                "#fbbf24",   // amber  — primary selection
                                ["boolean", ["feature-state", "hover"], false],
                                "#a3e635",   // lime   — comparison hover
                                [">", ["number", ["feature-state", "pinnedIdx"], 0], 0],
                                "#f472b6",   // pink   — pinned comparison fallback
                                "rgba(0,0,0,0)",
                            ],
                            "line-width": [
                                "case",
                                ["boolean", ["feature-state", "selected"], false],
                                4,
                                [">", ["number", ["feature-state", "pinnedIdx"], 0], 0],
                                3,
                                ["boolean", ["feature-state", "hover"], false],
                                3,
                                0,
                            ],
                            "line-opacity": [
                                "case",
                                [
                                    "any",
                                    ["boolean", ["feature-state", "selected"], false],
                                    ["boolean", ["feature-state", "hover"], false],
                                    [">", ["number", ["feature-state", "pinnedIdx"], 0], 0],
                                ],
                                1.0,
                                0,
                            ],
                        },
                    })
                }
            }

            addLayersForSource("forecast-a", "a", true)
            addLayersForSource("forecast-b", "b", false)

            // ─── Student inference: GeoJSON source + fill/outline layers for building polygons ───
            map.addSource('student-buildings', {
                type: 'geojson',
                data: { type: 'FeatureCollection', features: [] },
                promoteId: 'id',
            })
            map.addLayer({
                id: 'student-buildings-fill',
                type: 'fill',
                source: 'student-buildings',
                minzoom: 13,
                paint: {
                    'fill-color': [
                        'case',
                        ['==', ['get', 'p50'], 0],
                        '#6b7280',  // Light grey placeholder while forecast is loading (Phase 1)
                        ['interpolate', ['linear'],
                            ['get', 'p50'],
                            100000, '#1e1b4b',
                            200000, '#4c1d95',
                            335000, '#7c3aed',
                            525000, '#db2777',
                            1000000, '#fbbf24',
                        ],
                    ],
                    'fill-opacity': [
                        'case',
                        ['==', ['get', 'p50'], 0],
                        0.65,  // Semi-transparent grey during loading
                        0.8, // Full opacity when forecast is ready
                    ],
                },
            })
            map.addLayer({
                id: 'student-buildings-outline',
                type: 'line',
                source: 'student-buildings',
                minzoom: 13,
                paint: {
                    'line-color': [
                        'case',
                        ['==', ['get', 'p50'], 0],
                        '#67e8f9',  // Bright cyan outline during loading
                        '#ffffff',  // White outline when forecast is ready
                    ],
                    'line-width': [
                        'interpolate', ['linear'], ['zoom'],
                        13, 0.8,
                        16, 1.5,
                        18, 2,
                    ],
                    'line-opacity': 0.85,
                },
            })

            // Throttled tile/source error logging — one message per unique error per 5s window.
            // Previous handler silently swallowed all tile errors, hiding backend 500s/429s.
            const tileErrorSeen: Record<string, number> = {}
            map.on("error", (e: any) => {
                const rawMsg = String(
                    e?.error?.message || e?.message || "Unknown MapLibre error"
                )
                const now = Date.now()
                if (now - (tileErrorSeen[rawMsg] || 0) > 5000) {
                    tileErrorSeen[rawMsg] = now
                    console.warn("[MapLibre tile/source error]", rawMsg, e)
                }
            })


            // HOVER handling
            map.on("mousemove", (e: maplibregl.MapMouseEvent) => {
                const zoom = map.getZoom()
                const sourceLayer = getSourceLayer(zoom)

                // Query fill layers for active suffix
                const activeSuffix = (map as any)._activeSuffix || "a"
                const fillLayerId = `forecast-fill-${sourceLayer}-${activeSuffix}`

                let features = map.getLayer(fillLayerId)
                    ? map.queryRenderedFeatures(e.point, { layers: [fillLayerId] })
                    : []

                // FALLBACK: if finest-grained layer has no features (tiles still loading
                // or sparse tabblock data), try coarser layers that extend as fallback underlays
                let effectiveSourceLayer = sourceLayer
                if (features.length === 0) {
                    for (let i = GEO_LEVELS.length - 1; i >= 0; i--) {
                        const lvl = GEO_LEVELS[i]
                        if (lvl.name === sourceLayer) continue
                        if (zoom < lvl.minzoom || zoom > lvl.maxzoom) continue
                        const fallbackLayerId = `forecast-fill-${lvl.name}-${activeSuffix}`
                        if (!map.getLayer(fallbackLayerId)) continue
                        const fallbackFeatures = map.queryRenderedFeatures(e.point, { layers: [fallbackLayerId] })
                        if (fallbackFeatures.length > 0) {
                            features = fallbackFeatures
                            effectiveSourceLayer = lvl.name
                            break
                        }
                    }
                }

                if (features.length === 0) {
                    // No MVT features — check student buildings layer
                    const studentFeatures = map.getLayer('student-buildings-fill')
                        ? map.queryRenderedFeatures(e.point, { layers: ['student-buildings-fill'] })
                        : []
                    if (studentFeatures.length > 0) {
                        map.getCanvas().style.cursor = 'pointer'
                        const sf = studentFeatures[0]
                        const p = sf.properties
                        // Show student building tooltip using the same tooltip system
                        const smartPos = getSmartTooltipPos(
                            e.originalEvent.clientX,
                            e.originalEvent.clientY,
                            window.innerWidth,
                            window.innerHeight
                        )
                        // Debug badge: building ID + p50 value (only with ?debug=buildings)
                        const debugLabel = debugBuildings
                            ? `🏠 #${sf.id ?? '?'} | p50=$${p?.p50 ? Math.round(p.p50 / 1000) + 'k' : '—'}`
                            : undefined
                        setTooltipData({
                            globalX: smartPos.x,
                            globalY: smartPos.y,
                            properties: {
                                ...p,
                                id: `student-${sf.id || Math.random()}`,
                                p50: p?.p50,
                                p10: p?.p10,
                                p90: p?.p90,
                                _isStudent: true,
                                _debugLabel: debugLabel,
                            },
                        })

                        // Synthesize FanChartData from student model full trajectory
                        if (p.p50_arr) {
                            try {
                                const p10Arr = typeof p.p10_arr === 'string' ? JSON.parse(p.p10_arr) : p.p10_arr;
                                const p50Arr = typeof p.p50_arr === 'string' ? JSON.parse(p.p50_arr) : p.p50_arr;
                                const p90Arr = typeof p.p90_arr === 'string' ? JSON.parse(p.p90_arr) : p.p90_arr;

                                if (Array.isArray(p50Arr) && p50Arr.length > 0) {
                                    // Default forecast displays up to 5 years [2026, 2027, 2028, 2029, 2030]
                                    const len = Math.min(5, p50Arr.length)
                                    const synthYears = [2026, 2027, 2028, 2029, 2030].slice(0, len)

                                    setFanChartData({
                                        years: synthYears,
                                        p10: p10Arr.slice(0, len),
                                        p50: p50Arr.slice(0, len),
                                        p90: p90Arr.slice(0, len),
                                        y_med: p50Arr.slice(0, len)
                                    })
                                    setHistoricalValues(undefined)
                                } else {
                                    setFanChartData(null)
                                    setHistoricalValues(undefined)
                                }
                            } catch (err) {
                                console.error('Failed to parse student fan arrays:', err)
                                setFanChartData(null)
                                setHistoricalValues(undefined)
                            }
                        } else {
                            setFanChartData(null)
                            setHistoricalValues(undefined)
                        }

                        return
                    }
                    // Clear hover — targeted per-feature clear (preserves pinnedIdx on other features)
                    if (hoveredIdRef.current) {
                        const hoverSL = hoveredSourceLayerRef.current || effectiveSourceLayer
                        ;["forecast-a", "forecast-b"].forEach((s) => {
                            try {
                                map.setFeatureState(
                                    { source: s, sourceLayer: hoverSL, id: hoveredIdRef.current! },
                                    { hover: false }
                                )
                            } catch (err) {
                                /* ignore */
                            }
                        })
                        hoveredIdRef.current = null
                        hoveredSourceLayerRef.current = null
                        if (!selectedIdRef.current) {
                            setTooltipData(null)
                        }
                        onFeatureHover(null)
                        // Clear street view dwell timer
                        if (hoverDwellTimerRef.current) { clearTimeout(hoverDwellTimerRef.current); hoverDwellTimerRef.current = null }
                        setHoverDwell(false)
                    }
                    map.getCanvas().style.cursor = ""
                    return
                }

                map.getCanvas().style.cursor = "pointer"
                const feature = features[0]
                const id = (feature.properties?.id || feature.id) as string
                if (!id) return

                // Clear previous hover
                const isNewFeature = hoveredIdRef.current !== id
                if (hoveredIdRef.current && isNewFeature) {
                    const prevHoverSL = hoveredSourceLayerRef.current || effectiveSourceLayer
                    ;["forecast-a", "forecast-b"].forEach((s) => {
                        try {
                            map.setFeatureState(
                                { source: s, sourceLayer: prevHoverSL, id: hoveredIdRef.current! },
                                { hover: false }
                            )
                        } catch (err) {
                            /* ignore */
                        }
                    })
                }

                hoveredIdRef.current = id
                hoveredSourceLayerRef.current = effectiveSourceLayer
                onFeatureHover(id)

                    // Set hover state
                    ;["forecast-a", "forecast-b"].forEach((s) => {
                        try {
                            map.setFeatureState(
                                { source: s, sourceLayer: effectiveSourceLayer, id },
                                { hover: true }
                            )
                        } catch (err) {
                            /* ignore */
                        }
                    })


                // Debounced fan chart fetch on hover (only when NOT locked, only on new feature)
                if (!selectedIdRef.current && isNewFeature) {
                    if (hoverDetailTimerRef.current) clearTimeout(hoverDetailTimerRef.current)
                    hoverDetailTimerRef.current = setTimeout(() => {
                        fetchForecastDetailRef.current(id, effectiveSourceLayer)
                    }, 500)

                    // Street view dwell hover: 1.5s on same feature before loading images
                    if (hoverDwellTimerRef.current) clearTimeout(hoverDwellTimerRef.current)
                    setHoverDwell(false)
                    hoverDwellTimerRef.current = setTimeout(() => setHoverDwell(true), 1500)
                }

                if (selectedIdRef.current) {
                    // Locked mode: DON'T move tooltip (it stays pinned), but DO update
                    // tooltipData.properties with the hovered feature so comparison works.
                    setTooltipData(prev => prev ? { ...prev, properties: feature.properties } : prev)
                    setComparisonCoords([e.lngLat.lat, e.lngLat.lng])
                    return
                }

                // Unlocked: tooltip follows cursor
                const smartPos = getSmartTooltipPos(
                    e.originalEvent.clientX,
                    e.originalEvent.clientY,
                    window.innerWidth,
                    window.innerHeight
                )
                setTooltipData({
                    globalX: smartPos.x,
                    globalY: smartPos.y,
                    properties: feature.properties,
                })
                // Only update coords (used for StreetView) when the feature changes, not every pixel
                if (isNewFeature) {
                    setTooltipCoords([e.lngLat.lat, e.lngLat.lng])
                }
            })

            // MOBILE LONG-PRESS HOVER: simulate hover/comparison on touch hold
            let longPressTimer: ReturnType<typeof setTimeout> | null = null
            let longPressActive = false
            let touchStartPos: { x: number; y: number } | null = null

            const simulateHoverAtPoint = (clientX: number, clientY: number) => {
                if (!selectedIdRef.current) return
                const point = map.project(map.unproject([clientX - map.getCanvas().getBoundingClientRect().left, clientY - map.getCanvas().getBoundingClientRect().top]))
                const zoom = map.getZoom()
                const sourceLayer = getSourceLayer(zoom)
                const activeSuffix = (map as any)._activeSuffix || "a"
                const fillLayerId = `forecast-fill-${sourceLayer}-${activeSuffix}`
                let features = map.getLayer(fillLayerId)
                    ? map.queryRenderedFeatures(point, { layers: [fillLayerId] })
                    : []
                // FALLBACK: try coarser layers if finest has no features
                if (features.length === 0) {
                    for (let i = GEO_LEVELS.length - 1; i >= 0; i--) {
                        const lvl = GEO_LEVELS[i]
                        if (lvl.name === sourceLayer) continue
                        if (zoom < lvl.minzoom || zoom > lvl.maxzoom) continue
                        const fb = `forecast-fill-${lvl.name}-${activeSuffix}`
                        if (!map.getLayer(fb)) continue
                        const fbFeatures = map.queryRenderedFeatures(point, { layers: [fb] })
                        if (fbFeatures.length > 0) { features = fbFeatures; break }
                    }
                }
                if (features.length === 0) return
                const feature = features[0]
                const id = (feature.properties?.id || feature.id) as string
                if (!id || id === selectedIdRef.current) return
                // Update hover state for comparison
                hoveredIdRef.current = id
                onFeatureHover(id)
                setTooltipData(prev => prev ? { ...prev, properties: feature.properties } : prev)
            }

            map.getCanvas().addEventListener("touchstart", (e: TouchEvent) => {
                if (!selectedIdRef.current) return
                const touch = e.touches[0]
                touchStartPos = { x: touch.clientX, y: touch.clientY }
                longPressActive = false
                longPressTimer = setTimeout(() => {
                    longPressActive = true
                    simulateHoverAtPoint(touch.clientX, touch.clientY)
                }, 400)
            }, { passive: true })

            map.getCanvas().addEventListener("touchmove", (e: TouchEvent) => {
                const touch = e.touches[0]
                if (touchStartPos) {
                    const dx = touch.clientX - touchStartPos.x
                    const dy = touch.clientY - touchStartPos.y
                    if (Math.sqrt(dx * dx + dy * dy) > 10 && !longPressActive) {
                        // Moved too much before long-press activated — cancel
                        if (longPressTimer) { clearTimeout(longPressTimer); longPressTimer = null }
                        return
                    }
                }
                if (longPressActive) {
                    e.preventDefault()
                    simulateHoverAtPoint(touch.clientX, touch.clientY)
                }
            })

            map.getCanvas().addEventListener("touchend", () => {
                if (longPressTimer) { clearTimeout(longPressTimer); longPressTimer = null }
                longPressActive = false
                touchStartPos = null
            })

        })

        // MOUSELEAVE: clear tooltip when cursor exits the map (unless locked)
        map.getCanvas().addEventListener("mouseleave", () => {
            if (hoverDetailTimerRef.current) { clearTimeout(hoverDetailTimerRef.current); hoverDetailTimerRef.current = null }
            if (!selectedIdRef.current) {
                setTooltipData(null)
                detailFetchRef.current = null // allow re-fetch on re-hover
            }
            if (hoveredIdRef.current) {
                const hoverSL = hoveredSourceLayerRef.current || getSourceLayer(map.getZoom())
                    ;["forecast-a", "forecast-b"].forEach((s) => {
                        try {
                            map.setFeatureState(
                                { source: s, sourceLayer: hoverSL, id: hoveredIdRef.current! },
                                { hover: false }
                            )
                        } catch { }
                    })
                hoveredIdRef.current = null
                hoveredSourceLayerRef.current = null
                onFeatureHover(null)
                // Clear comparison data when mouse leaves (locked mode)
                if (selectedIdRef.current) {
                    setComparisonData(null)
                    setComparisonHistoricalValues(undefined)
                    comparisonFetchRef.current = null
                    // Restore tooltip to show selected feature's props (not last-hovered)
                    setTooltipData(prev => {
                        if (!prev) return prev
                        const sp = selectedPropsRef.current
                        return sp ? { ...prev, properties: sp } : prev
                    })
                }
            }
            map.getCanvas().style.cursor = ""
        })

        // CLICK handling
        map.on("click", (e: maplibregl.MapMouseEvent) => {
            const zoom = map.getZoom()
            const sourceLayer = getSourceLayer(zoom)
            const activeSuffix = (map as any)._activeSuffix || "a"
            const fillLayerId = `forecast-fill-${sourceLayer}-${activeSuffix}`

            let features = map.getLayer(fillLayerId)
                ? map.queryRenderedFeatures(e.point, { layers: [fillLayerId] })
                : []

            // FALLBACK: if finest-grained layer has no features (tiles still loading on mobile),
            // try coarser layers that extend as fallback underlays (tract → zcta)
            let effectiveSourceLayer = sourceLayer
            if (features.length === 0) {
                for (let i = GEO_LEVELS.length - 1; i >= 0; i--) {
                    const lvl = GEO_LEVELS[i]
                    if (lvl.name === sourceLayer) continue // already tried
                    if (zoom < lvl.minzoom || zoom > lvl.maxzoom) continue
                    const fallbackLayerId = `forecast-fill-${lvl.name}-${activeSuffix}`
                    if (!map.getLayer(fallbackLayerId)) continue
                    const fallbackFeatures = map.queryRenderedFeatures(e.point, { layers: [fallbackLayerId] })
                    if (fallbackFeatures.length > 0) {
                        features = fallbackFeatures
                        effectiveSourceLayer = lvl.name
                        console.log(`[CLICK FALLBACK] ${sourceLayer} had 0 features, fell back to ${lvl.name} (${fallbackFeatures.length} features)`)
                        break
                    }
                }
            }

            // Clear hover detail timer so click fetch takes precedence
            if (hoverDetailTimerRef.current) { clearTimeout(hoverDetailTimerRef.current); hoverDetailTimerRef.current = null }
            detailFetchRef.current = null // allow click to re-fetch even if same id

            // Always check student buildings when the layer exists
            const hasStudentLayer = !!map.getLayer('student-buildings-fill')
            const studentFeatures = hasStudentLayer
                ? map.queryRenderedFeatures(e.point, { layers: ['student-buildings-fill'] })
                : []

            console.log(`[CLICK DEBUG] zoom=${zoom.toFixed(1)} layer=${effectiveSourceLayer} mvtFeatures=${features.length} studentFeatures=${studentFeatures.length} hasStudentLayer=${hasStudentLayer}`)

            // Prioritize student buildings when we have them and no MVT features
            if (features.length === 0 && studentFeatures.length > 0) {
                const sf = studentFeatures[0]
                const id = `student-${sf.id || Math.random()}`

                // Toggle selection logic for student building
                if (selectedIdRef.current === id) {
                    clearAllLocalMapState(map)
                    resetLocalState()
                    onFeatureSelect(null)
                    return
                }

                // Clear any existing MVT primary/pins before installing student selection
                if (selectedIdRef.current) {
                    clearAllLocalMapState(map)
                    resetLocalState()
                }

                selectedIdRef.current = id
                selectedSourceLayerRef.current = "student"
                setSelectedId(id)
                const p = sf.properties
                const enhancedProps = {
                    ...p,
                    id,
                    p50: p?.p50,
                    p10: p?.p10,
                    p90: p?.p90,
                    _isStudent: true,
                }
                setSelectedProps(enhancedProps)
                setComparisonData(null)
                setComparisonHistoricalValues(undefined)
                comparisonFetchRef.current = null
                onFeatureSelect(id)

                // Debug: collect all visible student buildings' p50 trajectories for spaghetti plot
                if (debugBuildings) {
                    try {
                        const src = map.getSource('student-buildings') as maplibregl.GeoJSONSource
                        const srcData = (src as any)?._data
                        if (srcData?.features) {
                            const lines: number[][] = []
                            for (const feat of srcData.features) {
                                const fp = feat.properties
                                if (!fp?.p50_arr) continue
                                const arr = typeof fp.p50_arr === 'string' ? JSON.parse(fp.p50_arr) : fp.p50_arr
                                if (Array.isArray(arr) && arr.length > 0) {
                                    // Pad with NaN for historical years (2019-2025 = 7 values) + forecast years
                                    const padded = Array(7).fill(NaN).concat(arr.slice(0, 5))
                                    lines.push(padded)
                                }
                            }
                            console.log(`[STUDENT-DEBUG] Collected ${lines.length} building trajectories for childLines`)
                            setStudentChildLines(lines.length > 0 ? lines : undefined)
                        }
                    } catch (err) {
                        console.error('[STUDENT-DEBUG] Failed to collect childLines:', err)
                        setStudentChildLines(undefined)
                    }
                } else {
                    setStudentChildLines(undefined)
                }

                // Synthesize FanChartData from student model full trajectory
                console.log('[STUDENT CLICK] Properties:', JSON.stringify({
                    p50: p.p50,
                    p50_arr_type: typeof p.p50_arr,
                    p50_arr_sample: typeof p.p50_arr === 'string' ? p.p50_arr.slice(0, 100) : p.p50_arr,
                    p10_arr_type: typeof p.p10_arr,
                    p90_arr_type: typeof p.p90_arr,
                }))

                // Helper to robustly parse arrays from MapLibre properties
                const parseArr = (val: any): number[] => {
                    if (Array.isArray(val)) return val
                    if (typeof val === 'string') {
                        try {
                            const parsed = JSON.parse(val)
                            if (Array.isArray(parsed)) return parsed
                        } catch {
                            // MapLibre might store as comma-separated values
                            const nums = val.split(',').map(Number).filter(n => !isNaN(n))
                            if (nums.length > 0) return nums
                        }
                    }
                    return []
                }

                if (p.p50_arr) {
                    try {
                        const p10Arr = parseArr(p.p10_arr)
                        const p50Arr = parseArr(p.p50_arr)
                        const p90Arr = parseArr(p.p90_arr)

                        console.log('[STUDENT CLICK] Parsed arrays:', {
                            p10: p10Arr.slice(0, 3),
                            p50: p50Arr.slice(0, 3),
                            p90: p90Arr.slice(0, 3),
                            len: p50Arr.length,
                        })

                        if (Array.isArray(p50Arr) && p50Arr.length > 0) {
                            const len = Math.min(5, p50Arr.length)
                            const synthYears = [2026, 2027, 2028, 2029, 2030].slice(0, len)

                            setFanChartData({
                                years: synthYears,
                                p10: p10Arr.slice(0, len),
                                p50: p50Arr.slice(0, len),
                                p90: p90Arr.slice(0, len),
                                y_med: p50Arr.slice(0, len)
                            })
                            setHistoricalValues(undefined)
                            console.log('[STUDENT CLICK] ✅ setFanChartData called with', len, 'years')
                        } else {
                            console.log('[STUDENT CLICK] ⚠️ p50Arr empty or not array')
                            setFanChartData(null)
                            setHistoricalValues(undefined)
                        }
                    } catch (err) {
                        console.error('[STUDENT CLICK] Failed to parse student fan arrays:', err)
                        setFanChartData(null)
                        setHistoricalValues(undefined)
                    }
                } else {
                    console.log('[STUDENT CLICK] ⚠️ No p50_arr property found')
                    setFanChartData(null)
                    setHistoricalValues(undefined)
                }

                // Tooltip positioning
                const smartPos = getSmartTooltipPos(
                    e.originalEvent.clientX,
                    e.originalEvent.clientY,
                    window.innerWidth,
                    window.innerHeight
                )
                if (!userDraggedRef.current) {
                    setFixedTooltipPos({ globalX: smartPos.x, globalY: smartPos.y })
                }
                setTooltipData({
                    globalX: smartPos.x,
                    globalY: smartPos.y,
                    properties: enhancedProps,
                })
                setTooltipCoords([e.lngLat.lat, e.lngLat.lng])
                setSelectedCoords([e.lngLat.lat, e.lngLat.lng])
                return
            } else if (features.length === 0) {

                // Normal clear selection
                if (selectedIdRef.current) {
                    clearAllLocalMapState(map)
                    resetLocalState()
                    onFeatureSelect(null)
                }
                return
            } else if (features.length > 0) {

                const feature = features[0]
                const id = (feature.properties?.id || feature.id) as string
                if (!id) return

                // 1) Toggle off same primary — full clear
                if (selectedIdRef.current === id) {
                    clearAllLocalMapState(map)
                    resetLocalState()
                    onFeatureSelect(null)
                    return
                }

                // Shift+click OR Compare mode: PIN this area for persistent comparison
                if ((e.originalEvent.shiftKey || compareModeRef.current) && selectedIdRef.current) {
                    const existingIdx = pinnedComparisonsRef.current.findIndex(p => p.id === id && p.sourceLayer === effectiveSourceLayer)
                    if (existingIdx !== -1) {
                        // Unpin — clear only this feature, survivors keep their colorIdx
                        const pinEntry = pinnedComparisonsRef.current[existingIdx]
                        ;["forecast-a", "forecast-b"].forEach(s => {
                            try { map.setFeatureState({ source: s, sourceLayer: pinEntry.sourceLayer, id }, { pinned: false, pinnedIdx: 0 }) } catch { }
                        })
                        setPinnedComparisons(prev => prev.filter(p => !(p.id === id && p.sourceLayer === effectiveSourceLayer)))
                    } else {
                        // Pin: fetch detail and add
                        const pinLevel = effectiveSourceLayer
                        const cacheKey = `${pinLevel}:${id}`
                        const cached = detailCacheRef.current.get(cacheKey)

                        const addPin = (fanChart: FanChartData | null, histVals: number[] | undefined) => {
                            if (!fanChart) return
                            // Reverse geocode for label
                            let label = id
                            import("@/app/actions/geocode").then(mod => {
                                mod.reverseGeocode(e.lngLat.lat, e.lngLat.lng, 18).then(name => {
                                    if (name) {
                                        setPinnedComparisons(prev => prev.map(p => (p.id === id && p.sourceLayer === effectiveSourceLayer) ? { ...p, label: name } : p))
                                    }
                                }).catch(() => { })
                            }).catch(() => { })

                            const newPin: PinnedEntry = {
                                id,
                                data: fanChart,
                                historicalValues: histVals,
                                label,
                                coords: [e.lngLat.lat, e.lngLat.lng],
                                sourceLayer: effectiveSourceLayer,
                                colorIdx: 1, // placeholder, assigned below in setPinnedComparisons
                            }
                            setPinnedComparisons(prev => {
                                // Replace oldest if at max
                                if (prev.length >= MAX_PINNED) {
                                    const removed = prev[0]
                                        ;["forecast-a", "forecast-b"].forEach(s => {
                                            try { map.setFeatureState({ source: s, sourceLayer: removed.sourceLayer, id: removed.id }, { pinned: false, pinnedIdx: 0 }) } catch { }
                                        })
                                    // Inherit the removed pin's color slot
                                    newPin.colorIdx = removed.colorIdx
                                    const updated = [...prev.slice(1), newPin]
                                    // Apply only the new pin's feature state
                                    ;["forecast-a", "forecast-b"].forEach((s) => {
                                        try {
                                            map.setFeatureState(
                                                { source: s, sourceLayer: effectiveSourceLayer, id },
                                                { pinned: true, pinnedIdx: newPin.colorIdx }
                                            )
                                        } catch { }
                                    })
                                    return updated
                                }
                                // Assign next available color slot
                                newPin.colorIdx = getNextColorSlot(prev)
                                const updated = [...prev, newPin]
                                ;["forecast-a", "forecast-b"].forEach((s) => {
                                    try {
                                        map.setFeatureState(
                                            { source: s, sourceLayer: effectiveSourceLayer, id },
                                            { pinned: true, pinnedIdx: newPin.colorIdx }
                                        )
                                    } catch { }
                                })
                                return updated
                            })
                        }

                        if (cached) {
                            addPin(cached.fanChart, cached.historicalValues)
                        } else {
                            // Fetch
                            const schemaParam = schema ? `&schema=${schema}` : ""
                            fetch(`/api/forecast-detail?level=${pinLevel}&id=${encodeURIComponent(id)}&originYear=${originYear}${schemaParam}`)
                                .then(res => res.ok ? res.json() : null)
                                .then(json => {
                                    if (!json) return
                                    const fanChart = json.years?.length > 0 ? (json as FanChartData) : null
                                    const histVals = json.historicalValues?.some((v: any) => v != null) ? json.historicalValues : undefined
                                    if (fanChart) {
                                        detailCacheRef.current.set(cacheKey, { fanChart, historicalValues: histVals })
                                    }
                                    addPin(fanChart, histVals)
                                })
                                .catch(() => { })
                        }
                    }
                    return // Don't change primary selection
                }

                // 3) Only now clear the old primary — we are replacing it with a new one
                if (selectedIdRef.current) {
                    const prevLayer = selectedSourceLayerRef.current || effectiveSourceLayer
                    ;["forecast-a", "forecast-b"].forEach((s) => {
                        try {
                            map.setFeatureState(
                                { source: s, sourceLayer: prevLayer, id: selectedIdRef.current! },
                                { selected: false }
                            )
                        } catch (err) {
                            /* ignore */
                        }
                    })
                }

                selectedIdRef.current = id
                selectedSourceLayerRef.current = effectiveSourceLayer
                setSelectedId(id)
                setSelectedProps(feature.properties)
                setComparisonData(null)
                setComparisonHistoricalValues(undefined)
                comparisonFetchRef.current = null
                // Clear all pinned comparisons when changing primary selection
                pinnedComparisonsRef.current.forEach(pc => {
                    ;["forecast-a", "forecast-b"].forEach(s => {
                        try { map.setFeatureState({ source: s, sourceLayer: pc.sourceLayer, id: pc.id }, { pinned: false, pinnedIdx: 0 }) } catch { }
                    })
                })
                setPinnedComparisons([])
                onFeatureSelect(id)

                // PostHog: track map area selection
                try {
                    posthog?.capture('map_area_clicked', {
                        feature_id: id,
                        geo_level: effectiveSourceLayer,
                        lat: e.lngLat.lat,
                        lng: e.lngLat.lng,
                        zoom: map.getZoom(),
                    })
                } catch { }

                // Fetch fan chart detail for newly selected area (critical on mobile where hover doesn't fire)
                fetchForecastDetailRef.current(id, effectiveSourceLayer)

                    // Set selected state
                    ;["forecast-a", "forecast-b"].forEach((s) => {
                        try {
                            map.setFeatureState(
                                { source: s, sourceLayer: effectiveSourceLayer, id },
                                { selected: true }
                            )
                        } catch (err) {
                            /* ignore */
                        }
                    })

                // Fetch fan chart detail for newly selected area (critical on mobile where hover doesn't fire)
                fetchForecastDetailRef.current(id, effectiveSourceLayer)

                // Fix tooltip position
                const smartPos = getSmartTooltipPos(
                    e.originalEvent.clientX,
                    e.originalEvent.clientY,
                    window.innerWidth,
                    window.innerHeight
                )
                // If user has manually dragged the tooltip, keep it at that position.
                // Otherwise, position it near the new click.
                if (!userDraggedRef.current) {
                    setFixedTooltipPos({ globalX: smartPos.x, globalY: smartPos.y })
                }
                setTooltipData({
                    globalX: smartPos.x,
                    globalY: smartPos.y,
                    properties: feature.properties,
                })
                setTooltipCoords([e.lngLat.lat, e.lngLat.lng])
                setSelectedCoords([e.lngLat.lat, e.lngLat.lng])
            }
        })

            // Store refs
            ; (map as any)._activeSuffix = "a"
            ; (map as any)._isLoaded = true
        mapRef.current = map

        return () => {
            if (hoverDetailTimerRef.current) { clearTimeout(hoverDetailTimerRef.current); hoverDetailTimerRef.current = null }
            if (hoverDwellTimerRef.current) { clearTimeout(hoverDwellTimerRef.current); hoverDwellTimerRef.current = null }
            if (comparisonTimerRef.current) { clearTimeout(comparisonTimerRef.current); comparisonTimerRef.current = null }
            if (moveEndTimerRef.current) { clearTimeout(moveEndTimerRef.current); moveEndTimerRef.current = null }
            if (studentFetchRef.current) { clearTimeout(studentFetchRef.current); studentFetchRef.current = null }
            if (studentAbortRef.current) { studentAbortRef.current.abort(); studentAbortRef.current = null }
            map.remove()
        }
    }, []) // Init once

    // ESC key handler to clear selection
    useEffect(() => {
        const handleKeyDown = (e: KeyboardEvent) => {
            if (e.key === "Escape" && selectedIdRef.current) {
                const map = mapRef.current
                if (map) clearAllLocalMapState(map)
                if (hoverDetailTimerRef.current) { clearTimeout(hoverDetailTimerRef.current); hoverDetailTimerRef.current = null }
                resetLocalState()
                onFeatureSelect(null)
            }
        }
        window.addEventListener("keydown", handleKeyDown)
        return () => window.removeEventListener("keydown", handleKeyDown)
    }, [onFeatureSelect])

    // Dynamic hover outline color: when a primary is selected, hover previews
    // the color that will be assigned to the NEXT pinned comparable
    useEffect(() => {
        const map = mapRef.current
        if (!map || !isLoaded) return
        // Hover color = next available pin slot's color (preview)
        const nextSlot = selectedId ? getNextColorSlot(pinnedComparisons) : 0
        const hoverColor = selectedId ? PINNED_COLORS[(nextSlot - 1) % PINNED_COLORS.length] : "#fbbf24"

        // Update outline colors — pinned state with per-index colors + hover preview
        for (const suffix of ["a", "b"]) {
            for (const lvl of GEO_LEVELS) {
                const layerId = `forecast-outline-${lvl.name}-${suffix}`
                if (!map.getLayer(layerId)) continue
                try {
                    map.setPaintProperty(layerId, "line-color", [
                        "case",
                        ["boolean", ["feature-state", "selected"], false],
                        "#fbbf24",   // amber — always for locked selection
                        [">", ["number", ["feature-state", "pinnedIdx"], 0], 0],
                        ["match", ["number", ["feature-state", "pinnedIdx"], 0],
                            1, "#a3e635",  // Lime (1st comp)
                            2, "#38bdf8",  // Sky  (2nd comp)
                            3, "#f472b6",  // Pink (3rd comp)
                            4, "#facc15",  // Amber (4th comp)
                            "#a3e635",     // fallback
                        ],
                        ["boolean", ["feature-state", "hover"], false],
                        hoverColor,  // previews the next pin's color
                        "rgba(0,0,0,0)",
                    ])
                } catch { /* layer may not exist yet */ }
            }
        }
    }, [selectedId, isLoaded, pinnedComparisons])

    // Tavus map-action handler: clear_selection, fly_to_location
    useEffect(() => {
        const handleTavusAction = (e: Event) => {
            const { action, params } = (e as CustomEvent).detail || {}
            if (action === "clear_selection") {
                const map = mapRef.current
                if (map && selectedIdRef.current) {
                    const zoom = map.getZoom()
                    clearAllLocalMapState(map)
                }
                resetLocalState()
                onFeatureSelect(null)
            } else if (action === "clear_comparison") {
                // Clear only the comparison overlay, keep primary selection
                console.log('[ForecastMap] clear_comparison')
                setComparisonData(null)
                setComparisonHistoricalValues(undefined)
                comparisonFetchRef.current = null
            } else if (action === "fly_to_location" && params) {
                const map = mapRef.current
                if (map && params.lat && params.lng) {
                    const targetZoom = params.zoom || 14
                    map.flyTo({
                        center: [params.lng, params.lat],
                        zoom: targetZoom,
                        duration: 2000,
                    })
                    // Only auto-select if there's no existing selection
                    // fly_to_location is purely pan/zoom when a feature is already selected
                    if (!selectedIdRef.current) {
                        const attemptSelect = (retries: number) => {
                            const activeSuffix = (map as any)._activeSuffix || "a"
                            const center = map.project(map.getCenter())
                            const allFillLayers = getAllFillLayerIds(activeSuffix).filter(id => map.getLayer(id))
                            let features = map.queryRenderedFeatures(center, { layers: allFillLayers })
                            if (features.length === 0) {
                                const bbox: [maplibregl.PointLike, maplibregl.PointLike] = [
                                    [center.x - 10, center.y - 10],
                                    [center.x + 10, center.y + 10],
                                ]
                                features = map.queryRenderedFeatures(bbox, { layers: allFillLayers })
                            }
                            if (features.length > 0) {
                                const feature = features[0]
                                const sourceLayer = feature.sourceLayer || getSourceLayer(map.getZoom())
                                const id = (feature.properties?.id || feature.id) as string
                                if (id) {
                                    selectedIdRef.current = id
                                    selectedSourceLayerRef.current = sourceLayer
                                    setSelectedId(id)
                                    setSelectedProps(feature.properties)
                                    setSelectedCoords([params.lat, params.lng])
                                    setTooltipCoords([params.lat, params.lng])
                                    onFeatureSelect(id)
                                    const centerScreen = map.project(map.getCenter())
                                    const rect = map.getCanvas().getBoundingClientRect()
                                    const screenX = rect.left + centerScreen.x
                                    const screenY = rect.top + centerScreen.y
                                    const smartPos = getSmartTooltipPos(screenX, screenY, window.innerWidth, window.innerHeight)
                                    setFixedTooltipPos({ globalX: smartPos.x, globalY: smartPos.y })
                                    setTooltipData({ globalX: smartPos.x, globalY: smartPos.y, properties: feature.properties })
                                    userDraggedRef.current = false
                                        ;["forecast-a", "forecast-b"].forEach((s) => {
                                            try { map.setFeatureState({ source: s, sourceLayer, id }, { selected: true }) } catch { }
                                        })
                                    fetchForecastDetailRef.current(id, sourceLayer)
                                }
                            } else if (retries > 0) {
                                console.log(`[ForecastMap] fly_to_location: No features at center, retrying... (${retries} left)`)
                                setTimeout(() => attemptSelect(retries - 1), 1200)
                            }
                        }
                        map.once("idle", () => attemptSelect(8))
                    }
                }
            } else if (action === "add_location_to_selection") {
                // COMPARISON: keep primary selection, zoom to fit both, overlay comparison data
                const result = (e as CustomEvent).detail?.result || params
                const newLat = result?.chosen?.lat || result?.location?.lat || result?.area?.location?.lat || params?.lat
                const newLng = result?.chosen?.lng || result?.location?.lng || result?.area?.location?.lng || params?.lng
                const map = mapRef.current
                if (map && newLat && newLng) {
                    if (selectedIdRef.current && selectedCoordsRef.current) {
                        // Primary is locked — zoom to fit both areas
                        const [selLat, selLng] = selectedCoordsRef.current!
                        const latMin = Math.min(selLat, newLat)
                        const latMax = Math.max(selLat, newLat)
                        const lngMin = Math.min(selLng, newLng)
                        const lngMax = Math.max(selLng, newLng)
                        const latSpan = latMax - latMin
                        const lngSpan = lngMax - lngMin
                        const padding = 0.3 // 30% padding
                        // Preserve current zoom — never zoom OUT for comparison
                        const currentZoom = map.getZoom()
                        map.fitBounds(
                            [
                                [lngMin - lngSpan * padding, latMin - latSpan * padding],
                                [lngMax + lngSpan * padding, latMax + latSpan * padding],
                            ],
                            { duration: 2000, maxZoom: currentZoom, minZoom: Math.max(currentZoom - 3, 9) }
                        )
                        // After flight, query new location and set as comparison (hover)
                        const attemptComparison = (retries: number) => {
                            const activeSuffix = (map as any)._activeSuffix || "a"
                            const newPoint = map.project([newLng, newLat])
                            // Query ALL fill layers, not just the one for current zoom
                            const allFillLayers = getAllFillLayerIds(activeSuffix).filter(id => map.getLayer(id))
                            let features = map.queryRenderedFeatures(newPoint, { layers: allFillLayers })
                            if (features.length === 0) {
                                // Try a 10px bbox around the point
                                const bbox: [maplibregl.PointLike, maplibregl.PointLike] = [
                                    [newPoint.x - 10, newPoint.y - 10],
                                    [newPoint.x + 10, newPoint.y + 10],
                                ]
                                features = map.queryRenderedFeatures(bbox, { layers: allFillLayers })
                            }
                            if (features.length > 0) {
                                const feature = features[0]
                                const sourceLayer = feature.sourceLayer || getSourceLayer(map.getZoom())
                                const id = (feature.properties?.id || feature.id) as string
                                if (id && id !== selectedIdRef.current) {
                                    // Set hover state on comparison feature
                                    hoveredIdRef.current = id
                                    onFeatureHover(id)
                                        ;["forecast-a", "forecast-b"].forEach((s) => {
                                            try { map.setFeatureState({ source: s, sourceLayer, id }, { hover: true }) } catch { }
                                        })
                                    // Update tooltipData.properties to trigger comparison useEffect
                                    setTooltipData(prev => prev ? { ...prev, properties: feature.properties } : prev)
                                    console.log(`[ForecastMap] add_location_to_selection: comparison set to ${id}`)
                                }
                            } else if (retries > 0) {
                                console.log(`[ForecastMap] add_location_to_selection: No features at comparison point, retrying... (${retries} left)`)
                                setTimeout(() => attemptComparison(retries - 1), 1200)
                            } else {
                                console.warn(`[ForecastMap] add_location_to_selection: Exhausted retries, no comparison features found`)
                            }
                        }
                        map.once("idle", () => attemptComparison(8))
                    } else {
                        // No primary selection yet — treat as normal select
                        map.flyTo({
                            center: [newLng, newLat],
                            zoom: Math.max(map.getZoom(), 13),
                            duration: 2000,
                        })
                        const attemptFirst = (retries: number) => {
                            const zoom = map.getZoom()
                            const sourceLayer = getSourceLayer(zoom)
                            const activeSuffix = (map as any)._activeSuffix || "a"
                            const fillLayerId = `forecast-fill-${sourceLayer}-${activeSuffix}`
                            const center = map.project(map.getCenter())
                            const features = map.getLayer(fillLayerId)
                                ? map.queryRenderedFeatures(center, { layers: [fillLayerId] })
                                : []
                            if (features.length > 0) {
                                const feature = features[0]
                                const id = (feature.properties?.id || feature.id) as string
                                if (id) {
                                    selectedIdRef.current = id
                                    selectedSourceLayerRef.current = sourceLayer
                                    setSelectedId(id)
                                    setSelectedProps(feature.properties)
                                    setSelectedCoords([newLat, newLng])
                                    setTooltipCoords([newLat, newLng])
                                    onFeatureSelect(id)
                                    const centerScreen = map.project(map.getCenter())
                                    const rect = map.getCanvas().getBoundingClientRect()
                                    const screenX = rect.left + centerScreen.x
                                    const screenY = rect.top + centerScreen.y
                                    const smartPos = getSmartTooltipPos(screenX, screenY, window.innerWidth, window.innerHeight)
                                    setFixedTooltipPos({ globalX: smartPos.x, globalY: smartPos.y })
                                    setTooltipData({ globalX: smartPos.x, globalY: smartPos.y, properties: feature.properties })
                                    userDraggedRef.current = false
                                        ;["forecast-a", "forecast-b"].forEach((s) => {
                                            try { map.setFeatureState({ source: s, sourceLayer, id }, { selected: true }) } catch { }
                                        })
                                    fetchForecastDetailRef.current(id, sourceLayer)
                                }
                            } else if (retries > 0) {
                                setTimeout(() => attemptFirst(retries - 1), 800)
                            }
                        }
                        map.once("idle", () => attemptFirst(8))
                    }
                }
            } else if (action === "location_to_area" || action === "resolve_place" || action === "get_forecast_area") {
                // All these return lat/lng — fly to it and auto-select the center feature
                const result = (e as CustomEvent).detail?.result || params
                const lat = result?.chosen?.lat || result?.location?.lat || result?.area?.location?.lat || params?.lat
                const lng = result?.chosen?.lng || result?.location?.lng || result?.area?.location?.lng || params?.lng
                const map = mapRef.current
                if (map && lat && lng) {
                    map.flyTo({
                        center: [lng, lat],
                        zoom: Math.max(map.getZoom(), 13),
                        duration: 2000,
                    })
                    const attemptLocSelect = (retries: number) => {
                        const zoom = map.getZoom()
                        const sourceLayer = getSourceLayer(zoom)
                        const activeSuffix = (map as any)._activeSuffix || "a"
                        const fillLayerId = `forecast-fill-${sourceLayer}-${activeSuffix}`
                        const center = map.project(map.getCenter())
                        const features = map.getLayer(fillLayerId)
                            ? map.queryRenderedFeatures(center, { layers: [fillLayerId] })
                            : []
                        if (features.length > 0) {
                            const feature = features[0]
                            const id = (feature.properties?.id || feature.id) as string
                            if (id) {
                                if (selectedIdRef.current) {
                                    clearAllLocalMapState(map)
                                    resetLocalState()
                                }
                                selectedIdRef.current = id
                                selectedSourceLayerRef.current = sourceLayer
                                setSelectedId(id)
                                setSelectedProps(feature.properties)
                                setSelectedCoords([lat, lng])
                                setTooltipCoords([lat, lng])
                                onFeatureSelect(id)

                                // Position tooltip at center of viewport
                                const centerScreen = map.project(map.getCenter())
                                const rect = map.getCanvas().getBoundingClientRect()
                                const screenX = rect.left + centerScreen.x
                                const screenY = rect.top + centerScreen.y
                                const smartPos = getSmartTooltipPos(screenX, screenY, window.innerWidth, window.innerHeight)
                                setFixedTooltipPos({ globalX: smartPos.x, globalY: smartPos.y })
                                setTooltipData({ globalX: smartPos.x, globalY: smartPos.y, properties: feature.properties })
                                userDraggedRef.current = false

                                    ;["forecast-a", "forecast-b"].forEach((s) => {
                                        try { map.setFeatureState({ source: s, sourceLayer, id }, { selected: true }) } catch { }
                                    })
                                fetchForecastDetailRef.current(id, sourceLayer)
                            }
                        } else if (retries > 0) {
                            console.log(`[ForecastMap] location_to_area: No features at center, retrying... (${retries} left)`)
                            setTimeout(() => attemptLocSelect(retries - 1), 800)
                        }
                    }
                    map.once("idle", () => attemptLocSelect(8))
                }
            }
        }
        window.addEventListener("tavus-map-action", handleTavusAction)
        return () => window.removeEventListener("tavus-map-action", handleTavusAction)
    }, [onFeatureSelect])

    // UPDATE MODE — reactive fill-color paint
    useEffect(() => {
        if (!isLoaded || !mapRef.current) return
        const map = mapRef.current
        const applyColor = () => {
            const newColor = buildFillColor(filters.colorMode)
            for (const lvl of GEO_LEVELS) {
                for (const suffix of ["a", "b"]) {
                    const layerId = `forecast-fill-${lvl.name}-${suffix}`
                    if (map.getLayer(layerId)) {
                        map.setPaintProperty(layerId, "fill-color", newColor)
                    }
                }
            }
        }
        applyColor()

        // Re-apply when zoom changes (zoom-dependent scaling)
        if (filters.colorMode === "growth") {
            map.on("moveend", applyColor)
            return () => { map.off("moveend", applyColor) }
        }
    }, [filters.colorMode, isLoaded, growthStats])

    // UPDATE YEAR — Seamless A/B swap (same pattern as vector-map.tsx)
    // Note: tile URLs are managed by the earlier effect (which handles both sources correctly,
    // including originYear, horizonM, and schema). This effect only handles color + visibility swap.
    useEffect(() => {
        if (!isLoaded || !mapRef.current) return
        const map = mapRef.current

        const currentSuffix = (map as any)._activeSuffix || "a"
        const nextSuffix = currentSuffix === "a" ? "b" : "a"
        const nextSource = `forecast-${nextSuffix}`
        const currentSource = `forecast-${currentSuffix}`


        // Apply color logic to all fill layers
        const fillColor = buildFillColor(filters.colorMode)

        for (const lvl of GEO_LEVELS) {
            ;["a", "b"].forEach((s) => {
                const layerId = `forecast-fill-${lvl.name}-${s}`
                if (map.getLayer(layerId)) {
                    map.setPaintProperty(layerId, "fill-color", fillColor)
                }
            })
        }

        // Seamless swap
        let swapCompleted = false

        const performSwap = () => {
            if (swapCompleted) return
            swapCompleted = true

            const latestTarget = (map as any)._targetYear
            if (latestTarget !== year) return

            // Show next, hide current — for all geo levels
            if (!map.getStyle?.()) return // guard against hot-reload race
            
            // Batch visibility layout updates
            let styleUpdates = { ...map.getStyle() };
            let needsUpdate = false;
            
            for (const lvl of GEO_LEVELS) {
                const fillNext = `forecast-fill-${lvl.name}-${nextSuffix}`
                const outlineNext = `forecast-outline-${lvl.name}-${nextSuffix}`
                const patternNext = `forecast-pattern-${lvl.name}-${nextSuffix}`
                const fillCur = `forecast-fill-${lvl.name}-${currentSuffix}`
                const outlineCur = `forecast-outline-${lvl.name}-${currentSuffix}`
                const patternCur = `forecast-pattern-${lvl.name}-${currentSuffix}`

                if (map.getLayer(fillNext)) map.setLayoutProperty(fillNext, "visibility", "visible")
                if (map.getLayer(outlineNext)) map.setLayoutProperty(outlineNext, "visibility", "visible")
                if (map.getLayer(patternNext)) map.setLayoutProperty(patternNext, "visibility", "visible")
                if (map.getLayer(fillCur)) map.setLayoutProperty(fillCur, "visibility", "none")
                if (map.getLayer(outlineCur)) map.setLayoutProperty(outlineCur, "visibility", "none")
                if (map.getLayer(patternCur)) map.setLayoutProperty(patternCur, "visibility", "none")
            }

            ; (map as any)._activeSuffix = nextSuffix

            // Re-query selected feature from new tiles to refresh tooltip P-values
            const selId = selectedIdRef.current
            if (selId) {
                const zoom = map.getZoom()
                const sourceLayer = selectedSourceLayerRef.current || getSourceLayer(zoom)
                const features = map.querySourceFeatures(`forecast-${nextSuffix}`, { sourceLayer })
                const match = features.find(f => (f.properties?.id || f.id) === selId)
                if (match?.properties) {
                    setSelectedProps(match.properties)
                }
            }

            // Old source's tiles are intentionally kept — clearTiles() is not part of
            // MapLibre's public VectorTileSource API and nuking tiles immediately causes
            // extra reload churn if the user swaps back. The adaptive cache handles eviction.
        }

        const onSourceData = (e: any) => {
            if (
                e.sourceId === nextSource &&
                map.isSourceLoaded(nextSource) &&
                e.isSourceLoaded
            ) {
                map.off("sourcedata", onSourceData)
                map.once("idle", () => {
                    requestAnimationFrame(() => {
                        requestAnimationFrame(performSwap)
                    })
                })
                setTimeout(performSwap, 600)
            }
        }

            ; (map as any)._targetYear = year
        map.on("sourcedata", onSourceData)

        return () => {
            map.off("sourcedata", onSourceData)
        }
    }, [year, isLoaded, filters.colorMode])

    // SYNC VIEWPORT
    useEffect(() => {
        if (!mapRef.current || !isLoaded) return
        const map = mapRef.current

        try {
            if (map.getStyle()) {
                const center = map.getCenter()
                const zoom = map.getZoom()
                if (
                    Math.abs(center.lng - mapState.center[0]) > 0.001 ||
                    Math.abs(center.lat - mapState.center[1]) > 0.001 ||
                    Math.abs(zoom - mapState.zoom) > 0.1
                ) {
                    map.flyTo({
                        center: [mapState.center[0], mapState.center[1]],
                        zoom: mapState.zoom,
                        speed: 0.8,
                        curve: 1.42,
                        easing: (t: number) =>
                            t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2,
                    })
                }
            }
        } catch (e) {
            /* map might be in transition */
        }
    }, [mapState.center, mapState.zoom, isLoaded])

    // Determine display tooltip info
    const displayPos = selectedId && fixedTooltipPos ? fixedTooltipPos : tooltipData
    // When locked, show the SELECTED feature's properties (not hover)
    const displayProps = selectedId && selectedProps ? selectedProps : tooltipData?.properties

    // Effective y-domain: extend viewport range ONLY when selected/hovered
    // feature's P50 median or historical values fall outside. P10/P90 uncertainty
    // bands are allowed to clip — they shouldn't drive the axis scale.
    const effectiveYDomain = useMemo<[number, number] | null>(() => {
        if (!viewportYDomain) return null
        const [lo, hi] = viewportYDomain
        // Only gather P50 (median) and historical — NOT P10/P90 extremes
        const vals: number[] = []
        if (fanChartData?.p50) vals.push(...fanChartData.p50.filter(v => Number.isFinite(v)))
        if (historicalValues) vals.push(...historicalValues.filter(v => Number.isFinite(v)))
        if (comparisonData?.p50) vals.push(...comparisonData.p50.filter(v => Number.isFinite(v)))
        if (comparisonHistoricalValues) vals.push(...comparisonHistoricalValues.filter(v => Number.isFinite(v)))
        // Include pinned comparison data in Y range
        for (const pc of pinnedComparisons) {
            if (pc.historicalValues) vals.push(...pc.historicalValues.filter(v => Number.isFinite(v)))
            if (pc.data?.p50) vals.push(...pc.data.p50.filter(v => Number.isFinite(v)))
        }
        if (vals.length === 0) return viewportYDomain
        const dataMin = Math.min(...vals)
        const dataMax = Math.max(...vals)
        // Only extend, never shrink from viewport range
        if (dataMin >= lo && dataMax <= hi) return viewportYDomain // no extension needed
        return [Math.min(lo, dataMin), Math.max(hi, dataMax)]
    }, [viewportYDomain, fanChartData, historicalValues, comparisonData, comparisonHistoricalValues, pinnedComparisons])

    // Comparison: when locked, fetch comparison detail for hovered feature
    // Skip updates when Shift is held (freeze current comparison)
    useEffect(() => {
        if (isShiftHeld) return // freeze comparison
        if (!selectedId || !tooltipData?.properties) {
            setComparisonData(null)
            setComparisonHistoricalValues(undefined)
            comparisonFetchRef.current = null
            return
        }
        const hoveredId = tooltipData.properties.id as string
        if (!hoveredId || hoveredId === selectedId) {
            setComparisonData(null)
            setComparisonHistoricalValues(undefined)
            comparisonFetchRef.current = null
            return
        }
        // Debounce comparison fetch
        if (comparisonTimerRef.current) clearTimeout(comparisonTimerRef.current)
        comparisonTimerRef.current = setTimeout(async () => {
            const map = mapRef.current
            if (!map) return
            const zoom = map.getZoom()
            const level = hoveredSourceLayerRef.current || getSourceLayer(zoom)
            const cacheKey = `${level}:${hoveredId}`
            if (comparisonFetchRef.current === cacheKey) return
            comparisonFetchRef.current = cacheKey
            // Check shared cache first
            const cached = detailCacheRef.current.get(cacheKey)
            if (cached) {
                detailCacheRef.current.delete(cacheKey)
                detailCacheRef.current.set(cacheKey, cached)
                setComparisonData(cached.fanChart)
                setComparisonHistoricalValues(cached.historicalValues)
                return
            }
            try {
                const schemaParam = schema ? `&schema=${schema}` : ""
                const res = await fetch(`/api/forecast-detail?level=${level}&id=${encodeURIComponent(hoveredId)}&originYear=${originYear}${schemaParam}`)
                if (!res.ok) return
                const json = await res.json()
                const fanChart = json.years?.length > 0 ? (json as FanChartData) : null
                const histVals = json.historicalValues?.some((v: any) => v != null) ? json.historicalValues : undefined
                setComparisonData(fanChart)
                setComparisonHistoricalValues(histVals)
                // Store in shared cache
                detailCacheRef.current.set(cacheKey, { fanChart, historicalValues: histVals })
                if (detailCacheRef.current.size > DETAIL_CACHE_MAX) {
                    const oldest = detailCacheRef.current.keys().next().value
                    if (oldest) detailCacheRef.current.delete(oldest)
                }
            } catch {
                setComparisonData(null)
                setComparisonHistoricalValues(undefined)
            }
        }, 200)
        return () => {
            if (comparisonTimerRef.current) clearTimeout(comparisonTimerRef.current)
        }
    }, [selectedId, tooltipData?.properties?.id, originYear, isShiftHeld, schema])

    // Viewport Y domain: compute from visible features on moveend + after initial tile load
    useEffect(() => {
        if (!mapRef.current || !isLoaded) return
        const map = mapRef.current
        const computeYDomain = () => {
            // Freeze y-range while tooltip is locked — prevents jumps on pan
            if (selectedIdRef.current) return false
            const zoom = map.getZoom()
            const sourceLayer = getSourceLayer(zoom)
            const activeSuffix = (map as any)._activeSuffix || 'a'
            const layerId = `forecast-fill-${sourceLayer}-${activeSuffix}`
            if (!map.getLayer(layerId)) return false
            const features = map.queryRenderedFeatures(undefined, { layers: [layerId] })
            if (features.length === 0) return false
            const allVals: number[] = []
            for (const f of features) {
                const p = f.properties
                if (p?.value != null && Number.isFinite(p.value)) allVals.push(p.value)
                if (p?.p10 != null && Number.isFinite(p.p10)) allVals.push(p.p10)
                if (p?.p90 != null && Number.isFinite(p.p90)) allVals.push(p.p90)
            }
            if (allVals.length < 2) return false
            allVals.sort((a, b) => a - b)
            // Use P10/P90 of visible values to exclude outliers
            const lo = allVals[Math.floor(allVals.length * 0.1)]
            const hi = allVals[Math.ceil(allVals.length * 0.9) - 1]
            if (lo < hi) {
                setViewportYDomain([lo, hi])
                return true
            }
            return false
        }

        map.on('moveend', computeYDomain)

        // Initial tile load: tiles may not be rendered when isLoaded fires.
        // Listen for 'idle' (fires after all tiles painted) to catch initial load.
        let initialDone = computeYDomain()
        if (!initialDone) {
            const onIdle = () => {
                if (computeYDomain()) {
                    map.off('idle', onIdle) // success — stop listening
                }
            }
            map.on('idle', onIdle)
            // Safety: remove after 10s to avoid leaking
            const safetyTimer = setTimeout(() => map.off('idle', onIdle), 10000)
            return () => {
                map.off('moveend', computeYDomain)
                map.off('idle', onIdle)
                clearTimeout(safetyTimer)
            }
        }

        return () => { map.off('moveend', computeYDomain) }
    }, [isLoaded])

    return (
        <div className={cn("relative w-full h-full", className)}>
            <div ref={mapContainerRef} className="w-full h-full" />

            {!isLoaded && (
                <div className="absolute inset-0 flex items-center justify-center bg-background/50 backdrop-blur-sm z-50">
                    <div className="flex flex-col items-center gap-2">
                        <div className="w-8 h-8 border-4 border-primary border-t-transparent rounded-full animate-spin" />
                        <span className="text-sm font-medium">
                            Initializing Forecast Engine...
                        </span>
                    </div>
                </div>
            )}

            {studentLoading && (
                <div className="absolute top-4 left-1/2 -translate-x-1/2 z-40 bg-black/70 backdrop-blur-sm text-white text-xs font-medium px-3 py-1.5 rounded-full flex items-center gap-2">
                    <div className="w-3 h-3 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                    Loading building forecasts…
                </div>
            )}

            {/* Mobile: Floating StreetView button + overlay (DEACTIVATED) */}
            {false && isMobile && process.env.NEXT_PUBLIC_GOOGLE_MAPS_KEY && (selectedId ? selectedCoords : (hoverDwell ? tooltipCoords : null)) && (
                <>
                    {/* Floating button — bottom-left of map */}
                    <button
                        onClick={() => setMobileStreetViewOpen(!mobileStreetViewOpen)}
                        className="absolute bottom-[32vh] left-3 z-50 w-10 h-10 rounded-full glass-panel shadow-xl flex items-center justify-center active:scale-90 transition-transform border border-border/40"
                        aria-label={mobileStreetViewOpen ? "Close Street View" : "Open Street View"}
                        style={{ touchAction: 'manipulation' }}
                    >
                        <span className="text-base">{mobileStreetViewOpen ? '✕' : '📷'}</span>
                    </button>

                    {/* Overlay — fills top half when open */}
                    {mobileStreetViewOpen && (
                        <div className="absolute top-0 left-0 right-0 h-[50vh] z-40 bg-black/90 animate-in fade-in slide-in-from-top-4 duration-200">
                            <StreetViewCarousel
                                h3Ids={[]}
                                apiKey={process.env.NEXT_PUBLIC_GOOGLE_MAPS_KEY || ""}
                                coordinates={(selectedId ? selectedCoords : tooltipCoords)!}
                            />
                            <button
                                onClick={() => setMobileStreetViewOpen(false)}
                                className="absolute top-2 right-2 w-8 h-8 rounded-full bg-black/60 text-white flex items-center justify-center z-50"
                            >
                                ✕
                            </button>
                        </div>
                    )}
                </>
            )}
            {/* Forecast Tooltip — portal-based, responsive for mobile + desktop */}
            {isLoaded && ((displayPos && displayProps) || (isMobile && mobileContentOverride)) && createPortal(
                <div
                    className={cn(
                        "z-[9999] glass-panel md:shadow-2xl overflow-hidden flex flex-col",
                        isMobile
                            ? `fixed left-0 right-0 w-full rounded-t-xl rounded-b-none border-t border-x-0 border-b-0 pointer-events-auto`
                            : "fixed rounded-xl w-[320px]",
                        !isMobile && (selectedId ? "pointer-events-auto cursor-move" : "pointer-events-none")
                    )}
                    style={isMobile ? {
                        transform: `translateY(calc(${mobileMinimized ? '100% - 24px' : '0px'} + ${swipeDragOffset}px))`,
                        transition: swipeTouchStart === null ? 'transform 0.3s ease-out' : 'none',
                        height: '30vh',
                        maxHeight: '30vh',
                        bottom: '56px',
                        overflowY: 'hidden',
                    } : {
                        left: displayPos?.globalX ?? 0,
                        top: displayPos?.globalY ?? 0,
                        overflow: 'hidden',
                    }}
                    onMouseDown={!isMobile && selectedId ? (e) => {
                        // Don't drag when clicking interactive elements
                        const tag = (e.target as HTMLElement)?.tagName?.toLowerCase()
                        if (tag === 'button' || tag === 'a' || tag === 'input' || tag === 'select') return
                        e.preventDefault()
                        // Fall back to displayPos if fixedTooltipPos hasn't been set yet
                        const origin = fixedTooltipPos ?? displayPos
                        if (origin) {
                            dragRef.current = { startX: e.clientX, startY: e.clientY, origX: origin.globalX, origY: origin.globalY }
                        }
                    } : undefined}
                    onTouchStart={isMobile ? (e) => {
                        // Only allow swipe-to-dismiss from the handle area
                        const target = e.target as HTMLElement
                        if (!target.closest('[data-tooltip-header]')) return
                        setSwipeTouchStart(e.touches[0].clientY)
                    } : undefined}
                    onTouchMove={isMobile ? (e) => {
                        if (swipeTouchStart === null) return
                        const delta = e.touches[0].clientY - swipeTouchStart
                        if (mobileMinimized) { if (delta < 0) setSwipeDragOffset(delta) }
                        else { if (delta > 0) setSwipeDragOffset(delta) }
                    } : undefined}
                    onTouchEnd={isMobile ? () => {
                        if (mobileMinimized) {
                            if (swipeDragOffset < -50) setMobileMinimized(false)
                        } else {
                            if (swipeDragOffset > 150) {
                                // Dismiss completely
                                if (onMobileClose) {
                                    const map = mapRef.current
                                    if (map) clearAllLocalMapState(map)
                                    resetLocalState()
                                    onFeatureSelect(null)
                                    onMobileClose()
                                } else {
                                    const map = mapRef.current
                                    if (map) clearAllLocalMapState(map)
                                    resetLocalState()
                                    onFeatureSelect(null)
                                }
                            } else if (swipeDragOffset > 50) {
                                setMobileMinimized(true)
                            }
                        }
                        setSwipeDragOffset(0)
                        setSwipeTouchStart(null)
                    } : undefined}
                >
                    {/* Mobile: swipe handle + close + simplified header */}
                    {isMobile && (
                        <div className="flex flex-col border-b border-border/20 bg-muted/10 pb-1.5" data-tooltip-header="true">
                            <div className="w-full flex items-center justify-center pt-2 pb-1.5 shrink-0 relative">
                                {/* Swipe handle */}
                                <div className="w-8 h-1 rounded-full bg-muted-foreground/30" />
                                {/* Close button — absolute right */}
                                <button
                                    onClick={(e) => {
                                        e.stopPropagation();
                                        e.preventDefault();
                                        if (onMobileClose) {
                                            const map = mapRef.current;
                                            if (map) clearAllLocalMapState(map);
                                            resetLocalState();
                                            onFeatureSelect(null);
                                            onMobileClose();
                                        } else {
                                            const map = mapRef.current;
                                            if (map) clearAllLocalMapState(map);
                                            resetLocalState();
                                            onFeatureSelect(null);
                                        }
                                    }}
                                    className="absolute right-2 top-0.5 w-8 h-8 flex items-center justify-center rounded-full active:bg-muted/60 text-muted-foreground"
                                    aria-label="Close"
                                    style={{ touchAction: 'manipulation' }}
                                >
                                    <svg width="10" height="10" viewBox="0 0 12 12" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" style={{ pointerEvents: 'none' }}>
                                        <line x1="2" y1="2" x2="10" y2="10" /><line x1="10" y1="2" x2="2" y2="10" />
                                    </svg>
                                </button>
                            </div>

                            {/* Mobile Location Header */}
                            <div className="px-3 flex flex-col justify-center">
                                <div className="font-semibold text-[11px] text-foreground truncate flex items-center gap-1.5">
                                    {geocodedName || displayProps.id}
                                    {geocodedName && !geocodedName.startsWith('ZIP') && (
                                        <span className="font-mono text-[9px] text-muted-foreground/60 font-normal">
                                            {displayProps.id}
                                        </span>
                                    )}
                                </div>
                                {(comparisonData || pinnedComparisons.length > 0) && (
                                    <div className="mt-0.5 flex flex-wrap items-center gap-1">
                                        {comparisonData && tooltipData?.properties?.id && tooltipData.properties.id !== selectedId && (
                                            <span className="px-1 py-0.5 bg-lime-500/20 text-lime-400 text-[8px] font-semibold uppercase tracking-wider rounded inline-flex">
                                                vs {comparisonGeocodedName && comparisonGeocodedName !== geocodedName ? comparisonGeocodedName : tooltipData.properties.id}
                                            </span>
                                        )}
                                        {pinnedComparisons.map((pc) => {
                                            const color = PINNED_COLORS[(pc.colorIdx - 1) % PINNED_COLORS.length]
                                            return (
                                            <button
                                                key={`${pc.sourceLayer}:${pc.id}`}
                                                onClick={(ev) => {
                                                    ev.stopPropagation()
                                                    const map = mapRef.current
                                                    if (map) {
                                                        ;["forecast-a", "forecast-b"].forEach(s => {
                                                            try { map.setFeatureState({ source: s, sourceLayer: pc.sourceLayer, id: pc.id }, { pinned: false, pinnedIdx: 0 }) } catch { }
                                                        })
                                                    }
                                                    setPinnedComparisons(prev => prev.filter(p => !(p.id === pc.id && p.sourceLayer === pc.sourceLayer)))
                                                }}
                                                className="px-1 py-0.5 text-[8px] font-semibold uppercase tracking-wider rounded flex items-center gap-0.5 hover:opacity-70 transition-opacity cursor-pointer inline-flex"
                                                style={{ backgroundColor: `${color}20`, color }}
                                            >
                                                <span className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: color }} />
                                                {pc.label || pc.id}
                                                <span className="ml-[1px]">×</span>
                                            </button>
                                            )
                                        })}
                                    </div>
                                )}
                            </div>
                        </div>
                    )}

                    {/* Header - matching MapTooltip (hidden on mobile) */}
                    {!isMobile && (
                        <>
                            <div
                                className="flex items-center justify-between gap-2 px-3 py-2 border-b border-border/50 bg-muted/40 backdrop-blur-md select-none"
                            >
                                <div className="flex items-center gap-1.5 min-w-0">
                                    <HomecastrLogo variant="horizontal" size={18} />
                                    <span className={cn(
                                        "px-1.5 py-0.5 text-[8px] font-semibold uppercase tracking-wider rounded shrink-0 whitespace-nowrap",
                                        year > 2026
                                            ? "bg-violet-500/20 text-violet-400"
                                            : "bg-sky-500/20 text-sky-400"
                                    )}>{year > 2026 ? "Forecast" : "Historical"}</span>
                                </div>
                                <div className="flex items-center gap-1.5 shrink-0">
                                    {selectedId && selectedCoordsRef.current && (
                                        <button
                                            title="Re-center on selection"
                                            onClick={() => {
                                                const coords = selectedCoordsRef.current
                                                if (coords && mapRef.current) {
                                                    mapRef.current.flyTo({ center: [coords[1], coords[0]], speed: 1.2 })
                                                }
                                            }}
                                            className="flex items-center gap-1 px-1.5 py-0.5 rounded text-[9px] font-medium text-primary bg-primary/10 hover:bg-primary/20 transition-colors"
                                        >
                                            <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="3" /><line x1="12" y1="2" x2="12" y2="6" /><line x1="12" y1="18" x2="12" y2="22" /><line x1="2" y1="12" x2="6" y2="12" /><line x1="18" y1="12" x2="22" y2="12" /></svg>
                                            Find
                                        </button>
                                    )}
                                    {selectedId && <span className="text-[9px] text-muted-foreground">ESC to exit</span>}
                                </div>
                            </div>

                            {/* Subheader - geography level */}
                            <div className="p-3 border-b border-border/50 bg-muted/30">
                                <div className="flex justify-between items-start">
                                    <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold mb-0.5">
                                        {getLevelLabel(mapRef.current?.getZoom() || 10)} Scale
                                    </div>
                                    {/* Debug badge: shows building ID + p50 when ?debug=buildings is active */}
                                    {displayProps._debugLabel && (
                                        <div className="text-[8px] px-1.5 py-0.5 bg-cyan-500/20 text-cyan-400 rounded-full font-mono">
                                            {displayProps._debugLabel}
                                        </div>
                                    )}
                                    <div className="text-[9px] px-1.5 py-0.5 bg-primary/10 text-primary rounded-full font-bold">
                                        {displayProps.n != null ? `${displayProps.n} Prop` : ""}
                                    </div>
                                </div>
                                <div className="font-semibold text-xs text-foreground truncate">
                                    {geocodedName || displayProps.id}
                                </div>
                                {geocodedName && !geocodedName.startsWith('ZIP') && (
                                    <div className="font-mono text-[9px] text-muted-foreground/60 truncate">
                                        {displayProps.id}
                                    </div>
                                )}
                                {comparisonData && tooltipData?.properties?.id && tooltipData.properties.id !== selectedId && (
                                    <div className="mt-1 flex items-center gap-1">
                                        <span className="px-1.5 py-0.5 bg-lime-500/20 text-lime-400 text-[8px] font-semibold uppercase tracking-wider rounded">
                                            vs {comparisonGeocodedName && comparisonGeocodedName !== geocodedName ? comparisonGeocodedName : tooltipData.properties.id}
                                        </span>
                                    </div>
                                )}
                                {pinnedComparisons.length > 0 && (
                                    <div className="mt-1 flex flex-wrap items-center gap-1">
                                        {pinnedComparisons.map((pc) => {
                                            const color = PINNED_COLORS[(pc.colorIdx - 1) % PINNED_COLORS.length]
                                            return (
                                            <button
                                                key={`${pc.sourceLayer}:${pc.id}`}
                                                onClick={(ev) => {
                                                    ev.stopPropagation()
                                                    const map = mapRef.current
                                                    if (map) {
                                                        ;["forecast-a", "forecast-b"].forEach(s => {
                                                            try { map.setFeatureState({ source: s, sourceLayer: pc.sourceLayer, id: pc.id }, { pinned: false, pinnedIdx: 0 }) } catch { }
                                                        })
                                                    }
                                                    setPinnedComparisons(prev => prev.filter(p => !(p.id === pc.id && p.sourceLayer === pc.sourceLayer)))
                                                }}
                                                className="px-1.5 py-0.5 text-[8px] font-semibold uppercase tracking-wider rounded flex items-center gap-1 hover:opacity-70 transition-opacity cursor-pointer"
                                                style={{ backgroundColor: `${color}20`, color }}
                                                title={`Click to unpin ${pc.label || pc.id}`}
                                            >
                                                <span className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: color }} />
                                                {pc.label || pc.id}
                                                <span className="ml-0.5">×</span>
                                            </button>
                                            )
                                        })}
                                    </div>
                                )}
                            </div>
                        </>
                    )}

                    {/* Mobile: Full-width chart (StreetView via floating button) / Desktop: StreetView above chart */}
                    {isMobile && !(isKeyboardOpen) ? (
                        /* Mobile: Full-width chart + embedded bottom bar */
                        <>
                            <div className="flex-1 min-h-0 overflow-hidden flex flex-col">
                                {mobileContentOverride ? (
                                    /* Chat/Tavus/etc override fills the content area */
                                    mobileContentOverride
                                ) : (() => {
                                    const currentVal = fanChartData?.p50?.[0] ?? historicalValues?.[historicalValues.length - 1] ?? null
                                    const yearIdx = fanChartData?.years?.indexOf(year) ?? -1
                                    const histIdx = year >= 2019 && year <= 2025 ? year - 2019 : -1
                                    const forecastVal = (yearIdx >= 0 ? fanChartData?.p50?.[yearIdx] : null)
                                        ?? (histIdx >= 0 ? historicalValues?.[histIdx] : null)
                                        ?? displayProps.p50 ?? displayProps.value ?? null
                                    const isPast = year < originYear + 2
                                    const isPresent = year === originYear + 2
                                    const leftLabel = isPresent ? "Now" : isPast ? String(year) : "Now"
                                    const leftVal = isPresent ? currentVal : isPast ? forecastVal : currentVal
                                    const rightLabel = isPresent ? String(year) : isPast ? "Now" : String(year)
                                    const rightVal = isPresent ? currentVal : isPast ? currentVal : forecastVal
                                    const pctBase = isPresent ? null : isPast ? forecastVal : currentVal
                                    const pctTarget = isPresent ? null : isPast ? currentVal : forecastVal
                                    const pctChange = pctBase && pctTarget ? ((pctTarget - pctBase) / pctBase * 100) : null

                                    const isExtremeOutlier = pctChange !== null && (pctChange > 100 || pctChange < -50)
                                    if (isExtremeOutlier && !isPresent) {
                                        return (
                                            <div className="flex w-full flex-1 h-full items-center justify-center p-4 bg-destructive/5 text-center">
                                                <div className="space-y-2">
                                                    <AlertCircle className="w-6 h-6 text-destructive/80 mx-auto" />
                                                    <div className="text-xs font-bold text-destructive">Data Anomaly Detected</div>
                                                    <div className="text-[10px] text-muted-foreground max-w-[220px] mx-auto leading-tight">
                                                        This area shows anomalous forecasted growth ({pctChange > 0 ? '+' : ''}{pctChange.toFixed(1)}%). It may be a data artifact (e.g., an empty lot zoned for development).
                                                    </div>
                                                    <button onClick={(e) => { e.stopPropagation(); window.location.href = "mailto:daniel@homecastr.com?subject=Requesting Custom Analysis" }} className="mt-2 text-[10px] font-semibold px-3 py-1.5 bg-background text-foreground border border-border rounded shadow-sm hover:bg-muted">
                                                        Request Custom Analysis
                                                    </button>
                                                </div>
                                            </div>
                                        )
                                    }

                                    return (
                                        <div className="flex w-full flex-1 h-full">
                                            {/* Chart — takes remaining space */}
                                            <div className="flex-1 min-w-0 h-full">
                                                {fanChartData ? (
                                                    <FanChart data={fanChartData} currentYear={year} height={200} historicalValues={historicalValues} childLines={debugBuildings ? studentChildLines : undefined} comparisonData={comparisonData} comparisonHistoricalValues={comparisonHistoricalValues} pinnedComparisons={pinnedComparisons.map(pc => ({ data: pc.data, historicalValues: pc.historicalValues, label: pc.label, colorIdx: pc.colorIdx }))} yDomain={effectiveYDomain} />
                                                ) : isLoadingDetail ? (
                                                    <div className="h-full flex items-center justify-center">
                                                        <div className="w-4 h-4 border-2 border-primary/30 border-t-primary rounded-full animate-spin" />
                                                    </div>
                                                ) : null}
                                            </div>
                                            {/* Stats column — to the right of the chart */}
                                            <div className="shrink-0 w-[90px] flex flex-col justify-center gap-2 p-2 border-l border-border/20">
                                                <div>
                                                    <div className="text-[9px] uppercase tracking-wider text-muted-foreground font-semibold">{leftLabel}</div>
                                                    <div className="text-[12px] font-bold text-foreground leading-tight">{formatValue(leftVal)}</div>
                                                </div>
                                                {!isPresent && (
                                                    <div>
                                                        <div className="text-[9px] uppercase tracking-wider text-muted-foreground font-semibold">{rightLabel}</div>
                                                        <div className="text-[12px] font-bold text-foreground leading-tight">{formatValue(rightVal)}</div>
                                                        {pctChange != null && (
                                                            <div className={`text-[10px] font-bold ${pctChange >= 0 ? 'text-emerald-500' : 'text-red-500'}`}>
                                                                {pctChange >= 0 ? '▲' : '▼'} {Math.abs(pctChange).toFixed(1)}%
                                                            </div>
                                                        )}
                                                    </div>
                                                )}
                                            </div>
                                        </div>
                                    )
                                })()}
                            </div>
                            {/* Bottom bar — rendered as part of this same tooltip container */}
                            {mobileBottomBar && (
                                <div className="shrink-0 border-t border-border/30">
                                    {mobileBottomBar}
                                </div>
                            )}
                        </>
                    ) : (
                        <>
                            {/* Desktop: StreetView above chart (DEACTIVATED) */}
                            {false && !(isMobile && isKeyboardOpen) && process.env.NEXT_PUBLIC_GOOGLE_MAPS_KEY && (selectedId ? selectedCoords : (hoverDwell ? tooltipCoords : null)) && (
                                <StreetViewCarousel
                                    h3Ids={[]}
                                    apiKey={process.env.NEXT_PUBLIC_GOOGLE_MAPS_KEY || ""}
                                    coordinates={(selectedId ? selectedCoords : tooltipCoords)!}
                                />
                            )}
                            {/* Desktop Layout: Values above, full-width chart below */}
                            <div className="p-4 space-y-3">
                                {/* Current → Forecast header with % change */}
                                {(() => {
                                    // Use the fan chart's 2026 p50 as "Now" for consistency with the chart
                                    const currentVal = fanChartData?.p50?.[0] ?? historicalValues?.[historicalValues.length - 1] ?? null
                                    // Look up value for the selected year: fan chart (2026+), historicalValues (2019-2025), or tile props
                                    const yearIdx = fanChartData?.years?.indexOf(year) ?? -1
                                    const histIdx = year >= 2019 && year <= 2025 ? year - 2019 : -1
                                    const forecastVal = (yearIdx >= 0 ? fanChartData?.p50?.[yearIdx] : null)
                                        ?? (histIdx >= 0 ? historicalValues?.[histIdx] : null)
                                        ?? displayProps.p50 ?? displayProps.value ?? null
                                    const isPast = year < originYear + 2
                                    const isPresent = year === originYear + 2 // 2026 = "now"
                                    const leftLabel = isPresent ? "Now" : isPast ? String(year) : "Now"
                                    const leftVal = isPresent ? currentVal : isPast ? forecastVal : currentVal
                                    const rightLabel = isPresent ? String(year) : isPast ? "Now" : String(year)
                                    const rightVal = isPresent ? currentVal : isPast ? currentVal : forecastVal
                                    const pctBase = isPresent ? null : isPast ? forecastVal : currentVal
                                    const pctTarget = isPresent ? null : isPast ? currentVal : forecastVal
                                    const pctChange = pctBase && pctTarget ? ((pctTarget - pctBase) / pctBase * 100) : null

                                    const isExtremeOutlier = pctChange !== null && (pctChange > 100 || pctChange < -50)
                                    if (isExtremeOutlier && !isPresent) {
                                        return (
                                            <div className="flex w-full flex-col justify-center p-6 bg-destructive/5 text-center rounded-lg border border-destructive/20 mt-2">
                                                <div className="space-y-3">
                                                    <AlertCircle className="w-8 h-8 text-destructive/80 mx-auto" />
                                                    <div className="text-sm font-bold text-destructive">Data Anomaly Detected</div>
                                                    <div className="text-xs text-muted-foreground px-4 leading-relaxed">
                                                        This area shows anomalous forecasted growth ({pctChange > 0 ? '+' : ''}{pctChange.toFixed(1)}%). It may be a data artifact (e.g., an empty lot zoned for development).
                                                    </div>
                                                    <button onClick={(e) => { e.stopPropagation(); window.location.href = "mailto:daniel@homecastr.com?subject=Requesting Custom Analysis" }} className="mt-4 text-xs font-semibold px-4 py-2 bg-background text-foreground border border-border rounded-md shadow-sm hover:bg-muted transition-colors">
                                                        Request Custom Analysis
                                                    </button>
                                                </div>
                                            </div>
                                        )
                                    }

                                    return (
                                        <div className="flex items-center justify-between gap-3">
                                            <div className="text-center flex-1">
                                                <div className="text-[9px] uppercase tracking-wider text-muted-foreground font-semibold mb-0.5">{leftLabel}</div>
                                                <div className="text-lg font-bold text-foreground tracking-tight">{formatValue(leftVal)}</div>
                                            </div>
                                            <div className="text-center shrink-0">
                                                {pctChange != null && (
                                                    <div className={`text-sm font-bold ${pctChange >= 0 ? 'text-emerald-500' : 'text-red-500'}`}>
                                                        {pctChange >= 0 ? '▲' : '▼'} {Math.abs(pctChange).toFixed(1)}%
                                                    </div>
                                                )}
                                                <div className="text-[9px] text-muted-foreground">→ {rightLabel}</div>
                                            </div>
                                            <div className="text-center flex-1">
                                                <div className="text-[9px] uppercase tracking-wider text-muted-foreground font-semibold mb-0.5">{rightLabel}</div>
                                                <div className="text-lg font-bold text-foreground tracking-tight">{formatValue(rightVal)}</div>
                                            </div>
                                        </div>
                                    )
                                })()}

                                {/* Fan Chart full-width with P-values overlaid top-right */}
                                <div className="relative h-52 -mx-2">
                                    {fanChartData ? (
                                        <FanChart
                                            data={fanChartData}
                                            currentYear={year}
                                            height={200}
                                            historicalValues={historicalValues}
                                            childLines={debugBuildings ? studentChildLines : undefined}
                                            comparisonData={comparisonData}
                                            comparisonHistoricalValues={comparisonHistoricalValues}
                                            pinnedComparisons={pinnedComparisons.map(pc => ({ data: pc.data, historicalValues: pc.historicalValues, label: pc.label, colorIdx: pc.colorIdx }))}
                                            yDomain={effectiveYDomain}
                                        />
                                    ) : isLoadingDetail ? (
                                        <div className="h-full flex items-center justify-center">
                                            <div className="w-5 h-5 border-2 border-primary/30 border-t-primary rounded-full animate-spin" />
                                        </div>
                                    ) : null}

                                    {/* P-values overlay — only for forecast years (2027+), not historical/present */}
                                    {(() => {
                                        // P-values represent model uncertainty — only meaningful for forecast years
                                        const presentYear = originYear + 2  // 2026
                                        if (year <= presentYear) return null
                                        // Index into fanChartData for the current slider year
                                        const yIdx = fanChartData?.years?.indexOf(year) ?? -1
                                        const pv = {
                                            p90: (yIdx >= 0 ? fanChartData?.p90?.[yIdx] : null) ?? displayProps.p90,
                                            p75: displayProps.p75, // p75/p25 only in tile props
                                            p50: (yIdx >= 0 ? fanChartData?.p50?.[yIdx] : null) ?? displayProps.p50 ?? displayProps.value,
                                            p25: displayProps.p25,
                                            p10: (yIdx >= 0 ? fanChartData?.p10?.[yIdx] : null) ?? displayProps.p10,
                                        }
                                        if (pv.p10 == null && pv.p90 == null) return null
                                        return (
                                            <div className="absolute top-5 left-4 text-[8px] leading-snug rounded px-1 py-0.5" style={{ textShadow: '0 0 3px var(--background), 0 0 3px var(--background)' }}>
                                                <div className="flex items-baseline gap-1">
                                                    <span className="font-medium text-[9px]">{formatValue(pv.p90)}</span>
                                                    <span className="text-muted-foreground/50">P90</span>
                                                </div>
                                                {pv.p75 != null && (
                                                    <div className="flex items-baseline gap-1">
                                                        <span className="font-medium text-[9px]">{formatValue(pv.p75)}</span>
                                                        <span className="text-muted-foreground/50">P75</span>
                                                    </div>
                                                )}
                                                <div className="flex items-baseline gap-1 bg-primary/10 rounded px-0.5">
                                                    <span className="font-bold text-[9px] text-primary">{formatValue(pv.p50)}</span>
                                                    <span className="text-primary/70">P50</span>
                                                </div>
                                                {pv.p25 != null && (
                                                    <div className="flex items-baseline gap-1">
                                                        <span className="font-medium text-[9px]">{formatValue(pv.p25)}</span>
                                                        <span className="text-muted-foreground/50">P25</span>
                                                    </div>
                                                )}
                                                <div className="flex items-baseline gap-1">
                                                    <span className="font-medium text-[9px]">{formatValue(pv.p10)}</span>
                                                    <span className="text-muted-foreground/50">P10</span>
                                                </div>
                                            </div>
                                        )
                                    })()}
                                </div>

                                <div className="pt-1 mt-0 border-t border-border/50 text-center">
                                    <div className="text-[9px] text-muted-foreground flex justify-center items-center gap-1.5">
                                        <Bot className="w-3 h-3 text-primary/50" />
                                        <span>AI Forecast</span>
                                    </div>
                                </div>

                                {/* Talk to Homecastr button — only when selected */}
                                {selectedId && onConsultAI && (
                                    <div className="pt-2 mt-1 border-t border-border/50">
                                        <button
                                            onClick={(e) => {
                                                e.stopPropagation()
                                                onConsultAI({
                                                    predictedValue: displayProps.p50 ?? displayProps.value ?? null,
                                                    opportunityScore: null,
                                                    capRate: null,
                                                })
                                            }}
                                            className="w-full flex items-center justify-center gap-2 px-3 py-2 rounded-lg bg-primary/15 hover:bg-primary/25 border border-primary/30 text-primary text-xs font-semibold transition-all hover:scale-[1.02] active:scale-[0.98]"
                                        >
                                            <HomecastrLogo variant="horizontal" size={14} />
                                            <span>Talk to live agent</span>
                                        </button>
                                    </div>
                                )}
                            </div>
                        </>
                    )}
                </div>,
                document.body
            )}

        </div>
    )
}
