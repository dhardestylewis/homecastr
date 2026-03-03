"use client"

import React, { useEffect, useRef, useState, useCallback, useMemo } from "react"
import { createPortal } from "react-dom"
import maplibregl from "maplibre-gl"
import "maplibre-gl/dist/maplibre-gl.css"
import type { FilterState, MapState, FanChartData } from "@/lib/types"
import { cn } from "@/lib/utils"
import { useRouter, useSearchParams } from "next/navigation"
import { HomecastrLogo } from "@/components/homecastr-logo"
import { Bot } from "lucide-react"
import { FanChart } from "@/components/fan-chart"
import { StreetViewCarousel } from "@/components/street-view-carousel"
import { useKeyboardOpen } from "@/hooks/use-keyboard-open"

// Tooltip positioning constants
const SIDEBAR_WIDTH = 390
const TOOLTIP_WIDTH = 320
const TOOLTIP_HEIGHT = 620

// Geography level definitions — zoom breakpoints must match the SQL router
const GEO_LEVELS = [
    { name: "zip3", minzoom: 0, maxzoom: 4.99, label: "ZIP3" },
    { name: "zcta", minzoom: 5, maxzoom: 7.99, label: "ZIP Code" },
    { name: "tract", minzoom: 8, maxzoom: 22, label: "Tract" },  // Extends to z22 as fallback underlay
    { name: "tabblock", minzoom: 12, maxzoom: 22, label: "Block" },
    { name: "parcel", minzoom: 17, maxzoom: 22, label: "Parcel" },
] as const

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

// Get label for current zoom
function getLevelLabel(zoom: number): string {
    for (const lvl of GEO_LEVELS) {
        if (zoom >= lvl.minzoom && zoom <= lvl.maxzoom) return lvl.label
    }
    return "Parcel"
}

// Get source-layer name for current zoom
function getSourceLayer(zoom: number): string {
    for (const lvl of GEO_LEVELS) {
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
}: ForecastMapProps) {
    const mapContainerRef = useRef<HTMLDivElement>(null)
    const mapRef = useRef<maplibregl.Map | null>(null)
    const [isLoaded, setIsLoaded] = useState(false)

    const router = useRouter()
    const searchParams = useSearchParams()

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
            if (map && selectedIdRef.current) {
                const zoom = map.getZoom()
                const sourceLayer = getSourceLayer(zoom)
                    ;["forecast-a", "forecast-b"].forEach((s) => {
                        try {
                            map.setFeatureState(
                                { source: s, sourceLayer, id: selectedIdRef.current! },
                                { selected: false }
                            )
                        } catch (err) { /* ignore */ }
                    })
            }
            selectedIdRef.current = null
            hoveredIdRef.current = null
            setSelectedId(null)
            setSelectedProps(null)
            setTooltipData(null)
            setFixedTooltipPos(null)
            setSelectedCoords(null)
            setFanChartData(null)
            setHistoricalValues(undefined)
            setComparisonData(null)
            setComparisonHistoricalValues(undefined)
            comparisonFetchRef.current = null
            detailFetchRef.current = null
            onFeatureSelect(null)
        }
    }, [mapState.selectedId])

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

    // Desktop drag-to-reposition state (locked tooltip)
    const dragRef = useRef<{ startX: number; startY: number; origX: number; origY: number } | null>(null)
    const userDraggedRef = useRef(false) // true once user has manually repositioned

    useEffect(() => {
        const onMouseMove = (e: MouseEvent) => {
            if (!dragRef.current) return
            const dx = e.clientX - dragRef.current.startX
            const dy = e.clientY - dragRef.current.startY
            setFixedTooltipPos({ globalX: dragRef.current.origX + dx, globalY: dragRef.current.origY + dy })
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
        const geoLevel = getSourceLayer(zoom)

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
        const url = `/api/geocode?lat=${lat}&lng=${lng}&level=${geoLevel}`
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
                    // Tract: show neighbourhood
                    name = addr.suburb || addr.neighbourhood || null
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
        const geoLevel = getSourceLayer(zoom)

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
                    name = addr.suburb || addr.neighbourhood || null
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
    // LRU cache for forecast detail responses (key: "level:featureId", value: {fanChart, historicalValues})
    const detailCacheRef = useRef<Map<string, { fanChart: FanChartData | null; historicalValues: number[] | undefined }>>(new Map())
    const DETAIL_CACHE_MAX = 1000

    // Selected feature's properties (locked when clicked)
    const [selectedProps, setSelectedProps] = useState<any>(null)
    const selectedPropsRef = useRef<any>(null)
    useEffect(() => { selectedPropsRef.current = selectedProps }, [selectedProps])

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

    // Street view dwell hover: only show after hovering same feature for 1.5s
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
    // If the map center is inside Harris County, we have 2025 data. Otherwise, ACS is 2024.
    const [mapCenter, setMapCenter] = useState<{ lat: number, lng: number } | null>(null)
    const isHarrisCounty = mapCenter
        ? (mapCenter.lat >= 29.4 && mapCenter.lat <= 30.2 && mapCenter.lng >= -95.9 && mapCenter.lng <= -94.9)
        : true // Default to true on initial load since default coordinates are Houston

    const originYear = isHarrisCounty ? 2025 : 2024
    const horizonM = (year - originYear) * 12

    // Fetch all horizons for a given feature to build FanChart data
    const fetchForecastDetail = useCallback(async (featureId: string, level: string) => {
        const cacheKey = `${level}:${featureId}`
        // Check cache first — instant re-hover
        const cached = detailCacheRef.current.get(cacheKey)
        if (cached) {
            // Move to end for LRU freshness
            detailCacheRef.current.delete(cacheKey)
            detailCacheRef.current.set(cacheKey, cached)
            setFanChartData(cached.fanChart)
            setHistoricalValues(cached.historicalValues)
            detailFetchRef.current = cacheKey
            return
        }
        if (detailFetchRef.current === cacheKey) return // already fetching
        detailFetchRef.current = cacheKey
        setIsLoadingDetail(true)
        try {
            const res = await fetch(`/api/forecast-detail?level=${level}&id=${encodeURIComponent(featureId)}`)
            if (!res.ok) throw new Error(`HTTP ${res.status}`)
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
            console.error('[FORECAST-DETAIL] fetch error:', err)
            setFanChartData(null)
        } finally {
            setIsLoadingDetail(false)
        }
    }, [originYear])

    // VIEW SYNC: Update URL and Origin State when map moves
    useEffect(() => {
        if (!mapRef.current) return
        const map = mapRef.current

        const onMoveEnd = () => {
            const center = map.getCenter()
            const zoom = map.getZoom()

            // Spatial check for dynamic origin year
            setMapCenter({ lat: center.lat, lng: center.lng })

            const params = new URLSearchParams(searchParams.toString())
            params.set("lat", center.lat.toFixed(5))
            params.set("lng", center.lng.toFixed(5))
            params.set("zoom", zoom.toFixed(2))
            router.replace(`?${params.toString()}`, { scroll: false })
        }

        map.on("moveend", onMoveEnd)
        return () => {
            map.off("moveend", onMoveEnd)
        }
    }, [isLoaded, searchParams, router])

    // HORIZON OR ORIGIN YEAR CHANGED: refresh the vector tile URLs so map grabs correct data
    useEffect(() => {
        if (!isLoaded || !mapRef.current) return
        const map = mapRef.current
        const _v = "6" // bump cache buster to force URL reload — zip3 geometry fix

        const updateSource = (id: string) => {
            const src = map.getSource(id) as maplibregl.VectorTileSource
            if (src) {
                // Maplibre doesn't have setUrl but it has setTiles
                src.setTiles([
                    `${window.location.origin}/api/forecast-tiles/{z}/{x}/{y}?originYear=${originYear}&horizonM=${horizonM}&v=${_v}`,
                ])
            }
        }
        updateSource("forecast-a")
        updateSource("forecast-b")
    }, [year, originYear, horizonM, isLoaded])



    // Color ramp: growth mode uses growth_pct (% change from baseline).
    // Zero-centered: negative growth → blue, zero → neutral white, positive → amber/red.
    // This matches the tooltip which shows green ▲ for positive and red ▼ for negative.
    // The ramp is slightly asymmetric (wider on the positive side) because
    // the underlying distribution is right-skewed.
    // Value mode uses absolute p50 with fixed percentile breakpoints.
    const buildFillColor = (colorMode?: string): any => {
        if (colorMode === "growth") {
            const presentYear = originYear + 1  // 2026
            if (year === presentYear) return "#e5e5e5" // Present year: growth=0 → flat neutral
            const yrsFromPresent = Math.max(Math.abs(year - presentYear), 1)

            // Zero-centered breakpoints scaled by horizon
            const deepNeg = -5 - 4 * yrsFromPresent  // 1yr≈-9, 3yr≈-17, 5yr≈-25
            const slightNeg = -2 * yrsFromPresent     // 1yr≈-2, 3yr≈-6, 5yr≈-10
            const slightPos = 5 * yrsFromPresent      // 1yr≈5, 3yr≈15, 5yr≈25
            const hotPos = 20 * yrsFromPresent         // 1yr≈20, 3yr≈60, 5yr≈100
            return [
                "interpolate",
                ["linear"],
                ["coalesce", ["to-number", ["get", "growth_pct"], 0], 0],
                deepNeg, "#3b82f6",    // rare decline → deep blue
                slightNeg, "#93c5fd",  // slight decline → light blue
                0, "#f8f8f8",          // zero growth → neutral white
                slightPos, "#f59e0b",  // moderate growth → amber
                hotPos, "#ef4444",     // hot growth → deep red
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
        const initialLat = parseFloat(urlParams.get("lat") || "29.76")
        const initialLng = parseFloat(urlParams.get("lng") || "-95.37")
        const initialZoom = parseFloat(urlParams.get("zoom") || "10")

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
            maxZoom: 18,
            minZoom: 2,
            maxTileCacheSize: 30, // Keep very low — forces MapLibre to prioritize visible tiles over off-viewport prefetch
        })

        map.on("load", () => {
            setIsLoaded(true)

            const fillColor = buildFillColor()

            // Add A/B sources
            const addSource = (id: string) => {
                map.addSource(id, {
                    type: "vector",
                    url: "", // The URL is set dynamically in the useEffect hook based on originYear
                    minzoom: 0,
                    maxzoom: 18,
                    promoteId: "id",
                })
            }

            addSource("forecast-a")
            addSource("forecast-b")

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
                            "fill-opacity": 0.55,
                            "fill-outline-color": "rgba(255,255,255,0.2)",
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
                                "rgba(0,0,0,0)",
                            ],
                            "line-width": [
                                "case",
                                ["boolean", ["feature-state", "selected"], false],
                                3,
                                ["boolean", ["feature-state", "hover"], false],
                                2,
                                0,
                            ],
                            "line-opacity": [
                                "case",
                                [
                                    "any",
                                    ["boolean", ["feature-state", "selected"], false],
                                    ["boolean", ["feature-state", "hover"], false],
                                ],
                                1,
                                0,
                            ],
                        },
                    })
                }
            }

            addLayersForSource("forecast-a", "a", true)
            addLayersForSource("forecast-b", "b", false)
        })

        // Suppress MapLibre tile loading error events (e.g. transient 500s from Supabase)
        map.on("error", (e: any) => {
            const rawMsg = e?.error?.message || e?.message || (typeof e === 'string' ? e : "Unknown MapLibre error")
            if (typeof rawMsg === "string" && (
                rawMsg.includes("status") ||
                rawMsg.includes("AJAXError") ||
                rawMsg.includes("Cannot read properties of undefined")
            )) {
                // Silently ignore tile fetch errors — the retry + empty tile fallback handles these
                return
            }
            console.error("[MapLibre] Error:", rawMsg)
        })

        // HOVER handling
        map.on("mousemove", (e: maplibregl.MapMouseEvent) => {
            const zoom = map.getZoom()
            const sourceLayer = getSourceLayer(zoom)

            // Query fill layers for active suffix
            const activeSuffix = (map as any)._activeSuffix || "a"
            const fillLayerId = `forecast-fill-${sourceLayer}-${activeSuffix}`

            const features = map.getLayer(fillLayerId)
                ? map.queryRenderedFeatures(e.point, { layers: [fillLayerId] })
                : []

            if (features.length === 0) {
                // Clear hover
                if (hoveredIdRef.current) {
                    ;["forecast-a", "forecast-b"].forEach((s) => {
                        try {
                            map.removeFeatureState({ source: s, sourceLayer })
                        } catch (err) {
                            /* ignore */
                        }
                    })
                    hoveredIdRef.current = null
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
                ;["forecast-a", "forecast-b"].forEach((s) => {
                    try {
                        map.setFeatureState(
                            { source: s, sourceLayer, id: hoveredIdRef.current! },
                            { hover: false }
                        )
                    } catch (err) {
                        /* ignore */
                    }
                })
            }

            hoveredIdRef.current = id
            onFeatureHover(id)

                // Set hover state
                ;["forecast-a", "forecast-b"].forEach((s) => {
                    try {
                        map.setFeatureState(
                            { source: s, sourceLayer, id },
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
                    const hoverZoom = map.getZoom()
                    const hoverLevel = getSourceLayer(hoverZoom)
                    fetchForecastDetail(id, hoverLevel)
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
            const features = map.getLayer(fillLayerId)
                ? map.queryRenderedFeatures(point, { layers: [fillLayerId] })
                : []
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
        }, { passive: true })

        // MOUSELEAVE: clear tooltip when cursor exits the map (unless locked)
        map.getCanvas().addEventListener("mouseleave", () => {
            if (hoverDetailTimerRef.current) { clearTimeout(hoverDetailTimerRef.current); hoverDetailTimerRef.current = null }
            if (!selectedIdRef.current) {
                setTooltipData(null)
                detailFetchRef.current = null // allow re-fetch on re-hover
            }
            if (hoveredIdRef.current) {
                const zoom = map.getZoom()
                const sourceLayer = getSourceLayer(zoom)
                    ;["forecast-a", "forecast-b"].forEach((s) => {
                        try {
                            map.setFeatureState(
                                { source: s, sourceLayer, id: hoveredIdRef.current! },
                                { hover: false }
                            )
                        } catch { }
                    })
                hoveredIdRef.current = null
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

            const features = map.getLayer(fillLayerId)
                ? map.queryRenderedFeatures(e.point, { layers: [fillLayerId] })
                : []

            // Clear hover detail timer so click fetch takes precedence
            if (hoverDetailTimerRef.current) { clearTimeout(hoverDetailTimerRef.current); hoverDetailTimerRef.current = null }
            detailFetchRef.current = null // allow click to re-fetch even if same id

            if (features.length === 0) {
                // Clear selection
                if (selectedIdRef.current) {
                    ;["forecast-a", "forecast-b"].forEach((s) => {
                        try {
                            map.removeFeatureState({ source: s, sourceLayer })
                        } catch (err) {
                            /* ignore */
                        }
                    })
                    selectedIdRef.current = null
                    setSelectedId(null)
                    setSelectedProps(null)
                    setFixedTooltipPos(null)
                    userDraggedRef.current = false // reset for next lock
                    setSelectedCoords(null)
                    setComparisonData(null)
                    setComparisonHistoricalValues(undefined)
                    comparisonFetchRef.current = null
                    onFeatureSelect(null)
                }
                return
            }

            const feature = features[0]
            const id = (feature.properties?.id || feature.id) as string
            if (!id) return

            // Clear prev selection — use the sourceLayer where the selection was made,
            // NOT the current zoom's sourceLayer, to handle zoom-between-taps on mobile
            if (selectedIdRef.current) {
                const prevLayer = selectedSourceLayerRef.current || sourceLayer
                // Clear on BOTH the previous layer AND current layer to handle edge cases
                const layersToClean = new Set([prevLayer, sourceLayer])
                    ;["forecast-a", "forecast-b"].forEach((s) => {
                        layersToClean.forEach((sl) => {
                            try {
                                map.setFeatureState(
                                    { source: s, sourceLayer: sl, id: selectedIdRef.current! },
                                    { selected: false }
                                )
                            } catch (err) {
                                /* ignore */
                            }
                        })
                    })
            }

            // Toggle selection
            if (selectedIdRef.current === id) {
                selectedIdRef.current = null
                setSelectedId(null)
                setSelectedProps(null)
                setFixedTooltipPos(null)
                setSelectedCoords(null)
                setComparisonData(null)
                setComparisonHistoricalValues(undefined)
                comparisonFetchRef.current = null
                onFeatureSelect(null)
                return
            }

            selectedIdRef.current = id
            selectedSourceLayerRef.current = sourceLayer
            setSelectedId(id)
            setSelectedProps(feature.properties)
            setComparisonData(null)
            setComparisonHistoricalValues(undefined)
            comparisonFetchRef.current = null
            onFeatureSelect(id)

            // Fetch fan chart detail for newly selected area (critical on mobile where hover doesn't fire)
            const clickLevel = getSourceLayer(zoom)
            fetchForecastDetail(id, clickLevel)

                // Set selected state
                ;["forecast-a", "forecast-b"].forEach((s) => {
                    try {
                        map.setFeatureState(
                            { source: s, sourceLayer, id },
                            { selected: true }
                        )
                    } catch (err) {
                        /* ignore */
                    }
                })

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
        })

            // Store refs
            ; (map as any)._activeSuffix = "a"
            ; (map as any)._isLoaded = true
        mapRef.current = map

        return () => {
            map.remove()
        }
    }, []) // Init once

    // ESC key handler to clear selection
    useEffect(() => {
        const handleKeyDown = (e: KeyboardEvent) => {
            if (e.key === "Escape" && selectedIdRef.current) {
                const map = mapRef.current
                if (map) {
                    const zoom = map.getZoom()
                    const sourceLayer = getSourceLayer(zoom)
                        ;["forecast-a", "forecast-b"].forEach((s) => {
                            try {
                                map.setFeatureState(
                                    { source: s, sourceLayer, id: selectedIdRef.current! },
                                    { selected: false }
                                )
                            } catch (err) { /* ignore */ }
                        })
                }
                if (hoverDetailTimerRef.current) { clearTimeout(hoverDetailTimerRef.current); hoverDetailTimerRef.current = null }
                selectedIdRef.current = null
                setSelectedId(null)
                setSelectedProps(null)
                setFixedTooltipPos(null)
                setFanChartData(null)
                setHistoricalValues(undefined)
                setComparisonData(null)
                setComparisonHistoricalValues(undefined)
                comparisonFetchRef.current = null
                detailFetchRef.current = null
                onFeatureSelect(null)
            }
        }
        window.addEventListener("keydown", handleKeyDown)
        return () => window.removeEventListener("keydown", handleKeyDown)
    }, [onFeatureSelect])

    // Dynamic hover outline color: amber (primary) when nothing selected,
    // lime (comparison) when a feature is locked
    useEffect(() => {
        const map = mapRef.current
        if (!map || !isLoaded) return
        const hoverColor = selectedId ? "#a3e635" : "#fbbf24"
        for (const suffix of ["a", "b"]) {
            for (const lvl of GEO_LEVELS) {
                const layerId = `forecast-outline-${lvl.name}-${suffix}`
                if (!map.getLayer(layerId)) continue
                try {
                    map.setPaintProperty(layerId, "line-color", [
                        "case",
                        ["boolean", ["feature-state", "selected"], false],
                        "#fbbf24",   // amber — always for locked selection
                        ["boolean", ["feature-state", "hover"], false],
                        hoverColor,  // amber when previewing, lime when comparing
                        "rgba(0,0,0,0)",
                    ])
                } catch { /* layer may not exist yet */ }
            }
        }
    }, [selectedId, isLoaded])

    // Tavus map-action handler: clear_selection, fly_to_location
    useEffect(() => {
        const handleTavusAction = (e: Event) => {
            const { action, params } = (e as CustomEvent).detail || {}
            if (action === "clear_selection") {
                const map = mapRef.current
                if (map && selectedIdRef.current) {
                    const zoom = map.getZoom()
                    const sourceLayer = getSourceLayer(zoom)
                        ;["forecast-a", "forecast-b"].forEach((s) => {
                            try {
                                map.setFeatureState(
                                    { source: s, sourceLayer, id: selectedIdRef.current! },
                                    { selected: false }
                                )
                            } catch (err) { /* ignore */ }
                        })
                }
                selectedIdRef.current = null
                hoveredIdRef.current = null
                setSelectedId(null)
                setSelectedProps(null)
                setTooltipData(null)
                setFixedTooltipPos(null)
                setSelectedCoords(null)
                setFanChartData(null)
                setHistoricalValues(undefined)
                setComparisonData(null)
                setComparisonHistoricalValues(undefined)
                comparisonFetchRef.current = null
                detailFetchRef.current = null
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
                                    fetchForecastDetail(id, sourceLayer)
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
                                    fetchForecastDetail(id, sourceLayer)
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
                                    ;["forecast-a", "forecast-b"].forEach((s) => {
                                        try { map.setFeatureState({ source: s, sourceLayer, id: selectedIdRef.current! }, { selected: false }) } catch { }
                                    })
                                }
                                selectedIdRef.current = id
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
                                fetchForecastDetail(id, sourceLayer)
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
        const newColor = buildFillColor(filters.colorMode)
        for (const lvl of GEO_LEVELS) {
            for (const suffix of ["a", "b"]) {
                const layerId = `forecast-fill-${lvl.name}-${suffix}`
                if (map.getLayer(layerId)) {
                    map.setPaintProperty(layerId, "fill-color", newColor)
                }
            }
        }
    }, [filters.colorMode, isLoaded])

    // UPDATE YEAR — Seamless A/B swap (same pattern as vector-map.tsx)
    useEffect(() => {
        if (!isLoaded || !mapRef.current) return
        const map = mapRef.current

        const currentSuffix = (map as any)._activeSuffix || "a"
        const nextSuffix = currentSuffix === "a" ? "b" : "a"
        const nextSource = `forecast-${nextSuffix}`
        const currentSource = `forecast-${currentSuffix}`

        // Update NEXT source tiles
        const source = map.getSource(nextSource)
        if (source && source.type === "vector") {
            ; (source as any).setTiles([
                `${window.location.origin}/api/forecast-tiles/{z}/{x}/{y}?originYear=${originYear}&horizonM=${horizonM}&v=6`,
            ])
        }

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
            for (const lvl of GEO_LEVELS) {
                const fillNext = `forecast-fill-${lvl.name}-${nextSuffix}`
                const outlineNext = `forecast-outline-${lvl.name}-${nextSuffix}`
                const fillCur = `forecast-fill-${lvl.name}-${currentSuffix}`
                const outlineCur = `forecast-outline-${lvl.name}-${currentSuffix}`

                if (map.getLayer(fillNext)) map.setLayoutProperty(fillNext, "visibility", "visible")
                if (map.getLayer(outlineNext)) map.setLayoutProperty(outlineNext, "visibility", "visible")
                if (map.getLayer(fillCur)) map.setLayoutProperty(fillCur, "visibility", "none")
                if (map.getLayer(outlineCur)) map.setLayoutProperty(outlineCur, "visibility", "none")
            }

            ; (map as any)._activeSuffix = nextSuffix

            // Clear the old source's tile cache to free memory
            const oldSource = map.getSource(currentSource)
            if (oldSource && typeof (oldSource as any).clearTiles === "function") {
                (oldSource as any).clearTiles()
            }
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
    }, [year, isLoaded, filters.colorMode, originYear, horizonM])

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
        if (vals.length === 0) return viewportYDomain
        const dataMin = Math.min(...vals)
        const dataMax = Math.max(...vals)
        // Only extend, never shrink from viewport range
        if (dataMin >= lo && dataMax <= hi) return viewportYDomain // no extension needed
        return [Math.min(lo, dataMin), Math.max(hi, dataMax)]
    }, [viewportYDomain, fanChartData, historicalValues, comparisonData, comparisonHistoricalValues])

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
            const level = getSourceLayer(zoom)
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
                const res = await fetch(`/api/forecast-detail?level=${level}&id=${encodeURIComponent(hoveredId)}&originYear=${originYear}`)
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
    }, [selectedId, tooltipData?.properties?.id, originYear, isShiftHeld])

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

            {/* Forecast Tooltip — portal-based, responsive for mobile + desktop */}
            {isLoaded && displayPos && displayProps && createPortal(
                <div
                    className={cn(
                        "z-[9999] glass-panel shadow-2xl overflow-hidden flex flex-col",
                        isMobile
                            ? `fixed left-0 right-0 w-full rounded-t-xl rounded-b-none border-t border-x-0 border-b-0 pointer-events-auto`
                            : "fixed rounded-xl w-[320px]",
                        !isMobile && (selectedId ? "pointer-events-auto cursor-move" : "pointer-events-none")
                    )}
                    style={isMobile ? {
                        transform: `translateY(calc(${mobileMinimized ? '100% - 24px' : '0px'} + ${swipeDragOffset}px))`,
                        transition: swipeTouchStart === null ? 'transform 0.3s ease-out' : 'none',
                        height: '25vh',
                        maxHeight: '25vh',
                        bottom: (isChatOpen || isTavusOpen) ? '25vh' : '0px',
                        overflowY: 'hidden',
                    } : {
                        left: displayPos.globalX,
                        top: displayPos.globalY,
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
                        // Skip swipe tracking if touch is on the header bar
                        const target = e.target as HTMLElement
                        if (target.closest('[data-tooltip-header]')) return
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
                                onFeatureSelect(null)
                            } else if (swipeDragOffset > 50) {
                                setMobileMinimized(true)
                            }
                        }
                        setSwipeDragOffset(0)
                        setSwipeTouchStart(null)
                    } : undefined}
                >
                    {/* Mobile Header — compact, at top of tooltip */}
                    {isMobile && (
                        <div
                            className="w-full flex items-center justify-between px-2 pt-1 h-8 bg-muted/40 backdrop-blur-md border-b border-border/50 shrink-0"
                            data-tooltip-header="true"
                        >
                            <div className="flex items-center gap-1.5">
                                <HomecastrLogo variant="horizontal" size={14} />
                                <span className="px-1 py-0.5 bg-violet-500/20 text-violet-400 text-[7px] font-semibold uppercase tracking-wider rounded">Forecast</span>
                            </div>
                            <button
                                onClick={(e) => {
                                    e.stopPropagation();
                                    e.preventDefault();
                                    selectedIdRef.current = null;
                                    setSelectedId(null);
                                    hoveredIdRef.current = null;
                                    setTooltipData(null);
                                    setFixedTooltipPos(null);
                                    setSelectedCoords(null);
                                    onFeatureSelect(null);
                                }}
                                className="w-6 h-6 flex items-center justify-center rounded-full active:bg-muted/60 text-muted-foreground"
                                aria-label="Close tooltip"
                                style={{ touchAction: 'manipulation' }}
                            >
                                <svg width="10" height="10" viewBox="0 0 12 12" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" style={{ pointerEvents: 'none' }}>
                                    <line x1="2" y1="2" x2="10" y2="10" /><line x1="10" y1="2" x2="2" y2="10" />
                                </svg>
                            </button>
                        </div>
                    )}

                    {/* Header - matching MapTooltip (hidden on mobile) */}
                    {!isMobile && (
                        <>
                            <div
                                className="flex items-center justify-between px-3 py-2 border-b border-border/50 bg-muted/40 backdrop-blur-md select-none"
                            >
                                <div className="flex items-center gap-2">
                                    <HomecastrLogo variant="horizontal" size={18} />
                                    <span className={cn(
                                        "px-1.5 py-0.5 text-[8px] font-semibold uppercase tracking-wider rounded",
                                        year > 2026
                                            ? "bg-violet-500/20 text-violet-400"
                                            : "bg-sky-500/20 text-sky-400"
                                    )}>{year > 2026 ? "Forecast" : "Historical"}</span>
                                    {selectedId && (
                                        <span className="px-1.5 py-0.5 bg-yellow-500/20 text-yellow-600 dark:text-yellow-400 text-[8px] font-semibold uppercase tracking-wider rounded">Locked</span>
                                    )}
                                </div>
                                <div className="flex items-center gap-1.5">
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
                            </div>
                        </>
                    )}

                    {/* Street View Carousel — hidden when keyboard is open on mobile */}
                    {isMobile && !(isKeyboardOpen) ? (
                        /* Mobile: Side-by-side — StreetView left, Chart right */
                        <div className="flex flex-row flex-1 min-h-0 overflow-hidden">
                            {/* StreetView — left half (always visible, shows loading placeholder) */}
                            <div className="w-1/2 h-full overflow-hidden">
                                {process.env.NEXT_PUBLIC_GOOGLE_MAPS_KEY && (selectedId ? selectedCoords : (hoverDwell ? tooltipCoords : null)) ? (
                                    <StreetViewCarousel
                                        h3Ids={[]}
                                        apiKey={process.env.NEXT_PUBLIC_GOOGLE_MAPS_KEY}
                                        coordinates={(selectedId ? selectedCoords : tooltipCoords)!}
                                    />
                                ) : (
                                    /* Loading placeholder while dwell timer is active */
                                    <div className="w-full h-full bg-zinc-900/80 flex flex-col items-center justify-center gap-2">
                                        <div className="w-6 h-6 border-2 border-white/20 border-t-white/60 rounded-full animate-spin" />
                                        <span className="text-[10px] text-white/40 uppercase tracking-wider">Street View</span>
                                    </div>
                                )}
                            </div>

                            {/* Chart — right half */}
                            <div className="w-1/2 h-full overflow-hidden flex flex-col">
                                {(() => {
                                    const currentVal = historicalValues?.[historicalValues.length - 1] ?? null
                                    const forecastVal = displayProps.p50 ?? displayProps.value ?? null
                                    const isPast = year < originYear + 1
                                    const isPresent = year === originYear + 1 // 2026 = "now"
                                    const leftLabel = isPresent ? "Now" : isPast ? String(year) : "Now"
                                    const leftVal = isPresent ? currentVal : isPast ? forecastVal : currentVal
                                    const rightLabel = isPresent ? String(year) : isPast ? "Now" : String(year)
                                    const rightVal = isPresent ? currentVal : isPast ? currentVal : forecastVal
                                    const pctBase = isPresent ? null : isPast ? forecastVal : currentVal
                                    const pctTarget = isPresent ? null : isPast ? currentVal : forecastVal
                                    const pctChange = pctBase && pctTarget ? ((pctTarget - pctBase) / pctBase * 100) : null
                                    return (
                                        <div className="relative w-full flex-1 h-full">
                                            {/* Full-width chart */}
                                            <div className="w-full h-full">
                                                {fanChartData ? (
                                                    <FanChart data={fanChartData} currentYear={year} height={200} historicalValues={historicalValues} comparisonData={comparisonData} comparisonHistoricalValues={comparisonHistoricalValues} yDomain={effectiveYDomain} />
                                                ) : isLoadingDetail ? (
                                                    <div className="h-full flex items-center justify-center">
                                                        <div className="w-4 h-4 border-2 border-primary/30 border-t-primary rounded-full animate-spin" />
                                                    </div>
                                                ) : null}
                                                {/* Overlaid stat badges */}
                                                <div className="absolute top-1 left-1 px-1.5 py-0.5 rounded bg-background/80 backdrop-blur-sm border border-border/30">
                                                    <div className="text-[7px] uppercase tracking-wider text-muted-foreground font-semibold">{leftLabel}</div>
                                                    <div className="text-[10px] font-bold text-foreground">{formatValue(leftVal)}</div>
                                                </div>
                                                {!isPresent && (
                                                    <div className="absolute top-1 right-1 px-1.5 py-0.5 rounded bg-background/80 backdrop-blur-sm border border-border/30 text-right">
                                                        <div className="text-[7px] uppercase tracking-wider text-muted-foreground font-semibold">{rightLabel}</div>
                                                        <div className="text-[10px] font-bold text-foreground">{formatValue(rightVal)}</div>
                                                        {pctChange != null && (
                                                            <div className={`text-[8px] font-bold ${pctChange >= 0 ? 'text-emerald-500' : 'text-red-500'}`}>
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
                        </div>
                    ) : (
                        <>
                            {/* Desktop: StreetView above chart */}
                            {!(isMobile && isKeyboardOpen) && process.env.NEXT_PUBLIC_GOOGLE_MAPS_KEY && (selectedId ? selectedCoords : (hoverDwell ? tooltipCoords : null)) && (
                                <StreetViewCarousel
                                    h3Ids={[]}
                                    apiKey={process.env.NEXT_PUBLIC_GOOGLE_MAPS_KEY}
                                    coordinates={(selectedId ? selectedCoords : tooltipCoords)!}
                                />
                            )}
                            {/* Desktop Layout: Values above, full-width chart below */}
                            <div className="p-4 space-y-3">
                                {/* Current → Forecast header with % change */}
                                {(() => {
                                    const currentVal = historicalValues?.[historicalValues.length - 1] ?? null
                                    // At zcta/tract/block zoom levels the tile stores the area median as p50.
                                    // At parcel level, value is the individual estimate; p50 may not exist.
                                    const forecastVal = displayProps.p50 ?? displayProps.value ?? null
                                    const isPast = year < originYear + 1
                                    const isPresent = year === originYear + 1 // 2026 = "now"
                                    const leftLabel = isPresent ? "Now" : isPast ? String(year) : "Now"
                                    const leftVal = isPresent ? currentVal : isPast ? forecastVal : currentVal
                                    const rightLabel = isPresent ? String(year) : isPast ? "Now" : String(year)
                                    const rightVal = isPresent ? currentVal : isPast ? currentVal : forecastVal
                                    const pctBase = isPresent ? null : isPast ? forecastVal : currentVal
                                    const pctTarget = isPresent ? null : isPast ? currentVal : forecastVal
                                    const pctChange = pctBase && pctTarget ? ((pctTarget - pctBase) / pctBase * 100) : null
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
                                            comparisonData={comparisonData}
                                            comparisonHistoricalValues={comparisonHistoricalValues}
                                            yDomain={effectiveYDomain}
                                        />
                                    ) : isLoadingDetail ? (
                                        <div className="h-full flex items-center justify-center">
                                            <div className="w-5 h-5 border-2 border-primary/30 border-t-primary rounded-full animate-spin" />
                                        </div>
                                    ) : null}

                                    {/* P-values overlay — top-right corner */}
                                    {(displayProps.p10 != null || displayProps.p90 != null) && (
                                        <div className="absolute top-5 right-4 text-[8px] leading-snug rounded px-1 py-0.5" style={{ textShadow: '0 0 3px var(--background), 0 0 3px var(--background)' }}>
                                            <div className="flex items-baseline gap-1">
                                                <span className="font-medium text-[9px]">{formatValue(displayProps.p90)}</span>
                                                <span className="text-muted-foreground/50">P90</span>
                                            </div>
                                            <div className="flex items-baseline gap-1">
                                                <span className="font-medium text-[9px]">{formatValue(displayProps.p75)}</span>
                                                <span className="text-muted-foreground/50">P75</span>
                                            </div>
                                            <div className="flex items-baseline gap-1 bg-primary/10 rounded px-0.5">
                                                <span className="font-bold text-[9px] text-primary">{formatValue(displayProps.p50 ?? displayProps.value)}</span>
                                                <span className="text-primary/70">P50</span>
                                            </div>
                                            <div className="flex items-baseline gap-1">
                                                <span className="font-medium text-[9px]">{formatValue(displayProps.p25)}</span>
                                                <span className="text-muted-foreground/50">P25</span>
                                            </div>
                                            <div className="flex items-baseline gap-1">
                                                <span className="font-medium text-[9px]">{formatValue(displayProps.p10)}</span>
                                                <span className="text-muted-foreground/50">P10</span>
                                            </div>
                                        </div>
                                    )}
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
