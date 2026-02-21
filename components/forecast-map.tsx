"use client"

import React, { useEffect, useRef, useState, useCallback } from "react"
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

// Tooltip positioning constants
const SIDEBAR_WIDTH = 340
const TOOLTIP_WIDTH = 320
const TOOLTIP_HEIGHT = 620

// Geography level definitions — zoom breakpoints must match the SQL router
const GEO_LEVELS = [
    { name: "zcta", minzoom: 0, maxzoom: 7.99, label: "ZCTA" },
    { name: "tract", minzoom: 8, maxzoom: 11.99, label: "Tract" },
    { name: "tabblock", minzoom: 12, maxzoom: 16.99, label: "Block" },
    { name: "parcel", minzoom: 17, maxzoom: 22, label: "Parcel" },
] as const

function getSmartTooltipPos(x: number, y: number, windowWidth: number, windowHeight: number) {
    if (typeof window === "undefined") return { x, y }

    let left = x + 20
    let top = y - 20

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

    top = Math.max(10, Math.min(top, windowHeight - TOOLTIP_HEIGHT - 10))
    left = Math.max(10, left)

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
}

export function ForecastMap({
    filters,
    mapState,
    year,
    onFeatureSelect,
    onFeatureHover,
    className,
    onConsultAI,
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
    const hoveredIdRef = useRef<string | null>(null)

    // Geographic coordinates for StreetView (from MapLibre events)
    const [tooltipCoords, setTooltipCoords] = useState<[number, number] | null>(null)

    // Mobile detection
    const [isMobile, setIsMobile] = useState(false)
    useEffect(() => {
        const check = () => setIsMobile(window.innerWidth < 768)
        check()
        window.addEventListener('resize', check)
        return () => window.removeEventListener('resize', check)
    }, [])

    // Mobile swipe-to-minimize state
    const [mobileMinimized, setMobileMinimized] = useState(false)
    const [swipeTouchStart, setSwipeTouchStart] = useState<number | null>(null)
    const [swipeDragOffset, setSwipeDragOffset] = useState(0)

    // Reset minimize when selection changes
    useEffect(() => {
        if (selectedId) setMobileMinimized(false)
    }, [selectedId])

    // Fan chart detail state
    const [fanChartData, setFanChartData] = useState<FanChartData | null>(null)
    const [historicalValues, setHistoricalValues] = useState<number[] | undefined>(undefined)
    const [isLoadingDetail, setIsLoadingDetail] = useState(false)
    const detailFetchRef = useRef<string | null>(null)

    // Selected feature's properties (locked when clicked)
    const [selectedProps, setSelectedProps] = useState<any>(null)

    // Comparison state: hover overlay when a feature is selected
    const [comparisonData, setComparisonData] = useState<FanChartData | null>(null)
    const [comparisonHistoricalValues, setComparisonHistoricalValues] = useState<number[] | undefined>(undefined)
    const comparisonFetchRef = useRef<string | null>(null)
    const comparisonTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

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

    // Compute origin year and horizon from the "year" slider
    // origin_year is always 2025, horizon_m is (year - 2025) * 12
    const originYear = 2025
    const horizonM = Math.max(0, (year - originYear) * 12) || 12

    // Fetch all horizons for a given feature to build FanChart data
    const fetchForecastDetail = useCallback(async (featureId: string, level: string) => {
        const cacheKey = `${level}:${featureId}`
        if (detailFetchRef.current === cacheKey) return // already fetching or fetched
        detailFetchRef.current = cacheKey
        setIsLoadingDetail(true)
        try {
            const res = await fetch(`/api/forecast-detail?level=${level}&id=${encodeURIComponent(featureId)}&originYear=${originYear}`)
            if (!res.ok) throw new Error(`HTTP ${res.status}`)
            const json = await res.json()
            if (json.years?.length > 0) {
                setFanChartData(json as FanChartData)
            } else {
                setFanChartData(null)
            }
            // Store historical values if present
            if (json.historicalValues && json.historicalValues.some((v: any) => v != null)) {
                setHistoricalValues(json.historicalValues)
            } else {
                setHistoricalValues(undefined)
            }
        } catch (err) {
            console.error('[FORECAST-DETAIL] fetch error:', err)
            setFanChartData(null)
        } finally {
            setIsLoadingDetail(false)
        }
    }, [originYear])

    // VIEW SYNC: Update URL when map moves
    useEffect(() => {
        if (!mapRef.current) return
        const map = mapRef.current

        const onMoveEnd = () => {
            const center = map.getCenter()
            const zoom = map.getZoom()
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



    // Color ramp used for fill layers
    const buildFillColor = (colorMode?: string): any =>
        colorMode === "growth"
            ? [
                "interpolate",
                ["linear"],
                ["coalesce", ["get", "p50"], ["get", "value"], 0],
                100000, "#ef4444",   // Low-value areas → red
                200000, "#f59e0b",   // Below median → amber
                300000, "#f8f8f8",   // Median (~$300k) → neutral white
                450000, "#60a5fa",   // Above median → light blue
                700000, "#3b82f6",   // High-value areas → blue
            ]
            : [
                "interpolate",
                ["linear"],
                ["coalesce", ["get", "p50"], ["get", "value"], 0],
                50000, "#1e1b4b",
                150000, "#4c1d95",
                300000, "#7c3aed",
                500000, "#db2777",
                800000, "#f59e0b",
                1500000, "#fbbf24",
            ]

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
        })

        map.on("load", () => {
            setIsLoaded(true)

            const fillColor = buildFillColor()

            // Add A/B sources
            const addSource = (id: string) => {
                map.addSource(id, {
                    type: "vector",
                    tiles: [
                        `${window.location.origin}/api/forecast-tiles/{z}/{x}/{y}?originYear=${originYear}&horizonM=${horizonM}`,
                    ],
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
                                "#fbbf24",
                                ["boolean", ["feature-state", "hover"], false],
                                "#ffffff",
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
                }
                map.getCanvas().style.cursor = ""
                return
            }

            map.getCanvas().style.cursor = "pointer"
            const feature = features[0]
            const id = (feature.properties?.id || feature.id) as string
            if (!id) return

            // Clear previous hover
            if (hoveredIdRef.current && hoveredIdRef.current !== id) {
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

            // Fetch fan chart detail for this feature (only if NOT locked to a selection)
            if (!selectedIdRef.current) {
                const hoverLevel = getSourceLayer(zoom)
                fetchForecastDetail(id, hoverLevel)
            }

            // Always update tooltipData with hovered feature's properties.
            // When locked, displayProps will use selectedProps instead,
            // but tooltipData.properties.id is needed for comparison tracking.
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
            setTooltipCoords([e.lngLat.lat, e.lngLat.lng])
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

            // Clear prev selection
            if (selectedIdRef.current) {
                ;["forecast-a", "forecast-b"].forEach((s) => {
                    try {
                        map.setFeatureState(
                            { source: s, sourceLayer, id: selectedIdRef.current! },
                            { selected: false }
                        )
                    } catch (err) {
                        /* ignore */
                    }
                })
            }

            // Toggle selection
            if (selectedIdRef.current === id) {
                selectedIdRef.current = null
                setSelectedId(null)
                setSelectedProps(null)
                setFixedTooltipPos(null)
                setComparisonData(null)
                setComparisonHistoricalValues(undefined)
                comparisonFetchRef.current = null
                onFeatureSelect(null)
                return
            }

            selectedIdRef.current = id
            setSelectedId(id)
            setSelectedProps(feature.properties)
            setComparisonData(null)
            setComparisonHistoricalValues(undefined)
            comparisonFetchRef.current = null
            onFeatureSelect(id)

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
            setFixedTooltipPos({ globalX: smartPos.x, globalY: smartPos.y })
            setTooltipData({
                globalX: smartPos.x,
                globalY: smartPos.y,
                properties: feature.properties,
            })
            setTooltipCoords([e.lngLat.lat, e.lngLat.lng])
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

    // UPDATE MODE — reactive fill-color paint
    useEffect(() => {
        if (!isLoaded || !mapRef.current) return
        const map = mapRef.current
        const newColor = buildFillColor(filters.mode)
        for (const lvl of GEO_LEVELS) {
            for (const suffix of ["a", "b"]) {
                const layerId = `forecast-fill-${lvl.name}-${suffix}`
                if (map.getLayer(layerId)) {
                    map.setPaintProperty(layerId, "fill-color", newColor)
                }
            }
        }
    }, [filters.mode, isLoaded])

    // UPDATE YEAR — Seamless A/B swap (same pattern as vector-map.tsx)
    useEffect(() => {
        if (!isLoaded || !mapRef.current) return
        const map = mapRef.current

        const currentSuffix = (map as any)._activeSuffix || "a"
        const nextSuffix = currentSuffix === "a" ? "b" : "a"
        const nextSource = `forecast-${nextSuffix}`

        // Update NEXT source tiles
        const source = map.getSource(nextSource)
        if (source && source.type === "vector") {
            ; (source as any).setTiles([
                `${window.location.origin}/api/forecast-tiles/{z}/{x}/{y}?originYear=${originYear}&horizonM=${horizonM}`,
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
            try {
                const res = await fetch(`/api/forecast-detail?level=${level}&id=${encodeURIComponent(hoveredId)}&originYear=${originYear}`)
                if (!res.ok) return
                const json = await res.json()
                if (json.years?.length > 0) {
                    setComparisonData(json as FanChartData)
                } else {
                    setComparisonData(null)
                }
                if (json.historicalValues?.some((v: any) => v != null)) {
                    setComparisonHistoricalValues(json.historicalValues)
                } else {
                    setComparisonHistoricalValues(undefined)
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

    // Viewport Y domain: compute from visible features on moveend
    useEffect(() => {
        if (!mapRef.current || !isLoaded) return
        const map = mapRef.current
        const computeYDomain = () => {
            const zoom = map.getZoom()
            const sourceLayer = getSourceLayer(zoom)
            const activeSuffix = (map as any)._activeSuffix || 'a'
            const layerId = `forecast-fill-${sourceLayer}-${activeSuffix}`
            if (!map.getLayer(layerId)) return
            const features = map.queryRenderedFeatures(undefined, { layers: [layerId] })
            if (features.length === 0) return
            const allVals: number[] = []
            for (const f of features) {
                const p = f.properties
                if (p?.value != null && Number.isFinite(p.value)) allVals.push(p.value)
                if (p?.p10 != null && Number.isFinite(p.p10)) allVals.push(p.p10)
                if (p?.p90 != null && Number.isFinite(p.p90)) allVals.push(p.p90)
            }
            if (allVals.length < 2) return
            allVals.sort((a, b) => a - b)
            // Use P10/P90 of visible values to exclude outliers
            const lo = allVals[Math.floor(allVals.length * 0.1)]
            const hi = allVals[Math.ceil(allVals.length * 0.9) - 1]
            if (lo < hi) {
                setViewportYDomain([lo, hi])
            }
        }
        map.on('moveend', computeYDomain)
        computeYDomain() // compute initial
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
                        "z-[9999] glass-panel shadow-2xl overflow-hidden",
                        isMobile
                            ? "fixed bottom-0 left-0 right-0 w-full rounded-t-xl rounded-b-none border-t border-x-0 border-b-0 pointer-events-auto"
                            : "fixed rounded-xl w-[320px]",
                        !isMobile && (selectedId ? "pointer-events-auto cursor-move" : "pointer-events-none")
                    )}
                    style={isMobile ? {
                        transform: `translateY(calc(${mobileMinimized ? '100% - 24px' : '0px'} + ${swipeDragOffset}px))`,
                        transition: swipeTouchStart === null ? 'transform 0.3s ease-out' : 'none',
                        maxHeight: '55vh',
                        overflowY: 'hidden',
                    } : {
                        left: displayPos.globalX,
                        top: displayPos.globalY,
                        maxHeight: 'calc(100vh - 40px)',
                        overflowY: 'auto',
                    }}
                    onTouchStart={isMobile ? (e) => setSwipeTouchStart(e.touches[0].clientY) : undefined}
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
                    {/* Mobile Drag Handle */}
                    {isMobile && (
                        <div className="w-full flex justify-center py-2 bg-muted/40 backdrop-blur-md cursor-grab active:cursor-grabbing">
                            <div className="w-10 h-1 bg-muted-foreground/30 rounded-full" />
                        </div>
                    )}

                    {/* Header - matching MapTooltip (hidden on mobile) */}
                    {!isMobile && (
                        <>
                            <div className="flex items-center justify-between px-3 py-2 border-b border-border/50 bg-muted/40 backdrop-blur-md">
                                <div className="flex items-center gap-2">
                                    <HomecastrLogo size={18} />
                                    <span className="font-bold text-[10px] tracking-wide text-foreground uppercase">Homecastr</span>
                                    <span className="px-1.5 py-0.5 bg-violet-500/20 text-violet-400 text-[8px] font-semibold uppercase tracking-wider rounded">Forecast</span>
                                    {selectedId && (
                                        <span className="px-1.5 py-0.5 bg-yellow-500/20 text-yellow-600 dark:text-yellow-400 text-[8px] font-semibold uppercase tracking-wider rounded">Locked</span>
                                    )}
                                </div>
                                {selectedId && <span className="text-[9px] text-muted-foreground">ESC to exit</span>}
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
                                <div className="font-mono text-xs text-muted-foreground truncate">
                                    {displayProps.id}
                                </div>
                            </div>
                        </>
                    )}

                    {/* Street View Carousel */}
                    {process.env.NEXT_PUBLIC_GOOGLE_MAPS_KEY && tooltipCoords && (
                        <StreetViewCarousel
                            h3Ids={[]}
                            apiKey={process.env.NEXT_PUBLIC_GOOGLE_MAPS_KEY}
                            coordinates={tooltipCoords}
                        />
                    )}

                    {/* Content Body */}
                    {isMobile ? (
                        /* Mobile Layout: [Now] [Chart] [Forecast] with values flanking */
                        <div className="px-2 py-2">
                            {(() => {
                                const currentVal = historicalValues?.[historicalValues.length - 1] ?? null
                                const forecastVal = displayProps.p50 ?? displayProps.value ?? null
                                const pctChange = currentVal && forecastVal ? ((forecastVal - currentVal) / currentVal * 100) : null
                                return (
                                    <>
                                        <div className="flex items-stretch gap-1">
                                            {/* Left: Current value */}
                                            <div className="flex flex-col justify-center items-center min-w-[55px] max-w-[80px] shrink-0 text-center">
                                                <div className="text-[8px] uppercase tracking-wider text-muted-foreground font-semibold">2026</div>
                                                <div className="text-xs font-bold text-foreground">{formatValue(currentVal)}</div>
                                            </div>
                                            {/* Center: FanChart */}
                                            <div className="flex-1 min-w-0 h-[150px]">
                                                {fanChartData ? (
                                                    <FanChart data={fanChartData} currentYear={year} height={150} historicalValues={historicalValues} comparisonData={comparisonData} comparisonHistoricalValues={comparisonHistoricalValues} yDomain={viewportYDomain} />
                                                ) : isLoadingDetail ? (
                                                    <div className="h-full flex items-center justify-center">
                                                        <div className="w-4 h-4 border-2 border-primary/30 border-t-primary rounded-full animate-spin" />
                                                    </div>
                                                ) : null}
                                            </div>
                                            {/* Right: Forecast value + % change */}
                                            <div className="flex flex-col justify-center items-center min-w-[55px] max-w-[80px] shrink-0 text-center">
                                                <div className="text-[8px] uppercase tracking-wider text-muted-foreground font-semibold">{year}</div>
                                                <div className="text-xs font-bold text-foreground">{formatValue(forecastVal)}</div>
                                                {pctChange != null && (
                                                    <div className={`text-[9px] font-bold ${pctChange >= 0 ? 'text-emerald-500' : 'text-red-500'}`}>
                                                        {pctChange >= 0 ? '▲' : '▼'} {Math.abs(pctChange).toFixed(1)}%
                                                    </div>
                                                )}
                                            </div>
                                        </div>
                                    </>
                                )
                            })()}
                        </div>
                    ) : (
                        /* Desktop Layout: Values above, full-width chart below */
                        <div className="p-4 space-y-3">
                            {/* Current → Forecast header with % change */}
                            {(() => {
                                const currentVal = historicalValues?.[historicalValues.length - 1] ?? null
                                const forecastVal = displayProps.p50 ?? displayProps.value ?? null
                                const pctChange = currentVal && forecastVal ? ((forecastVal - currentVal) / currentVal * 100) : null
                                return (
                                    <div className="flex items-center justify-between gap-3">
                                        <div className="text-center flex-1">
                                            <div className="text-[9px] uppercase tracking-wider text-muted-foreground font-semibold mb-0.5">2026</div>
                                            <div className="text-lg font-bold text-foreground tracking-tight">{formatValue(currentVal)}</div>
                                        </div>
                                        <div className="text-center shrink-0">
                                            {pctChange != null && (
                                                <div className={`text-sm font-bold ${pctChange >= 0 ? 'text-emerald-500' : 'text-red-500'}`}>
                                                    {pctChange >= 0 ? '▲' : '▼'} {Math.abs(pctChange).toFixed(1)}%
                                                </div>
                                            )}
                                            <div className="text-[9px] text-muted-foreground">→ {year}</div>
                                        </div>
                                        <div className="text-center flex-1">
                                            <div className="text-[9px] uppercase tracking-wider text-muted-foreground font-semibold mb-0.5">{year}</div>
                                            <div className="text-lg font-bold text-foreground tracking-tight">{formatValue(forecastVal)}</div>
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
                                        yDomain={viewportYDomain}
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
                                    <span>AI Forecast • {displayProps.series_kind ?? "forecast"}</span>
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
                                        <Bot className="w-3.5 h-3.5" />
                                        Talk to Homecastr
                                    </button>
                                </div>
                            )}
                        </div>
                    )}
                </div>,
                document.body
            )}

            {/* Forecast mode badge */}
            <div className="absolute top-4 right-4 z-50">
                <div className="glass-panel rounded-lg px-3 py-1.5 flex items-center gap-2 shadow-lg border border-white/10">
                    <div className="w-2 h-2 rounded-full bg-violet-500 animate-pulse" />
                    <span className="text-[10px] font-bold uppercase tracking-wider text-violet-300">
                        Forecast Mode
                    </span>
                    <span className="text-[10px] font-mono text-muted-foreground">
                        {year}
                    </span>
                </div>
            </div>
        </div>
    )
}
