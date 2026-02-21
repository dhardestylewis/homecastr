"use client"

import React, { useEffect, useRef, useState, useCallback } from "react"
import maplibregl from "maplibre-gl"
import "maplibre-gl/dist/maplibre-gl.css"
import type { FilterState, MapState, FanChartData } from "@/lib/types"
import { cn } from "@/lib/utils"
import { useRouter, useSearchParams } from "next/navigation"
import { HomecastrLogo } from "@/components/homecastr-logo"
import { Bot } from "lucide-react"
import { FanChart } from "@/components/fan-chart"

// Tooltip positioning constants
const SIDEBAR_WIDTH = 340
const TOOLTIP_WIDTH = 320
const TOOLTIP_HEIGHT = 200

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

    // Fan chart detail state
    const [fanChartData, setFanChartData] = useState<FanChartData | null>(null)
    const [historicalValues, setHistoricalValues] = useState<number[] | undefined>(undefined)
    const [isLoadingDetail, setIsLoadingDetail] = useState(false)
    const detailFetchRef = useRef<string | null>(null)

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
                -100000, "#ef4444",
                0, "#f8f8f8",
                100000, "#3b82f6",
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

            // Fetch fan chart detail for this feature
            const hoverLevel = getSourceLayer(zoom)
            fetchForecastDetail(id, hoverLevel)

            // Update tooltip (only if no selection is fixed)
            if (!selectedIdRef.current) {
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
            }
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
                    setFixedTooltipPos(null)
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
                setFixedTooltipPos(null)
                onFeatureSelect(null)
                return
            }

            selectedIdRef.current = id
            setSelectedId(id)
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
        })

            // Store refs
            ; (map as any)._activeSuffix = "a"
            ; (map as any)._isLoaded = true
        mapRef.current = map

        return () => {
            map.remove()
        }
    }, []) // Init once

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
    const displayProps = tooltipData?.properties

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

            {/* Forecast Tooltip - styled to match MapTooltip */}
            {isLoaded && displayPos && displayProps && (
                <div
                    className="fixed z-[9999] pointer-events-none"
                    style={{
                        left: displayPos.globalX,
                        top: displayPos.globalY,
                    }}
                >
                    <div className="glass-panel shadow-2xl overflow-hidden rounded-xl w-[320px]">
                        {/* Header - matching MapTooltip */}
                        <div className="flex items-center justify-between px-3 py-2 border-b border-border/50 bg-muted/40 backdrop-blur-md">
                            <div className="flex items-center gap-2">
                                <HomecastrLogo size={18} />
                                <span className="font-bold text-[10px] tracking-wide text-foreground uppercase">Homecastr</span>
                                <span className="px-1.5 py-0.5 bg-violet-500/20 text-violet-400 text-[8px] font-semibold uppercase tracking-wider rounded">Forecast</span>
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
                            <div className="font-mono text-xs text-muted-foreground truncate">
                                {displayProps.id}
                            </div>
                        </div>

                        {/* Content Body - matching MapTooltip desktop layout */}
                        <div className="p-4 space-y-5">
                            <div className="grid grid-cols-2 gap-3">
                                <div className="text-center pl-6">
                                    <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold mb-1">
                                        Forecast ({year})
                                    </div>
                                    <div className="text-xl font-bold text-foreground tracking-tight">
                                        {formatValue(displayProps.p50 ?? displayProps.value)}
                                    </div>
                                </div>
                                <div className="text-center pr-6">
                                    <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold mb-1">Horizon</div>
                                    <div className="text-xl font-bold text-foreground tracking-tight">
                                        {displayProps.horizon_m != null ? `+${displayProps.horizon_m}mo` : "—"}
                                    </div>
                                    <div className="text-[10px] text-muted-foreground mt-1">
                                        Origin {displayProps.origin_year ?? originYear}
                                    </div>
                                </div>
                            </div>

                            {/* Fan Chart - multi-horizon forecast */}
                            {fanChartData ? (
                                <div className="space-y-2">
                                    <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold flex justify-between">
                                        <span>Value Timeline</span>
                                        <span className="text-primary/70">{year}</span>
                                    </div>
                                    <div className="h-44 -mx-2 mb-2">
                                        <FanChart
                                            data={fanChartData}
                                            currentYear={year}
                                            height={160}
                                            historicalValues={historicalValues}
                                        />
                                    </div>
                                </div>
                            ) : isLoadingDetail ? (
                                <div className="h-32 flex items-center justify-center">
                                    <div className="w-5 h-5 border-2 border-primary/30 border-t-primary rounded-full animate-spin" />
                                </div>
                            ) : null}

                            {/* Fan quantiles - P10 to P90 */}
                            {(displayProps.p10 != null || displayProps.p90 != null) && (
                                <div className="space-y-2">
                                    <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold">
                                        Prediction Interval
                                    </div>
                                    <div className="grid grid-cols-5 gap-1 text-center text-[9px]">
                                        <div>
                                            <div className="text-muted-foreground/60">P10</div>
                                            <div className="font-medium">{formatValue(displayProps.p10)}</div>
                                        </div>
                                        <div>
                                            <div className="text-muted-foreground/60">P25</div>
                                            <div className="font-medium">{formatValue(displayProps.p25)}</div>
                                        </div>
                                        <div className="bg-primary/10 rounded px-1">
                                            <div className="text-primary/80">P50</div>
                                            <div className="font-bold text-primary">{formatValue(displayProps.p50 ?? displayProps.value)}</div>
                                        </div>
                                        <div>
                                            <div className="text-muted-foreground/60">P75</div>
                                            <div className="font-medium">{formatValue(displayProps.p75)}</div>
                                        </div>
                                        <div>
                                            <div className="text-muted-foreground/60">P90</div>
                                            <div className="font-medium">{formatValue(displayProps.p90)}</div>
                                        </div>
                                    </div>
                                </div>
                            )}

                            <div className="pt-1 mt-0 border-t border-border/50 text-center">
                                <div className="text-[9px] text-muted-foreground flex justify-center items-center gap-1.5">
                                    <Bot className="w-3 h-3 text-primary/50" />
                                    <span>AI Forecast • {displayProps.series_kind ?? "forecast"}</span>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
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
