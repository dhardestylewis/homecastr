"use client"

import type React from "react"
import { getH3DataV2 } from "@/app/actions/h3-data-v2"
import { cellToBoundary, latLngToCell } from "h3-js"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { createPortal } from "react-dom"
import { cn } from "@/lib/utils"
import { getOpportunityColor, getValueColor, formatOpportunity, formatCurrency, formatReliability } from "@/lib/utils/colors"
import { TrendingUp, TrendingDown, Minus } from "lucide-react"
import type { FilterState, FeatureProperties, MapState, DetailsResponse } from "@/lib/types"
import { getH3CellDetails } from "@/app/actions/h3-details"
import { getH3ChildTimelines } from "@/app/actions/h3-children"
import { FanChart } from "./fan-chart"

// Helper to get trend icon
const getTrendIcon = (trend: "up" | "down" | "stable" | undefined) => {
    if (trend === "up") return <TrendingUp className="h-3 w-3 text-green-500" />
    if (trend === "down") return <TrendingDown className="h-3 w-3 text-red-500" />
    return <Minus className="h-3 w-3 text-muted-foreground" />
    return <Minus className="h-3 w-3 text-muted-foreground" />
}

function useIsMobile() {
    const [isMobile, setIsMobile] = useState(false)
    useEffect(() => {
        const check = () => setIsMobile(window.innerWidth < 768)
        check()
        window.addEventListener('resize', check)
        return () => window.removeEventListener('resize', check)
    }, [])
    return isMobile
}

// MapView Props Interface
interface MapViewProps {
    filters: FilterState
    mapState: MapState
    onFeatureSelect: (id: string | null) => void
    onFeatureHover: (id: string | null) => void
    year?: number
    className?: string
    onMockDataDetected?: () => void
}

const HARRIS_COUNTY_CENTER = { lng: -95.3698, lat: 29.7604 }

function getReliabilityStrokeWidth(r: number): number {
    if (r < 0.2) return 1
    if (r < 0.4) return 1.5
    if (r < 0.6) return 2
    if (r < 0.8) return 2.5
    return 3
}

function latLngToMercator(lng: number, lat: number) {
    const x = (lng + 180) / 360
    const latRad = (lat * Math.PI) / 180
    const y = (1 - Math.log(Math.tan(latRad) + 1 / Math.cos(latRad)) / Math.PI) / 2
    return { x, y }
}

function getTileUrl(x: number, y: number, z: number): string {
    return `https://tile.openstreetmap.org/${z}/${x}/${y}.png`
}

function getContinuousBasemapZoom(scale: number): number {
    const minScale = 1000
    const maxScale = 50000
    const minZoom = 9
    const maxZoom = 18
    const t = Math.log(scale / minScale) / Math.log(maxScale / minScale)
    return minZoom + t * (maxZoom - minZoom)
}

function getH3ResolutionForZoom(zoom: number): number {
    if (zoom < 10.5) return 7
    if (zoom < 12.0) return 8
    if (zoom < 13.5) return 9
    if (zoom < 15.0) return 10
    return 11
}

function getH3ResolutionFromScale(scale: number, layerOverride?: number): number {
    if (layerOverride !== undefined && layerOverride >= 5 && layerOverride <= 11) {
        return layerOverride
    }
    const zoom = getContinuousBasemapZoom(scale)
    return getH3ResolutionForZoom(zoom)
}

function getScaleFromZoom(zoom: number): number {
    const minScale = 1000
    const maxScale = 50000
    const minZoom = 9
    const maxZoom = 18
    const t = (zoom - minZoom) / (maxZoom - minZoom)
    return minScale * Math.pow(maxScale / minScale, t)
}

function mercatorToLatLng(x: number, y: number): { lat: number; lng: number } {
    const lng = x * 360 - 180
    const latRad = Math.atan(Math.sinh(Math.PI * (1 - 2 * y)))
    const lat = (latRad * 180) / Math.PI
    return { lat, lng }
}

function canvasToLatLng(
    canvasX: number,
    canvasY: number,
    canvasWidth: number,
    canvasHeight: number,
    transform: { offsetX: number; offsetY: number; scale: number },
    basemapCenter: { lng: number; lat: number },
): { lat: number; lng: number } {
    const zoom = getContinuousBasemapZoom(transform.scale)
    const z = Math.floor(zoom)
    const zoomFraction = zoom - z

    const worldScale = Math.pow(2, z)
    const worldSize = 256 * worldScale
    const tileScale = Math.pow(2, zoomFraction)

    const centerMerc = latLngToMercator(basemapCenter.lng, basemapCenter.lat)

    const centerPixelX = centerMerc.x * worldSize - transform.offsetX
    const centerPixelY = centerMerc.y * worldSize - transform.offsetY

    const pointPixelX = (canvasX - canvasWidth / 2) / tileScale + centerPixelX
    const pointPixelY = (canvasY - canvasHeight / 2) / tileScale + centerPixelY

    const mx = pointPixelX / worldSize
    const my = pointPixelY / worldSize

    return mercatorToLatLng(mx, my)
}

function getViewportBoundsAccurate(
    canvasWidth: number,
    canvasHeight: number,
    transform: { offsetX: number; offsetY: number; scale: number },
    basemapCenter: { lat: number; lng: number },
): { minLat: number; maxLat: number; minLng: number; maxLng: number } {
    const padFrac = 0.2
    const padX = canvasWidth * padFrac
    const padY = canvasHeight * padFrac

    const corners = [
        canvasToLatLng(-padX, -padY, canvasWidth, canvasHeight, transform, basemapCenter),
        canvasToLatLng(canvasWidth + padX, -padY, canvasWidth, canvasHeight, transform, basemapCenter),
        canvasToLatLng(-padX, canvasHeight + padY, canvasWidth, canvasHeight, transform, basemapCenter),
        canvasToLatLng(canvasWidth + padX, canvasHeight + padY, canvasWidth, canvasHeight, transform, basemapCenter),
    ]

    const lats = corners.map(c => c.lat)
    const lngs = corners.map(c => c.lng)

    return {
        minLat: Math.min(...lats),
        maxLat: Math.max(...lats),
        minLng: Math.min(...lngs),
        maxLng: Math.max(...lngs),
    }
}

interface TransformState {
    offsetX: number
    offsetY: number
    scale: number
}

/**
 * Ray-casting algorithm to check if a point is inside a polygon
 */
function pointInPolygon(x: number, y: number, vertices: Array<[number, number]>): boolean {
    let inside = false
    for (let i = 0, j = vertices.length - 1; i < vertices.length; j = i++) {
        const xi = vertices[i][0], yi = vertices[i][1]
        const xj = vertices[j][0], yj = vertices[j][1]

        if (((yi > y) !== (yj > y)) && (x < (xj - xi) * (y - yi) / (yj - yi) + xi)) {
            inside = !inside
        }
    }
    return inside
}

function getH3CellCanvasVertices(
    h3Id: string,
    canvasWidth: number,
    canvasHeight: number,
    transform: TransformState,
    basemapCenter: { lng: number; lat: number },
): Array<[number, number]> {
    const boundary = cellToBoundary(h3Id)

    return boundary.map(([lat, lng]) => {
        const pos = geoToCanvas(lng, lat, canvasWidth, canvasHeight, transform, basemapCenter)
        return [pos.x, pos.y] as [number, number]
    })
}

function geoToCanvas(
    lng: number,
    lat: number,
    canvasWidth: number,
    canvasHeight: number,
    transform: TransformState,
    basemapCenter: { lng: number; lat: number },
): { x: number; y: number } {
    const zoom = getContinuousBasemapZoom(transform.scale)
    const z = Math.floor(zoom)
    const zoomFraction = zoom - z
    const scale = Math.pow(2, z)
    const worldSize = 256 * scale
    const tileScale = Math.pow(2, zoomFraction)

    const centerMerc = latLngToMercator(basemapCenter.lng, basemapCenter.lat)
    const pointMerc = latLngToMercator(lng, lat)

    const centerPixelX = centerMerc.x * worldSize - transform.offsetX
    const centerPixelY = centerMerc.y * worldSize - transform.offsetY

    const pointPixelX = pointMerc.x * worldSize
    const pointPixelY = pointMerc.y * worldSize

    const canvasX = (pointPixelX - centerPixelX) * tileScale + canvasWidth / 2
    const canvasY = (pointPixelY - centerPixelY) * tileScale + canvasHeight / 2

    return { x: canvasX, y: canvasY }
}

function getZoomConstants(s: number) {
    const zoom = getContinuousBasemapZoom(s)
    const z = Math.floor(zoom)
    const zoomFraction = zoom - z
    const tileScale = Math.pow(2, zoomFraction)
    const worldScale = Math.pow(2, z)
    const worldSize = 256 * worldScale
    return { zoom, z, tileScale, worldSize }
}

interface HexagonData {
    id: string
    vertices: Array<[number, number]>
    centerX: number
    centerY: number
    properties: FeatureProperties
}

export function MapView({
    filters,
    mapState,
    onFeatureSelect,
    onFeatureHover,
    year = 2026,
    className,
    onMockDataDetected
}: MapViewProps) {
    const basemapCenter = useMemo(() => ({ lng: -95.3698, lat: 29.7604 }), [])
    const [h3Resolution, setH3Resolution] = useState<number>(0)
    const lastResolutionRef = useRef<number>(0) // Track resolution to clear data on change

    const hexCanvasRef = useRef<HTMLCanvasElement>(null)
    const highlightCanvasRef = useRef<HTMLCanvasElement>(null) // New: Dedicated layer for interactions
    const containerRef = useRef<HTMLDivElement>(null)
    const basemapCanvasRef = useRef<HTMLCanvasElement>(null)

    const tileCache = useRef<Map<string, HTMLImageElement>>(new Map())

    // Cache data by "res-year" key
    // CACHE VERSION: Increment to bust stale cache after server-side changes
    const CACHE_VERSION = 2
    const h3DataCache = useRef<Map<string, Array<any>>>(new Map())

    // Fast Lookup Map for O(1) access: H3ID -> Properties
    const hexPropertyMap = useRef<Map<string, FeatureProperties>>(new Map())

    // Touch Handling Refs
    const lastTouchPosRef = useRef<{ x: number; y: number } | null>(null)
    const lastPinchDistRef = useRef<number | null>(null)

    const animationFrameRef = useRef<number | null>(null)

    const [canvasSize, setCanvasSize] = useState({ width: 800, height: 600 })

    // Responsive check
    const isMobile = useIsMobile()

    // Initialize transform from mapState (URL/Props)
    const [transform, setTransform] = useState<TransformState>(() => {
        const initialScale = getScaleFromZoom(mapState.zoom)
        // We can't calculate exact offsets until we know canvas size (which starts at 800x600 but resizes)
        // But we can approximate using the default size to start close.
        // Or better: use a useEffect to sync once canvas is ready? 
        // For now, let's just default to 0 and let the useEffect below sync it.
        return {
            offsetX: 0,
            offsetY: 0,
            scale: initialScale,
        }
    })

    // Sync transform when mapState changes (e.g. Search or URL change)
    useEffect(() => {
        if (!mapState.center || !mapState.zoom) return

        const [lng, lat] = mapState.center
        const targetZoom = mapState.zoom

        // Calculate target scale
        const targetScale = getScaleFromZoom(targetZoom)

        // Calculate offsets to center the map on the target lat/lng
        const zoom = getContinuousBasemapZoom(targetScale)
        const z = Math.floor(zoom)
        const scale = Math.pow(2, z)
        const worldSize = 256 * scale

        const centerMerc = latLngToMercator(basemapCenter.lng, basemapCenter.lat)
        const targetMerc = latLngToMercator(lng, lat)

        // transform.offsetX = (centerMerc.x - targetMerc.x) * worldSize
        const newOffsetX = (centerMerc.x - targetMerc.x) * worldSize
        const newOffsetY = (centerMerc.y - targetMerc.y) * worldSize

        setTransform(prev => {
            // Avoid infinite loops / jitter if close enough?
            if (Math.abs(prev.offsetX - newOffsetX) < 1 &&
                Math.abs(prev.offsetY - newOffsetY) < 1 &&
                Math.abs(prev.scale - targetScale) < 1) {
                return prev
            }
            return {
                offsetX: newOffsetX,
                offsetY: newOffsetY,
                scale: targetScale
            }
        })
    }, [mapState.center, mapState.zoom, basemapCenter])

    const [isDragging, setIsDragging] = useState(false)
    const [dragStart, setDragStart] = useState({ x: 0, y: 0 })

    const [hoveredHex, setHoveredHex] = useState<string | null>(null)
    const [selectedHex, setSelectedHex] = useState<string | null>(null)

    const [tooltipData, setTooltipData] = useState<{ x: number; y: number; globalX: number; globalY: number; properties: FeatureProperties } | null>(null)
    const [mounted, setMounted] = useState(false)

    useEffect(() => setMounted(true), [])

    const [filteredHexes, setFilteredHexes] = useState<HexagonData[]>([])
    const [realHexData, setRealHexData] = useState<Array<any>>([])
    const [isLoadingData, setIsLoadingData] = useState(false)
    const [parcels, setParcels] = useState<Array<any>>([])
    const [isParcelsLoading, setIsParcelsLoading] = useState(false)
    const [hoveredDetails, setHoveredDetails] = useState<DetailsResponse | null>(null)
    const [hoveredChildLines, setHoveredChildLines] = useState<number[][] | undefined>(undefined)
    const [isLoadingDetails, setIsLoadingDetails] = useState(false)

    // Determine active hex for details (Hover on Desktop, Selection on Mobile)
    const activeHex = hoveredHex || (isMobile ? selectedHex : null)

    // Fetch detailed data for tooltip when hovering/selected
    useEffect(() => {
        if (!activeHex) {
            setHoveredDetails(null)
            setHoveredChildLines(undefined)
            return
        }

        const timer = setTimeout(() => {
            setIsLoadingDetails(true)
            Promise.all([
                getH3CellDetails(activeHex, year),
                getH3ChildTimelines(activeHex)
            ])
                .then(([details, lines]) => {
                    setHoveredDetails(details)
                    setHoveredChildLines(lines)
                })
                .catch(err => console.error("Failed to load details", err))
                .finally(() => setIsLoadingDetails(false))
        }, 150) // Small delay to prevent spamming

        return () => clearTimeout(timer)
    }, [activeHex, year])


    const basemapZoom = useMemo(() => getContinuousBasemapZoom(transform.scale), [transform.scale])

    const findHexAtPoint = useCallback(
        (x: number, y: number): HexagonData | null => {
            for (let i = filteredHexes.length - 1; i >= 0; i--) {
                const hex = filteredHexes[i]
                if (pointInPolygon(x, y, hex.vertices)) {
                    return hex
                }
            }
            return null
        },
        [filteredHexes],
    )

    // Debounced data fetching to prevent request spam
    const fetchTimeoutRef = useRef<NodeJS.Timeout | null>(null)
    const abortControllerRef = useRef<AbortController | null>(null)

    useEffect(() => {
        // Clear any pending fetch
        if (fetchTimeoutRef.current) {
            clearTimeout(fetchTimeoutRef.current)
        }

        // Debounce: wait 500ms after user stops moving before fetching
        // This prevents intermediate viewport states from triggering loads
        fetchTimeoutRef.current = setTimeout(() => {
            const currentH3Res = getH3ResolutionFromScale(transform.scale, filters.layerOverride)

            // Calculate viewport bounds for spatial filtering
            const bounds = getViewportBoundsAccurate(
                canvasSize.width,
                canvasSize.height,
                transform,
                basemapCenter
            )

            // PRECISE cache key - use 2 decimal places for better cache hit/miss ratio
            // Includes CACHE_VERSION to bust stale data after server updates
            const cacheKey = `v${CACHE_VERSION}-${currentH3Res}-${year}-${bounds.minLat.toFixed(2)}-${bounds.maxLat.toFixed(2)}-${bounds.minLng.toFixed(2)}-${bounds.maxLng.toFixed(2)}`

            // Check cache first - if hit, just use it (no clearing needed)
            if (h3DataCache.current.has(cacheKey)) {
                console.log(`[CACHE] HIT - using cached ${h3DataCache.current.get(cacheKey)!.length} rows (res ${currentH3Res})`)
                // Only clear if resolution changed, then immediately set new data
                if (currentH3Res !== lastResolutionRef.current) {
                    lastResolutionRef.current = currentH3Res
                }
                setRealHexData(h3DataCache.current.get(cacheKey)!)
                return
            }

            // CACHE MISS: Need to fetch new data
            // Clear old data ONLY when resolution changes to avoid hex-within-hex
            if (currentH3Res !== lastResolutionRef.current) {
                console.log(`[RES] Resolution changed: ${lastResolutionRef.current} ΓåÆ ${currentH3Res}, clearing old data`)
                setRealHexData([])
                lastResolutionRef.current = currentH3Res
            }

            console.log(`[CACHE] MISS - fetching new data for res ${currentH3Res}`)

            // Cancel any in-flight request
            if (abortControllerRef.current) {
                abortControllerRef.current.abort()
            }
            abortControllerRef.current = new AbortController()

            setIsLoadingData(true)

            getH3DataV2(currentH3Res, year, bounds)
                .then((data) => {
                    const isMockData = data.length > 0 && data[0].h3_id?.startsWith("mock_")
                    if (isMockData && onMockDataDetected) {
                        onMockDataDetected()
                    }

                    // Filter out NaN/invalid data entries
                    // Filter out invalid coordinates (lat/lng/id are required)
                    // But allow missing metrics (o/r are compact names) so we can see the grid even if data is partial
                    const validData = data.filter(h =>
                        !isNaN(h.lat) &&
                        !isNaN(h.lng) &&
                        h.h3_id
                    )

                    h3DataCache.current.set(cacheKey, validData)
                    setRealHexData(validData)
                    setIsLoadingData(false)

                    // BACKGROUND PREFETCH: DISABLE TO PREVENT DB OVERLOAD
                    // The aggressive prefetching of 14 years (28+ SQL queries) was causing timeouts
                    // and connection limits, leading to flickering/missing hexes.
                    /* 
                    const allYears = [2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026, 2027, 2028, 2029, 2030, 2031, 2032]
                    const otherYears = allYears.filter(y => y !== year)

                    otherYears.forEach(otherYear => {
                        const otherCacheKey = `${currentH3Res}-${otherYear}-${bounds.minLat.toFixed(2)}-${bounds.maxLat.toFixed(2)}-${bounds.minLng.toFixed(2)}-${bounds.maxLng.toFixed(2)}`
                        if (!h3DataCache.current.has(otherCacheKey)) {
                            getH3DataForResolution(currentH3Res, otherYear, bounds)
                                .then(otherData => {
                                    const otherValid = otherData.filter(h =>
                                        !isNaN(h.reliability) && !isNaN(h.opportunity) && h.h3_id
                                    )
                                    h3DataCache.current.set(otherCacheKey, otherValid)
                                    console.log(`[PREFETCH] Cached year ${otherYear}: ${otherValid.length} rows`)
                                })
                                .catch(() => { }) 
                        }
                    })
                    */
                })
                .catch((err) => {
                    if (err.name !== 'AbortError') {
                        console.error("[v0] Failed to load H3 data:", err)
                        // Do NOT clear the map on error - keep showing stale data if available
                        // setRealHexData([]) 
                    }
                    setIsLoadingData(false)
                })
        }, 500) // 500ms debounce - wait for user to stop moving

        return () => {
            if (fetchTimeoutRef.current) {
                clearTimeout(fetchTimeoutRef.current)
            }
        }
    }, [transform.scale, transform.offsetX, transform.offsetY, filters.layerOverride, year, canvasSize.width, canvasSize.height, basemapCenter])

    useEffect(() => {
        // Threshold: Zoom > 14 (approx scale < 4000)
        // Adjust threshold as needed. mapState.zoom is reliable.
        const ZOOM_THRESHOLD = 14.5

        if (mapState.zoom < ZOOM_THRESHOLD) {
            setParcels([])
            return
        }

        const bounds = getViewportBoundsAccurate(
            canvasSize.width,
            canvasSize.height,
            transform,
            basemapCenter
        )

        // Debounce parcel fetch
        const timeoutId = setTimeout(async () => {
            setIsParcelsLoading(true)
            try {
                const { getParcels } = await import("@/app/actions/parcels")
                const data = await getParcels(bounds)
                setParcels(data)
            } catch (e) {
                console.error("Failed to fetch parcels", e)
            } finally {
                setIsParcelsLoading(false)
            }
        }, 800)

        return () => clearTimeout(timeoutId)

    }, [mapState.zoom, mapState.center, transform, canvasSize, basemapCenter])

    // Throttled vertex computation using requestAnimationFrame
    const vertexComputeRef = useRef<number | null>(null)

    useEffect(() => {
        // Cancel any pending computation
        if (vertexComputeRef.current) {
            cancelAnimationFrame(vertexComputeRef.current)
        }

        // Use requestAnimationFrame to throttle to 60fps max
        vertexComputeRef.current = requestAnimationFrame(() => {
            // Apply filters to raw hex data first
            // Apply filters to raw hex data first
            const currentH3Res = getH3ResolutionFromScale(transform.scale, filters.layerOverride)

            // Reset selection if resolution changes to avoid ghost highlights
            if (lastResolutionRef.current !== currentH3Res) {
                setHoveredHex(null)
                setSelectedHex(null)
                setTooltipData(null)
                onFeatureSelect(null)
                onFeatureHover(null)
                lastResolutionRef.current = currentH3Res
            }

            // NO FILTERING - show all hexes regardless of account count or growth
            // All hexes pass through to be rendered
            const filteredData = realHexData

            // Clear fast lookup
            hexPropertyMap.current.clear()

            const hexagons: HexagonData[] = filteredData
                .map((hex) => {
                    // Populate fast lookup map
                    const properties: FeatureProperties = {
                        id: hex.h3_id,
                        O: (hex.o ?? hex.opportunity ?? 0) * 100, // Handle null/compact/full
                        R: hex.r ?? hex.reliability ?? 0,         // Handle null/compact/full
                        n_accts: hex.property_count,
                        med_mean_ape_pct: hex.sample_accuracy * 100,
                        med_predicted_value: hex.med_predicted_value, // Ensure this flows through
                        med_mean_pred_cv_pct: hex.sample_accuracy * 100,
                        stability_flag: hex.alert_pct > 0.15,
                        robustness_flag: hex.alert_pct > 0.25,
                        has_data: hex.has_data,
                        med_n_years: hex.med_years ?? undefined,
                    }
                    hexPropertyMap.current.set(hex.h3_id, properties)

                    const vertices = getH3CellCanvasVertices(
                        hex.h3_id,
                        canvasSize.width,
                        canvasSize.height,
                        transform,
                        basemapCenter,
                    )

                    const centerX = vertices.reduce((sum, v) => sum + v[0], 0) / vertices.length
                    const centerY = vertices.reduce((sum, v) => sum + v[1], 0) / vertices.length

                    return {
                        id: hex.h3_id,
                        vertices,
                        centerX,
                        centerY,
                        properties,
                    }
                })
                .filter((hex) => {
                    // Viewport culling
                    return hex.vertices.some(
                        ([x, y]) => x > -200 && x < canvasSize.width + 200 && y > -200 && y < canvasSize.height + 200,
                    )
                })

            setFilteredHexes(hexagons)
            setH3Resolution(currentH3Res)
        })

        return () => {
            if (vertexComputeRef.current) {
                cancelAnimationFrame(vertexComputeRef.current)
            }
        }
    }, [realHexData, transform, canvasSize, basemapCenter, filters])

    // Refresh tooltip when data changes or selection updates on mobile
    useEffect(() => {
        if (hoveredHex && hexPropertyMap.current.has(hoveredHex)) {
            const props = hexPropertyMap.current.get(hoveredHex)!
            setTooltipData(prev => prev ? { ...prev, properties: props } : null)
        } else if (isMobile && selectedHex && hexPropertyMap.current.has(selectedHex)) {
            // Mobile: Show tooltip for selected hex even if not hovering
            const props = hexPropertyMap.current.get(selectedHex)!
            setTooltipData({ x: 0, y: 0, globalX: 0, globalY: 0, properties: props })
        }
    }, [realHexData, hoveredHex, selectedHex, isMobile])

    useEffect(() => {
        const container = containerRef.current
        if (!container) return

        const resizeObserver = new ResizeObserver((entries) => {
            for (const entry of entries) {
                const { width, height } = entry.contentRect
                setCanvasSize({ width, height })
            }
        })

        resizeObserver.observe(container)
        return () => resizeObserver.disconnect()
    }, [])

    useEffect(() => {
        const basemapCanvas = basemapCanvasRef.current
        if (!basemapCanvas || !canvasSize) return

        const ctx = basemapCanvas.getContext("2d", { alpha: false })
        if (!ctx) return

        const dpr = window.devicePixelRatio || 1
        basemapCanvas.width = canvasSize.width * dpr
        basemapCanvas.height = canvasSize.height * dpr
        ctx.scale(dpr, dpr)

        ctx.fillStyle = "#ffffff"
        ctx.fillRect(0, 0, canvasSize.width, canvasSize.height)

        const z = Math.floor(basemapZoom)
        const zoomFraction = basemapZoom - z
        const tileScale = Math.pow(2, zoomFraction)
        const scale = Math.pow(2, z)
        const worldSize = 256 * scale

        const centerMerc = latLngToMercator(basemapCenter.lng, basemapCenter.lat)

        const centerPixelX = centerMerc.x * worldSize - transform.offsetX
        const centerPixelY = centerMerc.y * worldSize - transform.offsetY

        const tileSize = 256 * tileScale
        const tilesX = Math.ceil(canvasSize.width / tileSize) + 2
        const tilesY = Math.ceil(canvasSize.height / tileSize) + 2

        const centerTileX = centerPixelX / 256
        const centerTileY = centerPixelY / 256

        const startTileX = Math.floor(centerTileX - tilesX / 2)
        const startTileY = Math.floor(centerTileY - tilesY / 2)

        for (let ty = 0; ty < tilesY; ty++) {
            for (let tx = 0; tx < tilesX; tx++) {
                let tileX = startTileX + tx
                const tileY = startTileY + ty

                tileX = ((tileX % scale) + scale) % scale
                if (tileY < 0 || tileY >= scale) continue

                const tileKey = `${z}-${tileX}-${tileY}`
                const tileUrl = getTileUrl(tileX, tileY, z)

                const offsetX = (startTileX + tx - centerTileX) * tileSize + canvasSize.width / 2
                const offsetY = (startTileY + ty - centerTileY) * tileSize + canvasSize.height / 2

                if (tileCache.current.has(tileKey)) {
                    const img = tileCache.current.get(tileKey)!
                    if (img.complete) {
                        ctx.globalAlpha = 0.95
                        ctx.drawImage(img, offsetX, offsetY, tileSize, tileSize)
                        ctx.globalAlpha = 1
                    }
                } else {
                    const img = new Image()
                    img.crossOrigin = "anonymous"
                    img.onload = () => {
                        tileCache.current.set(tileKey, img)
                        if (basemapCanvas.isConnected) {
                            const event = new Event("basemapupdate")
                            basemapCanvas.dispatchEvent(event)
                        }
                    }
                    img.src = tileUrl
                    tileCache.current.set(tileKey, img)
                }
            }
        }
    }, [canvasSize, basemapCenter, basemapZoom, transform])

    useEffect(() => {
        const basemapCanvas = basemapCanvasRef.current
        if (!basemapCanvas) return

        const handleUpdate = () => {
            setCanvasSize((prev) => ({ ...prev }))
        }

        basemapCanvas.addEventListener("basemapupdate", handleUpdate)
        return () => basemapCanvas.removeEventListener("basemapupdate", handleUpdate)
    }, [])

    // DPR-size the hex canvas (match basemap canvas)
    useEffect(() => {
        const canvas = hexCanvasRef.current
        if (!canvas) return

        const dpr = window.devicePixelRatio || 1
        canvas.width = Math.round(canvasSize.width * dpr)
        canvas.height = Math.round(canvasSize.height * dpr)

        const ctx = canvas.getContext("2d")
        if (ctx) ctx.setTransform(dpr, 0, 0, dpr, 0, 0)

        // Trigger redraw after DPR setup
        drawHexagons()
    }, [canvasSize.width, canvasSize.height])

    // Redraw hexes when data, transform, or selection changes
    useEffect(() => {
        drawHexagons()
    }, [filteredHexes, transform])

    // LAYER 1: Static Hex Data (Only updates on zoom/pan/data change)
    const drawHexagons = useCallback(() => {
        const canvas = hexCanvasRef.current
        if (!canvas) return

        const ctx = canvas.getContext("2d", { alpha: true })
        if (!ctx) return

        ctx.clearRect(0, 0, canvas.width, canvas.height)

        // Coverage Layer & Data Layer combined loop
        for (const hex of filteredHexes) {
            const { vertices, properties } = hex

            // NO FILTERING - all cells with data are rendered as data cells
            const isDataCell = properties.has_data !== false

            // Coverage style (neutral) vs Data style (colored)
            // Neutral: Low opacity gray
            // Data: Opportunity color OR Value color
            const fillColor = isDataCell
                ? (filters.colorMode === "value" && properties.med_predicted_value !== undefined
                    ? getValueColor(properties.med_predicted_value)
                    : getOpportunityColor(properties.O)
                )
                : "rgba(128, 128, 128, 0.1)" // Neutral coverage fill

            // Draw path
            ctx.beginPath()
            ctx.moveTo(vertices[0][0], vertices[0][1])
            for (let i = 1; i < vertices.length; i++) {
                ctx.lineTo(vertices[i][0], vertices[i][1])
            }
            ctx.closePath()

            // Fill
            ctx.globalAlpha = isDataCell ? 0.5 : 1 // Coverage cells already have low opacity in color string
            ctx.fillStyle = fillColor
            ctx.fill()

            // Stability check
            const hasStabilityWarning = properties.stability_flag || properties.robustness_flag
            if (filters.highlightWarnings && isDataCell && hasStabilityWarning) {
                ctx.globalAlpha = 0.8
                ctx.strokeStyle = "#f59e0b" // Amber warning color
                ctx.lineWidth = 2
                ctx.stroke()
            }

            ctx.globalAlpha = 1
        }
    }, [filteredHexes, transform, filters]) // Re-render when filters change!

    // LAYER 2: Highlights (Updates on mousemove)
    const drawHighlights = useCallback(() => {
        const canvas = highlightCanvasRef.current
        if (!canvas) return

        const ctx = canvas.getContext("2d", { alpha: true })
        if (!ctx) return

        ctx.clearRect(0, 0, canvas.width, canvas.height)

        const drawOutcome = (id: string, color: string, width: number) => {
            // We need vertices. Since we have filteredHexes (which has vertices pre-calculated), 
            // we can find it there. For O(1) vertex lookup we could cache vertices in a Map too, 
            // but finding in array of ~1000 visible hexes is fast enough for just 1-2 items 
            // compared to O(N) hit testing every pixel.
            const hex = filteredHexes.find(h => h.id === id)
            if (!hex) return

            ctx.beginPath()
            ctx.moveTo(hex.vertices[0][0], hex.vertices[0][1])
            for (let i = 1; i < hex.vertices.length; i++) {
                ctx.lineTo(hex.vertices[i][0], hex.vertices[i][1])
            }
            ctx.closePath()
            ctx.strokeStyle = color
            ctx.lineWidth = width
            ctx.stroke()
        }

        if (hoveredHex) drawOutcome(hoveredHex, "#ffffff", 2)
        if (selectedHex) drawOutcome(selectedHex, "#ffffff", 3)

    }, [filteredHexes, hoveredHex, selectedHex])


    // Helper: Canvas X/Y -> Lat/Lng for O(1) Lookup
    const getLatLngFromCanvas = useCallback((x: number, y: number) => {
        const { worldSize, tileScale } = getZoomConstants(transform.scale)

        // Inverse of geoToCanvas logic
        // canvasX = (pointPixelX - centerPixelX) * tileScale + canvasWidth/2
        // pointPixelX = (canvasX - canvasWidth/2)/tileScale + centerPixelX

        const centerMerc = latLngToMercator(basemapCenter.lng, basemapCenter.lat)
        const centerPixelX = centerMerc.x * worldSize - transform.offsetX
        const centerPixelY = centerMerc.y * worldSize - transform.offsetY

        const pointPixelX = (x - canvasSize.width / 2) / tileScale + centerPixelX
        const pointPixelY = (y - canvasSize.height / 2) / tileScale + centerPixelY

        const mercX = pointPixelX / worldSize
        const mercY = pointPixelY / worldSize

        const lng = mercX * 360 - 180
        const latRad = Math.atan(Math.sinh(Math.PI * (1 - 2 * mercY)))
        const lat = latRad * 180 / Math.PI

        return { lat, lng }
    }, [transform, canvasSize, basemapCenter])


    useEffect(() => {
        drawHexagons()
    }, [drawHexagons])

    useEffect(() => {
        drawHighlights()
    }, [drawHighlights])


    // Track if user actually dragged (moved more than a few pixels)
    const hasDraggedRef = useRef(false)

    const handleMouseDown = (e: React.MouseEvent) => {
        setIsDragging(true)
        hasDraggedRef.current = false // Reset on new drag start
        setDragStart({ x: e.clientX, y: e.clientY })
    }

    // Handler: Mouse Move with O(1) Lookup
    const handleMouseMove = (e: React.MouseEvent) => {
        const rect = containerRef.current?.getBoundingClientRect()
        if (!rect) return

        const canvasX = e.clientX - rect.left
        const canvasY = e.clientY - rect.top

        if (isDragging) {
            const dx = e.clientX - dragStart.x
            const dy = e.clientY - dragStart.y

            // Mark as dragged if moved more than 5 pixels
            if (Math.abs(dx) > 5 || Math.abs(dy) > 5) {
                hasDraggedRef.current = true
            }

            setTransform((prev) => ({
                ...prev,
                offsetX: prev.offsetX + dx,
                offsetY: prev.offsetY + dy,
            }))
            setDragStart({ x: e.clientX, y: e.clientY })
        } else {
            // FAST LOOKUP
            const { lat, lng } = getLatLngFromCanvas(canvasX, canvasY)
            const cellId = latLngToCell(lat, lng, h3Resolution)

            // Check if this cell is in our data
            if (hexPropertyMap.current.has(cellId)) {
                if (hoveredHex !== cellId) {
                    setHoveredHex(cellId)
                    const props = hexPropertyMap.current.get(cellId)!
                    setTooltipData({ x: canvasX, y: canvasY, globalX: e.clientX, globalY: e.clientY, properties: props })
                    onFeatureHover(cellId)
                } else {
                    // Update tooltip pos even if hex didn't change
                    setTooltipData(prev => prev ? { ...prev, x: canvasX, y: canvasY, globalX: e.clientX, globalY: e.clientY } : null)
                }
            } else {
                if (hoveredHex) {
                    setHoveredHex(null)
                    setTooltipData(null)
                    onFeatureHover(null)
                }
            }
        }
    }

    const handleCanvasClick = (e: React.MouseEvent) => {
        // Don't trigger selection if user was dragging
        if (hasDraggedRef.current) {
            return
        }

        if (hoveredHex) {
            setSelectedHex(hoveredHex)
            onFeatureSelect(hoveredHex)
        } else {
            setSelectedHex(null)
            onFeatureSelect(null)
        }
    }

    const handleMouseUp = () => {
        setIsDragging(false)
    }

    const handleMouseLeave = () => {
        setIsDragging(false)
        setHoveredHex(null)
        setTooltipData(null)
        onFeatureHover(null)
    }



    // [FIX] Manually attach wheel listener to support non-passive prevention (Zoom blocking)
    useEffect(() => {
        const canvas = highlightCanvasRef.current
        if (!canvas) return

        const onWheel = (e: WheelEvent) => {
            e.preventDefault()
            const rect = canvas.getBoundingClientRect()
            const mouseX = e.clientX - rect.left
            const mouseY = e.clientY - rect.top

            setTransform(prev => {
                const { worldSize: w1, tileScale: s1 } = getZoomConstants(prev.scale)
                const centerMerc = latLngToMercator(basemapCenter.lng, basemapCenter.lat)

                // Mouse in World Pixels (Current)
                const centerWorldPixelX = centerMerc.x * w1 - prev.offsetX
                const centerWorldPixelY = centerMerc.y * w1 - prev.offsetY
                const mouseWorldPixelX = (mouseX - canvasSize.width / 2) / s1 + centerWorldPixelX
                const mouseWorldPixelY = (mouseY - canvasSize.height / 2) / s1 + centerWorldPixelY

                // Normalize to Mercator (0-1)
                const mouseMercX = mouseWorldPixelX / w1
                const mouseMercY = mouseWorldPixelY / w1

                // New Scale
                const zoomFactor = e.deltaY > 0 ? 0.9 : 1.1
                const newScale = Math.max(1000, Math.min(50000, prev.scale * zoomFactor))

                // New Constants
                const { worldSize: w2, tileScale: s2 } = getZoomConstants(newScale)

                // Reverse calculation: Where must Offset be so that MouseMerc aligns with MouseScreen?
                // mouseScreenX = (mouseMercX * w2 - newOffsetX - centerMercX*w2 + newOffsetX ... wait, complicated
                // Use the formula from before:
                // centerWorldPixel = merc * w - offset
                // offset = merc * w - centerWorldPixel

                // We know where we want the center of the screen (centerWorldPixel) to be? No.
                // We want the mouse point (MercX) to be at mouseScreenX.
                // mouseWorldPixelX2 = mouseMercX * w2
                // mouseScreenX = (mouseWorldPixelX2 - centerWorldPixelX2) * s2 + w/2
                // centerWorldPixelX2 = mouseWorldPixelX2 - (mouseScreenX - w/2)/s2
                // centerWorldPixelX2 = centerMerc * w2 - newOffsetX
                // newOffsetX = centerMerc * w2 - centerWorldPixelX2

                const newCenterWorldPixelX = mouseMercX * w2 - (mouseX - canvasSize.width / 2) / s2
                const newCenterWorldPixelY = mouseMercY * w2 - (mouseY - canvasSize.height / 2) / s2

                const newOffsetX = centerMerc.x * w2 - newCenterWorldPixelX
                const newOffsetY = centerMerc.y * w2 - newCenterWorldPixelY

                return { scale: newScale, offsetX: newOffsetX, offsetY: newOffsetY }
            })
        }

        // TOUCH HANDLING
        const onTouchStart = (e: TouchEvent) => {
            if (e.touches.length === 1) {
                lastTouchPosRef.current = { x: e.touches[0].clientX, y: e.touches[0].clientY }
                lastPinchDistRef.current = null
            } else if (e.touches.length === 2) {
                const dx = e.touches[0].clientX - e.touches[1].clientX
                const dy = e.touches[0].clientY - e.touches[1].clientY
                lastPinchDistRef.current = Math.sqrt(dx * dx + dy * dy)
                lastTouchPosRef.current = null
            }
        }

        const onTouchMove = (e: TouchEvent) => {
            if (e.cancelable) e.preventDefault()

            // PAN
            if (e.touches.length === 1 && lastTouchPosRef.current) {
                const dx = e.touches[0].clientX - lastTouchPosRef.current.x
                const dy = e.touches[0].clientY - lastTouchPosRef.current.y

                setTransform(prev => ({
                    ...prev,
                    offsetX: prev.offsetX + dx, // Matches mouse pan direction
                    offsetY: prev.offsetY + dy
                }))
                lastTouchPosRef.current = { x: e.touches[0].clientX, y: e.touches[0].clientY }
            }
            // PINCH ZOOM
            else if (e.touches.length === 2 && lastPinchDistRef.current) {
                const t1 = e.touches[0]
                const t2 = e.touches[1]
                const dx = t1.clientX - t2.clientX
                const dy = t1.clientY - t2.clientY
                const dist = Math.sqrt(dx * dx + dy * dy)

                const scaleFactor = dist / lastPinchDistRef.current
                lastPinchDistRef.current = dist

                const rect = canvas.getBoundingClientRect()
                const cx = (t1.clientX + t2.clientX) / 2 - rect.left
                const cy = (t1.clientY + t2.clientY) / 2 - rect.top

                setTransform(prev => {
                    // Reuse Wheel Logic for Pinch Center
                    const { worldSize: w1, tileScale: s1 } = getZoomConstants(prev.scale)
                    const centerMerc = latLngToMercator(basemapCenter.lng, basemapCenter.lat)

                    const centerWorldPixelX = centerMerc.x * w1 - prev.offsetX
                    const centerWorldPixelY = centerMerc.y * w1 - prev.offsetY

                    // Pinch Center in Mercator
                    const mouseWorldPixelX = (cx - canvasSize.width / 2) / s1 + centerWorldPixelX
                    const mouseWorldPixelY = (cy - canvasSize.height / 2) / s1 + centerWorldPixelY
                    const mouseMercX = mouseWorldPixelX / w1
                    const mouseMercY = mouseWorldPixelY / w1

                    const newScale = Math.max(1000, Math.min(50000, prev.scale * scaleFactor))
                    const { worldSize: w2, tileScale: s2 } = getZoomConstants(newScale)

                    const newCenterWorldPixelX = mouseMercX * w2 - (cx - canvasSize.width / 2) / s2
                    const newCenterWorldPixelY = mouseMercY * w2 - (cy - canvasSize.height / 2) / s2

                    const newOffsetX = centerMerc.x * w2 - newCenterWorldPixelX
                    const newOffsetY = centerMerc.y * w2 - newCenterWorldPixelY

                    return { scale: newScale, offsetX: newOffsetX, offsetY: newOffsetY }
                })
            }
        }

        canvas.addEventListener('wheel', onWheel, { passive: false })
        canvas.addEventListener("touchstart", onTouchStart, { passive: false })
        canvas.addEventListener("touchmove", onTouchMove, { passive: false })

        return () => {
            canvas.removeEventListener('wheel', onWheel)
            canvas.removeEventListener("touchstart", onTouchStart)
            canvas.removeEventListener("touchmove", onTouchMove)
        }
    }, [canvasSize, basemapCenter])

    const handleZoomIn = () => {
        setTransform((prev) => ({
            ...prev,
            scale: Math.min(50000, prev.scale * 1.3),
        }))
    }

    const handleZoomOut = () => {
        setTransform((prev) => ({
            ...prev,
            scale: Math.max(1000, prev.scale / 1.3),
        }))
    }

    const handleReset = () => {
        setTransform({ offsetX: 0, offsetY: 0, scale: 3000 }) // Match initial zoomed out view
        setSelectedHex(null)
        setHoveredHex(null)
        setTooltipData(null)
    }

    return (
        <div ref={containerRef} className={cn("relative w-full h-full overflow-hidden bg-[#0a0f14]", className)}>
            <canvas
                ref={basemapCanvasRef}
                style={{ width: canvasSize.width, height: canvasSize.height }}
                className="absolute inset-0 pointer-events-none z-0"
            />

            <canvas
                ref={hexCanvasRef}
                style={{ width: canvasSize.width, height: canvasSize.height }}
                className="absolute inset-0 z-10 pointer-events-none"
            />

            {/* Interaction Layer: Handles Events & Highlights */}
            <canvas
                ref={highlightCanvasRef}
                style={{ width: canvasSize.width, height: canvasSize.height }}
                className="cursor-grab active:cursor-grabbing absolute inset-0 z-20 touch-none"
                onMouseDown={handleMouseDown}
                onMouseMove={handleMouseMove}
                onMouseUp={handleMouseUp}
                onMouseLeave={handleMouseLeave}
                onClick={handleCanvasClick}
            />

            {isLoadingData && (
                <div className="absolute top-4 left-1/2 -translate-x-1/2 bg-card/95 backdrop-blur-sm border border-border rounded-lg px-4 py-2 text-sm text-muted-foreground z-40">
                    Loading H3 resolution {getH3ResolutionFromScale(transform.scale)} from Supabase...
                </div>
            )}

            {mounted && tooltipData && createPortal(
                <div
                    className={cn(
                        "z-[9999] bg-card/95 backdrop-blur-md border border-border shadow-2xl overflow-hidden pointer-events-none",
                        isMobile
                            ? "fixed bottom-0 left-0 right-0 w-full rounded-t-xl rounded-b-none border-t border-x-0 border-b-0 pointer-events-auto"
                            : "fixed rounded-xl w-[320px]"
                    )}
                    style={isMobile ? undefined : {
                        left: tooltipData.globalX + 20,
                        top: tooltipData.globalY + (tooltipData.globalY > window.innerHeight - 350 ? -20 : 20),
                        transform: `${tooltipData.globalX > window.innerWidth - 340 ? 'translateX(-100%) translateX(-40px)' : ''} ${tooltipData.globalY > window.innerHeight - 350 ? 'translateY(-100%)' : ''}`.trim() || 'none'
                    }}
                >
                    {tooltipData.properties.has_data ? (
                        <div className="flex flex-col">
                            {/* Header */}
                            <div className="p-3 border-b border-border/50 bg-muted/30">
                                <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold mb-0.5">
                                    H3 Cell (Res {h3Resolution})
                                </div>
                                <div className="font-mono text-xs text-muted-foreground truncate">
                                    {tooltipData.properties.id}
                                </div>
                            </div>

                            <div className="p-4 space-y-5">
                                {/* Top Stats Row */}
                                <div className="grid grid-cols-2 gap-3">
                                    {/* Value Stat */}
                                    <div>
                                        <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold mb-1">
                                            Predicted ({year})
                                        </div>
                                        <div className="text-xl font-bold text-foreground tracking-tight">
                                            {tooltipData.properties.med_predicted_value
                                                ? formatCurrency(tooltipData.properties.med_predicted_value)
                                                : "N/A"}
                                        </div>
                                        {/* Current Value Context */}
                                        {hoveredDetails?.historicalValues && (
                                            <div className="text-[10px] text-muted-foreground mt-1">
                                                Curr: <span className="text-foreground">{formatCurrency(hoveredDetails.historicalValues[hoveredDetails.historicalValues.length - 1])}</span>
                                            </div>
                                        )}
                                    </div>

                                    {/* Growth Stat */}
                                    <div>
                                        <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold mb-1">
                                            Growth
                                        </div>
                                        <div className={cn("text-xl font-bold tracking-tight flex items-center gap-1",
                                            tooltipData.properties.O >= 0 ? "text-green-500" : "text-destructive"
                                        )}>
                                            {formatOpportunity(tooltipData.properties.O)}
                                            {getTrendIcon(tooltipData.properties.O > 0.05 ? "up" : tooltipData.properties.O < -0.02 ? "down" : "stable")}
                                        </div>
                                        <div className="text-[10px] text-muted-foreground mt-1">
                                            Avg. Annual {year > 2025 ? "Forecast" : "History"}
                                        </div>
                                    </div>
                                </div>

                                {/* Mini Chart */}
                                {hoveredDetails?.fanChart ? (
                                    <div className="space-y-2">
                                        <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold flex justify-between">
                                            <span>Value Timeline</span>
                                            <span className="text-primary/70">{year}</span>
                                        </div>
                                        <div className="h-32 -mx-2">
                                            <FanChart
                                                data={hoveredDetails.fanChart}
                                                currentYear={year}
                                                height={120}
                                                historicalValues={hoveredDetails.historicalValues}
                                                childLines={hoveredChildLines}
                                            />
                                        </div>
                                    </div>
                                ) : (
                                    isLoadingDetails && (
                                        <div className="h-32 flex items-center justify-center">
                                            <div className="w-5 h-5 border-2 border-primary/30 border-t-primary rounded-full animate-spin" />
                                        </div>
                                    )
                                )}

                                {/* Footer Stats */}
                                <div className="pt-3 border-t border-border/50 grid grid-cols-2 gap-4">
                                    <div>
                                        <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold mb-0.5">
                                            Properties
                                        </div>
                                        <div className="text-sm font-medium text-foreground">
                                            {tooltipData.properties.n_accts}
                                        </div>
                                    </div>
                                    <div>
                                        <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold mb-0.5">
                                            Confidence
                                        </div>
                                        <div className="text-sm font-medium text-foreground">
                                            {formatReliability(tooltipData.properties.R)}
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    ) : (
                        <div className="p-3">
                            <div className="font-medium text-muted-foreground text-xs">No data available</div>
                        </div>
                    )}
                </div>,
                document.body
            )}

            <div className="absolute bottom-4 right-4 flex flex-col gap-2 z-30">
                <button
                    onClick={handleZoomIn}
                    className="w-10 h-10 bg-card/95 backdrop-blur-sm border border-border rounded-lg flex items-center justify-center text-foreground hover:bg-accent transition-colors shadow-lg"
                >
                    +
                </button>
                <button
                    onClick={handleZoomOut}
                    className="w-10 h-10 bg-card/95 backdrop-blur-sm border border-border rounded-lg flex items-center justify-center text-foreground hover:bg-accent transition-colors shadow-lg"
                >
                    -
                </button>
                <button
                    onClick={handleReset}
                    className="w-10 h-10 bg-card/95 backdrop-blur-sm border border-border rounded-lg flex items-center justify-center text-foreground hover:bg-accent transition-colors text-xs shadow-lg"
                >
                    Reset
                </button>
            </div>
        </div >
    )
}
