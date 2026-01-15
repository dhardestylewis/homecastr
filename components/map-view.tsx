"use client"

import type React from "react"
import { getH3DataV2 } from "@/app/actions/h3-data-v2"
import { cellToBoundary, latLngToCell } from "h3-js"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { cn } from "@/lib/utils"
import { getOpportunityColor, formatOpportunity, formatReliability } from "@/lib/utils/colors"
import type { FilterState, FeatureProperties, MapState } from "@/lib/types"

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

    const animationFrameRef = useRef<number | null>(null)

    const [canvasSize, setCanvasSize] = useState({ width: 800, height: 600 })

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
    const [tooltipData, setTooltipData] = useState<{ x: number; y: number; properties: FeatureProperties } | null>(null)

    const [filteredHexes, setFilteredHexes] = useState<HexagonData[]>([])
    const [realHexData, setRealHexData] = useState<Array<any>>([])
    const [isLoadingData, setIsLoadingData] = useState(false)


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

    // Throttled vertex computation using requestAnimationFrame
    const vertexComputeRef = useRef<number | null>(null)

    useEffect(() => {
        // Cancel any pending computation
        if (vertexComputeRef.current) {
            cancelAnimationFrame(vertexComputeRef.current)
        }

        // Use requestAnimationFrame to throttle to 60fps max
        vertexComputeRef.current = requestAnimationFrame(() => {
            const currentH3Res = getH3ResolutionFromScale(transform.scale, filters.layerOverride)

            // Apply filters to raw hex data first
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
                        med_mean_pred_cv_pct: hex.sample_accuracy * 100,
                        stability_flag: hex.alert_pct > 0.15,
                        robustness_flag: hex.alert_pct > 0.25,
                        has_data: hex.has_data,
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

            // Determine if this cell should be rendered as "Data" or "Coverage Only"
            // It is "Data" if:
            // 1. It has data (has_data=true)
            // 2. It passes user filters
            const passesFilters =
                properties.R >= filters.reliabilityMin &&
                properties.n_accts >= filters.nAcctsMin &&
                (filters.showUnderperformers || properties.O >= 0)

            const isDataCell = properties.has_data && passesFilters

            // Coverage style (neutral) vs Data style (colored)
            // Neutral: Low opacity gray
            // Data: Opportunity color
            const fillColor = isDataCell
                ? getOpportunityColor(properties.O)
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

            // Stroke (Continuous lattice)
            // Coverage cells get a subtle stroke, Data cells get same or none?
            // "Render a visible outline for the cell boundary at all zoom levels"
            ctx.globalAlpha = 0.2
            ctx.strokeStyle = "#888888" // Neutral stroke
            ctx.lineWidth = 1
            ctx.stroke()

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
                    setTooltipData({ x: canvasX, y: canvasY, properties: props })
                    onFeatureHover(cellId)
                } else {
                    // Update tooltip pos even if hex didn't change
                    setTooltipData(prev => prev ? { ...prev, x: canvasX, y: canvasY } : null)
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

    const getZoomConstants = (s: number) => {
        const zoom = getContinuousBasemapZoom(s)
        const z = Math.floor(zoom)
        const zoomFraction = zoom - z
        const tileScale = Math.pow(2, zoomFraction)
        const worldScale = Math.pow(2, z)
        const worldSize = 256 * worldScale
        return { zoom, z, tileScale, worldSize }
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

            // 1. Calculate Mercator position of mouse BEFORE zoom
            const { worldSize: w1, tileScale: s1 } = getZoomConstants(transform.scale)

            const centerMerc = latLngToMercator(basemapCenter.lng, basemapCenter.lat)
            const centerWorldPixelX = centerMerc.x * w1 - transform.offsetX
            const centerWorldPixelY = centerMerc.y * w1 - transform.offsetY

            const mouseWorldPixelX = (mouseX - canvasSize.width / 2) / s1 + centerWorldPixelX
            const mouseWorldPixelY = (mouseY - canvasSize.height / 2) / s1 + centerWorldPixelY

            // Normalized Mercator (0-1) is invariant across scales
            const mouseMercX = mouseWorldPixelX / w1
            const mouseMercY = mouseWorldPixelY / w1

            // 2. Calculate New Scale
            const zoomFactor = e.deltaY > 0 ? 0.9 : 1.1
            const newScale = Math.max(1000, Math.min(50000, transform.scale * zoomFactor))

            // 3. Calculate New Offset to keep Mouse stationary
            const { worldSize: w2, tileScale: s2 } = getZoomConstants(newScale)

            const newCenterWorldPixelX = mouseMercX * w2 - (mouseX - canvasSize.width / 2) / s2
            const newCenterWorldPixelY = mouseMercY * w2 - (mouseY - canvasSize.height / 2) / s2

            const newOffsetX = centerMerc.x * w2 - newCenterWorldPixelX
            const newOffsetY = centerMerc.y * w2 - newCenterWorldPixelY

            setTransform({
                scale: newScale,
                offsetX: newOffsetX,
                offsetY: newOffsetY,
            })
        }

        // Must be non-passive to preventDefault()
        canvas.addEventListener('wheel', onWheel, { passive: false })

        return () => {
            canvas.removeEventListener('wheel', onWheel)
        }
    }, [transform, canvasSize, basemapCenter]) // Re-bind if core params change to keep closure fresh

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
                className="cursor-grab active:cursor-grabbing absolute inset-0 z-20"
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

            {tooltipData && (
                <div
                    className="absolute pointer-events-none z-30 bg-card/95 backdrop-blur-sm border border-border rounded-lg px-3 py-2 text-sm shadow-lg"
                    style={{
                        left: tooltipData.x + 15,
                        top: tooltipData.y + 15,
                        maxWidth: 280,
                    }}
                >
                    {tooltipData.properties.has_data ? (
                        <>
                            <div className="font-medium text-foreground mb-1">Projected Growth</div>
                            <div className="grid grid-cols-2 gap-x-4 gap-y-1">
                                <span className="text-muted-foreground">CAGR:</span>
                                <span className={cn("font-mono font-medium", tooltipData.properties.O >= 0 ? "text-primary" : "text-destructive")}>
                                    {formatOpportunity(tooltipData.properties.O)}
                                </span>
                                <span className="text-muted-foreground">Confidence:</span>
                                <span className="font-mono text-foreground">{formatReliability(tooltipData.properties.R)}</span>
                                <span className="text-muted-foreground">Properties:</span>
                                <span className="text-foreground">{tooltipData.properties.n_accts}</span>
                                <span className="text-muted-foreground">Sample Accuracy:</span>
                                <span className="text-foreground">{tooltipData.properties.med_mean_ape_pct?.toFixed(1)}%</span>
                            </div>

                            {(tooltipData.properties.stability_flag || tooltipData.properties.robustness_flag) && (
                                <div className="mt-2 pt-2 border-t border-border text-xs text-amber-400">
                                    {tooltipData.properties.stability_flag && <div>Stability warning</div>}
                                    {tooltipData.properties.robustness_flag && <div>Robustness warning</div>}
                                </div>
                            )}
                        </>
                    ) : (
                        <>
                            <div className="font-medium text-muted-foreground mb-1">No data in this cell</div>
                            <div className="grid grid-cols-2 gap-x-4 gap-y-1">
                                <span className="text-muted-foreground">Properties:</span>
                                <span className="text-foreground">0</span>
                            </div>
                        </>
                    )}
                </div>
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

            {/* Debug Overlay - Shows viewport and data stats */}
            {(() => {
                const bounds = getViewportBoundsAccurate(canvasSize.width, canvasSize.height, transform, basemapCenter)
                return (
                    <div className="absolute bottom-4 left-4 bg-card/95 backdrop-blur-sm border border-border rounded-lg px-3 py-2 text-xs text-muted-foreground z-40 shadow-lg font-mono">
                        <div className="font-semibold text-foreground mb-1">Debug Info</div>
                        <div>H3 Res: {h3Resolution} | Zoom: {basemapZoom.toFixed(1)}</div>
                        <div>Fetched: {realHexData.length} | Rendered: {filteredHexes.length}</div>
                        <div className="text-[10px] opacity-70">
                            Lat: {bounds.minLat.toFixed(2)}ΓåÆ{bounds.maxLat.toFixed(2)}
                        </div>
                        <div className="text-[10px] opacity-70">
                            Lng: {bounds.minLng.toFixed(2)}ΓåÆ{bounds.maxLng.toFixed(2)}
                        </div>
                        {realHexData.length >= 9900 && (
                            <div className="text-amber-400 font-semibold">ΓÜá LIMIT HIT</div>
                        )}
                    </div>
                )
            })()}
        </div >
    )
}


