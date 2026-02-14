"use client"

import type React from "react"
import { getH3DataV2 } from "@/app/actions/h3-data-v2"
import { cellToBoundary, latLngToCell, cellToLatLng } from "h3-js"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { createPortal } from "react-dom"
import { cn } from "@/lib/utils"
import { getOpportunityColor, getValueColor, formatOpportunity, formatCurrency, formatReliability } from "@/lib/utils/colors"
import { TrendingUp, TrendingDown, Minus, Building2, X } from "lucide-react"
import type { FilterState, FeatureProperties, MapState, DetailsResponse } from "@/lib/types"
import { getH3CellDetails } from "@/app/actions/h3-details"
import { getH3ChildTimelines } from "@/app/actions/h3-children"
import { getH3DataBatch } from "@/app/actions/h3-data-batch"
import { FanChart } from "./fan-chart"
import { aggregateProperties, aggregateDetails } from "@/lib/utils/aggregation"
import { useRouter } from "next/navigation"

// Helper to get trend icon
const getTrendIcon = (trend: "up" | "down" | "stable" | undefined) => {
    if (trend === "up") return <TrendingUp className="h-3 w-3 text-green-500" />
    if (trend === "down") return <TrendingDown className="h-3 w-3 text-red-500" />
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




interface TransformState {
    offsetX: number
    offsetY: number
    scale: number
}

// Helper: Ensure tooltip stays within bounds and respects Sidebar
const SIDEBAR_WIDTH = 340
const TOOLTIP_WIDTH = 320
const TOOLTIP_HEIGHT = 450

function getSmartTooltipPos(x: number, y: number, windowWidth: number, windowHeight: number) {
    if (typeof window === 'undefined') return { x, y }

    // Default: Place to the RIGHT of the cursor
    let left = x + 20
    let top = y - 20

    // 1. Horizontal Constraint
    // If placing to the right goes off screen...
    if (left + TOOLTIP_WIDTH > windowWidth - 20) {
        // Try placing to the LEFT of the cursor
        const tryLeft = x - TOOLTIP_WIDTH - 20

        // If placing left overlaps Sidebar (and we are potentially covering it?)
        // Only if tryLeft < SIDEBAR_WIDTH. 
        // But if x is far right, tryLeft is likely > SIDEBAR_WIDTH.
        // If x is near SIDEBAR_WIDTH (e.g. 350), tryLeft = 10. Overlaps Sidebar?
        // Sidebar is technically "floating" but user wants to avoid covering it.
        // But if we are at x=350, Right is 370. That's fine.
        // If we are at x=WindowWidth (e.g. 1920), Left is 1580. Fine.

        // What if both block? (Narrow screen)
        // Check if Left placement hits Sidebar
        if (tryLeft < SIDEBAR_WIDTH + 10) {
            // We are squeezed between Sidebar and Right Edge.
            // Priority: Keep on screen.
            // If tryLeft < Sidebar, maybe we force Right but clamped?
            // Or force Left but clamped?

            // Let's stick to the side that has more space.
            const spaceRight = windowWidth - x
            const spaceLeft = x - SIDEBAR_WIDTH

            if (spaceRight > spaceLeft && spaceRight > TOOLTIP_WIDTH) {
                left = x + 20
            } else if (spaceLeft > TOOLTIP_WIDTH) {
                left = tryLeft
            } else {
                // Not enough space either side. 
                // Default to Right but clamped to window edge (covering whatever)
                left = Math.min(left, windowWidth - TOOLTIP_WIDTH - 10)
            }
        } else {
            left = tryLeft
        }
    }

    // 2. Vertical Constraint
    // Stay inside window height
    top = Math.max(10, Math.min(top, windowHeight - TOOLTIP_HEIGHT - 10))

    // 3. Final Safety Clamp (Left Edge / Sidebar)
    // If we ended up on the left side, ensure we don't go negative or into Sidebar if possible
    // Use SIDEBAR_WIDTH as the hard 'minX' roughly
    // But don't clamp strictly if it means going off screen right? 
    // Already handled above. Just standard min clamp.
    left = Math.max(10, left)

    return { x: left, y: top }
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
    onYearChange?: (year: number) => void
    mobileSelectionMode?: 'replace' | 'add' | 'range'
    onMobileSelectionModeChange?: (mode: 'replace' | 'add' | 'range') => void
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
    transform: TransformState,
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
    transform: TransformState,
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

    // Handle longitude wrap-around (anti-meridian)
    // If the gap between max and min is very large (> 180), we've likely crossed the line
    let minLng = Math.min(...lngs)
    let maxLng = Math.max(...lngs)

    if (maxLng - minLng > 180) {
        // Swap them for a global-wrapping fetch or just use the full range
        // For Harris County (primary AOI), this is unlikely unless the user pans far
        // but it's good practice.
    }

    return {
        minLat: Math.min(...lats),
        maxLat: Math.max(...lats),
        minLng: minLng,
        maxLng: maxLng,
    }
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
    onMockDataDetected,
    onYearChange,
    mobileSelectionMode,
    onMobileSelectionModeChange
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

    // Sync external mapState selection (mapState.selectedId) -> internal selection (selectedHexes)
    // This allows parent Reset button to clear selection
    useEffect(() => {
        if (!mapState.selectedId) {
            // Cleared externally
            if (selectedHexes.length > 0) {
                setSelectedHexes([])
                setFixedTooltipPos(null)
                setSelectedHexGeoCenter(null)
                setComparisonHex(null)
                setComparisonDetails(null)
            }
        } else {
            // Selected externally (e.g. initial URL load or future functionality)
            // Only update if not already matching to avoid loops (though array ref check helps)
            if (!selectedHexes.includes(mapState.selectedId)) {
                setSelectedHexes([mapState.selectedId])
                // We might want to auto-center or tooltip here, but let's stick to just state sync for now
            }
        }
    }, [mapState.selectedId])

    // Sync highlightedIds
    useEffect(() => {
        if (mapState.highlightedIds && mapState.highlightedIds.length > 0) {
            // Merge valid selectedId + highlightedIds
            const ids = new Set<string>()
            if (mapState.selectedId) ids.add(mapState.selectedId)
            mapState.highlightedIds.forEach(id => ids.add(id))

            // Only update if different
            const newSelection = Array.from(ids)
            if (newSelection.length !== selectedHexes.length || !newSelection.every(id => selectedHexes.includes(id))) {
                setSelectedHexes(newSelection)
            }
        } else if (selectedHexes.length > 1 && !mapState.selectedId) {
            // If we had multi-selection but highlights cleared (and no main selection), clear all
            setSelectedHexes([])
        }
    }, [mapState.highlightedIds, mapState.selectedId])

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

    // SYNC URL WITH MAP POSITION (Legacy Engine)
    const router = useRouter()

    // Sync map position to URL params.
    // IMPORTANT: Read current params from window.location instead of searchParams
    // to avoid a dependency loop (searchParams changes when ANY param changes,
    // which would re-trigger this effect causing cascading navigations and NetworkErrors).
    useEffect(() => {
        const timer = setTimeout(() => {
            const { zoom } = getZoomConstants(transform.scale)
            if (!containerRef.current) return
            const width = containerRef.current.clientWidth || 800
            const height = containerRef.current.clientHeight || 600

            const center = canvasToLatLng(width / 2, height / 2, width, height, transform, basemapCenter)

            const params = new URLSearchParams(window.location.search)
            params.set("lat", center.lat.toFixed(5))
            params.set("lng", center.lng.toFixed(5))
            params.set("zoom", zoom.toFixed(2))

            router.replace(`?${params.toString()}`, { scroll: false })
        }, 500) // Debounce URL updates
        return () => clearTimeout(timer)
    }, [transform, router, basemapCenter])


    const [isDragging, setIsDragging] = useState(false)
    const [dragStart, setDragStart] = useState({ x: 0, y: 0 })

    const [hoveredHex, setHoveredHex] = useState<string | null>(null)
    const [selectedHexes, setSelectedHexes] = useState<string[]>([])
    const [selectionDetails, setSelectionDetails] = useState<DetailsResponse | null>(null)
    const [hoveredDetails, setHoveredDetails] = useState<DetailsResponse | null>(null)

    // Derived State
    const [tooltipData, setTooltipData] = useState<{ x: number; y: number; globalX: number; globalY: number; properties: FeatureProperties } | null>(null)
    const [fixedTooltipPos, setFixedTooltipPos] = useState<{ globalX: number; globalY: number } | null>(null)
    const [selectedHexGeoCenter, setSelectedHexGeoCenter] = useState<{ lat: number; lng: number } | null>(null)
    const [comparisonHex, setComparisonHex] = useState<string | null>(null)
    const [comparisonDetails, setComparisonDetails] = useState<DetailsResponse | null>(null)
    const [primaryDetails, setPrimaryDetails] = useState<DetailsResponse | null>(null) // Line 1: Anchor
    const [isTooltipDragging, setIsTooltipDragging] = useState(false)
    const [showDragHint, setShowDragHint] = useState(false)

    // Client-side cache for details to prevent lag
    const detailsCache = useRef<Map<string, DetailsResponse>>(new Map())
    // Shift preview state - show which hexes would be selected on Shift+click
    const [isShiftHeld, setIsShiftHeld] = useState(false)
    const [shiftPreviewHexes, setShiftPreviewHexes] = useState<string[]>([])
    const [shiftPreviewDetails, setShiftPreviewDetails] = useState<DetailsResponse | null>(null)
    // Mobile Selection Mode State (Controlled or Internal fallback not really needed if fully controlled, but keeping simple)
    // Actually, if we want to move buttons to Page, we should rely on props.
    // But to avoid breaking if props missing, we can use a local state initialized from props? 
    // No, let's just use the prop. But we need to handle the case where it's not provided?
    // For now, let's assume it's passed or default to 'replace'.
    // We already defaulted it in destructuring.

    const [internalMobileMode, setInternalMobileMode] = useState<'replace' | 'add' | 'range'>('replace')

    // Use prop if handler provided, else local (backward compat)
    const effectiveMobileMode = onMobileSelectionModeChange ? mobileSelectionMode : internalMobileMode
    const setEffectiveMobileMode = onMobileSelectionModeChange ? onMobileSelectionModeChange : setInternalMobileMode

    const handleCellClick = (cellId: string, e: any) => {
        // ...
        // We need to check where mobileSelectionMode is used.
        // It's used in handleCanvasClick (lines ~1505)
    }
    const selectedHex = selectedHexes.length > 0 ? selectedHexes[selectedHexes.length - 1] : null
    const selectedHexRef = useRef<string | null>(null)

    // Sync ref
    useEffect(() => {
        selectedHexRef.current = selectedHex
    }, [selectedHex])

    // Compute Display Properties (Aggregate)
    const displayProperties = useMemo(() => {
        if (selectedHexes.length > 0) {
            // Aggregate from hexPropertyMap
            const props = selectedHexes
                .map(id => hexPropertyMap.current.get(id))
                .filter(p => p !== undefined) as FeatureProperties[]

            if (props.length > 0) return aggregateProperties(props)
            return null
        }
        if (hoveredHex && tooltipData) return tooltipData.properties
        return null
    }, [selectedHexes, hoveredHex, tooltipData])

    // Fetch Logic
    // Fetch Logic
    useEffect(() => {
        // Debounce fetch
        const timer = setTimeout(() => {
            // Determine what to fetch
            const needSelection = selectedHexes.length > 0
            const needHover = hoveredHex && !selectedHexes.includes(hoveredHex)

            if (!needSelection && !hoveredHex) {
                setHoveredDetails(null)
                setSelectionDetails(null)
                setPrimaryDetails(null)
                return
            }

            setIsLoadingDetails(true)

            // Helper to fetch with cache
            const fetchWithCache = async (id: string, y: number) => {
                const key = `${id}-${y}`
                if (detailsCache.current.has(key)) return detailsCache.current.get(key)!

                const data = await getH3CellDetails(id, y)
                if (data) detailsCache.current.set(key, data)
                return data
            }

            const selectionPromise = needSelection
                ? Promise.all(selectedHexes.map(id => fetchWithCache(id, year)))
                : Promise.resolve([])

            const hoverPromise = needHover
                ? fetchWithCache(hoveredHex!, year)
                : Promise.resolve(null)

            Promise.all([selectionPromise, hoverPromise])
                .then(([selectionResults, hoverResult]) => {
                    const validSelection = selectionResults.filter((d): d is DetailsResponse => d !== null)

                    // 1. Update Selection State
                    if (validSelection.length > 0) {
                        setSelectionDetails(aggregateDetails(validSelection))
                        // Primary is always the first selected hex
                        setPrimaryDetails(validSelection[0])
                    } else if (selectedHexes.length > 0) {
                        // Selection fetch failed completely
                        setSelectionDetails(null)
                        setPrimaryDetails(null)
                    } else {
                        // No active selection
                        setSelectionDetails(null)
                        // Primary will be cleared unless set by Hover below
                        setPrimaryDetails(null)
                    }

                    // 2. Resolve Hover Data
                    let finalHoverDetails = hoverResult

                    // If hover wasn't fetched separately, it might be in the selection
                    if (!finalHoverDetails && hoveredHex && selectedHexes.includes(hoveredHex)) {
                        const idx = selectedHexes.indexOf(hoveredHex)
                        if (idx !== -1) finalHoverDetails = selectionResults[idx]
                    }

                    setHoveredDetails(finalHoverDetails)

                    // 3. Fallback: If no selection, Hover becomes Primary
                    if (selectedHexes.length === 0 && finalHoverDetails) {
                        setPrimaryDetails(finalHoverDetails)
                    }
                })
                .catch(e => console.error(e))
                .finally(() => setIsLoadingDetails(false))

        }, 150)
        return () => clearTimeout(timer)
    }, [selectedHexes, hoveredHex, year])





    const [mounted, setMounted] = useState(false)

    useEffect(() => setMounted(true), [])

    const [filteredHexes, setFilteredHexes] = useState<HexagonData[]>([])
    const [realHexData, setRealHexData] = useState<Array<any>>([])
    const [isLoadingData, setIsLoadingData] = useState(false)
    const [parcels, setParcels] = useState<Array<any>>([])
    const [isParcelsLoading, setIsParcelsLoading] = useState(false)
    const [isLoadingDetails, setIsLoadingDetails] = useState(false)
    const [selectedHexResolution, setSelectedHexResolution] = useState<number | null>(null)

    // Clear selection when H3 resolution changes (prevents stale highlight at wrong scale)
    useEffect(() => {
        if (selectedHex && selectedHexResolution !== null && h3Resolution !== selectedHexResolution) {
            setSelectedHexes([]) // Changed from setSelectedHex(null)
            setFixedTooltipPos(null)
            setSelectedHexGeoCenter(null)
            setComparisonHex(null)
            setComparisonDetails(null)
        }
    }, [h3Resolution, selectedHex, selectedHexResolution])


    // --- COMPARISON & PREVIEW LOGIC ---

    // 1. Comparison Hex: The "Established" comparison (Blue line)
    // Updates when hovering a new hex, UNLESS Shift is held (Freeze behavior)
    useEffect(() => {
        if (selectedHexes.length === 0) {
            setComparisonHex(null)
            return
        }

        // If Shift is held (or Mobile Range Mode), DO NOT update comparison (Freeze)
        // Unless we have NO comparison yet, then take the current hover
        const isFreezeMode = isShiftHeld || (isMobile && mobileSelectionMode === 'range')

        if (isFreezeMode && comparisonHex) return

        if (hoveredHex && !selectedHexes.includes(hoveredHex)) {
            setComparisonHex(hoveredHex)
        } else if (!isFreezeMode) {
            // Only clear if not frozen
            // If we leave the hex, we might want to keep the last one?
            // User behavior: "hovering over already selected" -> Comparison should probably clear?
            // Current logic: clear if hovering selected or nothing
            setComparisonHex(null)
        }
    }, [hoveredHex, selectedHexes, isShiftHeld, isMobile, mobileSelectionMode])

    // 2. Fetch Comparison Details
    useEffect(() => {
        if (!comparisonHex) {
            setComparisonDetails(null)
            return
        }
        const timer = setTimeout(() => {
            getH3CellDetails(comparisonHex, year)
                .then(details => setComparisonDetails(details))
                .catch(err => console.error("Failed to load comparison details", err))
        }, 150)
        return () => clearTimeout(timer)
    }, [comparisonHex, year])


    // 3. Hover Details: Always fetch the details of what we are currently hovering (for Preview calculation)
    // This is distinct from 'hoveredDetails' state which drives the Main tooltip when no selection.
    // We need a subtle distinct state or reuse hoveredDetails?
    // 'hoveredDetails' is set in the main fetch effect above (lines 390-417). 
    // BUT! That effect only only runs if `targets` changes. 
    // If selectedHexes > 0, `targets` = selectedHexes. It IGNORES hoveredHex!
    // So `hoveredDetails` is NOT updated when we have a selection.
    // We need a separate fetch for the ephemeral hover target during selection.

    const [ephemeralHoverDetails, setEphemeralHoverDetails] = useState<DetailsResponse | null>(null)

    useEffect(() => {
        if (!hoveredHex || selectedHexes.length === 0) {
            setEphemeralHoverDetails(null)
            return
        }

        // Optimize: If hoveredHex == comparisonHex, we already have it in comparisonDetails
        if (hoveredHex === comparisonHex && comparisonDetails) {
            setEphemeralHoverDetails(comparisonDetails)
            return
        }

        const timer = setTimeout(() => {
            getH3CellDetails(hoveredHex, year)
                .then(d => setEphemeralHoverDetails(d))
        }, 150)
        return () => clearTimeout(timer)
    }, [hoveredHex, selectedHexes.length, comparisonHex, comparisonDetails, year])


    // 4. Compute Preview (Green/Purple line)
    const previewDetails = useMemo(() => {
        // Only show preview if Shift is held (Preview Mode) OR if we are hovering a selected hex (Removal Preview)
        // Logic:
        // A. Shift Held (Add/Range Mode):
        //    Preview = Selection + EphemeralHover (Approximation of range add)
        //    Ideally: Selection + ShiftPreviewHexes.
        //    Perf compromise: Selection + EphemeralHover.

        // B. Hovering Selected (Removal Mode):
        //    Preview = Selection - EphemeralHover

        if (!selectionDetails) return null
        if (!ephemeralHoverDetails && !comparisonDetails) return null

        // Use the details of what we are interacting with (Ephemeral / Hover)
        // If we are frozen (Shift), we use Ephemeral. If we are just hovering, Ephemeral should match Comparison.
        const interactionDetails = ephemeralHoverDetails

        if (!interactionDetails) return null

        if (selectedHexes.includes(interactionDetails.id)) {
            // REMOVAL PREVIEW: "What if I remove this hex?"
            // Aggregate = Selection - Interaction
            // Note: aggregateDetails might not support subtraction easily if it just averages.
            // If it averages properties, removing one from the set changes the average.
            // We can't "subtract" from the aggregate result. We need the raw list to re-aggregate.
            // Limitation: We don't have the raw list of all selected details here.
            // Fallback: Just show the interaction hex itself as "This is what you are removing"?
            // Or null? User request: "preview what you would remove".
            // Since we can't easily re-aggregate (we don't keep all N details in memory), maybe we skip Removal Preview for now?
            // OR: We interpret "preview what you would remove" as "Show me the line for the thing being removed".
            // Let's return the interactionDetails as the preview (to show "This is the outlier you are removing").
            return null
        } else if (isShiftHeld || (isMobile && mobileSelectionMode === 'range')) {
            // ADD PREVIEW: Selection + Interaction
            try {
                return aggregateDetails([selectionDetails, interactionDetails])
            } catch { return null }
        }

        return null
    }, [selectionDetails, ephemeralHoverDetails, selectedHexes, isShiftHeld, isMobile, mobileSelectionMode])

    // Tooltip drag handling (window-level events for smooth dragging)
    useEffect(() => {
        if (!isTooltipDragging) return

        const handleMouseMove = (e: MouseEvent) => {
            // Apply smart positioning
            const docked = getSmartTooltipPos(e.clientX, e.clientY, window.innerWidth, window.innerHeight)
            setFixedTooltipPos({ globalX: docked.x, globalY: docked.y })
        }

        const handleMouseUp = () => {
            setIsTooltipDragging(false)
        }

        window.addEventListener('mousemove', handleMouseMove)
        window.addEventListener('mouseup', handleMouseUp)

        return () => {
            window.removeEventListener('mousemove', handleMouseMove)
            window.removeEventListener('mouseup', handleMouseUp)
        }
    }, [isTooltipDragging])

    // Track Shift key state for selection preview
    useEffect(() => {
        const handleKeyDown = (e: KeyboardEvent) => {
            if (e.key === 'Shift') setIsShiftHeld(true)
        }
        const handleKeyUp = (e: KeyboardEvent) => {
            if (e.key === 'Shift') {
                setIsShiftHeld(false)
                setShiftPreviewHexes([])
                setShiftPreviewDetails(null)
            }
        }
        window.addEventListener('keydown', handleKeyDown)
        window.addEventListener('keyup', handleKeyUp)
        return () => {
            window.removeEventListener('keydown', handleKeyDown)
            window.removeEventListener('keyup', handleKeyUp)
        }
    }, [])

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

    const fetchTimeoutRef = useRef<NodeJS.Timeout | null>(null)
    const abortControllerRef = useRef<AbortController | null>(null)
    // Separate abort controller for batch prefetch - should not be cancelled when user scrubs
    const batchPrefetchAbortRef = useRef<AbortController | null>(null)
    const batchPrefetchInProgressRef = useRef(false)
    const hasRunFirstPrefetchRef = useRef(false) // Track if initial prefetch has completed
    const initialFetchCompleteRef = useRef(false) // Gate: don't batch until first main fetch completes
    const lastYearForBatchRef = useRef<number>(year) // Separate year tracking for batch effect (avoids shared ref race)

    // Offscreen canvas for double buffering to prevent flicker
    const offscreenCanvasRef = useRef<HTMLCanvasElement | null>(null)

    // Track last values to determine debounce strategy
    const lastYearRef = useRef<number>(year)
    const lastTransformRef = useRef(transform)

    useEffect(() => {
        // Clear any pending fetch
        if (fetchTimeoutRef.current) {
            clearTimeout(fetchTimeoutRef.current)
        }

        // SMART DEBOUNCE:
        // If year changed but viewport is stable -> Instant (0ms)
        // If viewport changed -> Debounce (200ms)
        const isYearChangeOnly = year !== lastYearRef.current &&
            transform.offsetX === lastTransformRef.current.offsetX &&
            transform.offsetY === lastTransformRef.current.offsetY &&
            transform.scale === lastTransformRef.current.scale

        const delay = isYearChangeOnly ? 0 : 200

        lastYearRef.current = year
        lastTransformRef.current = transform

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

            // We intentionally do NOT abort previous fetches here.
            // At this point we have a cache MISS (cache HITs return early above),
            // so the previous fetch is populating cache for timelapse playback.
            // Let it finish in the background while we start a new fetch.
            abortControllerRef.current = new AbortController()

            setIsLoadingData(true)

            // OPTIMIZATION: Do NOT clear old data until new data arrives to prevent flashing.
            // setRealHexData([]) <--- Removed

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
                    initialFetchCompleteRef.current = true // Ungate batch prefetch

                    // NOTE: Batch prefetch is now handled in a separate useEffect below

                })
                .catch((err) => {
                    if (err.name !== 'AbortError') {
                        console.error("[v0] Failed to load H3 data:", err)
                        // Do NOT clear the map on error - keep showing stale data if available
                        // setRealHexData([]) 
                    }
                    setIsLoadingData(false)
                })
        }, delay)

        return () => {
            if (fetchTimeoutRef.current) {
                clearTimeout(fetchTimeoutRef.current)
            }
        }
    }, [transform.scale, transform.offsetX, transform.offsetY, filters.layerOverride, year, canvasSize.width, canvasSize.height, basemapCenter])

    // BATCH PREFETCH - Independent effect that warms the cache for all years
    // Triggers on viewport/resolution changes, runs independently of current year
    // Gated: waits until the first main data fetch completes to avoid mount-storm spam
    useEffect(() => {
        // Don't start batch prefetching until the first main fetch has completed.
        // This prevents 4-6 redundant batch calls during initial mount (canvas resize,
        // transform sync from URL, etc.)
        if (!initialFetchCompleteRef.current) return

        // Smart debouncing strategy:
        // - 0ms on first run after gate opens → instant cache warming for timelapse
        // - 0ms on year change → immediate prefetch to fill gaps during scrubbing
        // - 1.5s on subsequent viewport changes → avoid spam during pan/zoom
        const isFirstRun = !hasRunFirstPrefetchRef.current
        const isYearChange = year !== lastYearForBatchRef.current
        const delay = isFirstRun || isYearChange ? 0 : 1500
        lastYearForBatchRef.current = year

        const timer = setTimeout(() => {
            // Skip if already in progress
            if (batchPrefetchInProgressRef.current) {
                console.log('[BATCH-PREFETCH] Skipping - already in progress')
                return
            }

            const currentH3Res = getH3ResolutionFromScale(transform.scale, filters.layerOverride)
            const bounds = getViewportBoundsAccurate(
                canvasSize.width,
                canvasSize.height,
                transform,
                basemapCenter
            )

            // Build list of years that need prefetching
            // Range 2019-2030 matches the TimeControls UI min/max year
            const MIN_YEAR = 2019
            const MAX_YEAR = 2030
            const yearsToFetch: number[] = []
            for (let targetYear = MIN_YEAR; targetYear <= MAX_YEAR; targetYear++) {
                if (targetYear === year) continue // Skip current year (fetched by main effect)

                const targetKey = `v${CACHE_VERSION}-${currentH3Res}-${targetYear}-${bounds.minLat.toFixed(2)}-${bounds.maxLat.toFixed(2)}-${bounds.minLng.toFixed(2)}-${bounds.maxLng.toFixed(2)}`
                if (!h3DataCache.current.has(targetKey)) {
                    yearsToFetch.push(targetYear)
                }
            }

            if (yearsToFetch.length === 0) {
                console.log('[BATCH-PREFETCH] All years cached, skipping')
                return
            }

            console.log(`[BATCH-PREFETCH] Fetching ${yearsToFetch.length} years: ${yearsToFetch.join(', ')}`)

            // Cancel previous batch prefetch if still running
            if (batchPrefetchAbortRef.current) {
                batchPrefetchAbortRef.current.abort()
            }
            batchPrefetchAbortRef.current = new AbortController()
            batchPrefetchInProgressRef.current = true

            getH3DataBatch(currentH3Res, yearsToFetch, bounds)
                .then(batchResults => {
                    Object.entries(batchResults).forEach(([yStr, resultData]) => {
                        const y = parseInt(yStr)
                        const key = `v${CACHE_VERSION}-${currentH3Res}-${y}-${bounds.minLat.toFixed(2)}-${bounds.maxLat.toFixed(2)}-${bounds.minLng.toFixed(2)}-${bounds.maxLng.toFixed(2)}`
                        h3DataCache.current.set(key, resultData)
                        console.log(`[BATCH-PREFETCH] Cached year ${y}: ${resultData.length} rows`)
                    })
                    batchPrefetchInProgressRef.current = false
                    hasRunFirstPrefetchRef.current = true // Mark first prefetch as complete
                })
                .catch(err => {
                    if (err.name !== 'AbortError') {
                        console.error('[BATCH-PREFETCH] Failed:', err)
                    }
                    batchPrefetchInProgressRef.current = false
                })
        }, delay)

        return () => clearTimeout(timer)
    }, [transform.scale, transform.offsetX, transform.offsetY, filters.layerOverride, year, canvasSize.width, canvasSize.height, basemapCenter])

    useEffect(() => {
        // Threshold: Zoom > 14 (approx scale < 4000)
        // Adjust threshold as needed. mapState.zoom is reliable.
        const ZOOM_THRESHOLD = 14.5

        if (mapState.zoom < ZOOM_THRESHOLD) {
            if (parcels.length > 0) setParcels([])
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
                setSelectedHexes([]) // Changed from setSelectedHex(null)
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
            // setH3Resolution(currentH3Res) // This might be redundant or causing loops if it updates mapState -> transform -> effect
            // If we need to sync resolution, do it only if changed?
            // Actually, setH3Resolution is likely updating a ref or state that triggers this?
            // checking usage: const [h3Resolution, setH3Resolution] = useState(6)
            // It is NOT in dependency array of this effect.
            // But if it causes a re-render, does it change `transform`? No.
            // However, let's play safe.
            if (h3Resolution !== currentH3Res) {
                setH3Resolution(currentH3Res)
            }
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

        // Reset and set DPR transform in one call
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0)

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

        // DOUBLE BUFFERING: Render to offscreen canvas first
        if (!offscreenCanvasRef.current) {
            offscreenCanvasRef.current = document.createElement("canvas")
        }
        const offCanvas = offscreenCanvasRef.current

        // Ensure offscreen canvas matches size
        if (offCanvas.width !== canvasSize.width || offCanvas.height !== canvasSize.height) {
            offCanvas.width = canvasSize.width
            offCanvas.height = canvasSize.height
        }

        const offCtx = offCanvas.getContext("2d", { alpha: true })
        if (!offCtx) return

        // Clear offscreen canvas
        offCtx.clearRect(0, 0, canvasSize.width, canvasSize.height)

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
            offCtx.beginPath()
            offCtx.moveTo(vertices[0][0], vertices[0][1])
            for (let i = 1; i < vertices.length; i++) {
                offCtx.lineTo(vertices[i][0], vertices[i][1])
            }
            offCtx.closePath()

            // Fill
            offCtx.globalAlpha = isDataCell ? 0.5 : 1 // Coverage cells already have low opacity in color string
            offCtx.fillStyle = fillColor
            offCtx.fill()

            // Sustainability check
            const hasStabilityWarning = properties.stability_flag || properties.robustness_flag
            if (filters.highlightWarnings && isDataCell && hasStabilityWarning) {
                offCtx.globalAlpha = 0.8
                offCtx.strokeStyle = "#f59e0b" // Amber warning color
                offCtx.lineWidth = 2
                offCtx.stroke()
            }

            offCtx.globalAlpha = 1
        }

        // SWAP: Clear main canvas and draw the offscreen image
        // This ensures the user never sees a partially drawn or cleared frame
        ctx.clearRect(0, 0, canvasSize.width, canvasSize.height)
        ctx.drawImage(offCanvas, 0, 0)

    }, [filteredHexes, transform, filters]) // Re-render when filters change!

    // LAYER 2: Highlights (Updates on mousemove)
    const drawHighlights = useCallback(() => {
        const canvas = highlightCanvasRef.current
        if (!canvas) return

        const ctx = canvas.getContext("2d", { alpha: true })
        if (!ctx) return

        // Clear using CSS pixels (viewport size), as the context is scaled by DPR
        ctx.clearRect(0, 0, canvasSize.width, canvasSize.height)

        const drawOutcome = (id: string, color: string, width: number, dashed: boolean = false): HexagonData | undefined => {
            const hex = filteredHexes.find(h => h.id === id)
            if (!hex) return undefined

            ctx.beginPath()
            ctx.moveTo(hex.vertices[0][0], hex.vertices[0][1])
            for (let i = 1; i < hex.vertices.length; i++) {
                ctx.lineTo(hex.vertices[i][0], hex.vertices[i][1])
            }
            ctx.closePath()
            ctx.strokeStyle = color
            ctx.lineWidth = width
            if (dashed) ctx.setLineDash([8, 4])
            else ctx.setLineDash([])
            ctx.stroke()
            ctx.setLineDash([])
            return hex
        }

        // Helper: convert lat/lng to canvas coordinates
        const geoToCanvasCoords = (lat: number, lng: number): { x: number; y: number } => {
            const { worldSize, tileScale } = getZoomConstants(transform.scale)
            const centerMerc = latLngToMercator(basemapCenter.lng, basemapCenter.lat)
            const pointMerc = latLngToMercator(lng, lat)

            const centerPixelX = centerMerc.x * worldSize - transform.offsetX
            const centerPixelY = centerMerc.y * worldSize - transform.offsetY
            const pointPixelX = pointMerc.x * worldSize
            const pointPixelY = pointMerc.y * worldSize

            const x = (pointPixelX - centerPixelX) * tileScale + canvasSize.width / 2
            const y = (pointPixelY - centerPixelY) * tileScale + canvasSize.height / 2
            return { x, y }
        }

        // Standard Hover (White) - only if nothing is selected or if hovering different hex while selected
        if (hoveredHex && hoveredHex !== selectedHex) {
            // If locked (selectedHex exists), the hovered hex is the COMPARISON target -> AMBER
            if (selectedHex) {
                drawOutcome(hoveredHex, "#f97316", 3, true) // Orange-500 dashed (matches fan chart comparison line)
            } else {
                // Normal hover -> White
                drawOutcome(hoveredHex, "#ffffff", 2)
            }
        }

        // Draw Highlights for Multi-Select
        ctx.lineJoin = 'round'
        ctx.setLineDash([]) // Solid line

        if (selectedHexes.length > 0) {
            // Draw non-primary hexes first (amber), then primary last (teal on top)
            const primaryHex = selectedHexes[0]
            const nonPrimaryHexes = selectedHexes.slice(1)

            // Draw non-primary first (amber)
            ctx.strokeStyle = '#f97316' // Orange (matches fan chart comparison line)
            ctx.lineWidth = 2.5
            nonPrimaryHexes.forEach(h => {
                const vertices = getH3CellCanvasVertices(h, canvasSize.width, canvasSize.height, transform, basemapCenter)
                ctx.beginPath()
                vertices.forEach((v, i) => {
                    if (i === 0) ctx.moveTo(v[0], v[1])
                    else ctx.lineTo(v[0], v[1])
                })
                ctx.closePath()
                ctx.stroke()
            })

            // Draw primary LAST (teal on top)
            ctx.strokeStyle = '#14b8a6' // Teal
            ctx.lineWidth = 3
            const vertices = getH3CellCanvasVertices(primaryHex, canvasSize.width, canvasSize.height, transform, basemapCenter)
            ctx.beginPath()
            vertices.forEach((v, i) => {
                if (i === 0) ctx.moveTo(v[0], v[1])
                else ctx.lineTo(v[0], v[1])
            })
            ctx.closePath()
            ctx.stroke()
            ctx.stroke()
        }

        // Draw Shift Preview Hexes (Dashed Amber)
        if (shiftPreviewHexes.length > 0) {
            ctx.strokeStyle = '#f97316' // Orange (matches fan chart comparison line)
            ctx.lineWidth = 2
            ctx.setLineDash([5, 5])
            shiftPreviewHexes.forEach(h => {
                // Don't draw over existing selection if we are adding (approx)
                // Actually, if we are subtracting, we might want to draw over? 
                // For now, just draw all preview hexes to show extent.
                if (selectedHexes.includes(h) && selectedHexes.length > 0 && selectedHexes[0] === h) return // Don't cover primary

                const vertices = getH3CellCanvasVertices(h, canvasSize.width, canvasSize.height, transform, basemapCenter)
                ctx.beginPath()
                vertices.forEach((v, i) => {
                    if (i === 0) ctx.moveTo(v[0], v[1])
                    else ctx.lineTo(v[0], v[1])
                })
                ctx.closePath()
                ctx.stroke()
            })
            ctx.setLineDash([])
        }

    }, [filteredHexes, hoveredHex, selectedHex, fixedTooltipPos, isMobile, transform, selectedHexGeoCenter, canvasSize, basemapCenter, shiftPreviewHexes, selectedHexes])


    // Helper: Canvas X/Y -> Lat/Lng for O(1) Lookup
    const getLatLngFromCanvas = useCallback((x: number, y: number) => {
        const { worldSize, tileScale } = getZoomConstants(transform.scale)

        // Inverse of geoToCanvas logic
        // canvasX = (pointPixelX - centerPixelX) * tileScale + canvasWidth/2
        // pointPixelX = (canvasX - canvasWidth/2)/tileScale + centerWorldPixelX

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

    // DPR-size the highlight canvas (must match hex canvas) & Trigger Redraw
    useEffect(() => {
        const canvas = highlightCanvasRef.current
        if (!canvas) return

        const dpr = window.devicePixelRatio || 1
        // Only set width/height if they differ to avoid clearing canvas unnecessarily
        // But here we want to ensure it matches always.
        canvas.width = Math.round(canvasSize.width * dpr)
        canvas.height = Math.round(canvasSize.height * dpr)

        const ctx = canvas.getContext("2d")
        if (ctx) ctx.setTransform(dpr, 0, 0, dpr, 0, 0)

        drawHighlights()
    }, [canvasSize.width, canvasSize.height, drawHighlights])


    // Track if user actually dragged (moved more than a few pixels)
    const hasDraggedRef = useRef(false)

    const handleMouseDown = (e: React.MouseEvent) => {
        setIsDragging(true)
        hasDraggedRef.current = false // Reset on new drag start
        setDragStart({ x: e.clientX, y: e.clientY })
    }

    // Double-click to zoom in
    const handleDoubleClick = (e: React.MouseEvent) => {
        const rect = containerRef.current?.getBoundingClientRect()
        if (!rect) return

        const mouseX = e.clientX - rect.left
        const mouseY = e.clientY - rect.top

        setTransform(prev => {
            const { worldSize: w1, tileScale: s1 } = getZoomConstants(prev.scale)
            const centerMerc = latLngToMercator(basemapCenter.lng, basemapCenter.lat)

            const centerWorldPixelX = centerMerc.x * w1 - prev.offsetX
            const centerWorldPixelY = centerMerc.y * w1 - prev.offsetY

            const mouseWorldPixelX = (mouseX - canvasSize.width / 2) / s1 + centerWorldPixelX
            const mouseWorldPixelY = (mouseY - canvasSize.height / 2) / s1 + centerWorldPixelY
            const mouseMercX = mouseWorldPixelX / w1
            const mouseMercY = mouseWorldPixelY / w1

            const newScale = Math.min(50000, prev.scale * 2) // Zoom in 2x
            const { worldSize: w2, tileScale: s2 } = getZoomConstants(newScale)

            const newCenterWorldPixelX = mouseMercX * w2 - (mouseX - canvasSize.width / 2) / s2
            const newCenterWorldPixelY = mouseMercY * w2 - (mouseY - canvasSize.height / 2) / s2

            const newOffsetX = centerMerc.x * w2 - newCenterWorldPixelX
            const newOffsetY = centerMerc.y * w2 - newCenterWorldPixelY

            return { scale: newScale, offsetX: newOffsetX, offsetY: newOffsetY }
        })
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

            // Only update if there's actual movement to prevent infinite re-renders
            if (dx === 0 && dy === 0) return

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
                // Always track hoveredHex for click detection
                if (hoveredHex !== cellId) {
                    setHoveredHex(cellId)
                }

                // Track comparison hex when we have any selection (single or multi)
                if (selectedHexes.length > 0) {
                    // Only show comparison for hexes NOT in selection
                    if (!selectedHexes.includes(cellId)) {
                        // Hovering a non-selected hex -> show comparison
                        if (comparisonHex !== cellId) {
                            setComparisonHex(cellId)
                        }

                        // Shift Preview Logic
                        if ((e.shiftKey || (isMobile && mobileSelectionMode === 'range')) && selectedHexes.length > 0) {
                            const anchorHex = selectedHexes[selectedHexes.length - 1]
                            const [lat1, lng1] = cellToLatLng(anchorHex)
                            const [lat2, lng2] = cellToLatLng(cellId)

                            const minLat = Math.min(lat1, lat2)
                            const maxLat = Math.max(lat1, lat2)
                            const minLng = Math.min(lng1, lng2)
                            const maxLng = Math.max(lng1, lng2)

                            const hexesInRange = filteredHexes
                                .filter(hex => {
                                    const [hLat, hLng] = cellToLatLng(hex.id)
                                    return hLat >= minLat && hLat <= maxLat && hLng >= minLng && hLng <= maxLng
                                })
                                .map(hex => hex.id)

                            // Only update if different (avoid loops)
                            if (hexesInRange.length !== shiftPreviewHexes.length || !hexesInRange.every((h, i) => h === shiftPreviewHexes[i])) {
                                setShiftPreviewHexes(hexesInRange)
                            }
                        } else {
                            if (shiftPreviewHexes.length > 0) setShiftPreviewHexes([])
                        }
                    } else {
                        // Hovering a selected hex -> clear comparison
                        if (comparisonHex) {
                            setComparisonHex(null)
                        }
                    }
                } else {
                    // No selection - normal hover behavior with tooltip
                    const props = hexPropertyMap.current.get(cellId)!
                    const smartPos = getSmartTooltipPos(e.clientX, e.clientY, window.innerWidth, window.innerHeight)
                    setTooltipData({ x: canvasX, y: canvasY, globalX: smartPos.x, globalY: smartPos.y, properties: props })
                    onFeatureHover(cellId)
                }
            } else {
                if (hoveredHex) {
                    setHoveredHex(null)
                }
                if (selectedHexes.length === 0) {
                    setTooltipData(null)
                    onFeatureHover(null)
                } else if (comparisonHex) {
                    setComparisonHex(null)
                }
            }
        }
    }

    const handleCanvasClick = (e: React.MouseEvent) => {
        if (hasDraggedRef.current) return

        if (hoveredHex) {
            const isShift = e.shiftKey || (isMobile && effectiveMobileMode === 'range')
            const isMulti = e.ctrlKey || e.metaKey || (isMobile && effectiveMobileMode === 'add')

            let newSet: string[]

            if (isShift && selectedHexes.length > 0) {
                // SHIFT-CLICK: Range selection (bounding box between most recent selection and click)
                const anchorHex = selectedHexes[selectedHexes.length - 1] // Most recently clicked
                const [lat1, lng1] = cellToLatLng(anchorHex)
                const [lat2, lng2] = cellToLatLng(hoveredHex)

                // Calculate bounding box
                const minLat = Math.min(lat1, lat2)
                const maxLat = Math.max(lat1, lat2)
                const minLng = Math.min(lng1, lng2)
                const maxLng = Math.max(lng1, lng2)

                // Find all visible hexes within bounding box
                const hexesInRange = filteredHexes
                    .filter(hex => {
                        const [hLat, hLng] = cellToLatLng(hex.id)
                        return hLat >= minLat && hLat <= maxLat && hLng >= minLng && hLng <= maxLng
                    })
                    .map(hex => hex.id)

                // If clicked hex is already selected -> SUBTRACT range
                // Otherwise -> EXPAND (add range to existing selection)
                const primaryHex = selectedHexes[0] // Always protect primary
                if (selectedHexes.includes(hoveredHex)) {
                    // Subtract: remove all hexes in range from selection EXCEPT primary
                    const toRemove = new Set(hexesInRange)
                    toRemove.delete(primaryHex) // Never remove primary
                    newSet = selectedHexes.filter(h => !toRemove.has(h))
                } else {
                    // Expand: add all hexes in range to existing selection
                    // Ensure primary stays first
                    const uniqueHexes = new Set([...selectedHexes, ...hexesInRange])
                    newSet = [primaryHex, ...Array.from(uniqueHexes).filter(h => h !== primaryHex)]
                }
            } else if (isMulti) {
                // CTRL/CMD-CLICK: Toggle individual hex
                if (selectedHexes.includes(hoveredHex)) {
                    newSet = selectedHexes.filter(h => h !== hoveredHex)
                } else {
                    newSet = [...selectedHexes, hoveredHex]
                }
            } else {
                // Simple click: Replace selection and clear any comparison
                newSet = [hoveredHex]
                setComparisonHex(null)
                setComparisonDetails(null)
            }

            setSelectedHexes(newSet)

            // Update Primary & Tooltip Pos
            if (newSet.length > 0) {
                const primary = newSet[newSet.length - 1]
                onFeatureSelect(primary)
                // Update Geo Center for connector
                const [lat, lng] = cellToLatLng(primary)
                setSelectedHexGeoCenter({ lat, lng })

                // If single click (replace), update tooltip pos
                // Use click coordinates directly for instant locking
                // This prevents jumping if tooltipData is stale or null
                const clamped = getSmartTooltipPos(e.clientX, e.clientY, window.innerWidth, window.innerHeight)
                setFixedTooltipPos({ globalX: clamped.x, globalY: clamped.y })
            }
        } else {
            // Clicked background -> Clear
            setSelectedHexes([])
            setFixedTooltipPos(null)
            setSelectedHexGeoCenter(null)
            setComparisonHex(null)
            setComparisonDetails(null)
            onFeatureSelect(null)
        }
    }

    const handleMouseUp = () => {
        setIsDragging(false)
    }

    const handleMouseLeave = () => {
        setIsDragging(false)
        // Only clear hover if no selection
        if (!selectedHex) {
            setHoveredHex(null)
            setTooltipData(null)
            onFeatureHover(null)
        } else {
            // Clear comparison when mouse leaves canvas (no stale comparison data)
            setComparisonHex(null)
            setComparisonDetails(null)
            setEphemeralHoverDetails(null)
            setHoveredHex(null)
        }
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



    // ESC key to exit static tooltip mode
    useEffect(() => {
        const handleKeyDown = (e: KeyboardEvent) => {
            if (e.key === 'Escape' && selectedHex) {
                setSelectedHexes([])
                setFixedTooltipPos(null)
                setSelectedHexGeoCenter(null)
                setComparisonHex(null)
                setComparisonDetails(null)
                onFeatureSelect(null)
            }
        }
        window.addEventListener('keydown', handleKeyDown)
        return () => window.removeEventListener('keydown', handleKeyDown)
    }, [selectedHex, onFeatureSelect])

    // Swipe logic for mobile tooltip
    const [touchStart, setTouchStart] = useState<number | null>(null)
    const [dragOffset, setDragOffset] = useState(0)
    const [isMinimized, setIsMinimized] = useState(false)

    const handleTooltipTouchStart = (e: React.TouchEvent) => {
        setTouchStart(e.touches[0].clientY)
    }

    const handleTooltipTouchMove = (e: React.TouchEvent) => {
        if (touchStart === null) return
        const currentY = e.touches[0].clientY
        const delta = currentY - touchStart

        // If minimized, allow dragging UP (negative delta) to expand
        // If expanded, allow dragging DOWN (positive delta) to minimize/dismiss
        if (isMinimized) {
            // Allow dragging up (negative), clamp dragging down
            if (delta < 0) setDragOffset(delta)
        } else {
            // Allow dragging down (positive), clamp dragging up
            if (delta > 0) setDragOffset(delta)
        }
    }

    const handleTooltipTouchEnd = () => {
        if (isMinimized) {
            // If minimized and dragged up significantly, expand
            if (dragOffset < -50) {
                setIsMinimized(false)
            }
        } else {
            // If expanded and dragged down significantly, minimize
            if (dragOffset > 50) {
                setIsMinimized(true)
            }
        }
        setDragOffset(0)
        setTouchStart(null)
    }

    // Reset minimized state when selection changes
    useEffect(() => {
        if (selectedHex) setIsMinimized(false)
    }, [selectedHex])

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
                onDoubleClick={handleDoubleClick}
            />

            {/* Compute display data: locked mode uses selectedHexData, otherwise dynamic */}
            {(() => {
                const lockedMode = selectedHexes.length > 0
                // We need to look up properties for the selected hex if we are in locked mode
                // This might have been missing or relied on tooltipData?
                const selectedProps = selectedHex ? hexPropertyMap.current.get(selectedHex) : null

                const displayProps = lockedMode ? selectedProps : tooltipData?.properties
                const displayDetails = lockedMode ? selectionDetails : hoveredDetails
                const displayPos = lockedMode && fixedTooltipPos ? fixedTooltipPos : tooltipData

                // logic for locked mode position fallback
                let effectivePos = displayPos
                if (lockedMode && !effectivePos && selectedHexGeoCenter) {
                    // We have a selection but no screen position (maybe selected via Chat)
                    // Project the geo center to screen coordinates
                    const screenPos = geoToCanvas(selectedHexGeoCenter.lng, selectedHexGeoCenter.lat, canvasSize.width, canvasSize.height, transform, basemapCenter)
                    const smartPos = getSmartTooltipPos(screenPos.x, screenPos.y, window.innerWidth, window.innerHeight)
                    effectivePos = { globalX: smartPos.x, globalY: smartPos.y }
                }

                if (!mounted || !effectivePos || !displayProps) return null

                // Update: use effectivePos instead of displayPos below
                const finalPos = effectivePos

                return createPortal(
                    <div
                        className={cn(
                            "z-[9999] glass-panel shadow-2xl overflow-hidden",
                            isMobile
                                ? "fixed bottom-0 left-0 right-0 w-full rounded-t-xl rounded-b-none border-t border-x-0 border-b-0 pointer-events-auto transition-transform duration-300 ease-out touch-none"
                                : "fixed rounded-xl w-[320px]",
                            lockedMode && !isMobile ? "pointer-events-auto cursor-move" : "pointer-events-none",
                            showDragHint && "animate-pulse"
                        )}
                        style={isMobile ? {
                            // If minimized, translate down to show only handle (approx 24px visible)
                            // handle is ~20px + padding ~ 8px = 28px
                            transform: `translateY(calc(${isMinimized ? '100% - 24px' : '0px'} + ${dragOffset}px))`,
                            transition: touchStart === null ? 'transform 0.3s ease-out' : 'none'
                        } : {
                            left: finalPos?.globalX ?? 0,
                            top: finalPos?.globalY ?? 0,
                            // Transform not needed for flip anymore as getSmartTooltipPos handles it.
                            // But we keep translation for Minimize logic ? No, Minimize is mobile-only.
                            // We might want verify transition?
                        }}
                        onMouseDown={lockedMode && !isMobile ? (e) => {
                            setIsTooltipDragging(true)
                            e.preventDefault()
                        } : undefined}
                        onTouchStart={isMobile ? handleTooltipTouchStart : undefined}
                        onTouchMove={isMobile ? handleTooltipTouchMove : undefined}
                        onTouchEnd={isMobile ? handleTooltipTouchEnd : undefined}
                    >
                        {displayProps.has_data ? (
                            <div className="flex flex-col">
                                {/* Mobile Drag Handle */}
                                {isMobile && (
                                    <div className="w-full flex justify-center py-2 bg-muted/40 backdrop-blur-md border-b-0 cursor-grab active:cursor-grabbing">
                                        <div className="w-10 h-1 bg-muted-foreground/30 rounded-full" />
                                    </div>
                                )}

                                {/* Desktop Headers (Hidden on Mobile) */}
                                {!isMobile && (
                                    <>
                                        <div className="flex items-center justify-between px-3 py-2 border-b border-border/50 bg-muted/40 backdrop-blur-md">
                                            <div className="flex items-center gap-2">
                                                <Building2 className="w-3.5 h-3.5 text-primary" />
                                                <span className="font-bold text-[10px] tracking-wide text-foreground uppercase">Homecastr</span>
                                                {lockedMode && (
                                                    <span className="px-1.5 py-0.5 bg-yellow-500/20 text-yellow-600 dark:text-yellow-400 text-[8px] font-semibold uppercase tracking-wider rounded">Locked</span>
                                                )}
                                            </div>
                                            <div className="flex items-center gap-2">
                                                {lockedMode && <span className="text-[9px] text-muted-foreground">ESC to exit</span>}
                                            </div>
                                        </div>
                                        <div className="p-3 border-b border-border/50 bg-muted/30">
                                            <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold mb-0.5">
                                                {h3Resolution <= 7 ? "District Scale" : h3Resolution <= 9 ? "Neighborhood Scale" : h3Resolution <= 10 ? "Block Scale" : "Property Scale"} (Res {h3Resolution})
                                            </div>
                                            <div className="font-mono text-xs text-muted-foreground truncate">
                                                {cellToLatLng(displayProps.id).map((n: number) => n.toFixed(5)).join(", ")}
                                            </div>
                                        </div>
                                    </>
                                )}

                                {/* Content Body */}
                                {isMobile ? (
                                    /* Mobile Layout: 2x2 Grid + Chart Side-by-Side */
                                    <div className="p-3 flex gap-2 items-stretch h-[140px]">
                                        {/* Left: Stats Grid (45%) - Centered */}
                                        <div className="flex flex-col justify-center items-center w-[45%] shrink-0">
                                            <div className="grid grid-cols-2 gap-x-2 gap-y-1 w-full text-center">
                                                <div>
                                                    <div className="text-[9px] uppercase tracking-wider text-muted-foreground font-semibold truncate">Value</div>
                                                    <div className="text-sm font-bold text-foreground tracking-tight truncate">
                                                        {displayProps.med_predicted_value ? formatCurrency(displayProps.med_predicted_value) : "N/A"}
                                                    </div>
                                                </div>
                                                <div>
                                                    <div className="text-[9px] uppercase tracking-wider text-muted-foreground font-semibold truncate">Growth</div>
                                                    <div className={cn("text-sm font-bold tracking-tight truncate", displayProps.O >= 0 ? "text-green-500" : "text-destructive")}>
                                                        {formatOpportunity(displayProps.O)}
                                                    </div>
                                                </div>
                                                <div>
                                                    <div className="text-[9px] uppercase tracking-wider text-muted-foreground font-semibold truncate">Properties</div>
                                                    <div className="text-sm font-medium text-foreground truncate">{displayProps.n_accts}</div>
                                                </div>
                                                <div>
                                                    <div className="text-[9px] uppercase tracking-wider text-muted-foreground font-semibold truncate">Confidence</div>
                                                    <div className="text-sm font-medium text-foreground truncate">{formatReliability(displayProps.R)}</div>
                                                </div>
                                            </div>
                                        </div>

                                        {/* Right: Fan Chart (Remaining) */}
                                        <div className="flex-1 min-w-0 h-full relative">
                                            {displayDetails?.fanChart ? (
                                                <div className="h-full w-full">
                                                    <FanChart
                                                        data={displayDetails.fanChart}
                                                        currentYear={year}
                                                        height={130}
                                                        historicalValues={displayDetails.historicalValues}
                                                        comparisonData={selectedHexes.length > 1 ? previewDetails?.fanChart : comparisonDetails?.fanChart}
                                                        comparisonHistoricalValues={selectedHexes.length > 1 ? previewDetails?.historicalValues : comparisonDetails?.historicalValues}
                                                        onYearChange={onYearChange}
                                                    />
                                                </div>
                                            ) : (
                                                <div className="h-full flex items-center justify-center">
                                                    {isLoadingDetails && <div className="w-4 h-4 border-2 border-primary/30 border-t-primary rounded-full animate-spin" />}
                                                </div>
                                            )}
                                        </div>
                                    </div>
                                ) : (
                                    /* Desktop Layout: Stacked */
                                    <div className="p-4 space-y-5">
                                        <div className="grid grid-cols-2 gap-3">
                                            <div className="text-center pl-6">
                                                <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold mb-1">
                                                    {year <= 2025 ? "Actual" : "Predicted"} ({year})
                                                </div>
                                                <div className="text-xl font-bold text-foreground tracking-tight">
                                                    {displayProps.med_predicted_value ? formatCurrency(displayProps.med_predicted_value) : "N/A"}
                                                </div>
                                                {displayDetails?.historicalValues && (
                                                    <div className="text-[10px] text-muted-foreground mt-1">
                                                        Current (2025): <span className="text-foreground">{formatCurrency(displayDetails.historicalValues[displayDetails.historicalValues.length - 1])}</span>
                                                    </div>
                                                )}
                                            </div>
                                            <div className="text-center pr-6">
                                                <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold mb-1">Growth</div>
                                                <div className={cn("text-xl font-bold tracking-tight flex items-center justify-center gap-1", displayProps.O >= 0 ? "text-green-500" : "text-destructive")}>
                                                    {formatOpportunity(displayProps.O)}
                                                    {getTrendIcon(displayProps.O >= 0 ? "up" : "down")}
                                                </div>
                                                <div className="text-[10px] text-muted-foreground mt-1">
                                                    Avg. Annual {year > 2025 ? "Forecast" : "History"}
                                                </div>
                                            </div>
                                        </div>

                                        {(displayDetails?.fanChart || primaryDetails?.fanChart) ? (
                                            <div className="space-y-2">
                                                <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold flex justify-between">
                                                    <span>Value Timeline</span>
                                                    <span className="text-primary/70">{year}</span>
                                                </div>
                                                <div className="h-44 -mx-2 mb-2">
                                                    <FanChart
                                                        // Line 1: Primary (Anchor) - always first hex's data
                                                        // Fallback to displayDetails (hovered) if no primary (i.e. no selection)
                                                        data={(primaryDetails?.fanChart || displayDetails?.fanChart)!}
                                                        currentYear={year}
                                                        height={160}
                                                        historicalValues={primaryDetails?.historicalValues || displayDetails?.historicalValues}

                                                        // Line 2: Selection Aggregate (Orange)
                                                        comparisonData={selectedHexes.length > 1 ? selectionDetails?.fanChart : null}
                                                        comparisonHistoricalValues={selectedHexes.length > 1 ? selectionDetails?.historicalValues : null}

                                                        // Line 3: Preview (Fuchsia) - Aggregate of Selection + Candidate
                                                        // Show hoveredDetails explicitly as Candidate only if it's NOT already selected
                                                        previewData={previewDetails?.fanChart || (selectedHexes.length > 0 && hoveredDetails && !selectedHexes.includes(hoveredDetails.id) ? hoveredDetails?.fanChart : null)}
                                                        previewHistoricalValues={previewDetails?.historicalValues || (selectedHexes.length > 0 && hoveredDetails && !selectedHexes.includes(hoveredDetails.id) ? hoveredDetails?.historicalValues : null)}

                                                        onYearChange={onYearChange}
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

                                        <div className="pt-1 mt-0 border-t border-border/50 grid grid-cols-2 gap-4 text-center">
                                            <div>
                                                <div className="text-[9px] uppercase tracking-wider text-muted-foreground font-semibold">Properties</div>
                                                <div className="text-xs font-medium text-foreground">{displayProps.n_accts}</div>
                                            </div>
                                            <div>
                                                <div className="text-[9px] uppercase tracking-wider text-muted-foreground font-semibold">Confidence</div>
                                                <div className="text-xs font-medium text-foreground">{formatReliability(displayProps.R)}</div>
                                            </div>
                                        </div>
                                    </div>
                                )
                                }
                            </div>
                        ) : (
                            <div className="p-3">
                                <div className="font-medium text-muted-foreground text-xs">No Residential Properties</div>
                            </div>
                        )
                        }
                    </div >,
                    document.body
                )
            })()}

            <div className="absolute top-[120px] left-4 md:top-[120px] md:left-4 md:right-auto flex flex-row md:flex-col gap-2 z-30">
                {/* Mobile Selection Mode Toggles - REMOVED (Moved to Parent) */}
            </div>
        </div >
    )
}
