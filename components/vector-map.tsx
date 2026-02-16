"use client"

import React, { useEffect, useRef, useState, useCallback, useMemo } from "react"
import { createPortal } from "react-dom"
import maplibregl from "maplibre-gl"
import "maplibre-gl/dist/maplibre-gl.css"
import type { FilterState, MapState, DetailsResponse } from "@/lib/types"
import { getH3CellDetails } from "@/app/actions/h3-details"
import { getParcels } from "@/app/actions/parcels"
import { cn } from "@/lib/utils"
import { getOpportunityColor, getValueColor, formatOpportunity, formatCurrency, formatReliability } from "@/lib/utils/colors"
import { TrendingUp, TrendingDown, Minus, Building2 } from "lucide-react"
import { FanChart } from "./fan-chart"
import { cellToLatLng } from "h3-js"
import { MapTooltip } from "./map-tooltip"
import { useRouter, useSearchParams } from "next/navigation"
import { useMapInteraction } from "@/hooks/use-map-interaction"

// Tooltip positioning constants (must match MapView)
const SIDEBAR_WIDTH = 340
const TOOLTIP_WIDTH = 320
const TOOLTIP_HEIGHT = 450

// Helper: Ensure tooltip stays within bounds and respects Sidebar
function getSmartTooltipPos(x: number, y: number, windowWidth: number, windowHeight: number) {
    if (typeof window === 'undefined') return { x, y }

    let left = x + 20
    let top = y - 20

    // Horizontal Constraint
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

    // Vertical Constraint
    top = Math.max(10, Math.min(top, windowHeight - TOOLTIP_HEIGHT - 10))
    left = Math.max(10, left)

    return { x: left, y: top }
}

interface VectorMapProps {
    filters: FilterState
    mapState: MapState
    year: number
    onFeatureSelect: (id: string | null) => void
    onFeatureHover: (id: string | null) => void
    className?: string
    onConsultAI?: (details: { predictedValue: number | null; opportunityScore: number | null; capRate: number | null }) => void
}

export function VectorMap({
    filters,
    mapState,
    year,
    onFeatureSelect,
    onFeatureHover,
    className,
    onConsultAI
}: VectorMapProps) {
    const mapContainerRef = useRef<HTMLDivElement>(null)
    const mapRef = useRef<maplibregl.Map | null>(null)
    const [isLoaded, setIsLoaded] = useState(false)

    // SHARED INTERACTION HOOK
    const {
        // Selection
        hoveredHex, setHoveredHex, selectedHexes, setSelectedHexes, selectedHexesRef,
        // Details
        selectionDetails, primaryDetails, hoveredDetails, comparisonDetails, previewDetails, isLoadingDetails,
        // Interaction
        isShiftHeld, lockedMode, setLockedMode, showDragHint, setShowDragHint, isTooltipDragging, setIsTooltipDragging,
        tooltipOffset, setTooltipOffset, dragStartRef,
        // Mobile
        isMobile, isMinimized, setIsMinimized, dragOffset, setDragOffset, touchStart, setTouchStart,
        // Timelapse
        localYear, setLocalYear,
        // Comparison
        comparisonHex,
        // Handlers
        handleHexClick, handleHexHover, aggregateDetails
    } = useMapInteraction({
        year,
        mapState, // Pass mapState for internal sync
        onFeatureSelect,
        onFeatureHover
    })

    // SYNC TOOLTIP POSITION WHEN SELECTED EXTERNALLY
    // Unlike MapView which builds its own state, VectorMap uses the hook.
    // However, projection (getCenter/unproject) needs the map instance.
    useEffect(() => {
        if (!mapRef.current || !isLoaded || !mapState.selectedId) return

        if (!fixedTooltipPos) {
            const [lat, lng] = cellToLatLng(mapState.selectedId)
            const point = mapRef.current.project([lng, lat])
            const clamped = getSmartTooltipPos(point.x, point.y, window.innerWidth, window.innerHeight)
            setFixedTooltipPos({ globalX: clamped.x, globalY: clamped.y })
        }
    }, [mapState.selectedId, isLoaded])


    const router = useRouter()
    const searchParams = useSearchParams()

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
        return () => { map.off("moveend", onMoveEnd) }
    }, [isLoaded, searchParams, router])

    // TRACK MOBILE
    // TRACK MOBILE - Handled by useMapInteraction hook

    // DATA CACHE & REFS (Avoid stale closures and trails)
    const hoveredHexRef = useRef<string | null>(null)
    const activeHoverIds = useRef<Set<string>>(new Set()) // Track all for sweep
    // Unified tooltip state (matches MapView pattern)
    const [tooltipData, setTooltipData] = useState<{ x: number; y: number; globalX: number; globalY: number; properties: any } | null>(null)
    const [fixedTooltipPos, setFixedTooltipPos] = useState<{ globalX: number; globalY: number } | null>(null)

    // Fix stale closure for isMobile in map event handlers
    const isMobileRef = useRef(isMobile)
    useEffect(() => { isMobileRef.current = isMobile }, [isMobile])

    // PAINT SELECTION REFS
    const isPaintingRef = useRef(false)
    const paintModeRef = useRef<"add" | "remove" | null>(null)
    const paintCandidatesRef = useRef<Set<string>>(new Set())

    // HELPERS
    const getTrendIcon = (trend: "up" | "down" | "stable" | undefined) => {
        if (trend === "up") return <TrendingUp className="h-3 w-3 text-green-500" />
        if (trend === "down") return <TrendingDown className="h-3 w-3 text-red-500" />
        return <Minus className="h-3 w-3 text-muted-foreground" />
    }



    // INITIALIZE MAP (Read from URL for persistence across engine switches)
    useEffect(() => {
        if (!mapContainerRef.current) return

        // Read initial position from URL params (synced by both engines)
        // Defaults show Houston at city scale (zoom 10) for first-time visitors
        const urlParams = new URLSearchParams(window.location.search)
        const initialLat = parseFloat(urlParams.get("lat") || "29.76")  // Houston center
        const initialLng = parseFloat(urlParams.get("lng") || "-95.37") // Houston center
        const initialZoom = parseFloat(urlParams.get("zoom") || "10")   // City scale

        const map = new maplibregl.Map({
            container: mapContainerRef.current,
            style: {
                version: 8,
                sources: {
                    "osm": {
                        type: "raster",
                        tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
                        tileSize: 256,
                        attribution: "&copy; OpenStreetMap contributors"
                    }
                },
                layers: [
                    {
                        id: "osm-layer",
                        type: "raster",
                        source: "osm"
                    }
                ]
            },
            center: [initialLng, initialLat],
            zoom: initialZoom,
            maxZoom: 18,
            minZoom: 2
        })

        map.on("load", () => {
            setIsLoaded(true)

            // Add Double-Buffered H3 Sources
            const addH3Source = (id: string) => {
                map.addSource(id, {
                    type: "vector",
                    tiles: [`${window.location.origin}/api/tiles/{z}/{x}/{y}?year=${year}`],
                    minzoom: 2,
                    maxzoom: 16,
                    promoteId: "h3_id"
                })
            }

            const addH3Layers = (suffix: string, sourceId: string, visible: boolean) => {
                // Hex Fill Layer
                map.addLayer({
                    id: `h3-fill-${suffix}`,
                    type: "fill",
                    source: sourceId,
                    "source-layer": "default",
                    layout: { visibility: visible ? "visible" : "none" },
                    paint: {
                        "fill-color": [
                            "interpolate", ["linear"], ["get", "val"],
                            100000, "#3b0764",
                            800000, "#f43f5e",
                            1500000, "#fbbf24"
                        ],
                        "fill-opacity": 0.5,
                        "fill-outline-color": "rgba(255,255,255,0.2)"
                    }
                })

                // Comparison Layer
                map.addLayer({
                    id: `h3-comparison-${suffix}`,
                    type: "line",
                    source: sourceId,
                    "source-layer": "default",
                    layout: { visibility: visible ? "visible" : "none" },
                    paint: {
                        "line-color": [
                            "case",
                            ["boolean", ["feature-state", "candidate"], false], "#d946ef", // Fuchsia for Candidate
                            "#f97316" // Orange for Comparison
                        ],
                        "line-width": 2.5,
                        "line-dasharray": [3, 2],
                        "line-opacity": [
                            "case",
                            ["any", ["boolean", ["feature-state", "comparison"], false], ["boolean", ["feature-state", "candidate"], false]], 0.8,
                            0
                        ]
                    }
                })

                // Hover/Selected Layer
                map.addLayer({
                    id: `h3-selected-${suffix}`,
                    type: "line",
                    source: sourceId,
                    "source-layer": "default",
                    layout: { visibility: visible ? "visible" : "none" },
                    paint: {
                        "line-color": [
                            "case",
                            ["boolean", ["feature-state", "primary"], false], "#14b8a6",
                            ["boolean", ["feature-state", "selected"], false], "#f97316",
                            ["boolean", ["feature-state", "hover"], false], "#14b8a6", // Teal for Hover (Primary Candidate)
                            "rgba(0,0,0,0)" // Transparent default
                        ],
                        "line-width": ["case", ["boolean", ["feature-state", "primary"], false], 4, 3],
                        "line-opacity": [
                            "case",
                            ["any", ["boolean", ["feature-state", "selected"], false], ["boolean", ["feature-state", "primary"], false], ["boolean", ["feature-state", "hover"], false]], 1,
                            0
                        ]
                    }
                })
            }

            addH3Source("h3-a")
            addH3Source("h3-b")
            addH3Layers("a", "h3-a", true)
            addH3Layers("b", "h3-b", false)

            // Sources was added above

            // Add Parcels Source & Layer (Empty initially)
            map.addSource("parcels", {
                type: "geojson",
                data: { type: "FeatureCollection", features: [] }
            })

            map.addLayer({
                id: "parcels-layer",
                type: "line",
                source: "parcels",
                paint: {
                    "line-color": "rgba(255,255,255,0.4)",
                    "line-width": 1
                },
                minzoom: 14
            })
        })

        // INTERACTION HANDLING
        const activeSourceRef = { current: "h3-a" }
        const activeSuffixRef = { current: "a" }

        const handleMove = (e: maplibregl.MapLayerMouseEvent) => {
            const feature = e.features?.[0]
            if (!feature) return

            const id = feature.properties?.h3_id as string

            // PAINT SELECTION LOGIC
            const isLeftButtonDown = (e.originalEvent.buttons & 1) === 1
            const isShift = e.originalEvent.shiftKey
            const isAlt = e.originalEvent.altKey

            if (isLeftButtonDown && (isShift || isAlt)) {
                // Determine Paint Mode
                if (!isPaintingRef.current) {
                    isPaintingRef.current = true
                    paintModeRef.current = isAlt ? "remove" : "add"
                    paintCandidatesRef.current.clear()
                }

                // Add to Paint Candidate Set
                if (id && !paintCandidatesRef.current.has(id)) {
                    paintCandidatesRef.current.add(id)
                    // Visual Feedback: Mark as Candidate
                    const sources = ["h3-a", "h3-b"]
                    sources.forEach(s => {
                        try {
                            map.setFeatureState(
                                { source: s, sourceLayer: "default", id: id },
                                { candidate: true }
                            )
                        } catch (err) { /* ignore */ }
                    })
                }
                return // Skip standard hover while painting
            }

            // Standard Hover Logic (only if not painting)
            if (isPaintingRef.current) return

            if (!id) return

            // Map Vector Tile Props -> FeatureProperties
            const p = feature.properties || {}
            const properties = {
                id: p.h3_id,
                O: (p.opp || 0) * 100,
                R: p.rel || 0,
                n_accts: p.count || 0,
                med_mean_ape_pct: (p.acc || 0) * 100,
                med_mean_pred_cv_pct: 0,
                stability_flag: (p.ap || 0) > 0.15,
                robustness_flag: (p.ap || 0) > 0.25,
                has_data: (p.count || 0) > 0,
                med_predicted_value: p.val || 0
            }

            // Only update tooltip if not in locked mode (has selection)
            if (selectedHexesRef.current.length === 0) {
                const smartPos = getSmartTooltipPos(e.originalEvent.clientX, e.originalEvent.clientY, window.innerWidth, window.innerHeight)
                setTooltipData({
                    x: e.originalEvent.clientX,
                    y: e.originalEvent.clientY,
                    globalX: smartPos.x,
                    globalY: smartPos.y,
                    properties
                })
            }

            // RANGE PREVIEW & HOVER LOGIC
            // Clear previous states
            const sources = ["h3-a", "h3-b"]
            activeHoverIds.current.forEach(oldId => {
                sources.forEach(s => {
                    try {
                        map.setFeatureState(
                            { source: s, sourceLayer: "default", id: oldId },
                            { hover: false, comparison: false, candidate: false }
                        )
                    } catch (err) { /* ignore */ }
                })
            })
            activeHoverIds.current.clear()

            // Update refs
            hoveredHexRef.current = id

            // Interaction State
            const isSelected = selectedHexesRef.current.includes(id)
            const hasSelection = selectedHexesRef.current.length > 0

            let idsToHighlight = [id]
            let isCandidate = false
            let isComparison = false

            if (isShift && hasSelection) {
                // RANGE PREVIEW
                const anchorId = selectedHexesRef.current[selectedHexesRef.current.length - 1]
                const anchorCoords = cellToLatLng(anchorId)
                const hoverCoords = cellToLatLng(id)

                const p1 = map.project([anchorCoords[1], anchorCoords[0]])
                const p2 = map.project([hoverCoords[1], hoverCoords[0]])

                const minX = Math.min(p1.x, p2.x)
                const maxX = Math.max(p1.x, p2.x)
                const minY = Math.min(p1.y, p2.y)
                const maxY = Math.max(p1.y, p2.y)

                const features = map.queryRenderedFeatures(
                    [[minX, minY], [maxX, maxY]],
                    { layers: ["h3-fill-a", "h3-fill-b"] }
                )

                idsToHighlight = Array.from(new Set(features.map(f => f.properties?.h3_id).filter(Boolean) as string[]))
                isCandidate = true
            } else if (hasSelection && !isSelected) {
                // COMPARISON PREVIEW
                isComparison = true
            }

            // Apply New States
            idsToHighlight.forEach(hId => {
                activeHoverIds.current.add(hId)
                sources.forEach(s => {
                    try {
                        map.setFeatureState(
                            { source: s, sourceLayer: "default", id: hId },
                            {
                                hover: !hasSelection && hId === id, // Single hover only if no selection
                                comparison: isComparison && hId === id, // Single comparison
                                candidate: isCandidate // Range candidate
                            }
                        )
                    } catch (err) { /* ignore */ }
                })
            })

            if (hoveredHexRef.current !== id || activeHoverIds.current.size > 1) {
                // Trigger React Update for Tooltip (debounced/handled by hook mostly, but we set local state)
                setHoveredHex(id)
                onFeatureHover(id)
                hoveredHexRef.current = id
            }
        }

        map.on("mousemove", "h3-fill-a", handleMove)
        map.on("mousemove", "h3-fill-b", handleMove)

        const handleLeave = () => {
            setTooltipData(null)
            const sources = ["h3-a", "h3-b"]
            activeHoverIds.current.forEach(oldId => {
                sources.forEach(s => {
                    map.setFeatureState(
                        { source: s, sourceLayer: "default", id: oldId },
                        { hover: false, comparison: false }
                    )
                })
            })
            activeHoverIds.current.clear()

            hoveredHexRef.current = null
            setHoveredHex(null)
            onFeatureHover(null)
        }

        map.on("mouseleave", "h3-fill-a", handleLeave)
        map.on("mouseleave", "h3-fill-b", handleLeave)

        const handleClick = (e: maplibregl.MapLayerMouseEvent) => {
            if (!e.features || e.features.length === 0) {
                // Clicked background -> Clear Selection
                const sources = ["h3-a", "h3-b"]
                selectedHexesRef.current.forEach(hId => {
                    sources.forEach(s => {
                        try {
                            map.setFeatureState(
                                { source: s, sourceLayer: "default", id: hId },
                                { selected: false, primary: false, comparison: false }
                            )
                        } catch (err) { /* ignore */ }
                    })
                })
                selectedHexesRef.current = []
                setSelectedHexes([])
                setFixedTooltipPos(null)
                onFeatureSelect(null)
                return
            }

            const id = e.features[0].properties?.h3_id as string
            if (!id) return

            // Interaction Modes:
            // Shift: Range (Bounding Box)
            // Ctrl/Meta: Toggle (Add/Remove)
            // None: Replace (Select Single)
            const isShift = (e.originalEvent as MouseEvent).shiftKey
            const isCtrl = (e.originalEvent as MouseEvent).ctrlKey || (e.originalEvent as MouseEvent).metaKey

            const prev = selectedHexesRef.current
            let next: string[] = []

            if (isShift && prev.length > 0) {
                // RANGE SELECTION using rendered features
                const anchorId = prev[prev.length - 1] // Last selected is anchor

                // Get coordinates for Anchor and Click
                const anchorCoords = cellToLatLng(anchorId)
                const clickCoords = cellToLatLng(id) // e.lngLat is also available but let's be consistent

                // Project to pixels to get bounding box
                const p1 = map.project([anchorCoords[1], anchorCoords[0]])
                const p2 = map.project([clickCoords[1], clickCoords[0]])

                // Construct Bounding Box (minX, minY, maxX, maxY)
                const minX = Math.min(p1.x, p2.x)
                const maxX = Math.max(p1.x, p2.x)
                const minY = Math.min(p1.y, p2.y)
                const maxY = Math.max(p1.y, p2.y)

                // Query features in this box
                // We must query both layers to be safe
                const features = map.queryRenderedFeatures(
                    [[minX, minY], [maxX, maxY]],
                    { layers: ["h3-fill-a", "h3-fill-b"] }
                )

                const rangeIds = Array.from(new Set(features.map(f => f.properties?.h3_id).filter(Boolean) as string[]))

                // Strategy: Union new range with existing selection (Add-Range)
                // Or should we just select the range? 
                // Legacy MapView: "Expand: add all hexes in range to existing selection"
                // Let's Add-Range.
                next = Array.from(new Set([...prev, ...rangeIds]))

            } else if (isCtrl) {
                // TOGGLE SELECTION
                next = prev.includes(id)
                    ? prev.filter(x => x !== id)
                    : [...prev, id]
            } else {
                // REPLACE SELECTION
                next = (prev.length === 1 && prev[0] === id) ? [] : [id]
            }

            // Sync visual state
            const sources = ["h3-a", "h3-b"]
            // 1. Clear OLD states not in NEW
            prev.filter(pId => !next.includes(pId)).forEach(pId => {
                sources.forEach(s => {
                    try {
                        map.setFeatureState({ source: s, sourceLayer: "default", id: pId }, { selected: false, primary: false })
                    } catch (err) { /* ignore */ }
                })
            })

            // 2. Set NEW state
            next.forEach(((nId, idx) => {
                const isPrimary = idx === 0
                // If it was already selected, we might update primary status?
                // Just force set everyone
                sources.forEach(s => {
                    try {
                        map.setFeatureState(
                            { source: s, sourceLayer: "default", id: nId },
                            { selected: true, primary: isPrimary }
                        )
                    } catch (err) { /* ignore */ }
                })
            }))

            selectedHexesRef.current = next
            setSelectedHexes(next)

            // LOCKED MODE: Activate on selection, show drag hint once
            if (next.length > 0 && !isMobileRef.current) {
                setLockedMode(true)
                setTooltipOffset({ x: 0, y: 0 }) // Reset position
                // Capture current mouse position for locked tooltip
                if (e.lngLat) {
                    const point = map.project(e.lngLat)
                    const rect = mapContainerRef.current?.getBoundingClientRect()
                    setFixedTooltipPos({ globalX: point.x + (rect?.left || 0) + 20, globalY: point.y + (rect?.top || 0) - 20 })
                }

                // First-time drag hint (localStorage check)
                const hasSeenHint = localStorage.getItem("vectormap_drag_hint")
                if (!hasSeenHint) {
                    setShowDragHint(true)
                    localStorage.setItem("vectormap_drag_hint", "true")
                    setTimeout(() => setShowDragHint(false), 3000)
                }
            } else if (next.length === 0) {
                setFixedTooltipPos(null)
            }

            // Update Parent
            // Only fire select if not Ctrl/Shift? Or fire generally?
            // Parent usually expects the PRIMARY ID or just "something selected".
            // We pass the last selected (id) or the primary (next[0])?
            // MapToolip uses next[0] as primary.
            // onFeatureSelect(next.length > 0 ? id : null) // 'id' matches click.
            // If we deselected 'id' (Ctrl click), we should pass null? Or the new primary?
            // "onFeatureSelect" usually drives the sidebar.
            // Let's pass the Clicked ID if it is IN the set, otherwise null or primary.
            // Actually, for multi-select, the parent often cares about the *Primary* (first).
            // But if we just clicked 'id', maybe that should be primary?
            // Current list logic: new items appended.
            // Let's stick to: if we have selection, notify.
            if (!isShift && !isCtrl) {
                onFeatureSelect(next.length > 0 ? id : null)
            } else {
                // For multi/range, we don't necessarily change the "active" sidebar context
                // unless we switch primary.
                // Let's just notify the parent if selection became empty.
                if (next.length === 0) onFeatureSelect(null)
                // If we have selection, maybe update to primary?
                else if (prev.length === 0) onFeatureSelect(next[0])
            }
        }

        const handleMouseUp = (e: maplibregl.MapMouseEvent) => {
            if (isPaintingRef.current) {
                // COMMIT PAINT
                const painted = Array.from(paintCandidatesRef.current)
                const prev = selectedHexesRef.current
                const mode = paintModeRef.current

                let next: string[] = []
                if (mode === "add") {
                    next = Array.from(new Set([...prev, ...painted]))
                } else if (mode === "remove") {
                    next = prev.filter(id => !painted.includes(id))
                }

                // Clear Paint State (Visuals)
                const sources = ["h3-a", "h3-b"]
                painted.forEach(id => {
                    sources.forEach(s => {
                        try {
                            map.setFeatureState(
                                { source: s, sourceLayer: "default", id: id },
                                { candidate: false } // Clear candidate
                            )
                        } catch (err) { }
                    })
                })

                // Update Selection with Visuals
                // 1. Clear OLD
                prev.filter(pId => !next.includes(pId)).forEach(pId => {
                    sources.forEach(s => {
                        try {
                            map.setFeatureState({ source: s, sourceLayer: "default", id: pId }, { selected: false, primary: false })
                        } catch (err) { /* ignore */ }
                    })
                })

                // 2. Set NEW
                next.forEach(((nId, idx) => {
                    const isPrimary = idx === 0
                    sources.forEach(s => {
                        try {
                            map.setFeatureState(
                                { source: s, sourceLayer: "default", id: nId },
                                { selected: true, primary: isPrimary }
                            )
                        } catch (err) { /* ignore */ }
                    })
                }))

                selectedHexesRef.current = next
                setSelectedHexes(next)

                // Cleanup
                isPaintingRef.current = false
                paintCandidatesRef.current.clear()
                paintModeRef.current = null

                // Notify
                if (next.length === 0) onFeatureSelect(null)
                else if (prev.length === 0) onFeatureSelect(next[0])
            }
        }

        // Register click and mouseup handlers
        map.on("click", "h3-fill-a", handleClick)
        map.on("click", "h3-fill-b", handleClick)
        map.on("mouseup", handleMouseUp)
        // Handle clicks on background (not on hexes) to clear selection
        map.on("click", (e: maplibregl.MapMouseEvent) => {
            // Only trigger if no features under click
            const features = map.queryRenderedFeatures(e.point, { layers: ["h3-fill-a", "h3-fill-b"] })
            if (features.length === 0) {
                // Clear selection
                const sources = ["h3-a", "h3-b"]
                selectedHexesRef.current.forEach(hId => {
                    sources.forEach(s => {
                        try {
                            map.setFeatureState(
                                { source: s, sourceLayer: "default", id: hId },
                                { selected: false, primary: false, comparison: false }
                            )
                        } catch (err) { /* ignore */ }
                    })
                })
                selectedHexesRef.current = []
                setSelectedHexes([])
                setFixedTooltipPos(null)
                onFeatureSelect(null)
            }
        })


            // Storage for toggle logic
            ; (map as any)._activeSuffix = "a";
        ; (map as any)._isLoaded = true;

        mapRef.current = map
        return () => { map.remove() }
    }, []) // Initialize map ONCE. Updates happen via other effects.

    // UPDATE YEAR & COLORS & SEAMLESS TOGGLE
    useEffect(() => {
        if (!isLoaded || !mapRef.current) return
        const map = mapRef.current

        const currentSuffix = (map as any)._activeSuffix || "a"
        const nextSuffix = currentSuffix === "a" ? "b" : "a"
        const nextSource = `h3-${nextSuffix}`

        // Update NEXT source tiles
        const source = map.getSource(nextSource)
        if (source && source.type === 'vector') {
            (source as any).setTiles([`${window.location.origin}/api/tiles/{z}/{x}/{y}?year=${year}`])
        }

        // Apply Color Logic (HEX equivalents of OKLCH for MapLibre compatibility)
        const fillColor: any = filters.colorMode === "growth"
            ? [
                "interpolate", ["linear"], ["get", "opp"],
                -0.50, "#b159ff", // Deep Purple
                0.00, "#f8f8f8", // Whiteish
                0.50, "#3b82f6"  // Deep Blue
            ]
            : [
                "interpolate", ["linear"], ["get", "val"],
                100000, "#3b0764",
                800000, "#f43f5e",
                1500000, "#fbbf24"
            ];

        // Update colors on BOTH layers to be ready
        ["a", "b"].forEach(s => {
            const layerId = `h3-fill-${s}`
            if (map.getLayer(layerId)) {
                map.setPaintProperty(layerId, "fill-color", fillColor)
            }
        })

        // Seamless Swap: Wait for source + render completion
        // Use idle event (fires after all tiles rendered) with timeout fallback
        let swapCompleted = false

        const performSwap = () => {
            if (swapCompleted) return
            swapCompleted = true

            // Ensure this is still the target year (handle fast scrubbing)
            const latestTargetYear = (map as any)._targetYear
            if (latestTargetYear !== year) return

            // Toggle Visibility
            map.setLayoutProperty(`h3-fill-${nextSuffix}`, "visibility", "visible")
            map.setLayoutProperty(`h3-comparison-${nextSuffix}`, "visibility", "visible")
            map.setLayoutProperty(`h3-selected-${nextSuffix}`, "visibility", "visible")

            map.setLayoutProperty(`h3-fill-${currentSuffix}`, "visibility", "none")
            map.setLayoutProperty(`h3-comparison-${currentSuffix}`, "visibility", "none")
            map.setLayoutProperty(`h3-selected-${currentSuffix}`, "visibility", "none")

                // Update active state
                ; (map as any)._activeSuffix = nextSuffix
        }

        const onSourceData = (e: any) => {
            if (e.sourceId === nextSource && map.isSourceLoaded(nextSource) && e.isSourceLoaded) {
                map.off("sourcedata", onSourceData)
                // Wait for idle (render complete) then one extra frame for GPU flush
                map.once("idle", () => {
                    requestAnimationFrame(() => {
                        requestAnimationFrame(performSwap) // Double RAF for GPU sync
                    })
                })
                // Timeout fallback in case idle never fires (edge case)
                setTimeout(performSwap, 600)
            }
        }

            // Store target year to handle fast scrubbing
            ; (map as any)._targetYear = year
        map.on("sourcedata", onSourceData)

    }, [year, isLoaded, filters.colorMode])





    useEffect(() => {
        if (!mapRef.current || !isLoaded || !comparisonHex) return
        const map = mapRef.current

        try {
            if (map.getStyle()) {
                ["h3-a", "h3-b"].forEach(s => {
                    map.setFeatureState({ source: s, sourceLayer: "default", id: comparisonHex }, { comparison: true })
                })
            }
        } catch (e) {
            console.warn("Failed to set comparison state", e)
        }

        return () => {
            if (comparisonHex) {
                try {
                    if (mapRef.current && mapRef.current.getStyle()) {
                        ["h3-a", "h3-b"].forEach(s => {
                            mapRef.current?.setFeatureState({ source: s, sourceLayer: "default", id: comparisonHex }, { comparison: false })
                        })
                    }
                } catch (e) { /* ignore */ }
            }
        }
    }, [comparisonHex, isLoaded])

    // SYNC CANDIDATE STATE WHEN SHIFT CHANGES
    useEffect(() => {
        if (!mapRef.current || !isLoaded) return
        const map = mapRef.current

        // Update state for currently hovered hex when Shift changes
        if (hoveredHex && selectedHexes.length > 0 && !selectedHexes.includes(hoveredHex)) {
            try {
                const sources = ["h3-a", "h3-b"]
                sources.forEach(s => {
                    map.setFeatureState(
                        { source: s, sourceLayer: "default", id: hoveredHex },
                        {
                            comparison: !isShiftHeld,
                            candidate: isShiftHeld
                        }
                    )
                })
            } catch (e) { /* ignore */ }
        }
    }, [isShiftHeld, hoveredHex, selectedHexes, isLoaded])

    // DYNAMIC FILTERING (Disabled for Parity with MapView)
    useEffect(() => {
        if (!mapRef.current || !isLoaded) return
        const map = mapRef.current

        // Update state for currently hovered hex when Shift changes
        if (hoveredHex && selectedHexes.length > 0 && !selectedHexes.includes(hoveredHex)) {
            try {
                const sources = ["h3-a", "h3-b"]
                sources.forEach(s => {
                    map.setFeatureState(
                        { source: s, sourceLayer: "default", id: hoveredHex },
                        {
                            comparison: !isShiftHeld,
                            candidate: isShiftHeld
                        }
                    )
                })
            } catch (e) { /* ignore */ }
        }
    }, [isShiftHeld, hoveredHex, selectedHexes, isLoaded])

    useEffect(() => {
        if (!mapRef.current || !isLoaded) return
        const map = mapRef.current

        // MapView shows ALL data regardless of filters
        // We ensure "all" filter to show everything
        try {
            if (map.getStyle()) {
                const layers = [
                    "h3-fill-a", "h3-fill-b",
                    "h3-selected-a", "h3-selected-b",
                    "h3-comparison-a", "h3-comparison-b"
                ]
                layers.forEach(id => {
                    if (map.getLayer(id)) map.setFilter(id, null) // Clear any filters
                })
            }
        } catch (e) {
            console.warn("Failed to apply filters", e)
        }
    }, [isLoaded])

    // PARCEL FETCHING
    useEffect(() => {
        if (!isLoaded || !mapRef.current) return
        const map = mapRef.current

        const updateParcels = async () => {
            const zoom = map.getZoom()
            if (zoom < 14) return

            const bounds = map.getBounds()
            const p = await getParcels({
                minLat: bounds.getSouth(),
                maxLat: bounds.getNorth(),
                minLng: bounds.getWest(),
                maxLng: bounds.getEast()
            })

            const source = map.getSource("parcels")
            if (source && source.type === 'geojson') {
                (source as any).setData({
                    type: "FeatureCollection",
                    features: p.map(parcel => ({
                        type: "Feature",
                        geometry: parcel.geometry,
                        properties: { acct_key: parcel.acct_key }
                    }))
                })
            }
        }

        const handler = () => {
            clearTimeout((map as any)._parcelTimer)
                ; (map as any)._parcelTimer = setTimeout(updateParcels, 500)
        }

        map.on("moveend", handler)
        handler() // Initial fetch
        return () => { map.off("moveend", handler) }
    }, [isLoaded])

    // SYNC VIEWPORT
    useEffect(() => {
        if (!mapRef.current || !isLoaded) return
        const map = mapRef.current

        try {
            if (map.getStyle()) {
                const center = map.getCenter()
                const zoom = map.getZoom()
                if (Math.abs(center.lng - mapState.center[0]) > 0.001 ||
                    Math.abs(center.lat - mapState.center[1]) > 0.001 ||
                    Math.abs(zoom - mapState.zoom) > 0.1) {
                    map.flyTo({
                        center: [mapState.center[0], mapState.center[1]],
                        zoom: mapState.zoom,
                        speed: 0.8,
                        curve: 1.42,
                        easing: (t: number) => t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2, // easeInOutCubic
                    })
                }
            }
        } catch (e) { /* map might be in transition or removed */ }
    }, [mapState.center, mapState.zoom, isLoaded])

    // TOOLTIP POSITIONING & RENDER PARITY
    // If locked (selection active), we prioritize the selection for content AND position (via fixedTooltipPos)
    // to prevents "flickering" or showing hover data while pinned to selection.
    const lockedModeActive = selectedHexes.length > 0
    const displayId = lockedModeActive ? selectedHexes[0] : (hoveredHex || null)
    const displayDetails = lockedModeActive ? selectionDetails : (hoveredDetails || null)
    const h3Resolution = mapRef.current ? Math.floor(mapRef.current.getZoom()) : 0

    return (
        <div className={cn("relative w-full h-full", className)}>
            <div ref={mapContainerRef} className="w-full h-full" />

            {!isLoaded && (
                <div className="absolute inset-0 flex items-center justify-center bg-background/50 backdrop-blur-sm z-50">
                    <div className="flex flex-col items-center gap-2">
                        <div className="w-8 h-8 border-4 border-primary border-t-transparent rounded-full animate-spin" />
                        <span className="text-sm font-medium">Initializing Vector Engine...</span>
                    </div>
                </div>
            )}

            {/* SHARED TOOLTIP UI - Match MapView pattern */}
            {(() => {
                const displayProps = lockedModeActive ? (tooltipData?.properties?.id === selectedHexes[0] ? tooltipData.properties : null) : tooltipData?.properties
                // Note: displayProps might be stale if we hover away. 
                // Ideally we should look up props for the selected ID if we have them. 
                // But VectorMap relies on 'tooltipData' from hover event.
                // If we are locked, we might not have 'tooltipData' for the selected hex if we moved mouse.
                // However, 'selectionDetails' (passed from parent) should has the data we need for the CHART.
                // For the HEADER (value/growth), we used 'displayProps' from the vector tile features.
                // If we don't hover it, we don't have it? 
                // MapView solves this by fetching data. VectorMap relies on vector tile properties for instant display.
                // If we are locked, we should probably stick to what we have.

                const displayPos = lockedModeActive && fixedTooltipPos ? fixedTooltipPos : tooltipData

                if (!isLoaded || !displayPos || !displayId) return null

                return (
                    <MapTooltip
                        x={displayPos.globalX ?? 0}
                        y={displayPos.globalY ?? 0}
                        coordinates={cellToLatLng(displayId)}
                        displayProps={displayProps || {
                            id: displayId,
                            O: 0, R: 0, n_accts: 0, med_mean_ape_pct: 0, med_mean_pred_cv_pct: 0,
                            stability_flag: false, robustness_flag: false, has_data: false
                        }}
                        displayDetails={displayDetails}
                        primaryDetails={primaryDetails}
                        selectionDetails={selectionDetails}
                        comparisonDetails={comparisonDetails}
                        previewDetails={previewDetails}
                        selectedHexes={selectedHexes}
                        hoveredDetails={hoveredDetails}
                        year={localYear}
                        h3Resolution={h3Resolution}
                        isLoadingDetails={isLoadingDetails}
                        isMobile={isMobile}
                        isMinimized={isMinimized}
                        lockedMode={lockedMode}
                        showDragHint={showDragHint}
                        onYearChange={setLocalYear}
                        onMouseDown={lockedMode && !isMobile ? (e: React.MouseEvent) => {
                            setIsTooltipDragging(true)
                            dragStartRef.current = { x: e.clientX - tooltipOffset.x, y: e.clientY - tooltipOffset.y }
                        } : undefined}
                        onTouchStart={isMobile ? (e: React.TouchEvent) => {
                            setTouchStart(e.touches[0].clientY)
                        } : undefined}
                        onTouchMove={isMobile ? (e: React.TouchEvent) => {
                            if (touchStart === null) return
                            const delta = e.touches[0].clientY - touchStart
                            if (delta > 0) setDragOffset(delta)
                        } : undefined}
                        onTouchEnd={isMobile ? () => {
                            if (dragOffset > 100) {
                                setIsMinimized(true)
                            }
                            setDragOffset(0)
                            setTouchStart(null)
                        } : undefined}
                        dragOffset={dragOffset}
                        onConsultAI={onConsultAI ? () => {
                            const details = selectionDetails || primaryDetails || hoveredDetails
                            onConsultAI({
                                predictedValue: details?.proforma?.predicted_value ?? null,
                                opportunityScore: details?.opportunity?.value ?? null,
                                capRate: details?.proforma?.cap_rate ?? null,
                            })
                        } : undefined}
                        googleMapsApiKey={process.env.NEXT_PUBLIC_GOOGLE_MAPS_KEY}
                    />
                )
            })()}
        </div>
    )
}
