"use client"

import { useState, useRef, useEffect, useMemo, useCallback } from "react"
import { getH3CellDetails } from "@/app/actions/h3-details"
import type { DetailsResponse, FeatureProperties, MapState } from "@/lib/types"

// ========================================
// SHARED MAP INTERACTION HOOK
// ========================================
// Contains ALL shared interaction state and logic for MapView and VectorMap.
// Eliminates code duplication so features are automatically consistent.

export interface UseMapInteractionProps {
    year: number
    mapState: MapState
    onFeatureSelect: (id: string | null) => void
    onFeatureHover: (id: string | null) => void
}

export function useMapInteraction({ year, mapState, onFeatureSelect, onFeatureHover }: UseMapInteractionProps) {
    // ========================================
    // CORE SELECTION STATE
    // ========================================
    const [hoveredHex, setHoveredHex] = useState<string | null>(null)
    const [selectedHexes, setSelectedHexes] = useState<string[]>([])
    const selectedHexesRef = useRef<string[]>([])

    // ========================================
    // DETAILS STATE
    // ========================================
    const [selectionDetails, setSelectionDetails] = useState<DetailsResponse | null>(null)
    const [primaryDetails, setPrimaryDetails] = useState<DetailsResponse | null>(null)
    const [hoveredDetails, setHoveredDetails] = useState<DetailsResponse | null>(null)
    const [comparisonDetails, setComparisonDetails] = useState<DetailsResponse | null>(null)
    const [isLoadingDetails, setIsLoadingDetails] = useState(false)
    const detailsCache = useRef<Map<string, DetailsResponse>>(new Map())

    // ========================================
    // INTERACTION STATE
    // ========================================
    const [isShiftHeld, setIsShiftHeld] = useState(false)
    const [lockedMode, setLockedMode] = useState(false)
    const [showDragHint, setShowDragHint] = useState(false)
    const [isTooltipDragging, setIsTooltipDragging] = useState(false)
    const [tooltipOffset, setTooltipOffset] = useState({ x: 0, y: 0 })
    const dragStartRef = useRef<{ x: number; y: number } | null>(null)
    const lockedModeRef = useRef(lockedMode)
    lockedModeRef.current = lockedMode

    // ========================================
    // MOBILE STATE
    // ========================================
    const [isMobile, setIsMobile] = useState(false)
    const [isMinimized, setIsMinimized] = useState(false)
    const [dragOffset, setDragOffset] = useState(0)
    const [touchStart, setTouchStart] = useState<number | null>(null)

    // ========================================
    // TIMELAPSE STATE
    // ========================================
    const [localYear, setLocalYear] = useState(year)

    // ========================================
    // COMPARISON STATE
    // ========================================
    const [comparisonHex, setComparisonHex] = useState<string | null>(null)

    // ========================================
    // SYNC REFS
    // ========================================
    useEffect(() => {
        selectedHexesRef.current = selectedHexes
    }, [selectedHexes])

    useEffect(() => {
        setLocalYear(year)
    }, [year])

    // ========================================
    // EXTERNAL SELECTION SYNC (mapState -> local)
    // ========================================
    useEffect(() => {
        if (!mapState.selectedId) {
            if (selectedHexes.length === 1) {
                setSelectedHexes([])
                setLockedMode(false)
            }
        } else {
            if (!selectedHexes.includes(mapState.selectedId)) {
                setSelectedHexes([mapState.selectedId])
                setLockedMode(true)
                setIsMinimized(false)
            }
        }
    }, [mapState.selectedId])

    useEffect(() => {
        if (mapState.highlightedIds && mapState.highlightedIds.length > 0) {
            // Merge selection + highlights
            const ids = new Set<string>()
            if (mapState.selectedId) ids.add(mapState.selectedId)
            mapState.highlightedIds.forEach(id => ids.add(id))

            const next = Array.from(ids)
            if (next.length !== selectedHexes.length || !next.every(id => selectedHexes.includes(id))) {
                setSelectedHexes(next)
                setLockedMode(true)
                setIsMinimized(false)
            }
        } else if (selectedHexes.length > 1 && !mapState.selectedId) {
            setSelectedHexes([])
            setLockedMode(false)
        }
    }, [mapState.highlightedIds, mapState.selectedId])

    // ========================================
    // MOBILE DETECTION
    // ========================================
    useEffect(() => {
        const check = () => setIsMobile(window.innerWidth < 768)
        check()
        window.addEventListener('resize', check)
        return () => window.removeEventListener('resize', check)
    }, [])

    // ========================================
    // KEYBOARD HANDLERS
    // ========================================
    useEffect(() => {
        const handleDown = (e: KeyboardEvent) => {
            if (e.key === "Shift") setIsShiftHeld(true)
            if (e.key === "Escape" && lockedModeRef.current) {
                setLockedMode(false)
                setIsTooltipDragging(false)
                setTooltipOffset({ x: 0, y: 0 })
            }
        }
        const handleUp = (e: KeyboardEvent) => {
            if (e.key === "Shift") setIsShiftHeld(false)
        }
        window.addEventListener("keydown", handleDown)
        window.addEventListener("keyup", handleUp)
        return () => {
            window.removeEventListener("keydown", handleDown)
            window.removeEventListener("keyup", handleUp)
        }
    }, [])

    // ========================================
    // DESKTOP DRAG HANDLERS
    // ========================================
    useEffect(() => {
        if (!isTooltipDragging) return

        const handleMouseMove = (e: MouseEvent) => {
            if (!dragStartRef.current) return
            setTooltipOffset({
                x: e.clientX - dragStartRef.current.x,
                y: e.clientY - dragStartRef.current.y
            })
        }

        const handleMouseUp = () => {
            setIsTooltipDragging(false)
        }

        window.addEventListener("mousemove", handleMouseMove)
        window.addEventListener("mouseup", handleMouseUp)
        return () => {
            window.removeEventListener("mousemove", handleMouseMove)
            window.removeEventListener("mouseup", handleMouseUp)
        }
    }, [isTooltipDragging])

    // ========================================
    // AGGREGATION HELPER
    // ========================================
    const aggregateDetails = useCallback((list: DetailsResponse[]): DetailsResponse => {
        if (list.length === 0) throw new Error("Empty list")
        const primary = list[0]
        return {
            ...primary,
            metrics: {
                ...primary.metrics,
                n_accts: list.reduce((sum, d) => sum + (d.metrics?.n_accts || 0), 0)
            },
            proforma: {
                predicted_value: list.reduce((sum, d) => sum + (d.proforma?.predicted_value || 0), 0) / list.length,
                noi: null, monthly_rent: null, dscr: null,
                breakeven_occ: null, cap_rate: null, liquidity_rank: null
            },
            opportunity: {
                ...primary.opportunity,
                value: list.reduce((sum, d) => sum + (d.opportunity?.value || 0), 0) / list.length
            },
            reliability: {
                ...primary.reliability,
                value: list.reduce((sum, d) => sum + (d.reliability?.value || 0), 0) / list.length
            },
            fanChart: primary.fanChart ? {
                years: [...primary.fanChart.years],
                p10: primary.fanChart.p10.map((_, i) => list.reduce((sum, d) => sum + (d.fanChart?.p10[i] || 0), 0) / list.length),
                p50: primary.fanChart.p50.map((_, i) => list.reduce((sum, d) => sum + (d.fanChart?.p50[i] || 0), 0) / list.length),
                p90: primary.fanChart.p90.map((_, i) => list.reduce((sum, d) => sum + (d.fanChart?.p90[i] || 0), 0) / list.length),
                y_med: primary.fanChart.y_med.map((_, i) => list.reduce((sum, d) => sum + (d.fanChart?.y_med[i] || 0), 0) / list.length),
            } : undefined
        } as DetailsResponse
    }, [])

    // ========================================
    // DATA FETCHING
    // ========================================
    useEffect(() => {
        const timer = setTimeout(async () => {
            if (selectedHexes.length === 0 && !hoveredHex) {
                setHoveredDetails(null)
                setSelectionDetails(null)
                setPrimaryDetails(null)
                return
            }

            setIsLoadingDetails(true)

            const fetchWithCache = async (id: string, y: number) => {
                const key = `${id}-${y}`
                if (detailsCache.current.has(key)) return detailsCache.current.get(key)!
                const d = await getH3CellDetails(id, y)
                if (d) detailsCache.current.set(key, d)
                return d
            }

            try {
                const selectionPromise = selectedHexes.length > 0
                    ? Promise.all(selectedHexes.map(id => fetchWithCache(id, localYear)))
                    : Promise.resolve([])

                const hoverPromise = hoveredHex
                    ? fetchWithCache(hoveredHex, localYear)
                    : Promise.resolve(null)

                const [selectionResults, hoverResult] = await Promise.all([selectionPromise, hoverPromise])
                const validSelection = selectionResults.filter((d): d is DetailsResponse => d !== null)

                if (validSelection.length > 0) {
                    setPrimaryDetails(validSelection[0])
                    setSelectionDetails(aggregateDetails(validSelection))
                } else {
                    setPrimaryDetails(null)
                    setSelectionDetails(null)
                }
                setHoveredDetails(hoverResult)
            } finally {
                setIsLoadingDetails(false)
            }
        }, 150)

        return () => clearTimeout(timer)
    }, [selectedHexes, hoveredHex, localYear, aggregateDetails])

    // ========================================
    // COMPARISON LOGIC
    // ========================================
    useEffect(() => {
        if (selectedHexes.length === 0) {
            setComparisonHex(null)
            setComparisonDetails(null)
            return
        }
        if (hoveredHex && !selectedHexes.includes(hoveredHex)) {
            setComparisonHex(hoveredHex)
            setComparisonDetails(hoveredDetails)
        } else {
            setComparisonHex(null)
            setComparisonDetails(null)
        }
    }, [hoveredHex, selectedHexes, hoveredDetails])

    // ========================================
    // PREVIEW DETAILS
    // ========================================
    const previewDetails = useMemo(() => {
        if (!selectionDetails || !hoveredDetails || !isShiftHeld) return null
        if (selectedHexes.includes(hoveredHex!)) return null
        try {
            return aggregateDetails([selectionDetails, hoveredDetails])
        } catch { return null }
    }, [selectionDetails, hoveredDetails, isShiftHeld, selectedHexes, hoveredHex, aggregateDetails])

    // ========================================
    // CLICK HANDLER
    // ========================================
    const handleHexClick = useCallback((id: string, isShift: boolean, isMobileDevice: boolean): string[] => {
        const prev = selectedHexesRef.current
        const next = isShift
            ? (prev.includes(id) ? prev.filter(x => x !== id) : [...prev, id])
            : ((prev.length === 1 && prev[0] === id) ? [] : [id])

        selectedHexesRef.current = next
        setSelectedHexes(next)

        if (next.length > 0 && !isMobileDevice) {
            setLockedMode(true)
            setTooltipOffset({ x: 0, y: 0 })

            const hasSeenHint = localStorage.getItem("map_drag_hint")
            if (!hasSeenHint) {
                setShowDragHint(true)
                localStorage.setItem("map_drag_hint", "true")
                setTimeout(() => setShowDragHint(false), 3000)
            }
        }

        if (!isShift) {
            onFeatureSelect(next.length > 0 ? id : null)
        }

        return next
    }, [onFeatureSelect])

    const handleHexHover = useCallback((id: string | null) => {
        setHoveredHex(id)
        onFeatureHover(id)
    }, [onFeatureHover])

    // ========================================
    // RETURN ALL STATE AND HANDLERS
    // ========================================
    return {
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
        // Cache
        detailsCache,
        // Handlers
        handleHexClick, handleHexHover, aggregateDetails,
    }
}
