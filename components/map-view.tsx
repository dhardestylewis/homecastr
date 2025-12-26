"use client"

import type React from "react"
import { useCallback, useEffect, useMemo, useRef, useState, useTransition } from "react"
import { cn } from "@/lib/utils"
import { formatOpportunity, formatReliability } from "@/lib/utils/colors"
import type { FilterState, MapState } from "@/lib/types"
import { getH3DataForResolution } from "@/app/actions/h3-data"

interface H3Hexagon {
  h3_id: string
  lat: number
  lng: number
  opportunity: number
  reliability: number
  property_count: number
  alert_pct: number
}

interface MappedFeature {
  id: string
  lat: number
  lng: number
  O: number // Opportunity
  R: number // Reliability
  n_accts: number // property_count
  alert_pct: number
  stability_flag: boolean
  robustness_flag: boolean
}

interface MapViewProps {
  filters: FilterState
  mapState: MapState
  onFeatureSelect: (id: string | null) => void
  onFeatureHover: (id: string | null) => void
  className?: string
}

const HARRIS_COUNTY_CENTER = { lng: -95.3698, lat: 29.7604 }
const HARRIS_COUNTY_BOUNDS = {
  minLng: -95.8,
  maxLng: -95.0,
  minLat: 29.5,
  maxLat: 30.1,
}

// Color helper based on Opportunity value
function getOpportunityColor(o: number): string {
  if (o < 0) return "rgb(200, 80, 80)"
  if (o < 0.03) return "rgb(220, 180, 80)"
  if (o < 0.06) return "rgb(180, 200, 80)"
  if (o < 0.1) return "rgb(80, 180, 120)"
  return "rgb(60, 160, 160)"
}

function getReliabilityStrokeWidth(r: number): number {
  if (r < 0.2) return 1
  if (r < 0.4) return 1.5
  if (r < 0.6) return 2
  if (r < 0.8) return 2.5
  return 3
}

// Web Mercator projection
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

function getH3ResolutionFromScale(scale: number): number {
  if (scale < 2500) return 5
  if (scale < 5000) return 6
  if (scale < 10000) return 7
  if (scale < 18000) return 8
  if (scale < 35000) return 9
  return 10
}

function getHexSizeForResolution(h3Res: number): number {
  // Approximate pixel sizes for each H3 resolution at county scale
  const sizes: Record<number, number> = {
    5: 80,
    6: 50,
    7: 35,
    8: 25,
    9: 18,
    10: 12,
  }
  return sizes[h3Res] ?? 30
}

// Generate hexagon vertices (flat-top orientation)
function getHexVertices(centerX: number, centerY: number, radius: number): Array<[number, number]> {
  const vertices: Array<[number, number]> = []
  for (let i = 0; i < 6; i++) {
    const angle = (Math.PI / 3) * i - Math.PI / 6
    vertices.push([centerX + radius * Math.cos(angle), centerY + radius * Math.sin(angle)])
  }
  return vertices
}

interface TransformState {
  offsetX: number
  offsetY: number
  scale: number
}

function mapH3ToFeatures(hexagons: H3Hexagon[]): MappedFeature[] {
  return hexagons.map((hex) => ({
    id: hex.h3_id,
    lat: hex.lat,
    lng: hex.lng,
    O: hex.opportunity,
    R: hex.reliability,
    n_accts: hex.property_count,
    alert_pct: hex.alert_pct,
    stability_flag: hex.alert_pct > 0.15,
    robustness_flag: hex.reliability < 0.3,
  }))
}

export function MapView({ filters, mapState, onFeatureSelect, onFeatureHover, className }: MapViewProps) {
  const hexCanvasRef = useRef<HTMLCanvasElement>(null)
  const containerRef = useRef<HTMLDivElement>(null)
  const basemapCanvasRef = useRef<HTMLCanvasElement>(null)
  const tileCache = useRef<Map<string, HTMLImageElement>>(new Map())
  const animationFrameRef = useRef<number | null>(null)
  const lastRenderRef = useRef<{ scale: number; h3Res: number; filterHash: string }>({
    scale: 0,
    h3Res: 0,
    filterHash: "",
  })

  const h3DataCache = useRef<Map<number, MappedFeature[]>>(new Map())
  const [isPending, startTransition] = useTransition()
  const dataFetchTimerRef = useRef<NodeJS.Timeout | null>(null)

  const [transform, setTransform] = useState<TransformState>({ offsetX: 0, offsetY: 0, scale: 5000 })
  const [isDragging, setIsDragging] = useState(false)
  const [lastMouse, setLastMouse] = useState({ x: 0, y: 0 })
  const [hoveredFeature, setHoveredFeature] = useState<MappedFeature | null>(null)
  const [tooltipPos, setTooltipPos] = useState({ x: 0, y: 0 })
  const [canvasSize, setCanvasSize] = useState({ width: 800, height: 600 })
  const [basemapCenter] = useState(HARRIS_COUNTY_CENTER)

  const [h3Features, setH3Features] = useState<MappedFeature[]>([])
  const [currentH3Res, setCurrentH3Res] = useState(6)
  const [isLoading, setIsLoading] = useState(true)

  const basemapZoom = useMemo(() => getContinuousBasemapZoom(transform.scale), [transform.scale])
  const targetH3Res = useMemo(() => getH3ResolutionFromScale(transform.scale), [transform.scale])
  const hexPixelRadius = useMemo(() => getHexSizeForResolution(currentH3Res), [currentH3Res])

  useEffect(() => {
    if (dataFetchTimerRef.current) {
      clearTimeout(dataFetchTimerRef.current)
    }

    dataFetchTimerRef.current = setTimeout(() => {
      // Check cache first
      if (h3DataCache.current.has(targetH3Res)) {
        setH3Features(h3DataCache.current.get(targetH3Res)!)
        setCurrentH3Res(targetH3Res)
        setIsLoading(false)
        return
      }

      // Fetch from Supabase
      startTransition(async () => {
        setIsLoading(true)
        try {
          const hexagons = await getH3DataForResolution(targetH3Res)
          const features = mapH3ToFeatures(hexagons)
          h3DataCache.current.set(targetH3Res, features)
          setH3Features(features)
          setCurrentH3Res(targetH3Res)
          console.log(`[v0] Loaded ${features.length} hexagons at H3 res ${targetH3Res}`)
        } catch (error) {
          console.error("[v0] Failed to fetch H3 data:", error)
        } finally {
          setIsLoading(false)
        }
      })
    }, 300)

    return () => {
      if (dataFetchTimerRef.current) {
        clearTimeout(dataFetchTimerRef.current)
      }
    }
  }, [targetH3Res])

  useEffect(() => {
    const loadInitialData = async () => {
      try {
        const hexagons = await getH3DataForResolution(6) // Start at res 6
        const features = mapH3ToFeatures(hexagons)
        h3DataCache.current.set(6, features)
        setH3Features(features)
        setCurrentH3Res(6)
        console.log(`[v0] Initial load: ${features.length} hexagons`)
      } catch (error) {
        console.error("[v0] Failed to load initial H3 data:", error)
      } finally {
        setIsLoading(false)
      }
    }
    loadInitialData()
  }, [])

  const filteredHexes = useMemo(() => {
    return h3Features.filter((hex) => {
      if (filters.reliabilityMin > 0 && hex.R < filters.reliabilityMin) return false
      if (filters.nAcctsMin > 0 && hex.n_accts < filters.nAcctsMin) return false
      if (!filters.showUnderperformers && hex.O < 0) return false
      return true
    })
  }, [h3Features, filters])

  const geoToCanvas = useCallback(
    (lng: number, lat: number) => {
      const centerMerc = latLngToMercator(basemapCenter.lng, basemapCenter.lat)
      const pointMerc = latLngToMercator(lng, lat)

      const pixelsPerUnit = transform.scale * 100
      const dx = (pointMerc.x - centerMerc.x) * pixelsPerUnit
      const dy = (pointMerc.y - centerMerc.y) * pixelsPerUnit

      return {
        x: canvasSize.width / 2 + dx + transform.offsetX,
        y: canvasSize.height / 2 + dy + transform.offsetY,
      }
    },
    [transform, canvasSize, basemapCenter],
  )

  const canvasToGeo = useCallback(
    (x: number, y: number) => {
      const centerMerc = latLngToMercator(basemapCenter.lng, basemapCenter.lat)
      const pixelsPerUnit = transform.scale * 100

      const dx = (x - canvasSize.width / 2 - transform.offsetX) / pixelsPerUnit
      const dy = (y - canvasSize.height / 2 - transform.offsetY) / pixelsPerUnit

      const mercX = centerMerc.x + dx
      const mercY = centerMerc.y + dy

      const lng = mercX * 360 - 180
      const latRad = Math.atan(Math.sinh(Math.PI * (1 - 2 * mercY)))
      const lat = (latRad * 180) / Math.PI

      return { lng, lat }
    },
    [transform, canvasSize, basemapCenter],
  )

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

  // Basemap rendering
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

    const centerWorldCoord = latLngToMercator(basemapCenter.lng, basemapCenter.lat)
    const scale = Math.pow(2, z)
    const centerTileX = centerWorldCoord.x * scale
    const centerTileY = centerWorldCoord.y * scale

    const tileSize = 256 * tileScale
    const tilesX = Math.ceil(canvasSize.width / tileSize) + 2
    const tilesY = Math.ceil(canvasSize.height / tileSize) + 2

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

        const offsetX = (tileX - centerTileX) * tileSize + canvasSize.width / 2 + transform.offsetX
        const offsetY = (tileY - centerTileY) * tileSize + canvasSize.height / 2 + transform.offsetY

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
              basemapCanvas.dispatchEvent(new Event("basemapupdate"))
            }
          }
          img.src = tileUrl
          tileCache.current.set(tileKey, img)
        }
      }
    }
  }, [canvasSize, basemapCenter, basemapZoom, transform.offsetX, transform.offsetY])

  useEffect(() => {
    const basemapCanvas = basemapCanvasRef.current
    if (!basemapCanvas) return

    const handleUpdate = () => setCanvasSize((prev) => ({ ...prev }))
    basemapCanvas.addEventListener("basemapupdate", handleUpdate)
    return () => basemapCanvas.removeEventListener("basemapupdate", handleUpdate)
  }, [])

  useEffect(() => {
    const hexCanvas = hexCanvasRef.current
    if (!hexCanvas || !canvasSize || filteredHexes.length === 0) return

    const filterHash = `${filters.reliabilityMin}-${filters.nAcctsMin}-${filters.showUnderperformers}`

    if (animationFrameRef.current) {
      cancelAnimationFrame(animationFrameRef.current)
    }

    animationFrameRef.current = requestAnimationFrame(() => {
      const ctx = hexCanvas.getContext("2d", { alpha: true, desynchronized: true })
      if (!ctx) return

      const dpr = window.devicePixelRatio || 1
      hexCanvas.width = canvasSize.width * dpr
      hexCanvas.height = canvasSize.height * dpr
      ctx.scale(dpr, dpr)

      ctx.clearRect(0, 0, canvasSize.width, canvasSize.height)

      let renderedCount = 0
      filteredHexes.forEach((hex) => {
        const { x, y } = geoToCanvas(hex.lng, hex.lat)

        // Skip hexagons outside visible area (with padding)
        const padding = hexPixelRadius * 2
        if (x < -padding || x > canvasSize.width + padding || y < -padding || y > canvasSize.height + padding) {
          return
        }

        renderedCount++
        const isSelected = mapState.selectedId === hex.id
        const isHovered = hoveredFeature?.id === hex.id
        const hasWarning = hex.stability_flag || hex.robustness_flag

        const vertices = getHexVertices(x, y, hexPixelRadius)

        ctx.beginPath()
        vertices.forEach((vertex, i) => {
          if (i === 0) ctx.moveTo(vertex[0], vertex[1])
          else ctx.lineTo(vertex[0], vertex[1])
        })
        ctx.closePath()

        const baseColor = getOpportunityColor(hex.O)
        const strokeWidth = getReliabilityStrokeWidth(hex.R)

        ctx.strokeStyle = isSelected
          ? "rgba(100, 200, 180, 1)"
          : isHovered
            ? "rgba(150, 220, 200, 0.9)"
            : hasWarning
              ? "rgba(220, 180, 80, 0.95)"
              : baseColor

        ctx.lineWidth = isSelected ? 4 : isHovered ? 3 : hasWarning ? 2.5 : strokeWidth

        if (hasWarning && !isSelected && !isHovered) {
          ctx.setLineDash([4, 4])
        } else {
          ctx.setLineDash([])
        }

        ctx.stroke()
      })
    })

    return () => {
      if (animationFrameRef.current) {
        cancelAnimationFrame(animationFrameRef.current)
      }
    }
  }, [filteredHexes, transform, mapState.selectedId, hoveredFeature, canvasSize, hexPixelRadius, geoToCanvas, filters])

  const pointInPolygon = useCallback((x: number, y: number, polygon: [number, number][]) => {
    let inside = false
    for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
      const xi = polygon[i][0],
        yi = polygon[i][1]
      const xj = polygon[j][0],
        yj = polygon[j][1]
      if (yi > y !== yj > y && x < ((xj - xi) * (y - yi)) / (yj - yi) + xi) {
        inside = !inside
      }
    }
    return inside
  }, [])

  const findFeatureAt = useCallback(
    (canvasX: number, canvasY: number) => {
      for (const hex of filteredHexes) {
        const { x, y } = geoToCanvas(hex.lng, hex.lat)
        const vertices = getHexVertices(x, y, hexPixelRadius)
        if (pointInPolygon(canvasX, canvasY, vertices as [number, number][])) {
          return hex
        }
      }
      return null
    },
    [filteredHexes, geoToCanvas, pointInPolygon, hexPixelRadius],
  )

  const handleMouseDown = (e: React.MouseEvent) => {
    setIsDragging(true)
    setLastMouse({ x: e.clientX, y: e.clientY })
  }

  const handleMouseMove = (e: React.MouseEvent) => {
    const rect = hexCanvasRef.current?.getBoundingClientRect()
    if (!rect) return

    const canvasX = e.clientX - rect.left
    const canvasY = e.clientY - rect.top

    if (isDragging) {
      const dx = e.clientX - lastMouse.x
      const dy = e.clientY - lastMouse.y
      setTransform((prev) => ({
        ...prev,
        offsetX: prev.offsetX + dx,
        offsetY: prev.offsetY + dy,
      }))
      setLastMouse({ x: e.clientX, y: e.clientY })
    } else {
      const feature = findFeatureAt(canvasX, canvasY)
      setHoveredFeature(feature)
      setTooltipPos({ x: canvasX, y: canvasY })
      onFeatureHover(feature?.id ?? null)
    }
  }

  const handleMouseUp = () => setIsDragging(false)
  const handleMouseLeave = () => {
    setIsDragging(false)
    setHoveredFeature(null)
    onFeatureHover(null)
  }

  const handleClick = (e: React.MouseEvent) => {
    const rect = hexCanvasRef.current?.getBoundingClientRect()
    if (!rect) return
    const canvasX = e.clientX - rect.left
    const canvasY = e.clientY - rect.top
    const feature = findFeatureAt(canvasX, canvasY)
    onFeatureSelect(feature?.id ?? null)
  }

  const handleWheel = (e: React.WheelEvent) => {
    e.preventDefault()
    const zoomFactor = e.deltaY > 0 ? 0.9 : 1.1
    const newScale = Math.max(1000, Math.min(50000, transform.scale * zoomFactor))
    setTransform((prev) => ({ ...prev, scale: newScale }))
  }

  const handleZoomIn = () => setTransform((prev) => ({ ...prev, scale: Math.min(50000, prev.scale * 1.3) }))
  const handleZoomOut = () => setTransform((prev) => ({ ...prev, scale: Math.max(1000, prev.scale / 1.3) }))
  const handleReset = () => setTransform({ offsetX: 0, offsetY: 0, scale: 5000 })

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
        className="cursor-grab active:cursor-grabbing absolute inset-0 z-10"
        onMouseDown={handleMouseDown}
        onMouseMove={handleMouseMove}
        onMouseUp={handleMouseUp}
        onMouseLeave={handleMouseLeave}
        onClick={handleClick}
        onWheel={handleWheel}
      />

      {(isLoading || isPending) && (
        <div className="absolute top-4 left-1/2 -translate-x-1/2 z-30 bg-card/90 backdrop-blur px-3 py-1.5 rounded-full border border-border text-sm text-muted-foreground">
          Loading H3 res {targetH3Res}...
        </div>
      )}

      {/* Zoom controls */}
      <div className="absolute bottom-4 right-4 flex flex-col gap-1 z-20">
        <button
          onClick={handleZoomIn}
          className="w-8 h-8 bg-card/95 backdrop-blur-sm border border-border rounded flex items-center justify-center text-foreground hover:bg-card transition-colors shadow-lg"
          aria-label="Zoom in"
        >
          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
          </svg>
        </button>
        <button
          onClick={handleZoomOut}
          className="w-8 h-8 bg-card/95 backdrop-blur-sm border border-border rounded flex items-center justify-center text-foreground hover:bg-card transition-colors shadow-lg"
          aria-label="Zoom out"
        >
          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M20 12H4" />
          </svg>
        </button>
        <button
          onClick={handleReset}
          className="w-8 h-8 bg-card/95 backdrop-blur-sm border border-border rounded flex items-center justify-center text-foreground hover:bg-card transition-colors shadow-lg"
          aria-label="Reset view"
        >
          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"
            />
          </svg>
        </button>
      </div>

      {/* Status bar */}
      <div className="absolute bottom-4 left-4 z-20 bg-card/90 backdrop-blur px-3 py-1.5 rounded border border-border text-xs text-muted-foreground">
        H3 Res: {currentH3Res} | Zoom: {basemapZoom.toFixed(1)} | Hexes: {filteredHexes.length}
      </div>

      {/* Tooltip */}
      {hoveredFeature && !isDragging && (
        <div
          className="absolute pointer-events-none z-30 bg-card/95 backdrop-blur border border-border rounded-lg p-3 text-sm shadow-xl"
          style={{
            left: Math.min(tooltipPos.x + 12, canvasSize.width - 200),
            top: Math.min(tooltipPos.y + 12, canvasSize.height - 150),
          }}
        >
          <div className="font-medium text-foreground mb-2 text-xs truncate max-w-[180px]">{hoveredFeature.id}</div>
          <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs">
            <span className="text-muted-foreground">Opportunity:</span>
            <span className="text-foreground font-medium">{formatOpportunity(hoveredFeature.O)}</span>
            <span className="text-muted-foreground">Reliability:</span>
            <span className="text-foreground font-medium">{formatReliability(hoveredFeature.R)}</span>
            <span className="text-muted-foreground">Properties:</span>
            <span className="text-foreground font-medium">{hoveredFeature.n_accts}</span>
            <span className="text-muted-foreground">Alert %:</span>
            <span className="text-foreground font-medium">{(hoveredFeature.alert_pct * 100).toFixed(1)}%</span>
          </div>
          {(hoveredFeature.stability_flag || hoveredFeature.robustness_flag) && (
            <div className="mt-2 pt-2 border-t border-border text-xs text-amber-400">
              {hoveredFeature.stability_flag && <div>High alert rate</div>}
              {hoveredFeature.robustness_flag && <div>Low reliability</div>}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
