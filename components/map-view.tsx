"use client"

import type React from "react"
import { getH3DataForResolution } from "@/app/actions/h3-data"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { cn } from "@/lib/utils"
import { formatOpportunity, formatReliability } from "@/lib/utils/colors"
import type { FilterState, FeatureProperties, MapState } from "@/lib/types"

interface MapViewProps {
  filters: FilterState
  mapState: MapState
  onFeatureSelect: (id: string | null) => void
  onFeatureHover: (id: string | null) => void
  className?: string
}

const HARRIS_COUNTY_CENTER = { lng: -95.3698, lat: 29.7604 } // Houston coordinates

// Color helper based on Opportunity value
function getOpportunityColor(o: number): string {
  if (o < 0) return "rgb(200, 80, 80)"
  if (o < 3) return "rgb(220, 180, 80)"
  if (o < 6) return "rgb(180, 200, 80)"
  if (o < 10) return "rgb(80, 180, 120)"
  return "rgb(60, 160, 160)"
}

// Opacity helper based on Reliability value
function getReliabilityOpacity(r: number): number {
  if (r < 0.2) return 0.25
  if (r < 0.4) return 0.45
  if (r < 0.6) return 0.65
  if (r < 0.8) return 0.85
  return 1
}

// Parse RGB string to components
function parseRgb(color: string): { r: number; g: number; b: number } {
  const match = color.match(/rgb$$(\d+),\s*(\d+),\s*(\d+)$$/)
  if (match) {
    return { r: Number.parseInt(match[1]), g: Number.parseInt(match[2]), b: Number.parseInt(match[3]) }
  }
  return { r: 100, g: 100, b: 100 }
}

// Web Mercator projection for basemap tiles
function latLngToMercator(lng: number, lat: number) {
  const x = (lng + 180) / 360
  const latRad = (lat * Math.PI) / 180
  const y = (1 - Math.log(Math.tan(latRad) + 1 / Math.cos(latRad)) / Math.PI) / 2
  return { x, y }
}

// Tile URL helper
function getTileUrl(x: number, y: number, z: number): string {
  return `https://tile.openstreetmap.org/${z}/${x}/${y}.png`
}

function getContinuousBasemapZoom(scale: number): number {
  // Scale ranges from 1000 (zoomed out) to 50000 (zoomed in)
  // Map to zoom levels 9-18 continuously
  const minScale = 1000
  const maxScale = 50000
  const minZoom = 9
  const maxZoom = 18

  // Logarithmic interpolation for smoother zoom
  const t = Math.log(scale / minScale) / Math.log(maxScale / minScale)
  return minZoom + t * (maxZoom - minZoom)
}

function getH3ResolutionFromScale(scale: number): number {
  if (scale > 35000) return 5
  if (scale > 18000) return 6
  if (scale > 10000) return 7
  if (scale > 5000) return 8
  if (scale > 2500) return 9
  return 10
}

function getHexSizeFromScale(scale: number): number {
  const baseSize = 40
  const minSize = 30
  const maxSize = 50
  const scaledSize = baseSize * (scale / 10000)
  return Math.max(minSize, Math.min(maxSize, scaledSize))
}

// Generate regular hexagon vertices relative to center (flat-top orientation)
function getHexVertices(centerX: number, centerY: number, radius: number): Array<[number, number]> {
  const vertices: Array<[number, number]> = []
  // Flat-top hexagon: start at 0° (right), go counter-clockwise
  for (let i = 0; i < 6; i++) {
    const angle = (Math.PI / 3) * i - Math.PI / 6 // offset by 30° for flat-top
    vertices.push([centerX + radius * Math.cos(angle), centerY + radius * Math.sin(angle)])
  }
  return vertices
}

interface TransformState {
  offsetX: number
  offsetY: number
  scale: number
}

function getReliabilityStrokeWidth(r: number): number {
  if (r < 0.2) return 1
  if (r < 0.4) return 1.5
  if (r < 0.6) return 2
  if (r < 0.8) return 2.5
  return 3
}

function geoToCanvas(
  lng: number,
  lat: number,
  canvasWidth: number,
  canvasHeight: number,
  transform: TransformState,
  basemapCenter: { lng: number; lat: number },
): { x: number; y: number } {
  // Web Mercator projection
  const centerMerc = latLngToMercator(basemapCenter.lng, basemapCenter.lat)
  const pointMerc = latLngToMercator(lng, lat)

  // Scale factor based on zoom/scale
  const metersPerPixel = 40075016.686 / (256 * Math.pow(2, getContinuousBasemapZoom(transform.scale)))

  // Convert mercator difference to pixels
  const dx = ((pointMerc.x - centerMerc.x) * 40075016.686) / metersPerPixel
  const dy = ((pointMerc.y - centerMerc.y) * 40075016.686) / metersPerPixel

  // Apply transform offset and center on canvas
  return {
    x: canvasWidth / 2 + dx + transform.offsetX,
    y: canvasHeight / 2 + dy + transform.offsetY,
  }
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

  const h3DataCache = useRef<Map<number, any[]>>(new Map())
  const [isLoadingData, setIsLoadingData] = useState(false)
  const [realHexData, setRealHexData] = useState<any[]>([])

  const dataUpdateTimerRef = useRef<NodeJS.Timeout | null>(null)
  const isInteractingRef = useRef(false)

  const [transform, setTransform] = useState<TransformState>({ offsetX: 0, offsetY: 0, scale: 5000 })
  const [isDragging, setIsDragging] = useState(false)
  const [lastMouse, setLastMouse] = useState({ x: 0, y: 0 })
  const [hoveredFeature, setHoveredFeature] = useState<FeatureProperties | null>(null)
  const [tooltipPos, setTooltipPos] = useState({ x: 0, y: 0 })
  const [canvasSize, setCanvasSize] = useState({ width: 800, height: 600 })
  const [basemapCenter] = useState(HARRIS_COUNTY_CENTER)

  const [filteredHexes, setFilteredHexes] = useState<
    Array<{ id: string; pixelX: number; pixelY: number; properties: FeatureProperties }>
  >([])
  const [hexPixelRadius, setHexPixelRadius] = useState<number>(0)
  const [h3Resolution, setH3Resolution] = useState<number>(0)

  const basemapZoom = useMemo(() => {
    return getContinuousBasemapZoom(transform.scale)
  }, [transform.scale])

  useEffect(() => {
    const currentH3Res = getH3ResolutionFromScale(transform.scale)

    if (h3DataCache.current.has(currentH3Res)) {
      setRealHexData(h3DataCache.current.get(currentH3Res)!)
      return
    }

    setIsLoadingData(true)
    getH3DataForResolution(currentH3Res, 2025)
      .then((data) => {
        console.log(`[v0] Loaded ${data.length} REAL hexagons from Supabase`)
        h3DataCache.current.set(currentH3Res, data)
        setRealHexData(data)
        setIsLoadingData(false)
      })
      .catch((err) => {
        console.error("[v0] Failed to load H3 data:", err)
        setIsLoadingData(false)
      })
  }, [transform.scale])

  useEffect(() => {
    const currentH3Res = getH3ResolutionFromScale(transform.scale)
    const hexSize = getHexSizeFromScale(transform.scale)

    const hexagons = realHexData
      .map((hex) => {
        const canvasPos = geoToCanvas(hex.lng, hex.lat, canvasSize.width, canvasSize.height, transform, basemapCenter)

        // Convert real Supabase fields to FeatureProperties
        const properties: FeatureProperties = {
          id: hex.h3_id,
          O: hex.opportunity * 100, // Convert 0.03 → 3%
          R: hex.reliability,
          n_accts: hex.property_count,
          med_mean_ape_pct: hex.sample_accuracy * 100,
          med_mean_pred_cv_pct: hex.sample_accuracy * 100,
          stability_flag: hex.alert_pct > 0.15,
          robustness_flag: hex.alert_pct > 0.25,
        }

        return {
          id: hex.h3_id,
          pixelX: canvasPos.x,
          pixelY: canvasPos.y,
          properties,
        }
      })
      .filter((hex) => {
        // Only render hexes visible on canvas
        return (
          hex.pixelX > -100 &&
          hex.pixelX < canvasSize.width + 100 &&
          hex.pixelY > -100 &&
          hex.pixelY < canvasSize.height + 100
        )
      })

    setFilteredHexes(hexagons)
    setHexPixelRadius(hexSize)
    setH3Resolution(currentH3Res)
  }, [realHexData, transform, canvasSize, basemapCenter])

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

    const centerWorldCoord = latLngToMercator(basemapCenter.lng, basemapCenter.lat)
    const scale = Math.pow(2, z)

    const metersPerPixel = 40075016.686 / (256 * Math.pow(2, basemapZoom))
    const offsetLng = (-transform.offsetX / canvasSize.width) * (360 / scale)
    const offsetLat = (transform.offsetY / canvasSize.height) * (180 / scale)

    const adjustedCenter = latLngToMercator(basemapCenter.lng + offsetLng, basemapCenter.lat + offsetLat)

    const centerTileX = adjustedCenter.x * scale
    const centerTileY = adjustedCenter.y * scale

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

        const offsetX = (tileX - centerTileX) * tileSize + canvasSize.width / 2
        const offsetY = (tileY - centerTileY) * tileSize + canvasSize.height / 2

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

  useEffect(() => {
    const hexCanvas = hexCanvasRef.current
    if (!hexCanvas || !canvasSize) return

    const filterHash = `${filters.reliabilityMin}-${filters.nAcctsMin}-${filters.showUnderperformers}`

    if (
      lastRenderRef.current.scale === transform.scale &&
      lastRenderRef.current.h3Res === h3Resolution &&
      lastRenderRef.current.filterHash === filterHash &&
      !mapState.selectedId &&
      !hoveredFeature &&
      !isDragging
    ) {
      return
    }

    lastRenderRef.current = { scale: transform.scale, h3Res: h3Resolution, filterHash }

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

      const regularHexes = filteredHexes.filter(
        (hex) => hex.properties.id !== mapState.selectedId && hex.properties.id !== hoveredFeature?.id,
      )
      const specialHexes = filteredHexes.filter(
        (hex) => hex.properties.id === mapState.selectedId || hex.properties.id === hoveredFeature?.id,
      )

      const renderHex = (hex: (typeof filteredHexes)[0], isSpecial: boolean) => {
        const props = hex.properties
        const x = hex.pixelX
        const y = hex.pixelY

        const isSelected = mapState.selectedId === props.id
        const isHovered = hoveredFeature?.id === props.id
        const hasWarning = props.stability_flag || props.robustness_flag

        const vertices = getHexVertices(x, y, hexPixelRadius)

        ctx.beginPath()
        vertices.forEach((vertex, i) => {
          if (i === 0) ctx.moveTo(vertex[0], vertex[1])
          else ctx.lineTo(vertex[0], vertex[1])
        })
        ctx.closePath()

        const baseColor = getOpportunityColor(props.O)
        const strokeWidth = getReliabilityStrokeWidth(props.R)

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

        if (isSelected) {
          ctx.fillStyle = baseColor
          ctx.fill()
        }
      }

      // Render regular hexes first
      regularHexes.forEach((hex) => renderHex(hex, false))
      specialHexes.forEach((hex) => renderHex(hex, true))
    })
  }, [filteredHexes, hexPixelRadius, mapState.selectedId, hoveredFeature, isDragging, canvasSize])

  const pointInPolygon = useCallback((point: [number, number], polygon: [number, number][]) => {
    let inside = false
    for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
      const xi = polygon[i][0],
        yi = polygon[i][1]
      const xj = polygon[j][0],
        yj = polygon[j][1]

      if (yi > point[1] !== yj > point[1] && point[0] < ((xj - xi) * (point[1] - yi)) / (yj - yi) + xi) {
        inside = !inside
      }
    }
    return inside
  }, [])

  const handleCanvasClick = useCallback(
    (e: React.MouseEvent<HTMLCanvasElement>) => {
      if (!hexCanvasRef.current) return

      const rect = hexCanvasRef.current.getBoundingClientRect()
      const x = e.clientX - rect.left
      const y = e.clientY - rect.top

      for (const hex of filteredHexes) {
        const vertices = getHexVertices(hex.pixelX, hex.pixelY, hexPixelRadius)
        if (pointInPolygon([x, y], vertices)) {
          onFeatureSelect(hex.properties.id)
          break
        }
      }
    },
    [filteredHexes, hexPixelRadius, onFeatureSelect],
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
      for (const hex of filteredHexes) {
        const vertices = getHexVertices(hex.pixelX, hex.pixelY, hexPixelRadius)
        if (pointInPolygon([canvasX, canvasY], vertices)) {
          setHoveredFeature(hex.properties)
          setTooltipPos({ x: canvasX, y: canvasY })
          onFeatureHover(hex.properties.id)
          return
        }
      }
      setHoveredFeature(null)
      onFeatureHover(null)
    }
  }

  const handleMouseUp = () => {
    setIsDragging(false)
  }

  const handleMouseLeave = () => {
    setIsDragging(false)
    setHoveredFeature(null)
    onFeatureHover(null)
  }

  const handleWheel = (e: React.WheelEvent) => {
    e.preventDefault()

    const zoomFactor = e.deltaY > 0 ? 0.9 : 1.1
    const newScale = Math.max(1000, Math.min(50000, transform.scale * zoomFactor))

    setTransform((prev) => ({
      ...prev,
      scale: newScale,
    }))
  }

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
    setTransform({ offsetX: 0, offsetY: 0, scale: 5000 })
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
        className="cursor-grab active:cursor-grabbing absolute inset-0 z-10"
        onMouseDown={handleMouseDown}
        onMouseMove={handleMouseMove}
        onMouseUp={handleMouseUp}
        onMouseLeave={handleMouseLeave}
        onClick={handleCanvasClick}
        onWheel={handleWheel}
      />

      {isLoadingData && (
        <div className="absolute top-4 left-1/2 -translate-x-1/2 bg-card/95 backdrop-blur-sm border border-border rounded-lg px-4 py-2 text-sm text-muted-foreground z-20">
          Loading H3 resolution {getH3ResolutionFromScale(transform.scale)} from Supabase...
        </div>
      )}

      {hoveredFeature && !isDragging && (
        <div
          className="absolute pointer-events-none z-10 bg-card/95 backdrop-blur border border-border rounded-lg shadow-xl p-3 min-w-[200px]"
          style={{
            left: Math.min(tooltipPos.x + 12, canvasSize.width - 220),
            top: Math.min(tooltipPos.y + 12, canvasSize.height - 180),
          }}
        >
          <div className="space-y-2">
            <div className="font-medium text-sm text-foreground truncate">{hoveredFeature.id.slice(0, 16)}...</div>
            <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs">
              <span className="text-muted-foreground">Opportunity:</span>
              <span className="font-mono text-foreground">{formatOpportunity(hoveredFeature.O)}</span>
              <span className="text-muted-foreground">Reliability:</span>
              <span className="font-mono text-foreground">{formatReliability(hoveredFeature.R)}</span>
              <span className="text-muted-foreground">Accounts:</span>
              <span className="font-mono text-foreground">{hoveredFeature.n_accts}</span>
              <span className="text-muted-foreground">APE:</span>
              <span className="font-mono text-foreground">{hoveredFeature.med_mean_ape_pct.toFixed(1)}%</span>
              <span className="text-muted-foreground">CV:</span>
              <span className="font-mono text-foreground">{hoveredFeature.med_mean_pred_cv_pct.toFixed(1)}%</span>
              {(hoveredFeature.stability_flag || hoveredFeature.robustness_flag) && (
                <>
                  <span className="text-muted-foreground">Warning:</span>
                  <span className="text-amber-500 font-medium">
                    {hoveredFeature.stability_flag ? "Stability" : ""}
                    {hoveredFeature.stability_flag && hoveredFeature.robustness_flag ? ", " : ""}
                    {hoveredFeature.robustness_flag ? "Robustness" : ""}
                  </span>
                </>
              )}
            </div>
          </div>
        </div>
      )}

      <div className="absolute bottom-4 left-4 bg-card/80 backdrop-blur border border-border rounded px-3 py-1.5 text-sm font-mono text-muted-foreground z-20">
        <div className="flex gap-3">
          <span>H3: {h3Resolution}</span>
          <span>Zoom: {basemapZoom.toFixed(1)}</span>
          <span>Hexes: {filteredHexes.length}</span>
        </div>
      </div>
    </div>
  )
}
