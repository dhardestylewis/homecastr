"use client"

import type React from "react"
import { getDataForResolution } from "@/lib/hierarchical-data"
import { getHexDataForResolution, convertPrecomputedHexes } from "@/lib/supabase/hex-data"

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
  // Switch resolutions only at specific thresholds
  if (scale < 2500) return 5
  if (scale < 5000) return 6
  if (scale < 10000) return 7
  if (scale < 18000) return 8
  if (scale < 35000) return 9
  return 10
}

function getHexSizeFromScale(scale: number): number {
  // Hex size scales smoothly from 30px to 50px
  const minScale = 1000
  const maxScale = 50000
  const minSize = 30
  const maxSize = 50

  const t = (scale - minScale) / (maxScale - minScale)
  return minSize + t * (maxSize - minSize)
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

async function generateHexGrid(scale: number, width: number, height: number, centerLng: number, centerLat: number) {
  const h3Resolution = getH3ResolutionFromScale(scale)

  // Try to fetch precomputed data from Supabase
  const precomputedPayload = await getHexDataForResolution(h3Resolution)

  if (precomputedPayload?.hexagons) {
    // Convert precomputed hexagons and return
    const hexagons = convertPrecomputedHexes(precomputedPayload, h3Resolution)
    const hexPixelRadius = getHexSizeFromScale(scale)
    console.log(`[v0] Using precomputed ${hexagons.length} hexagons for H3 resolution ${h3Resolution}`)
    return { hexagons, hexPixelRadius, h3Resolution }
  }

  // Fallback to mock data generation
  const hexagons: Array<{ id: string; centerLng: number; centerLat: number; properties: FeatureProperties }> = []
  const hexPixelRadius = getHexSizeFromScale(scale)

  // ... existing mock data generation code ...
  const hexWidth = Math.sqrt(3) * hexPixelRadius
  const hexHeight = 2 * hexPixelRadius
  const horizontalSpacing = hexWidth
  const verticalSpacing = hexHeight * 0.75

  const cols = Math.ceil(width / horizontalSpacing) + 2
  const rows = Math.ceil(height / verticalSpacing) + 2

  for (let row = -1; row < rows; row++) {
    for (let col = -1; col < cols; col++) {
      const xOffset = (row % 2) * (hexWidth / 2)
      const pixelX = col * horizontalSpacing + xOffset
      const pixelY = row * verticalSpacing

      const properties = getDataForResolution(row, col, h3Resolution)

      hexagons.push({
        id: properties.id,
        centerLng: pixelX,
        centerLat: pixelY,
        properties,
      })
    }
  }

  console.log(
    `[v0] Generated ${hexagons.length} mock hexagons at H3 resolution ${h3Resolution}, size ${hexPixelRadius.toFixed(1)}px`,
  )

  return { hexagons, hexPixelRadius, h3Resolution }
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

  const dataUpdateTimerRef = useRef<NodeJS.Timeout | null>(null)
  const isInteractingRef = useRef(false)

  const [transform, setTransform] = useState<TransformState>({ offsetX: 0, offsetY: 0, scale: 5000 })
  const [isDragging, setIsDragging] = useState(false)
  const [lastMouse, setLastMouse] = useState({ x: 0, y: 0 })
  const [hoveredFeature, setHoveredFeature] = useState<FeatureProperties | null>(null)
  const [tooltipPos, setTooltipPos] = useState({ x: 0, y: 0 })
  const [canvasSize, setCanvasSize] = useState({ width: 800, height: 600 })
  const [basemapCenter] = useState(HARRIS_COUNTY_CENTER)

  const [frozenHexData, setFrozenHexData] = useState<{
    hexagons: Array<{ id: string; centerLng: number; centerLat: number; properties: FeatureProperties }>
    hexPixelRadius: number
    h3Resolution: number
  } | null>(null)

  const basemapZoom = useMemo(() => {
    return getContinuousBasemapZoom(transform.scale)
  }, [transform.scale])

  const {
    hexagons: hexGrid,
    hexPixelRadius,
    h3Resolution,
  } = useMemo(() => {
    // If we're interacting and have frozen data, return it
    if (isInteractingRef.current && frozenHexData) {
      return frozenHexData
    }

    // For now, return frozen data or empty (will load async)
    if (frozenHexData) {
      return frozenHexData
    }

    // Placeholder until async data loads
    return { hexagons: [], hexPixelRadius: 40, h3Resolution: 5 }
  }, [frozenHexData])

  useEffect(() => {
    const loadData = async () => {
      if (isInteractingRef.current && frozenHexData) {
        return
      }

      const newData = await generateHexGrid(transform.scale, canvasSize.width, canvasSize.height, 0, 0)
      setFrozenHexData(newData)
    }

    loadData()
  }, [transform.scale])

  useEffect(() => {
    // Clear existing timer
    if (dataUpdateTimerRef.current) {
      clearTimeout(dataUpdateTimerRef.current)
    }

    // Set flag that we're interacting
    isInteractingRef.current = true

    // Freeze current data if not already frozen
    if (!frozenHexData) {
      setFrozenHexData(generateHexGrid(transform.scale, canvasSize.width, canvasSize.height, 0, 0))
    }

    // Set timer to recompute after 300ms of no changes
    dataUpdateTimerRef.current = setTimeout(() => {
      isInteractingRef.current = false
      setFrozenHexData(null) // Unfreeze to trigger recomputation
    }, 300)

    return () => {
      if (dataUpdateTimerRef.current) {
        clearTimeout(dataUpdateTimerRef.current)
      }
    }
  }, [transform.scale])

  const filteredHexes = useMemo(() => {
    return hexGrid.filter((hex) => {
      const props = hex.properties

      if (filters.reliabilityMin > 0 && props.R < filters.reliabilityMin) {
        return false
      }

      if (filters.nAcctsMin > 0 && props.n_accts < filters.nAcctsMin) {
        return false
      }

      if (!filters.showUnderperformers && props.O < 0) {
        return false
      }

      return true
    })
  }, [hexGrid, filters])

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
    const centerTileX = centerWorldCoord.x * scale
    const centerTileY = centerWorldCoord.y * scale

    const tileSize = 256 * tileScale
    const tilesX = Math.ceil(canvasSize.width / tileSize) + 2
    const tilesY = Math.ceil(canvasSize.height / tileSize) + 2

    const startTileX = Math.floor(centerTileX - tilesX / 2)
    const startTileY = Math.floor(centerTileY - tilesY / 2)

    let loadedCount = 0
    let totalTiles = 0

    for (let ty = 0; ty < tilesY; ty++) {
      for (let tx = 0; tx < tilesX; tx++) {
        let tileX = startTileX + tx
        const tileY = startTileY + ty

        tileX = ((tileX % scale) + scale) % scale

        if (tileY < 0 || tileY >= scale) continue

        totalTiles++
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
            loadedCount++
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
  }, [canvasSize, basemapCenter, basemapZoom])

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

      filteredHexes.forEach((hex) => {
        const props = hex.properties
        const { x, y } = geoToCanvas(hex.centerLng, hex.centerLat)

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
      })
    })

    return () => {
      if (animationFrameRef.current) {
        cancelAnimationFrame(animationFrameRef.current)
      }
    }
  }, [
    filteredHexes,
    transform,
    mapState.selectedId,
    hoveredFeature,
    canvasSize,
    hexPixelRadius,
    h3Resolution,
    filters,
    isDragging,
  ])

  const geoToCanvas = useCallback(
    (lng: number, lat: number) => {
      return {
        x: lng + transform.offsetX,
        y: lat + transform.offsetY,
      }
    },
    [transform],
  )

  const canvasToGeo = useCallback(
    (x: number, y: number) => {
      const lng = x - transform.offsetX
      const lat = y - transform.offsetY
      return { lng, lat }
    },
    [transform],
  )

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
        const { x, y } = geoToCanvas(hex.centerLng, hex.centerLat)
        const vertices = getHexVertices(x, y, hexPixelRadius)

        if (pointInPolygon(canvasX, canvasY, vertices as [number, number][])) {
          return hex.properties
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

      if (feature) {
        onFeatureHover(feature.id)
      } else {
        onFeatureHover(null)
      }
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

  const handleClick = (e: React.MouseEvent) => {
    const rect = hexCanvasRef.current?.getBoundingClientRect()
    if (!rect) return

    const canvasX = e.clientX - rect.left
    const canvasY = e.clientY - rect.top

    const feature = findFeatureAt(canvasX, canvasY)
    if (feature) {
      onFeatureSelect(feature.id)
    } else {
      onFeatureSelect(null)
    }
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
        onClick={handleClick}
        onWheel={handleWheel}
      />

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

      <div className="absolute bottom-4 left-4 bg-card/80 backdrop-blur border border-border rounded px-3 py-1.5 text-xs font-mono text-muted-foreground z-20">
        <div className="flex gap-3">
          <span>H3: {h3Resolution}</span>
          <span>Zoom: {basemapZoom.toFixed(1)}</span>
          <span>Hexes: {filteredHexes.length}</span>
        </div>
      </div>
    </div>
  )
}
