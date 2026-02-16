"use client"

import { useMemo, useState } from "react"
import type { FanChartData } from "@/lib/types"

interface FanChartProps {
  data: FanChartData
  height?: number
  currentYear?: number // The currently selected year (for vertical marker)
  historicalValues?: number[] // Actual values for 2019-2025 (7 values)
  childLines?: number[][] // Optional: Timelines for child hexes (spaghetti plot)
  // Comparison mode: overlay second hex's data
  comparisonData?: FanChartData | null
  comparisonHistoricalValues?: number[] | null
  // Preview mode: overlay shift-select aggregation
  previewData?: FanChartData | null
  previewHistoricalValues?: number[] | null
}


// Fixed timeline: 2019-2032 (14 years)
const TIMELINE_START = 2019
const TIMELINE_END = 2030
const BASELINE_YEAR = 2025 // Dividing line between history and forecast
const YEARS = Array.from({ length: TIMELINE_END - TIMELINE_START + 1 }, (_, i) => TIMELINE_START + i)

/**
 * Format large numbers for Y-axis (e.g., $1.2M, $850K)
 */
function formatYAxisValue(value: number): string {
  if (value >= 1_000_000) {
    return `$${(value / 1_000_000).toFixed(1)}M`
  }
  if (value >= 1_000) {
    return `$${Math.round(value / 1_000)}K`
  }
  return `$${Math.round(value)}`
}

/**
 * Calculate "nice" tick values for Y-axis that are human-readable
 */
function getNiceYTicks(minVal: number, maxVal: number, targetCount = 3): number[] {
  const range = maxVal - minVal
  if (range === 0) return [minVal]

  const roughStep = range / (targetCount - 1)
  const magnitude = Math.pow(10, Math.floor(Math.log10(roughStep)))
  const normalizedStep = roughStep / magnitude

  let niceStep: number
  if (normalizedStep <= 1.5) {
    niceStep = 1 * magnitude
  } else if (normalizedStep <= 3) {
    niceStep = 2 * magnitude
  } else if (normalizedStep <= 7) {
    niceStep = 5 * magnitude
  } else {
    niceStep = 10 * magnitude
  }

  const niceMin = Math.floor(minVal / niceStep) * niceStep
  const niceMax = Math.ceil(maxVal / niceStep) * niceStep

  const ticks: number[] = []
  for (let v = niceMin; v <= niceMax; v += niceStep) {
    ticks.push(v)
  }

  if (ticks.length > 5) {
    return [ticks[0], ticks[Math.floor(ticks.length / 2)], ticks[ticks.length - 1]]
  }

  return ticks
}

export function FanChart({
  data,
  height = 180,
  currentYear = 2026,
  historicalValues,
  childLines,
  comparisonData,
  comparisonHistoricalValues,
  previewData,
  previewHistoricalValues,
  onYearChange
}: FanChartProps & { onYearChange?: (year: number) => void }) {
  const { p10, p50, p90, y_med } = data
  const [hoveredYear, setHoveredYear] = useState<number | null>(null)

  const svgContent = useMemo(() => {
    const width = 300
    const padding = { top: 20, right: 15, bottom: 50, left: 55 }
    const chartWidth = width - padding.left - padding.right
    const chartHeight = height - padding.top - padding.bottom

    // Combine historical and forecast data for Y range calculation
    const allValues: number[] = []
    if (historicalValues) {
      allValues.push(...historicalValues.filter(v => Number.isFinite(v)))
    }
    if (p10) allValues.push(...p10.filter(v => Number.isFinite(v)))
    if (p90) allValues.push(...p90.filter(v => Number.isFinite(v)))
    if (p50) allValues.push(...p50.filter(v => Number.isFinite(v)))

    // Include comparison data in Y range
    if (comparisonHistoricalValues) {
      allValues.push(...comparisonHistoricalValues.filter(v => Number.isFinite(v)))
    }
    if (comparisonData?.p10) allValues.push(...comparisonData.p10.filter(v => Number.isFinite(v)))
    if (comparisonData?.p90) allValues.push(...comparisonData.p90.filter(v => Number.isFinite(v)))
    if (comparisonData?.p50) allValues.push(...comparisonData.p50.filter(v => Number.isFinite(v)))

    // Include preview data in Y range
    if (previewHistoricalValues) {
      allValues.push(...previewHistoricalValues.filter(v => Number.isFinite(v)))
    }
    if (previewData?.p10) allValues.push(...previewData.p10.filter(v => Number.isFinite(v)))
    if (previewData?.p90) allValues.push(...previewData.p90.filter(v => Number.isFinite(v)))
    if (previewData?.p50) allValues.push(...previewData.p50.filter(v => Number.isFinite(v)))


    // Fallback if no data
    if (allValues.length === 0) {
      return (
        <div className="text-xs text-muted-foreground text-center p-4">
          No Residential Properties
        </div>
      )
    }

    const dataMinY = Math.min(...allValues)
    const dataMaxY = Math.max(...allValues)

    const yTicks = getNiceYTicks(dataMinY, dataMaxY, 4)
    const minY = yTicks[0]
    const maxY = yTicks[yTicks.length - 1]
    const yRange = maxY - minY || 1

    const xScale = (year: number) =>
      padding.left + ((year - TIMELINE_START) / (TIMELINE_END - TIMELINE_START)) * chartWidth
    const yScale = (v: number) =>
      padding.top + chartHeight - ((v - minY) / yRange) * chartHeight

    // Build Historical line (solid, for actuals 2019-2025)
    let histPath = ""
    if (historicalValues && historicalValues.length > 0) {
      const histYears = [2019, 2020, 2021, 2022, 2023, 2024, 2025]
      histPath = histYears
        .map((year, i) => {
          if (!Number.isFinite(historicalValues[i])) return null
          return `${i === 0 ? "M" : "L"} ${xScale(year)} ${yScale(historicalValues[i])}`
        })
        .filter(Boolean)
        .join(" ")
    }

    // Build Forecast fan (P10-P90 bands for 2026-2032)
    const forecastYears = [2026, 2027, 2028, 2029, 2030]

    // Fan area path
    const hasForcastData = p10 && p90 && p10.some(v => Number.isFinite(v))
    let fanPath = ""
    let p50Line = ""
    let medLine = ""

    if (hasForcastData) {
      const p90Path = forecastYears
        .map((year, i) => {
          if (!Number.isFinite(p90[i])) return null
          return `${i === 0 ? "M" : "L"} ${xScale(year)} ${yScale(p90[i])}`
        })
        .filter(Boolean)
        .join(" ")

      const p10PathReverse = [...forecastYears]
        .reverse()
        .map((year, i) => {
          const idx = forecastYears.length - 1 - i
          if (!Number.isFinite(p10[idx])) return null
          return `L ${xScale(year)} ${yScale(p10[idx])}`
        })
        .filter(Boolean)
        .join(" ")

      if (p90Path && p10PathReverse) {
        fanPath = `${p90Path} ${p10PathReverse} Z`
      }

      p50Line = forecastYears
        .map((year, i) => {
          if (!Number.isFinite(p50[i])) return null
          return `${i === 0 ? "M" : "L"} ${xScale(year)} ${yScale(p50[i])}`
        })
        .filter(Boolean)
        .join(" ")

      medLine = forecastYears
        .map((year, i) => {
          if (!Number.isFinite(y_med[i])) return null
          return `${i === 0 ? "M" : "L"} ${xScale(year)} ${yScale(y_med[i])}`
        })
        .filter(Boolean)
        .join(" ")
    }

    // Connect historical to forecast with a dashed line
    let connectorPath = ""
    if (histPath && p50Line && historicalValues?.[6] && p50?.[0]) {
      connectorPath = `M ${xScale(2025)} ${yScale(historicalValues[6])} L ${xScale(2026)} ${yScale(p50[0])}`
    }

    // --- COMPARISON DATA PATHS --- //
    // Build Comparison Historical line
    let comparisonHistPath = ""
    if (comparisonHistoricalValues && comparisonHistoricalValues.length > 0) {
      const histYears = [2019, 2020, 2021, 2022, 2023, 2024, 2025]
      comparisonHistPath = histYears
        .map((year, i) => {
          if (!Number.isFinite(comparisonHistoricalValues[i])) return null
          return `${i === 0 ? "M" : "L"} ${xScale(year)} ${yScale(comparisonHistoricalValues[i])}`
        })
        .filter(Boolean)
        .join(" ")
    }

    // Build Comparison Forecast fan
    let comparisonFanPath = ""
    let comparisonP50Line = ""

    if (comparisonData && comparisonData.p10 && comparisonData.p90 && comparisonData.p50) {
      const p90Comp = comparisonData.p90
      const p10Comp = comparisonData.p10
      const p50Comp = comparisonData.p50

      const compP90Path = forecastYears
        .map((year, i) => {
          if (!Number.isFinite(p90Comp[i])) return null
          return `${i === 0 ? "M" : "L"} ${xScale(year)} ${yScale(p90Comp[i])}`
        })
        .filter(Boolean)
        .join(" ")

      const compP10PathReverse = [...forecastYears]
        .reverse()
        .map((year, i) => {
          const idx = forecastYears.length - 1 - i
          if (!Number.isFinite(p10Comp[idx])) return null
          return `L ${xScale(year)} ${yScale(p10Comp[idx])}`
        })
        .filter(Boolean)
        .join(" ")

      if (compP90Path && compP10PathReverse) {
        comparisonFanPath = `${compP90Path} ${compP10PathReverse} Z`
      }

      comparisonP50Line = forecastYears
        .map((year, i) => {
          if (!Number.isFinite(p50Comp[i])) return null
          return `${i === 0 ? "M" : "L"} ${xScale(year)} ${yScale(p50Comp[i])}`
        })
        .filter(Boolean)
        .join(" ")
    }

    // Comparison connector
    let comparisonConnectorPath = ""
    if (comparisonHistPath && comparisonP50Line && comparisonHistoricalValues?.[6] && comparisonData?.p50?.[0]) {
      comparisonConnectorPath = `M ${xScale(2025)} ${yScale(comparisonHistoricalValues[6])} L ${xScale(2026)} ${yScale(comparisonData.p50[0])}`
    }

    // --- PREVIEW DATA PATHS --- //
    // Build Preview Historical line
    let previewHistPath = ""
    if (previewHistoricalValues && previewHistoricalValues.length > 0) {
      const histYears = [2019, 2020, 2021, 2022, 2023, 2024, 2025]
      previewHistPath = histYears
        .map((year, i) => {
          if (!Number.isFinite(previewHistoricalValues[i])) return null
          return `${i === 0 ? "M" : "L"} ${xScale(year)} ${yScale(previewHistoricalValues[i])}`
        })
        .filter(Boolean)
        .join(" ")
    }

    // Build Preview Forecast fan
    let previewFanPath = ""
    let previewP50Line = ""

    if (previewData && previewData.p10 && previewData.p90 && previewData.p50) {
      const p90Prev = previewData.p90
      const p10Prev = previewData.p10
      const p50Prev = previewData.p50

      const p90Path = forecastYears
        .map((year, i) => {
          if (!Number.isFinite(p90Prev[i])) return null
          return `${i === 0 ? "M" : "L"} ${xScale(year)} ${yScale(p90Prev[i])}`
        })
        .filter(Boolean)
        .join(" ")

      const p10PathReverse = [...forecastYears]
        .reverse()
        .map((year, i) => {
          const idx = forecastYears.length - 1 - i
          if (!Number.isFinite(p10Prev[idx])) return null
          return `L ${xScale(year)} ${yScale(p10Prev[idx])}`
        })
        .filter(Boolean)
        .join(" ")

      if (p90Path && p10PathReverse) {
        previewFanPath = `${p90Path} ${p10PathReverse} Z`
      }

      previewP50Line = forecastYears
        .map((year, i) => {
          if (!Number.isFinite(p50Prev[i])) return null
          return `${i === 0 ? "M" : "L"} ${xScale(year)} ${yScale(p50Prev[i])}`
        })
        .filter(Boolean)
        .join(" ")
    }

    // Preview connector
    let previewConnectorPath = ""
    if (previewHistPath && previewP50Line && previewHistoricalValues?.[6] && previewData?.p50?.[0]) {
      previewConnectorPath = `M ${xScale(2025)} ${yScale(previewHistoricalValues[6])} L ${xScale(2026)} ${yScale(previewData.p50[0])}`
    }


    // X-axis labels - show every 2 years for clarity
    const labelYears = [2019, 2021, 2023, 2025, 2027, 2029]

    const handleMouseMove = (e: React.MouseEvent<SVGSVGElement, MouseEvent>) => {
      const rect = e.currentTarget.getBoundingClientRect()
      const x = e.clientX - rect.left
      // Inverse scale to find year
      // x = padding.left + ratio * chartWidth
      // ratio = (x - padding.left) / chartWidth
      const ratio = (x - padding.left) / chartWidth
      const yearRaw = TIMELINE_START + ratio * (TIMELINE_END - TIMELINE_START)
      const year = Math.round(yearRaw)

      // Clamp
      const clampedYear = Math.max(TIMELINE_START, Math.min(TIMELINE_END, year))
      setHoveredYear(clampedYear)
    }

    const handleClick = () => {
      if (hoveredYear && onYearChange) {
        onYearChange(hoveredYear)
      }
    }

    return (
      <svg
        viewBox={`0 0 ${width} ${height}`}
        className={onYearChange ? "w-full h-full cursor-crosshair" : "w-full h-full"}
        preserveAspectRatio="xMidYMid meet"
        style={{ maxHeight: height }}
        onMouseMove={onYearChange ? handleMouseMove : undefined}
        onMouseLeave={onYearChange ? () => setHoveredYear(null) : undefined}
        onClick={onYearChange ? handleClick : undefined}
      >
        {/* Clip path to prevent lines extending outside chart area */}
        <defs>
          <clipPath id="chart-area">
            <rect x={padding.left} y={padding.top} width={chartWidth} height={chartHeight} />
          </clipPath>
        </defs>
        {/* Grid lines */}
        {yTicks.map((tick) => (
          <line
            key={`grid-${tick}`}
            x1={padding.left}
            y1={yScale(tick)}
            x2={width - padding.right}
            y2={yScale(tick)}
            stroke="currentColor"
            strokeOpacity={0.1}
          />
        ))}

        {/* Child Lines (Spaghetti Plot) - clipped */}
        <g clipPath="url(#chart-area)">
          {childLines && childLines.map((line, idx) => {
            const d = line.map((val, i) => {
              const year = TIMELINE_START + i;
              if (!Number.isFinite(val)) return null;
              return `${i === 0 ? "M" : "L"} ${xScale(year)} ${yScale(val)}`
            }).filter(Boolean).join(" ");

            if (!d) return null;

            return (
              <path
                key={`child-${idx}`}
                d={d}
                fill="none"
                stroke="currentColor"
                strokeOpacity={0.06}
                strokeWidth={1}
              />
            )
          })}
        </g>

        {/* Y-axis line */}
        <line
          x1={padding.left}
          y1={padding.top}
          x2={padding.left}
          y2={height - padding.bottom}
          stroke="currentColor"
          strokeOpacity={0.2}
        />

        {/* X-axis line */}
        <line
          x1={padding.left}
          y1={height - padding.bottom}
          x2={width - padding.right}
          y2={height - padding.bottom}
          stroke="currentColor"
          strokeOpacity={0.2}
        />

        {/* Baseline divider (2025 - Now marker) */}
        <line
          x1={xScale(BASELINE_YEAR)}
          y1={padding.top}
          x2={xScale(BASELINE_YEAR)}
          y2={height - padding.bottom}
          stroke="oklch(0.7 0.1 250)"
          strokeWidth={1}
          strokeDasharray="4 2"
          strokeOpacity={0.5}
        />
        <text
          x={xScale(BASELINE_YEAR)}
          y={padding.top - 5}
          textAnchor="middle"
          className="text-[8px] fill-muted-foreground"
        >
          Now
        </text>

        {/* Ghost Line (Hover) */}
        {hoveredYear !== null && hoveredYear !== currentYear && (
          <line
            x1={xScale(hoveredYear)}
            y1={padding.top}
            x2={xScale(hoveredYear)}
            y2={height - padding.bottom}
            stroke="oklch(0.65 0.2 30)"
            strokeWidth={2}
            strokeOpacity={0.4}
            strokeDasharray="4 2"
          />
        )}

        {/* Current year marker (vertical line) */}
        {currentYear >= TIMELINE_START && currentYear <= TIMELINE_END && (
          <>
            <line
              x1={xScale(currentYear)}
              y1={padding.top}
              x2={xScale(currentYear)}
              y2={height - padding.bottom}
              stroke="oklch(0.65 0.2 30)"
              strokeWidth={2}
              strokeOpacity={0.7}
            />
            <circle
              cx={xScale(currentYear)}
              cy={padding.top}
              r={3}
              fill="oklch(0.65 0.2 30)"
            />
          </>
        )}

        {/* Historical shading (left of baseline) */}
        <rect
          x={padding.left}
          y={padding.top}
          width={xScale(BASELINE_YEAR) - padding.left}
          height={chartHeight}
          fill="oklch(0.5 0.05 250)"
          fillOpacity={0.05}
        />

        {/* Fan area (forecast uncertainty) */}
        {fanPath && <path d={fanPath} fill="#4a5568" fillOpacity={0.2} />}
        {comparisonFanPath && <path d={comparisonFanPath} fill="#ca8a04" fillOpacity={0.15} />}

        {/* Historical line (solid - actual values) */}
        {histPath && (
          <path d={histPath} fill="none" stroke="#4a5568" strokeWidth={2} />
        )}
        {comparisonHistPath && (
          <path d={comparisonHistPath} fill="none" stroke="#ca8a04" strokeWidth={2} strokeDasharray="3 3" />
        )}

        {/* Connector from historical to forecast */}
        {connectorPath && (
          <path d={connectorPath} fill="none" stroke="#4a5568" strokeWidth={1} strokeDasharray="2 2" />
        )}
        {comparisonConnectorPath && (
          <path d={comparisonConnectorPath} fill="none" stroke="#ca8a04" strokeWidth={1} strokeDasharray="2 2" />
        )}

        {/* P50 forecast line */}
        {p50Line && <path d={p50Line} fill="none" stroke="#4a5568" strokeWidth={2} />}
        {comparisonP50Line && <path d={comparisonP50Line} fill="none" stroke="#ca8a04" strokeWidth={2} />}

        {/* Preview Layer (Fuchsia for visibility) */}
        {previewFanPath && <path d={previewFanPath} fill="#d946ef" fillOpacity={0.15} />}
        {previewHistPath && (
          <path d={previewHistPath} fill="none" stroke="#d946ef" strokeWidth={2} strokeDasharray="3 3" />
        )}
        {previewConnectorPath && (
          <path d={previewConnectorPath} fill="none" stroke="#d946ef" strokeWidth={1} strokeDasharray="2 2" />
        )}
        {previewP50Line && <path d={previewP50Line} fill="none" stroke="#d946ef" strokeWidth={2} />}

        {/* X-axis labels */}
        {labelYears.map((year) => (
          <text
            key={year}
            x={xScale(year)}
            y={height - padding.bottom + 15}
            textAnchor="middle"
            className="text-[9px] fill-muted-foreground font-mono"
            style={{ pointerEvents: 'none' }}
          >
            {'\'' + year.toString().slice(2)}
          </text>
        ))}

        {/* Y-axis labels */}
        {yTicks.map((tick) => (
          <text
            key={tick}
            x={padding.left - 5}
            y={yScale(tick) + 3}
            textAnchor="end"
            className="text-[10px] fill-muted-foreground font-mono"
            style={{ pointerEvents: 'none' }}
          >
            {formatYAxisValue(tick)}
          </text>
        ))}

        {/* Legend - Consolidated & Fixed Clipping */}
        {/* Raised y-position to -35 (from height+5) to fit inside viewbox and avoid clipping */}
        <g transform={`translate(${padding.left}, ${height - 5})`} style={{ pointerEvents: 'none' }}>
          {/* Row 1: Lines */}
          {/* Col 1: Property */}
          <line x1={0} y1={-18} x2={12} y2={-18} stroke="#4a5568" strokeWidth={2} />
          <text x={16} y={-15} className="text-[8px] fill-muted-foreground">Area</text>

          {/* Col 2: Comparison */}
          {comparisonData && (
            <g transform="translate(60, 0)">
              <line x1={0} y1={-18} x2={12} y2={-18} stroke="#ca8a04" strokeWidth={2} strokeDasharray="3 3" />
              <text x={16} y={-15} className="text-[8px] fill-muted-foreground">Comparison</text>
            </g>
          )}

          {/* Col 3: Candidate */}
          {previewData && (
            <g transform="translate(125, 0)">
              <line x1={0} y1={-18} x2={12} y2={-18} stroke="#d946ef" strokeWidth={2} strokeDasharray="3 3" />
              <text x={16} y={-15} className="text-[8px] fill-muted-foreground">Candidate Comparison</text>
            </g>
          )}

          {/* Row 2: Range (Applies to all) */}
          <rect x={0} y={-8} width={10} height={6} fill="#888888" fillOpacity={0.3} />
          <text x={14} y={-2} className="text-[8px] fill-muted-foreground">Forecast Range</text>
        </g>
      </svg>
    )
  }, [data, height, currentYear, historicalValues, p10, p50, p90, y_med, childLines, comparisonData, comparisonHistoricalValues, previewData, previewHistoricalValues, hoveredYear, onYearChange])

  return <div className="bg-secondary/30 rounded-lg p-2">{svgContent}</div>
}
