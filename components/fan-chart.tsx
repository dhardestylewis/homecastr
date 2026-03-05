"use client"

import { useMemo, useState } from "react"
import type { FanChartData } from "@/lib/types"

interface PinnedComparison {
  data: FanChartData
  historicalValues?: number[]
  label?: string
}

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
  // Pinned comparisons: persistent multi-area comparison (up to 4)
  pinnedComparisons?: PinnedComparison[]
  // Optional fixed Y-axis domain [min, max] for consistent scaling across charts
  yDomain?: [number, number] | null
}

const COLORS = [
  "#fb923c", // Orange (Default)
  "#8b5cf6", // Purple (Secondary origin year)
  "#38bdf8", // Light Blue (Tertiary)
]

const PINNED_COLORS = [
  "#a3e635", // Lime
  "#38bdf8", // Sky
  "#f472b6", // Pink
  "#facc15", // Amber
]


// Fixed timeline: 2019-2032 (14 years)
const TIMELINE_START = 2019
const TIMELINE_END = 2030
const BASELINE_YEAR = 2026 // Dividing line between history and forecast ("Now")
const YEARS = Array.from({ length: TIMELINE_END - TIMELINE_START + 1 }, (_, i) => TIMELINE_START + i)

/** Build an SVG path string from year/value pairs.
 *  Always emits "M" for the first valid point (not just index 0),
 *  preventing the "Expected moveto path command" error when
 *  early values are missing/NaN. */
function buildPath(
  years: number[],
  values: number[],
  xScale: (y: number) => number,
  yScale: (v: number) => number,
): string {
  let first = true
  return years
    .map((year, i) => {
      if (i >= values.length || !Number.isFinite(values[i])) return null
      const cmd = first ? "M" : "L"
      first = false
      return `${cmd} ${xScale(year)} ${yScale(values[i])}`
    })
    .filter(Boolean)
    .join(" ")
}

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
function getNiceYTicks(minVal: number, maxVal: number, targetCount = 5): number[] {
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

  // Keep up to 8 ticks; if more, take every other one (keep first and last)
  if (ticks.length > 8) {
    const filtered = ticks.filter((_, i) => i % 2 === 0 || i === ticks.length - 1)
    return filtered
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
  pinnedComparisons,
  yDomain,
  onYearChange
}: FanChartProps & { onYearChange?: (year: number) => void }) {
  const { p10, p50, p90, y_med } = data
  const [hoveredYear, setHoveredYear] = useState<number | null>(null)

  const svgContent = useMemo(() => {
    const width = 300
    const padding = { top: 10, right: 15, bottom: 20, left: 55 }
    const chartWidth = width - padding.left - padding.right
    const chartHeight = height - padding.top - padding.bottom

    // Combine historical and forecast data for Y range calculation
    const allValues: number[] = []
    if (historicalValues) {
      allValues.push(...historicalValues.filter(v => Number.isFinite(v)))
    }

    // Process forecastVariants if available, else fallback to root arrays
    if (data.forecastVariants && Object.keys(data.forecastVariants).length > 0) {
      Object.values(data.forecastVariants).forEach(variant => {
        if (variant.p10) allValues.push(...variant.p10.filter((v: number) => Number.isFinite(v)))
        if (variant.p90) allValues.push(...variant.p90.filter((v: number) => Number.isFinite(v)))
        if (variant.p50) allValues.push(...variant.p50.filter((v: number) => Number.isFinite(v)))
      })
    } else {
      if (p10) allValues.push(...p10.filter(v => Number.isFinite(v)))
      if (p90) allValues.push(...p90.filter(v => Number.isFinite(v)))
      if (p50) allValues.push(...p50.filter(v => Number.isFinite(v)))
    }

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

    // Include pinned comparisons in Y range
    if (pinnedComparisons) {
      for (const pc of pinnedComparisons) {
        if (pc.historicalValues) allValues.push(...pc.historicalValues.filter(v => Number.isFinite(v)))
        if (pc.data?.p10) allValues.push(...pc.data.p10.filter(v => Number.isFinite(v)))
        if (pc.data?.p90) allValues.push(...pc.data.p90.filter(v => Number.isFinite(v)))
        if (pc.data?.p50) allValues.push(...pc.data.p50.filter(v => Number.isFinite(v)))
      }
    }


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

    // Use fixed yDomain if provided, otherwise auto-compute
    const effectiveMinY = yDomain ? yDomain[0] : dataMinY
    const effectiveMaxY = yDomain ? yDomain[1] : dataMaxY

    const yTicks = getNiceYTicks(effectiveMinY, effectiveMaxY, 4)
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
      histPath = buildPath(histYears, historicalValues, xScale, yScale)
    }

    // Build Forecast fans dynamically from forecastVariants if available
    const forecastYears = [2026, 2027, 2028, 2029, 2030]

    // Ensure we always have an iterable array of variants (fallback to root if single)
    const activeVariants: Array<{ p10: number[], p50: number[], p90: number[] }> = []
    if (data.forecastVariants && Object.keys(data.forecastVariants).length > 0) {
      // Sort keys ascending so oldest origin year gets primary color, newer gets secondary
      const sortedKeys = Object.keys(data.forecastVariants).sort()
      sortedKeys.forEach(k => activeVariants.push(data.forecastVariants![Number(k)]))
    } else if (p10 && p50 && p90) {
      activeVariants.push({ p10, p50, p90 })
    }

    const renderedVariants = activeVariants.map((variant, index) => {
      const _p10 = variant.p10
      const _p50 = variant.p50
      const _p90 = variant.p90

      let pathConf = { fanPath: "", p50Line: "", connectorPath: "" }
      if (_p10 && _p90 && _p10.some(v => Number.isFinite(v))) {
        const p90Path = forecastYears.map((year, i) => {
          if (!Number.isFinite(_p90[i])) return null
          return `${i === 0 ? "M" : "L"} ${xScale(year)} ${yScale(_p90[i])}`
        }).filter(Boolean).join(" ")

        const p10PathReverse = [...forecastYears].reverse().map((year, i) => {
          const idx = forecastYears.length - 1 - i
          if (!Number.isFinite(_p10[idx])) return null
          return `L ${xScale(year)} ${yScale(_p10[idx])}`
        }).filter(Boolean).join(" ")

        if (p90Path && p10PathReverse) {
          pathConf.fanPath = `${p90Path} ${p10PathReverse} Z`
        }

        pathConf.p50Line = buildPath(forecastYears, _p50, xScale, yScale)

        if (histPath && pathConf.p50Line && historicalValues?.[6] && _p50?.[0]) {
          pathConf.connectorPath = `M ${xScale(2025)} ${yScale(historicalValues[6])} L ${xScale(2026)} ${yScale(_p50[0])}`
        }
      }
      return { ...pathConf, color: COLORS[index % COLORS.length] }
    })

    // --- COMPARISON DATA PATHS --- //
    // Build Comparison Historical line
    let comparisonHistPath = ""
    if (comparisonHistoricalValues && comparisonHistoricalValues.length > 0) {
      const histYears = [2019, 2020, 2021, 2022, 2023, 2024, 2025]
      comparisonHistPath = buildPath(histYears, comparisonHistoricalValues, xScale, yScale)
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

      comparisonP50Line = buildPath(forecastYears, p50Comp, xScale, yScale)
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
      previewHistPath = buildPath(histYears, previewHistoricalValues, xScale, yScale)
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

      previewP50Line = buildPath(forecastYears, p50Prev, xScale, yScale)
    }

    // Preview connector
    let previewConnectorPath = ""
    if (previewHistPath && previewP50Line && previewHistoricalValues?.[6] && previewData?.p50?.[0]) {
      previewConnectorPath = `M ${xScale(2025)} ${yScale(previewHistoricalValues[6])} L ${xScale(2026)} ${yScale(previewData.p50[0])}`
    }


    // X-axis labels - show every 2 years for clarity
    const labelYears = [2020, 2022, 2024, 2026, 2028, 2030]

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
        preserveAspectRatio="none"
        style={{ display: 'block' }}
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

        {/* Fan area (forecast uncertainty) - Dynamic Multi-Origin Support */}
        {renderedVariants.map((rv, idx) => (
          rv.fanPath && <path key={`fan-${idx}`} d={rv.fanPath} fill={rv.color} fillOpacity={0.20} />
        ))}
        {comparisonFanPath && <path d={comparisonFanPath} fill="#a3e635" fillOpacity={0.18} />}

        {/* Pinned Comparisons — rendered below primary but above grid */}
        {pinnedComparisons?.map((pc, pcIdx) => {
          if (!pc.data?.p10 || !pc.data?.p90 || !pc.data?.p50) return null
          const color = PINNED_COLORS[pcIdx % PINNED_COLORS.length]

          // Fan area
          const pcP90Path = forecastYears.map((year, i) => {
            if (!Number.isFinite(pc.data.p90[i])) return null
            return `${i === 0 ? "M" : "L"} ${xScale(year)} ${yScale(pc.data.p90[i])}`
          }).filter(Boolean).join(" ")
          const pcP10Reverse = [...forecastYears].reverse().map((year, i) => {
            const idx = forecastYears.length - 1 - i
            if (!Number.isFinite(pc.data.p10[idx])) return null
            return `L ${xScale(year)} ${yScale(pc.data.p10[idx])}`
          }).filter(Boolean).join(" ")
          const pcFanPath = pcP90Path && pcP10Reverse ? `${pcP90Path} ${pcP10Reverse} Z` : ""

          // Historical line
          let pcHistPath = ""
          if (pc.historicalValues && pc.historicalValues.length > 0) {
            pcHistPath = buildPath([2019, 2020, 2021, 2022, 2023, 2024, 2025], pc.historicalValues, xScale, yScale)
          }

          // P50 forecast line
          const pcP50Line = buildPath(forecastYears, pc.data.p50, xScale, yScale)

          // Connector
          let pcConnector = ""
          if (pcHistPath && pcP50Line && pc.historicalValues?.[6] && pc.data.p50?.[0]) {
            pcConnector = `M ${xScale(2025)} ${yScale(pc.historicalValues[6])} L ${xScale(2026)} ${yScale(pc.data.p50[0])}`
          }

          return (
            <g key={`pinned-${pcIdx}`}>
              {pcFanPath && <path d={pcFanPath} fill={color} fillOpacity={0.12} />}
              {pcHistPath && <path d={pcHistPath} fill="none" stroke={color} strokeWidth={2} />}
              {pcConnector && <path d={pcConnector} fill="none" stroke={color} strokeWidth={2} />}
              {pcP50Line && (() => {
                const solidDot = `M ${xScale(forecastYears[0])} ${yScale(pc.data.p50[0])}`
                const dashedPc = forecastYears.slice(1).map((year, i) => {
                  if (!Number.isFinite(pc.data.p50[i + 1])) return null
                  return `${i === 0 ? `M ${xScale(forecastYears[0])} ${yScale(pc.data.p50[0])} L` : "L"} ${xScale(year)} ${yScale(pc.data.p50[i + 1])}`
                }).filter(Boolean).join(" ")
                return (
                  <>
                    <path d={solidDot} fill="none" stroke={color} strokeWidth={2} />
                    {dashedPc && <path d={dashedPc} fill="none" stroke={color} strokeWidth={2} strokeDasharray="5 3" />}
                  </>
                )
              })()}
            </g>
          )
        })}

        {/* Historical line (solid - actual values) */}
        {histPath && (
          <path d={histPath} fill="none" stroke="#fb923c" strokeWidth={2.5} />
        )}
        {comparisonHistPath && (
          <path d={comparisonHistPath} fill="none" stroke="#a3e635" strokeWidth={2} />
        )}

        {renderedVariants.map((rv, idx) => (
          rv.connectorPath && <path key={`conn-${idx}`} d={rv.connectorPath} fill="none" stroke={rv.color} strokeWidth={2.5} />
        ))}
        {comparisonConnectorPath && (
          <path d={comparisonConnectorPath} fill="none" stroke="#a3e635" strokeWidth={2} />
        )}

        {/* P50 forecast line — solid at 2026, dashed from 2027 onward (forecast) */}
        {renderedVariants.map((rv, idx) => {
          if (!rv.p50Line) return null
          // Note: we're recalculating dashed logic manually here for the multiple arrays
          const variantData = activeVariants[idx].p50
          const solidEnd = `M ${xScale(forecastYears[0])} ${yScale(variantData[0])}`
          const dashedStart = forecastYears.slice(1).map((year, i) => {
            if (!Number.isFinite(variantData[i + 1])) return null
            return `${i === 0 ? `M ${xScale(forecastYears[0])} ${yScale(variantData[0])} L` : "L"} ${xScale(year)} ${yScale(variantData[i + 1])}`
          }).filter(Boolean).join(" ")
          return (
            <g key={`p50line-${idx}`}>
              <path d={solidEnd} fill="none" stroke={rv.color} strokeWidth={2.5} />
              {dashedStart && <path d={dashedStart} fill="none" stroke={rv.color} strokeWidth={2.5} strokeDasharray="5 3" />}
            </g>
          )
        })}
        {comparisonP50Line && comparisonData?.p50 && (() => {
          const p50Comp = comparisonData.p50
          const solidDot = `M ${xScale(forecastYears[0])} ${yScale(p50Comp[0])}`
          const dashedComp = forecastYears.slice(1).map((year, i) => {
            if (!Number.isFinite(p50Comp[i + 1])) return null
            return `${i === 0 ? `M ${xScale(forecastYears[0])} ${yScale(p50Comp[0])} L` : "L"} ${xScale(year)} ${yScale(p50Comp[i + 1])}`
          }).filter(Boolean).join(" ")
          return (
            <>
              <path d={solidDot} fill="none" stroke="#a3e635" strokeWidth={2} />
              {dashedComp && <path d={dashedComp} fill="none" stroke="#a3e635" strokeWidth={2} strokeDasharray="5 3" />}
            </>
          )
        })()}

        {/* Preview Layer (Fuchsia for visibility) */}
        {previewFanPath && <path d={previewFanPath} fill="#d946ef" fillOpacity={0.15} />}
        {previewHistPath && (
          <path d={previewHistPath} fill="none" stroke="#d946ef" strokeWidth={2} />
        )}
        {previewConnectorPath && (
          <path d={previewConnectorPath} fill="none" stroke="#d946ef" strokeWidth={2} />
        )}
        {previewP50Line && <path d={previewP50Line} fill="none" stroke="#d946ef" strokeWidth={2} strokeDasharray="5 3" />}

        {/* X-axis labels */}
        {labelYears.map((yr) => (
          <text
            key={yr}
            x={xScale(yr)}
            y={height - padding.bottom + 15}
            textAnchor="middle"
            className="text-[9px] fill-muted-foreground font-mono"
            style={{ pointerEvents: 'none' }}
          >
            {'\'' + yr.toString().slice(2)}
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


      </svg>
    )
  }, [data, height, currentYear, historicalValues, p10, p50, p90, y_med, childLines, comparisonData, comparisonHistoricalValues, previewData, previewHistoricalValues, pinnedComparisons, hoveredYear, onYearChange, yDomain])

  return <div className="w-full h-full">{svgContent}</div>
}
