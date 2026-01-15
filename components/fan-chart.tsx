"use client"

import { useMemo } from "react"
import type { FanChartData } from "@/lib/types"

interface FanChartProps {
  data: FanChartData
  height?: number
  currentYear?: number // The currently selected year (for vertical marker)
  historicalValues?: number[] // Actual values for 2019-2025 (7 values)
}

// Fixed timeline: 2019-2035 (17 years)
const TIMELINE_START = 2019
const TIMELINE_END = 2035
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
  historicalValues
}: FanChartProps) {
  const { p10, p50, p90, y_med } = data

  const svgContent = useMemo(() => {
    const width = 300
    const padding = { top: 20, right: 15, bottom: 35, left: 55 }
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

    // Fallback if no data
    if (allValues.length === 0) {
      return (
        <div className="text-xs text-muted-foreground text-center p-4">
          No projection data available
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

    // Build Forecast fan (P10-P90 bands for 2026-2030)
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

    // X-axis labels - show every 2 years for clarity
    const labelYears = [2019, 2021, 2023, 2025, 2027, 2029, 2031, 2033, 2035]

    return (
      <svg width={width} height={height} className="w-full h-auto">
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
        {fanPath && <path d={fanPath} fill="oklch(0.65 0.15 180 / 0.2)" />}

        {/* Historical line (solid - actual values) */}
        {histPath && (
          <path d={histPath} fill="none" stroke="oklch(0.6 0.1 250)" strokeWidth={2} />
        )}

        {/* Connector from historical to forecast */}
        {connectorPath && (
          <path d={connectorPath} fill="none" stroke="oklch(0.6 0.08 200)" strokeWidth={1} strokeDasharray="2 2" />
        )}

        {/* P50 forecast line */}
        {p50Line && <path d={p50Line} fill="none" stroke="oklch(0.65 0.15 180)" strokeWidth={2} />}

        {/* Y_med line (dashed) */}
        {medLine && <path d={medLine} fill="none" stroke="oklch(0.75 0.12 60)" strokeWidth={1.5} strokeDasharray="4 2" />}

        {/* X-axis labels */}
        {labelYears.map((year) => (
          <text
            key={year}
            x={xScale(year)}
            y={height - padding.bottom + 15}
            textAnchor="middle"
            className="text-[9px] fill-muted-foreground font-mono"
          >
            {year.toString().slice(2)}
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
          >
            {formatYAxisValue(tick)}
          </text>
        ))}

        {/* Legend */}
        <g transform={`translate(${padding.left}, ${height - 10})`}>
          <line x1={0} y1={-3} x2={12} y2={-3} stroke="oklch(0.6 0.1 250)" strokeWidth={2} />
          <text x={15} y={0} className="text-[8px] fill-muted-foreground">
            Actual
          </text>
          <line x1={50} y1={-3} x2={62} y2={-3} stroke="oklch(0.65 0.15 180)" strokeWidth={2} />
          <text x={65} y={0} className="text-[8px] fill-muted-foreground">
            Forecast
          </text>
          <rect x={105} y={-6} width={12} height={6} fill="oklch(0.65 0.15 180 / 0.2)" />
          <text x={120} y={0} className="text-[8px] fill-muted-foreground">
            Range
          </text>
        </g>
      </svg>
    )
  }, [data, height, currentYear, historicalValues, p10, p50, p90, y_med])

  return <div className="bg-secondary/30 rounded-lg p-2">{svgContent}</div>
}
