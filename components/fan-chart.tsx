"use client"

import { useMemo } from "react"
import type { FanChartData } from "@/lib/types"

interface FanChartProps {
  data: FanChartData
  height?: number
  startYear?: number // The forecast year (first year in the projection)
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
 * e.g., $500K, $750K, $1M instead of $487,213, $743,891, $1,000,569
 */
function getNiceYTicks(minVal: number, maxVal: number, targetCount = 3): number[] {
  const range = maxVal - minVal

  // Find a "nice" step size (multiples of 1, 2, 2.5, 5 × power of 10)
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

  // Round min down and max up to nice values
  const niceMin = Math.floor(minVal / niceStep) * niceStep
  const niceMax = Math.ceil(maxVal / niceStep) * niceStep

  // Generate ticks
  const ticks: number[] = []
  for (let v = niceMin; v <= niceMax; v += niceStep) {
    ticks.push(v)
  }

  // Limit to ~3-5 ticks
  if (ticks.length > 5) {
    const filtered = [ticks[0], ticks[Math.floor(ticks.length / 2)], ticks[ticks.length - 1]]
    return filtered
  }

  return ticks
}

export function FanChart({ data, height = 180, startYear = 2026 }: FanChartProps) {
  const { p10, p50, p90, y_med } = data

  // Generate actual year labels (e.g., 2026, 2027, 2028, 2029, 2030)
  const actualYears = useMemo(() =>
    Array.from({ length: 5 }, (_, i) => startYear + i),
    [startYear]
  )

  const svgContent = useMemo(() => {
    const width = 300
    // Increased left padding for Y-axis labels
    const padding = { top: 20, right: 15, bottom: 35, left: 55 }
    const chartWidth = width - padding.left - padding.right
    const chartHeight = height - padding.top - padding.bottom

    // Calculate data range
    const dataMinY = Math.min(...p10)
    const dataMaxY = Math.max(...p90)

    // Get nice Y-axis ticks
    const yTicks = getNiceYTicks(dataMinY, dataMaxY, 4)
    const minY = yTicks[0]
    const maxY = yTicks[yTicks.length - 1]
    const yRange = maxY - minY

    const xScale = (i: number) => padding.left + (i / (actualYears.length - 1)) * chartWidth
    const yScale = (v: number) =>
      padding.top + chartHeight - ((v - minY) / yRange) * chartHeight

    // Build paths
    const p90Path = actualYears.map((_, i) => `${i === 0 ? "M" : "L"} ${xScale(i)} ${yScale(p90[i])}`).join(" ")
    const p10PathReverse = [...actualYears]
      .reverse()
      .map((_, i) => `L ${xScale(actualYears.length - 1 - i)} ${yScale(p10[actualYears.length - 1 - i])}`)
      .join(" ")
    const fanPath = `${p90Path} ${p10PathReverse} Z`

    const p50Line = actualYears.map((_, i) => `${i === 0 ? "M" : "L"} ${xScale(i)} ${yScale(p50[i])}`).join(" ")
    const medLine = actualYears.map((_, i) => `${i === 0 ? "M" : "L"} ${xScale(i)} ${yScale(y_med[i])}`).join(" ")

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

        {/* Fan area */}
        <path d={fanPath} fill="oklch(0.65 0.15 180 / 0.2)" />

        {/* P50 line */}
        <path d={p50Line} fill="none" stroke="oklch(0.65 0.15 180)" strokeWidth={2} />

        {/* Y_med line (dashed) */}
        <path d={medLine} fill="none" stroke="oklch(0.75 0.12 60)" strokeWidth={1.5} strokeDasharray="4 2" />

        {/* X-axis labels - show all years */}
        {actualYears.map((year, i) => (
          <text
            key={year}
            x={xScale(i)}
            y={height - padding.bottom + 15}
            textAnchor="middle"
            className="text-[10px] fill-muted-foreground font-mono"
          >
            {year}
          </text>
        ))}

        {/* Y-axis labels - nice rounded values on LEFT side */}
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

        {/* Legend - moved below chart */}
        <g transform={`translate(${padding.left}, ${height - 10})`}>
          <line x1={0} y1={-3} x2={12} y2={-3} stroke="oklch(0.65 0.15 180)" strokeWidth={2} />
          <text x={15} y={0} className="text-[8px] fill-muted-foreground">
            P50
          </text>
          <line x1={40} y1={-3} x2={52} y2={-3} stroke="oklch(0.75 0.12 60)" strokeWidth={1.5} strokeDasharray="4 2" />
          <text x={55} y={0} className="text-[8px] fill-muted-foreground">
            Med
          </text>
          <rect x={80} y={-6} width={12} height={6} fill="oklch(0.65 0.15 180 / 0.2)" />
          <text x={95} y={0} className="text-[8px] fill-muted-foreground">
            P10-P90
          </text>
        </g>
      </svg>
    )
  }, [data, height, actualYears, p10, p50, p90, y_med])

  return <div className="bg-secondary/30 rounded-lg p-2">{svgContent}</div>
}
