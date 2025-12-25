"use client"

import { useMemo } from "react"
import type { FanChartData } from "@/lib/types"

interface FanChartProps {
  data: FanChartData
  height?: number
}

export function FanChart({ data, height = 160 }: FanChartProps) {
  const { years, p10, p50, p90, y_med } = data

  const svgContent = useMemo(() => {
    const width = 300
    const padding = { top: 20, right: 40, bottom: 30, left: 10 }
    const chartWidth = width - padding.left - padding.right
    const chartHeight = height - padding.top - padding.bottom

    // Calculate scales
    const minY = Math.min(...p10)
    const maxY = Math.max(...p90)
    const yRange = maxY - minY
    const yPadding = yRange * 0.1

    const xScale = (i: number) => padding.left + (i / (years.length - 1)) * chartWidth
    const yScale = (v: number) =>
      padding.top + chartHeight - ((v - minY + yPadding) / (yRange + 2 * yPadding)) * chartHeight

    // Build paths
    const p90Path = years.map((_, i) => `${i === 0 ? "M" : "L"} ${xScale(i)} ${yScale(p90[i])}`).join(" ")
    const p10PathReverse = [...years]
      .reverse()
      .map((_, i) => `L ${xScale(years.length - 1 - i)} ${yScale(p10[years.length - 1 - i])}`)
      .join(" ")
    const fanPath = `${p90Path} ${p10PathReverse} Z`

    const p50Line = years.map((_, i) => `${i === 0 ? "M" : "L"} ${xScale(i)} ${yScale(p50[i])}`).join(" ")
    const medLine = years.map((_, i) => `${i === 0 ? "M" : "L"} ${xScale(i)} ${yScale(y_med[i])}`).join(" ")

    // Y-axis labels
    const yTicks = [minY, minY + yRange / 2, maxY].map((v) => Math.round(v))

    return (
      <svg width={width} height={height} className="w-full h-auto">
        {/* Fan area */}
        <path d={fanPath} fill="oklch(0.65 0.15 180 / 0.2)" />

        {/* P50 line */}
        <path d={p50Line} fill="none" stroke="oklch(0.65 0.15 180)" strokeWidth={2} />

        {/* Y_med line (dashed) */}
        <path d={medLine} fill="none" stroke="oklch(0.75 0.12 60)" strokeWidth={1.5} strokeDasharray="4 2" />

        {/* X-axis labels */}
        {years
          .filter((_, i) => i % 2 === 0)
          .map((year, i) => {
            const idx = years.indexOf(year)
            return (
              <text
                key={year}
                x={xScale(idx)}
                y={height - 8}
                textAnchor="middle"
                className="text-[10px] fill-muted-foreground"
              >
                {year}
              </text>
            )
          })}

        {/* Y-axis labels */}
        {yTicks.map((tick) => (
          <text
            key={tick}
            x={width - 5}
            y={yScale(tick) + 3}
            textAnchor="end"
            className="text-[10px] fill-muted-foreground font-mono"
          >
            {tick}
          </text>
        ))}

        {/* Legend */}
        <g transform={`translate(${padding.left}, ${height - 8})`}>
          <line x1={0} y1={-3} x2={15} y2={-3} stroke="oklch(0.65 0.15 180)" strokeWidth={2} />
          <text x={20} y={0} className="text-[9px] fill-muted-foreground">
            P50
          </text>
          <line x1={50} y1={-3} x2={65} y2={-3} stroke="oklch(0.75 0.12 60)" strokeWidth={1.5} strokeDasharray="4 2" />
          <text x={70} y={0} className="text-[9px] fill-muted-foreground">
            Med
          </text>
          <rect x={100} y={-6} width={15} height={6} fill="oklch(0.65 0.15 180 / 0.2)" />
          <text x={120} y={0} className="text-[9px] fill-muted-foreground">
            P10-P90
          </text>
        </g>
      </svg>
    )
  }, [data, height])

  return <div className="bg-secondary/30 rounded-lg p-2">{svgContent}</div>
}
