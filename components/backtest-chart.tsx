"use client"

import { useEffect, useState, useMemo } from "react"

/**
 * BacktestChart — interactive SVG that overlays backtest predictions
 * from multiple origin years against actual historical outcomes.
 *
 * Used on the methodology page to visually demonstrate calibration.
 */

interface VintageData {
    years: number[]
    p10: number[]
    p50: number[]
    p90: number[]
}

interface BacktestData {
    historicalYears: number[]
    historicalValues: (number | null)[]
    vintages: Record<number, VintageData>
}

// Colors for each origin year vintage — visually distinct
const VINTAGE_COLORS: Record<number, string> = {
    2019: "#f97316", // orange
    2020: "#a855f7", // purple
    2021: "#3b82f6", // blue
    2022: "#22c55e", // green
    2023: "#ec4899", // pink
    2024: "#eab308", // amber
    2025: "#06b6d4", // cyan
}

function getVintageColor(origin: number): string {
    return VINTAGE_COLORS[origin] || "#9ca3af"
}

function formatYAxisValue(value: number): string {
    if (value >= 1_000_000) return `$${(value / 1_000_000).toFixed(1)}M`
    if (value >= 1_000) return `$${Math.round(value / 1_000)}K`
    return `$${Math.round(value)}`
}

function getNiceYTicks(minVal: number, maxVal: number, targetCount = 5): number[] {
    const range = maxVal - minVal
    if (range === 0) return [minVal]

    const roughStep = range / (targetCount - 1)
    const magnitude = Math.pow(10, Math.floor(Math.log10(roughStep)))
    const normalizedStep = roughStep / magnitude

    let niceStep: number
    if (normalizedStep <= 1.5) niceStep = 1 * magnitude
    else if (normalizedStep <= 3) niceStep = 2 * magnitude
    else if (normalizedStep <= 7) niceStep = 5 * magnitude
    else niceStep = 10 * magnitude

    const niceMin = Math.floor(minVal / niceStep) * niceStep
    const niceMax = Math.ceil(maxVal / niceStep) * niceStep

    const ticks: number[] = []
    for (let v = niceMin; v <= niceMax; v += niceStep) ticks.push(v)

    if (ticks.length > 8) return ticks.filter((_, i) => i % 2 === 0 || i === ticks.length - 1)
    return ticks
}

interface BacktestChartProps {
    level?: string
    id?: string
    schema?: string
    height?: number
}

export function BacktestChart({
    level = "zcta",
    id = "77079",
    schema = "forecast_queue",
    height = 280,
}: BacktestChartProps) {
    const [data, setData] = useState<BacktestData | null>(null)
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState<string | null>(null)
    const [hoveredYear, setHoveredYear] = useState<number | null>(null)
    const [hiddenVintages, setHiddenVintages] = useState<Set<number>>(new Set())

    useEffect(() => {
        const fetchData = async () => {
            setLoading(true)
            try {
                // Try live API first
                const res = await fetch(
                    `/api/backtest-coverage?level=${level}&id=${id}&schema=${schema}`
                )
                if (!res.ok) throw new Error(`HTTP ${res.status}`)
                const json = await res.json()

                // If live data has enough vintages (3+), use it
                const vintageCount = Object.keys(json.vintages || {}).length
                if (vintageCount >= 3) {
                    setData(json)
                    return
                }

                // Otherwise fall back to static demo data
                const demoRes = await fetch("/data/backtest-demo.json")
                if (demoRes.ok) {
                    const demoJson = await demoRes.json()
                    setData(demoJson)
                    return
                }

                // If demo also fails, use whatever the API returned
                setData(json)
            } catch (e: any) {
                // API failed entirely — try static demo
                try {
                    const demoRes = await fetch("/data/backtest-demo.json")
                    if (demoRes.ok) {
                        const demoJson = await demoRes.json()
                        setData(demoJson)
                        return
                    }
                } catch { /* ignore */ }
                setError(e.message)
            } finally {
                setLoading(false)
            }
        }
        fetchData()
    }, [level, id, schema])

    const toggleVintage = (origin: number) => {
        setHiddenVintages(prev => {
            const next = new Set(prev)
            if (next.has(origin)) next.delete(origin)
            else next.add(origin)
            return next
        })
    }

    const svgContent = useMemo(() => {
        if (!data) return null

        const width = 600
        const padding = { top: 15, right: 20, bottom: 30, left: 60 }
        const chartWidth = width - padding.left - padding.right
        const chartHeight = height - padding.top - padding.bottom

        // Determine timeline from data
        const allYears: number[] = [...data.historicalYears]
        for (const v of Object.values(data.vintages)) {
            allYears.push(...v.years)
        }
        const timelineStart = Math.min(...allYears)
        const timelineEnd = Math.max(...allYears, 2030)

        // Collect all values for Y range
        const allValues: number[] = []
        for (const v of data.historicalValues) {
            if (v != null && Number.isFinite(v)) allValues.push(v)
        }
        for (const [oy, v] of Object.entries(data.vintages)) {
            if (hiddenVintages.has(Number(oy))) continue
            allValues.push(...v.p10.filter(x => Number.isFinite(x) && x > 0))
            allValues.push(...v.p90.filter(x => Number.isFinite(x) && x > 0))
        }

        if (allValues.length === 0) return null

        const yTicks = getNiceYTicks(Math.min(...allValues), Math.max(...allValues), 5)
        const minY = yTicks[0]
        const maxY = yTicks[yTicks.length - 1]
        const yRange = maxY - minY || 1

        const xScale = (year: number) =>
            padding.left + ((year - timelineStart) / (timelineEnd - timelineStart)) * chartWidth
        const yScale = (v: number) =>
            padding.top + chartHeight - ((v - minY) / yRange) * chartHeight

        // Build historical actuals path
        let histPath = ""
        {
            let first = true
            for (let i = 0; i < data.historicalYears.length; i++) {
                const v = data.historicalValues[i]
                if (v == null || !Number.isFinite(v)) continue
                const cmd = first ? "M" : "L"
                first = false
                histPath += `${cmd} ${xScale(data.historicalYears[i])} ${yScale(v)} `
            }
        }

        // Build vintage fans + p50 lines
        const sortedOrigins = Object.keys(data.vintages)
            .map(Number)
            .sort((a, b) => a - b)

        const vintagePaths = sortedOrigins
            .filter(oy => !hiddenVintages.has(oy))
            .map(oy => {
                const v = data.vintages[oy]
                const color = getVintageColor(oy)

                // Fan polygon: p90 forward, p10 backward
                let fanPath = ""
                const p90Fwd = v.years.map((yr, i) => {
                    if (!Number.isFinite(v.p90[i])) return null
                    return `${i === 0 ? "M" : "L"} ${xScale(yr)} ${yScale(v.p90[i])}`
                }).filter(Boolean).join(" ")

                const p10Rev = [...v.years].reverse().map((yr, i) => {
                    const idx = v.years.length - 1 - i
                    if (!Number.isFinite(v.p10[idx])) return null
                    return `L ${xScale(yr)} ${yScale(v.p10[idx])}`
                }).filter(Boolean).join(" ")

                if (p90Fwd && p10Rev) fanPath = `${p90Fwd} ${p10Rev} Z`

                // P50 dashed line
                let p50Path = ""
                let first = true
                for (let i = 0; i < v.years.length; i++) {
                    if (!Number.isFinite(v.p50[i])) continue
                    p50Path += `${first ? "M" : "L"} ${xScale(v.years[i])} ${yScale(v.p50[i])} `
                    first = false
                }

                return { oy, color, fanPath, p50Path }
            })

        // X-axis labels
        const labelYears: number[] = []
        for (let yr = Math.ceil(timelineStart / 2) * 2; yr <= timelineEnd; yr += 2) {
            labelYears.push(yr)
        }

        // Hover handler
        const handleMouseMove = (e: React.MouseEvent<SVGSVGElement>) => {
            const rect = e.currentTarget.getBoundingClientRect()
            const x = e.clientX - rect.left
            // SVG is scaled via viewBox; compute ratio based on actual rect width
            const svgX = (x / rect.width) * width
            const ratio = (svgX - padding.left) / chartWidth
            const yearRaw = timelineStart + ratio * (timelineEnd - timelineStart)
            const year = Math.round(yearRaw)
            setHoveredYear(Math.max(timelineStart, Math.min(timelineEnd, year)))
        }

        // Tooltip data for hovered year
        let tooltipContent: { year: number; actual: number | null; predictions: { oy: number; p50: number; color: string }[] } | null = null
        if (hoveredYear != null) {
            const histIdx = data.historicalYears.indexOf(hoveredYear)
            const actual = histIdx >= 0 ? data.historicalValues[histIdx] : null
            const predictions: { oy: number; p50: number; color: string }[] = []
            for (const oy of sortedOrigins) {
                if (hiddenVintages.has(oy)) continue
                const v = data.vintages[oy]
                const idx = v.years.findIndex(yr => Math.round(yr) === hoveredYear)
                if (idx >= 0 && Number.isFinite(v.p50[idx])) {
                    predictions.push({ oy, p50: v.p50[idx], color: getVintageColor(oy) })
                }
            }
            if (actual != null || predictions.length > 0) {
                tooltipContent = { year: hoveredYear, actual, predictions }
            }
        }

        return (
            <div className="relative">
                <svg
                    viewBox={`0 0 ${width} ${height}`}
                    className="w-full cursor-crosshair"
                    preserveAspectRatio="xMidYMid meet"
                    style={{ display: "block" }}
                    onMouseMove={handleMouseMove}
                    onMouseLeave={() => setHoveredYear(null)}
                >
                    {/* Grid lines */}
                    {yTicks.map(tick => (
                        <line
                            key={`grid-${tick}`}
                            x1={padding.left} y1={yScale(tick)}
                            x2={width - padding.right} y2={yScale(tick)}
                            stroke="currentColor" strokeOpacity={0.08}
                        />
                    ))}

                    {/* Y-axis line */}
                    <line
                        x1={padding.left} y1={padding.top}
                        x2={padding.left} y2={height - padding.bottom}
                        stroke="currentColor" strokeOpacity={0.15}
                    />

                    {/* X-axis line */}
                    <line
                        x1={padding.left} y1={height - padding.bottom}
                        x2={width - padding.right} y2={height - padding.bottom}
                        stroke="currentColor" strokeOpacity={0.15}
                    />

                    {/* "Now" divider at 2026 */}
                    <line
                        x1={xScale(2026)} y1={padding.top}
                        x2={xScale(2026)} y2={height - padding.bottom}
                        stroke="oklch(0.7 0.1 250)" strokeWidth={1}
                        strokeDasharray="4 2" strokeOpacity={0.5}
                    />
                    <text
                        x={xScale(2026)} y={padding.top - 4}
                        textAnchor="middle"
                        className="text-[9px] fill-muted-foreground"
                    >
                        Now
                    </text>

                    {/* Historical shading */}
                    <rect
                        x={padding.left} y={padding.top}
                        width={xScale(2026) - padding.left}
                        height={chartHeight}
                        fill="oklch(0.5 0.05 250)" fillOpacity={0.04}
                    />

                    {/* Vintage fans (back to front — oldest first, most transparent) */}
                    {vintagePaths.map(({ oy, color, fanPath }) => (
                        fanPath && (
                            <path
                                key={`fan-${oy}`}
                                d={fanPath}
                                fill={color}
                                fillOpacity={0.12}
                            />
                        )
                    ))}

                    {/* Vintage P50 lines (dashed) */}
                    {vintagePaths.map(({ oy, color, p50Path }) => (
                        p50Path && (
                            <path
                                key={`p50-${oy}`}
                                d={p50Path}
                                fill="none"
                                stroke={color}
                                strokeWidth={1.5}
                                strokeDasharray="4 3"
                                strokeOpacity={0.7}
                            />
                        )
                    ))}

                    {/* Historical actuals line (solid, prominent) */}
                    {histPath && (
                        <path
                            d={histPath}
                            fill="none"
                            stroke="currentColor"
                            strokeWidth={2.5}
                            strokeOpacity={0.9}
                        />
                    )}

                    {/* Historical dots */}
                    {data.historicalYears.map((yr, i) => {
                        const v = data.historicalValues[i]
                        if (v == null || !Number.isFinite(v)) return null
                        return (
                            <circle
                                key={`dot-${yr}`}
                                cx={xScale(yr)} cy={yScale(v)}
                                r={2.5}
                                fill="currentColor"
                                fillOpacity={0.8}
                            />
                        )
                    })}

                    {/* Hover line */}
                    {hoveredYear != null && (
                        <line
                            x1={xScale(hoveredYear)} y1={padding.top}
                            x2={xScale(hoveredYear)} y2={height - padding.bottom}
                            stroke="currentColor" strokeWidth={1}
                            strokeDasharray="3 2" strokeOpacity={0.3}
                        />
                    )}

                    {/* X-axis labels */}
                    {labelYears.map(yr => (
                        <text
                            key={yr}
                            x={xScale(yr)} y={height - padding.bottom + 16}
                            textAnchor="middle"
                            className="text-[9px] fill-muted-foreground font-mono"
                            style={{ pointerEvents: "none" }}
                        >
                            {"'" + yr.toString().slice(2)}
                        </text>
                    ))}

                    {/* Y-axis labels */}
                    {yTicks.map(tick => (
                        <text
                            key={tick}
                            x={padding.left - 5} y={yScale(tick) + 3}
                            textAnchor="end"
                            className="text-[10px] fill-muted-foreground font-mono"
                            style={{ pointerEvents: "none" }}
                        >
                            {formatYAxisValue(tick)}
                        </text>
                    ))}
                </svg>

                {/* Tooltip */}
                {tooltipContent && (
                    <div className="absolute top-2 right-2 bg-background/90 backdrop-blur-sm border border-border/50 rounded-lg px-3 py-2 text-xs shadow-md pointer-events-none min-w-[140px]">
                        <div className="font-semibold mb-1">{tooltipContent.year}</div>
                        {tooltipContent.actual != null && (
                            <div className="flex items-center gap-2">
                                <span className="w-2 h-2 rounded-full bg-foreground inline-block" />
                                <span className="text-muted-foreground">Actual:</span>
                                <span className="font-medium">{formatYAxisValue(tooltipContent.actual)}</span>
                            </div>
                        )}
                        {tooltipContent.predictions.map(p => (
                            <div key={p.oy} className="flex items-center gap-2">
                                <span
                                    className="w-2 h-2 rounded-full inline-block"
                                    style={{ backgroundColor: p.color }}
                                />
                                <span className="text-muted-foreground">o={p.oy}:</span>
                                <span className="font-medium">{formatYAxisValue(p.p50)}</span>
                            </div>
                        ))}
                    </div>
                )}

                {/* Legend — clickable to toggle vintages */}
                <div className="flex flex-wrap gap-x-3 gap-y-1 mt-3 justify-center">
                    <button
                        className="flex items-center gap-1.5 text-[11px] text-foreground/80 hover:text-foreground transition-colors"
                        style={{ cursor: "default" }}
                    >
                        <span className="w-2.5 h-0.5 bg-foreground rounded inline-block" />
                        Actual
                    </button>
                    {sortedOrigins.map(oy => (
                        <button
                            key={oy}
                            onClick={() => toggleVintage(oy)}
                            className={`flex items-center gap-1.5 text-[11px] transition-colors ${
                                hiddenVintages.has(oy)
                                    ? "text-muted-foreground/40 line-through"
                                    : "text-foreground/80 hover:text-foreground"
                            }`}
                        >
                            <span
                                className="w-2.5 h-2.5 rounded-sm inline-block"
                                style={{
                                    backgroundColor: getVintageColor(oy),
                                    opacity: hiddenVintages.has(oy) ? 0.3 : 1,
                                }}
                            />
                            o={oy}
                        </button>
                    ))}
                </div>
            </div>
        )
    }, [data, height, hoveredYear, hiddenVintages])

    if (loading) {
        return (
            <div className="flex items-center justify-center py-12 text-sm text-muted-foreground">
                <div className="animate-pulse">Loading backtest data…</div>
            </div>
        )
    }

    if (error) {
        return (
            <div className="text-sm text-destructive py-8 text-center">
                Failed to load backtest data: {error}
            </div>
        )
    }

    if (!data || Object.keys(data.vintages).length === 0) {
        return (
            <div className="text-sm text-muted-foreground py-8 text-center">
                No backtest data available for this geography.
            </div>
        )
    }

    return <div className="w-full">{svgContent}</div>
}
