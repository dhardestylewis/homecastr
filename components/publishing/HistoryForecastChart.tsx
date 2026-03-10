"use client"

import { useMemo } from "react"
import {
    ResponsiveContainer,
    ComposedChart,
    Area,
    Line,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    ReferenceLine,
} from "recharts"
import type { ForecastHorizon, HistoryPoint } from "@/lib/publishing/forecast-data"

interface Props {
    history: HistoryPoint[]
    horizons: ForecastHorizon[]
    originYear: number
    suppressConfidence?: boolean
}

export function HistoryForecastChart({ history, horizons, originYear, suppressConfidence = false }: Props) {
    const bridgeYear = originYear + 1

    const chartData = useMemo(() => {
        const data: {
            year: number
            historical?: number
            forecast?: number
            p10?: number
            p25?: number
            p50?: number
            p75?: number
            p90?: number
        }[] = []

        for (const h of history) {
            data.push({ year: h.year, historical: h.value })
        }

        const h12 = horizons.find(h => h.horizon_m === 12)
        if (h12) {
            const bridgeExists = data.find(d => d.year === bridgeYear)
            if (bridgeExists) {
                bridgeExists.historical = h12.p50
            } else {
                data.push({ year: bridgeYear, historical: h12.p50 })
            }
        }

        for (const h of horizons) {
            const yr = h.forecastYear
            if (yr <= bridgeYear) continue
            const existing = data.find(d => d.year === yr)
            if (existing) {
                existing.forecast = h.p50
                existing.p10 = h.p10
                existing.p25 = h.p25
                existing.p50 = h.p50
                existing.p75 = h.p75
                existing.p90 = h.p90
            } else {
                data.push({
                    year: yr,
                    forecast: h.p50,
                    p10: h.p10,
                    p25: h.p25,
                    p50: h.p50,
                    p75: h.p75,
                    p90: h.p90,
                })
            }
        }

        const bridgeExists = data.find(d => d.year === bridgeYear)
        const bridgeHorizon = horizons.find(h => h.horizon_m === 12)
        if (bridgeExists && bridgeHorizon) {
            bridgeExists.forecast = bridgeHorizon.p50
        }

        data.sort((a, b) => a.year - b.year)

        return data.map(d => ({
            ...d,
            range_p10_p90: d.p10 != null && d.p90 != null ? [d.p10, d.p90] as [number, number] : undefined,
            range_p25_p75: d.p25 != null && d.p75 != null ? [d.p25, d.p75] as [number, number] : undefined,
        }))
    }, [history, horizons, bridgeYear])

    const fmtVal = (v: any): string => {
        if (typeof v !== 'number' || isNaN(v)) {
            if (Array.isArray(v) && v.length === 2) {
                return `${fmtVal(v[0])} to ${fmtVal(v[1])}`
            }
            return String(v ?? '')
        }
        if (v >= 1_000_000) return `$${(v / 1_000_000).toFixed(1)}M`
        if (v >= 1_000) return `$${(v / 1_000).toFixed(0)}K`
        return `$${v.toFixed(0)}`
    }

    if (chartData.length === 0) return null

    const primaryColor = "oklch(0.75 0.15 165)"
    const axisColor = "oklch(0.65 0.01 250)"
    const gridColor = "oklch(0.28 0.01 250)"
    const bgColor = "oklch(0.16 0.01 250)"

    const tooltipNames: Record<string, string> = {
        historical: "Historical",
        forecast: "Forecast (p50)",
        range_p10_p90: "P10 to P90 range",
        range_p25_p75: "P25 to P75 range",
    }

    // Build unified timeline columns
    const historyYears = history.map(h => h.year)
    const forecastHorizonsSorted = [...horizons]
        .filter(h => h.forecastYear > bridgeYear)
        .sort((a, b) => a.forecastYear - b.forecastYear)
    const forecastYears = forecastHorizonsSorted.map(h => h.forecastYear)
    // Include bridge year as transition point
    const allYears = [...historyYears, bridgeYear, ...forecastYears]
    // Deduplicate and sort
    const uniqueYears = [...new Set(allYears)].sort((a, b) => a - b)

    // Value lookup map
    const histMap = new Map(history.map(h => [h.year, h.value]))
    const fcstMap = new Map(forecastHorizonsSorted.map(h => [h.forecastYear, h]))
    const bridgeP50 = horizons.find(h => h.horizon_m === 12)?.p50 ?? 0

    return (
        <section id="historical-trend" className="space-y-4">
            <h2 className="text-xl font-semibold text-foreground">Historical Trend & Forecast</h2>

            <div className="glass-panel rounded-xl p-5">
                <div className="h-[340px]">
                    <ResponsiveContainer width="100%" height="100%">
                        <ComposedChart data={chartData} margin={{ top: 10, right: 20, bottom: 10, left: 10 }}>
                            <defs>
                                <linearGradient id="fanOuterGrad" x1="0" y1="0" x2="0" y2="1">
                                    <stop offset="0%" stopColor={primaryColor} stopOpacity={0.12} />
                                    <stop offset="100%" stopColor={primaryColor} stopOpacity={0.02} />
                                </linearGradient>
                                <linearGradient id="fanInnerGrad" x1="0" y1="0" x2="0" y2="1">
                                    <stop offset="0%" stopColor={primaryColor} stopOpacity={0.25} />
                                    <stop offset="100%" stopColor={primaryColor} stopOpacity={0.05} />
                                </linearGradient>
                            </defs>

                            <CartesianGrid strokeDasharray="3 3" stroke={gridColor} />
                            <XAxis dataKey="year" tick={{ fontSize: 11, fill: axisColor }} axisLine={{ stroke: gridColor }} tickLine={false} />
                            <YAxis tickFormatter={fmtVal} tick={{ fontSize: 11, fill: axisColor }} axisLine={false} tickLine={false} width={70} />
                            <Tooltip
                                formatter={(value: any, name: string) => [fmtVal(value), tooltipNames[name] || name]}
                                labelFormatter={(label) => `${label}`}
                                contentStyle={{ background: bgColor, border: `1px solid ${gridColor}`, borderRadius: "8px", fontSize: "12px", color: axisColor }}
                            />
                            <ReferenceLine x={bridgeYear} stroke={gridColor} strokeDasharray="4 4"
                                label={{ value: "Forecast →", position: "top", fill: axisColor, fontSize: 10 }} />

                            {!suppressConfidence && <Area dataKey="range_p10_p90" fill="url(#fanOuterGrad)" stroke="none" isAnimationActive={false} />}
                            {!suppressConfidence && <Area dataKey="range_p25_p75" fill="url(#fanInnerGrad)" stroke="none" isAnimationActive={false} />}
                            <Line dataKey="forecast" stroke={primaryColor} strokeWidth={2} strokeDasharray="6 3" dot={false} isAnimationActive={false} />
                            <Line dataKey="historical" stroke={primaryColor} strokeWidth={2} dot={{ r: 3, fill: primaryColor, strokeWidth: 0 }} isAnimationActive={false} />
                        </ComposedChart>
                    </ResponsiveContainer>
                </div>

                {/* Legend */}
                <div className="flex items-center justify-center gap-6 mt-3 text-xs text-muted-foreground">
                    <span className="flex items-center gap-1.5">
                        <span className="w-4 h-0.5 bg-primary rounded-full" />
                        Historical
                    </span>
                    <span className="flex items-center gap-1.5">
                        <span className="w-4 h-0.5 bg-primary rounded-full" style={{ borderTop: "2px dashed", height: 0, width: 16 }} />
                        Forecast (p50)
                    </span>
                    {!suppressConfidence && (
                        <span className="flex items-center gap-1.5">
                            <span className="w-4 h-3 bg-primary/15 rounded-sm" />
                            Confidence range
                        </span>
                    )}
                </div>
            </div>

            {/* Unified Timeline Table: Historical → Forecast */}
            {(history.length > 0 || horizons.length > 0) && (
                <div className="glass-panel rounded-xl p-5">
                    <h3 className="text-sm font-medium text-muted-foreground mb-3">Value Timeline</h3>
                    <div className="overflow-x-auto">
                        <table className="w-full text-sm">
                            <thead>
                                <tr className="text-xs uppercase tracking-wider border-b border-border">
                                    <th className="text-left py-2 pr-3 text-muted-foreground whitespace-nowrap sticky left-0 bg-inherit">Year</th>
                                    {uniqueYears.map(yr => (
                                        <th key={yr} className={`text-right py-2 px-1.5 whitespace-nowrap ${yr > bridgeYear ? 'text-primary/70' : yr === bridgeYear ? 'text-primary font-bold' : 'text-muted-foreground'}`}>
                                            {yr}
                                        </th>
                                    ))}
                                </tr>
                            </thead>
                            <tbody>
                                {/* Row 1: Median Value (continuous historical → forecast) */}
                                <tr>
                                    <td className="py-2 pr-3 text-muted-foreground whitespace-nowrap sticky left-0 bg-inherit">Median Value</td>
                                    {uniqueYears.map(yr => {
                                        let value: number | null = null
                                        const isForecast = yr > bridgeYear
                                        const isBridge = yr === bridgeYear

                                        if (isBridge) {
                                            value = bridgeP50
                                        } else if (isForecast) {
                                            value = fcstMap.get(yr)?.p50 ?? null
                                        } else {
                                            value = histMap.get(yr) ?? null
                                        }

                                        return (
                                            <td key={yr} className={`text-right py-2 px-1.5 font-mono whitespace-nowrap ${isForecast ? 'text-primary/80' : isBridge ? 'text-primary font-semibold' : 'text-foreground/70'}`}>
                                                {value != null ? fmtVal(value) : '—'}
                                            </td>
                                        )
                                    })}
                                </tr>

                                {/* Row 2: YoY Change */}
                                <tr className="border-t border-border/50">
                                    <td className="py-2 pr-3 text-muted-foreground whitespace-nowrap sticky left-0 bg-inherit">YoY Change</td>
                                    {uniqueYears.map((yr, i) => {
                                        if (i === 0) return <td key={yr} className="text-right py-2 px-1.5 text-xs text-muted-foreground/40">—</td>

                                        const prevYr = uniqueYears[i - 1]
                                        let currVal: number | null = null
                                        let prevVal: number | null = null

                                        // Get current value
                                        if (yr === bridgeYear) currVal = bridgeP50
                                        else if (yr > bridgeYear) currVal = fcstMap.get(yr)?.p50 ?? null
                                        else currVal = histMap.get(yr) ?? null

                                        // Get previous value
                                        if (prevYr === bridgeYear) prevVal = bridgeP50
                                        else if (prevYr > bridgeYear) prevVal = fcstMap.get(prevYr)?.p50 ?? null
                                        else prevVal = histMap.get(prevYr) ?? null

                                        if (currVal == null || prevVal == null || prevVal === 0) {
                                            return <td key={yr} className="text-right py-2 px-1.5 text-xs text-muted-foreground/40">—</td>
                                        }

                                        const yoy = ((currVal - prevVal) / prevVal) * 100
                                        return (
                                            <td key={yr} className={`text-right py-2 px-1.5 font-mono text-xs whitespace-nowrap ${yoy >= 0 ? 'text-chart-high' : 'text-chart-negative'}`}>
                                                {yoy >= 0 ? '+' : ''}{yoy.toFixed(1)}%
                                            </td>
                                        )
                                    })}
                                </tr>

                                {!suppressConfidence && (
                                    <>
                                        {/* Row 3: P10 (downside) — only for forecast years */}
                                        <tr className="border-t border-border/30">
                                            <td className="py-2 pr-3 text-muted-foreground/60 whitespace-nowrap sticky left-0 bg-inherit text-xs">Downside (P10)</td>
                                            {uniqueYears.map(yr => {
                                                if (yr <= bridgeYear) return <td key={yr} className="text-right py-2 px-1.5" />
                                                const h = fcstMap.get(yr)
                                                return (
                                                    <td key={yr} className="text-right py-2 px-1.5 font-mono text-xs text-muted-foreground/60 whitespace-nowrap">
                                                        {h?.p10 != null ? fmtVal(h.p10) : '—'}
                                                    </td>
                                                )
                                            })}
                                        </tr>

                                        {/* Row 4: P90 (upside) — only for forecast years */}
                                        <tr className="border-t border-border/30">
                                            <td className="py-2 pr-3 text-muted-foreground/60 whitespace-nowrap sticky left-0 bg-inherit text-xs">Upside (P90)</td>
                                            {uniqueYears.map(yr => {
                                                if (yr <= bridgeYear) return <td key={yr} className="text-right py-2 px-1.5" />
                                                const h = fcstMap.get(yr)
                                                return (
                                                    <td key={yr} className="text-right py-2 px-1.5 font-mono text-xs text-muted-foreground/60 whitespace-nowrap">
                                                        {h?.p90 != null ? fmtVal(h.p90) : '—'}
                                                    </td>
                                                )
                                            })}
                                        </tr>
                                    </>)}
                            </tbody>
                        </table>
                    </div>

                </div>
            )}
        </section>
    )
}
