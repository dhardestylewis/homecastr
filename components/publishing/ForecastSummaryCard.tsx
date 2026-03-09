"use client"

import type { ForecastHorizon } from "@/lib/publishing/forecast-data"

interface Props {
    horizons: ForecastHorizon[]
    baselineP50: number
    metroRank?: number
    metroTotal?: number
    nationalPercentile?: number
    neighborhoodName: string
}

export function ForecastSummaryCard({
    horizons,
    baselineP50,
    neighborhoodName,
}: Props) {
    const h1 = horizons.find(h => h.horizon_m === 12)
    const h3 = horizons.find(h => h.horizon_m === 36)
    const h5 = horizons.find(h => h.horizon_m === 60)

    const keyHorizons = [
        { label: "1-Year", data: h1 },
        { label: "3-Year", data: h3 },
        { label: "5-Year", data: h5 },
    ].filter(h => h.data)

    const fmtVal = (v: number) => {
        if (v >= 1_000_000) return `$${(v / 1_000_000).toFixed(2)}M`
        if (v >= 1_000) return `$${(v / 1_000).toFixed(0)}K`
        return `$${v.toFixed(0)}`
    }

    const fmtPct = (v: number) => `${v >= 0 ? "+" : ""}${v.toFixed(1)}%`

    return (
        <section id="forecast-summary" className="space-y-5">
            <h2 className="text-xl font-semibold text-foreground">Forecast Summary</h2>

            {/* Current + Forecast horizon cards in a single row */}
            <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
                {/* Current value card */}
                <div className="glass-panel rounded-xl p-5 space-y-3 border-l-2 border-l-primary/40">
                    <p className="text-xs uppercase tracking-wider text-muted-foreground">Current Value</p>
                    <div className="space-y-1">
                        <p className="text-2xl font-bold tracking-tight text-foreground">
                            {fmtVal(baselineP50)}
                        </p>
                        <p className="text-sm font-medium text-muted-foreground">
                            Estimated median
                        </p>
                    </div>
                </div>

                {/* Forecast horizon cards */}
                {keyHorizons.map(({ label, data }) => {
                    if (!data) return null
                    const bullish = data.appreciation > 2
                    const bearish = data.appreciation < -2
                    return (
                        <div
                            key={label}
                            className="glass-panel rounded-xl p-5 space-y-3 hover:border-primary/30 transition-colors"
                        >
                            <p className="text-xs uppercase tracking-wider text-muted-foreground">{label} Forecast</p>
                            <div className="space-y-1">
                                <p className="text-2xl font-bold tracking-tight text-foreground">
                                    {fmtVal(data.p50)}
                                </p>
                                <p className={`text-sm font-medium ${bullish ? "text-chart-high" : bearish ? "text-chart-negative" : "text-chart-mid"}`}>
                                    {fmtPct(data.appreciation)} expected
                                </p>
                            </div>
                            <div className="space-y-1.5 pt-1">
                                <div className="flex justify-between text-xs">
                                    <span className="text-muted-foreground">Downside</span>
                                    <span className="text-chart-negative font-mono">{fmtVal(data.p10)}</span>
                                </div>
                                <div className="flex justify-between text-xs">
                                    <span className="text-muted-foreground">Median</span>
                                    <span className="text-foreground/80 font-mono">{fmtVal(data.p50)}</span>
                                </div>
                                <div className="flex justify-between text-xs">
                                    <span className="text-muted-foreground">Upside</span>
                                    <span className="text-chart-high font-mono">{fmtVal(data.p90)}</span>
                                </div>
                            </div>
                        </div>
                    )
                })}
            </div>
        </section>
    )
}
