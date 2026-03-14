"use client"

import type { ForecastHorizon } from "@/lib/publishing/forecast-data"

interface Props {
    horizons: ForecastHorizon[]
}

export function UncertaintyBand({ horizons }: Props) {
    const h5 = horizons.find(h => h.horizon_m === 60) || horizons[horizons.length - 1]
    const h1 = horizons.find(h => h.horizon_m === 12) || horizons[0]

    if (!h5 || !h1) return null

    const spreadPct = h5.p50 > 0 ? ((h5.spread / h5.p50) * 100) : 0
    const spreadPct1 = h1.p50 > 0 ? ((h1.spread / h1.p50) * 100) : 0

    let barWidth: string
    let barColor: string
    let uncertaintyText: string

    if (spreadPct < 60) {
        barWidth = "w-[20%]"
        barColor = "bg-chart-high"
        uncertaintyText = "Low uncertainty. The model is relatively confident in this forecast range."
    } else if (spreadPct < 120) {
        barWidth = "w-[45%]"
        barColor = "bg-chart-mid"
        uncertaintyText = "Moderate uncertainty, typical for this area and time horizon."
    } else if (spreadPct < 150) {
        barWidth = "w-[70%]"
        barColor = "bg-warning"
        uncertaintyText = "Higher uncertainty than usual. The range of possible outcomes is wide — likely due to limited nearby sales data, recent market volatility, or ongoing neighborhood change."
    } else {
        barWidth = "w-[90%]"
        barColor = "bg-chart-negative"
        uncertaintyText = "Very high uncertainty. The gap between the optimistic and pessimistic scenarios is unusually large. Treat any single number here with caution."
    }

    const fmtVal = (v: number) => {
        if (v >= 1_000_000) return `$${(v / 1_000_000).toFixed(2)}M`
        if (v >= 1_000) return `$${(v / 1_000).toFixed(0)}K`
        return `$${v.toFixed(0)}`
    }

    return (
        <section id="uncertainty" className="space-y-4">
            <h2 className="text-xl font-semibold text-foreground">Forecast Uncertainty</h2>

            <div className="glass-panel rounded-xl p-5 space-y-4">
                {/* Confidence bar */}
                <div className="space-y-2">
                    <div className="flex justify-between text-xs text-muted-foreground">
                        <span>Narrow range</span>
                        <span>Wide range</span>
                    </div>
                    <div className="h-2 rounded-full bg-secondary overflow-hidden">
                        <div className={`h-full rounded-full transition-all ${barColor} ${barWidth}`} />
                    </div>
                </div>

                <p className="text-sm text-muted-foreground leading-relaxed">{uncertaintyText}</p>

                {/* Spread details */}
                <div className="grid grid-cols-2 gap-4 pt-1">
                    <div className="space-y-1">
                        <p className="text-xs text-muted-foreground uppercase tracking-wider">1-Year Spread</p>
                        <p className="text-sm font-mono text-foreground/70">
                            {fmtVal(h1.p10)} to {fmtVal(h1.p90)}
                        </p>
                        <p className="text-xs text-muted-foreground">{spreadPct1.toFixed(1)}% of median</p>
                    </div>
                    <div className="space-y-1">
                        <p className="text-xs text-muted-foreground uppercase tracking-wider">5-Year Spread</p>
                        <p className="text-sm font-mono text-foreground/70">
                            {fmtVal(h5.p10)} to {fmtVal(h5.p90)}
                        </p>
                        <p className="text-xs text-muted-foreground">{spreadPct.toFixed(1)}% of median</p>
                    </div>
                </div>
            </div>
        </section>
    )
}
