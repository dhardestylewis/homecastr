import type { ForecastHorizon } from "@/lib/publishing/forecast-data"

interface Props {
    horizons: ForecastHorizon[]
    neighborhoodName: string
    city: string
}

/**
 * Generates deterministic plain-English interpretation text
 * grounded in the page's numeric metrics.
 */
export function InterpretationSection({ horizons, neighborhoodName, city }: Props) {
    const h1 = horizons.find(h => h.horizon_m === 12)
    const h5 = horizons.find(h => h.horizon_m === 60)

    if (!h1 || !h5) return null

    const appreciation5 = h5.appreciation
    const spread5Pct = h5.p50 > 0 ? ((h5.spread / h5.p50) * 100) : 0

    let outlook: string
    if (appreciation5 > 10) outlook = "strongly bullish"
    else if (appreciation5 > 3) outlook = "moderately bullish"
    else if (appreciation5 > -3) outlook = "neutral"
    else if (appreciation5 > -10) outlook = "cautious"
    else outlook = "bearish"

    let dispersion: string
    if (spread5Pct < 15) dispersion = "tight"
    else if (spread5Pct < 30) dispersion = "moderate"
    else if (spread5Pct < 50) dispersion = "wide"
    else dispersion = "very wide"

    const sentences: string[] = []

    if (appreciation5 > 3) {
        sentences.push(`${neighborhoodName} in ${city} is forecast to appreciate ${appreciation5.toFixed(1)}% over five years, with a ${dispersion} confidence range.`)
    } else if (appreciation5 < -3) {
        sentences.push(`${neighborhoodName} in ${city} is forecast to decline ${Math.abs(appreciation5).toFixed(1)}% over five years, with a ${dispersion} confidence range.`)
    } else {
        sentences.push(`${neighborhoodName} in ${city} is forecast to remain roughly flat over five years (${appreciation5 > 0 ? "+" : ""}${appreciation5.toFixed(1)}%), with a ${dispersion} confidence range.`)
    }

    if (h1.appreciation > 0 && appreciation5 < 0) {
        sentences.push("Short-term momentum is positive, but the longer-term outlook is weaker — suggesting potential mean-reversion risk.")
    } else if (h1.appreciation < 0 && appreciation5 > 0) {
        sentences.push("Near-term pressure is forecast, but the longer-term trend is constructive — suggesting a potential recovery trajectory.")
    }

    if (spread5Pct > 40) {
        sentences.push("The forecast range is exceptionally wide, indicating high outcome uncertainty. This area may be subject to structural changes or sparse transaction data.")
    } else if (spread5Pct > 25) {
        sentences.push("Forecast dispersion is elevated. The range of plausible outcomes is wider than average, warranting consideration of both upside and downside scenarios.")
    }

    const downside1 = h1.p50 > 0 ? ((h1.p10 / h1.p50) - 1) * 100 : 0
    if (downside1 < -10) {
        sentences.push(`In the downside scenario (p10), the model sees a potential ${Math.abs(downside1).toFixed(0)}% decline within one year.`)
    }

    const fmtPct = (v: number) => `${v >= 0 ? "+" : ""}${v.toFixed(1)}%`

    return (
        <section id="interpretation" className="space-y-4">
            <h2 className="text-xl font-semibold text-foreground">Interpretation</h2>

            <div className="glass-panel rounded-xl p-5 space-y-4">
                {/* Outlook badge */}
                <div className="flex items-center gap-3">
                    <span className={`inline-flex items-center gap-1.5 rounded-full px-3 py-1 text-xs font-medium border ${outlook.includes("bullish")
                        ? "bg-chart-high/15 text-chart-high border-chart-high/25"
                        : outlook === "neutral"
                            ? "bg-chart-mid/15 text-chart-mid border-chart-mid/25"
                            : "bg-chart-negative/15 text-chart-negative border-chart-negative/25"
                        }`}>
                        Homecastr outlook: {outlook}
                    </span>
                    <span className="text-xs text-muted-foreground">
                        5yr expected: {fmtPct(appreciation5)} · Spread: {spread5Pct.toFixed(0)}%
                    </span>
                </div>

                {/* Narrative — compact bullet list */}
                <ul className="space-y-1.5 text-sm text-muted-foreground leading-relaxed">
                    {sentences.map((s, i) => (
                        <li key={i} className="flex gap-2">
                            <span className="text-muted-foreground/40 select-none">·</span>
                            <span>{s}</span>
                        </li>
                    ))}
                </ul>
            </div>
        </section>
    )
}
