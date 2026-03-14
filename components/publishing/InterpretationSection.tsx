import type { ForecastHorizon } from "@/lib/publishing/forecast-data"
import type { SeoNarrative } from "@/lib/publishing/seo-narratives"

interface Props {
    horizons: ForecastHorizon[]
    neighborhoodName: string
    city: string
    stateAbbr: string
    aiNarrative?: SeoNarrative | null
}

/**
 * Generates deterministic plain-English interpretation text
 * grounded in the page's numeric metrics.
 * When an AI narrative is available, renders it as a richer lead paragraph.
 */
export function InterpretationSection({ horizons, neighborhoodName, city, stateAbbr, aiNarrative }: Props) {
    const h1 = horizons.find(h => h.horizon_m === 12)
    const h5 = horizons.find(h => h.horizon_m === 60)

    if (!h1 || !h5) return null

    const appreciation5 = h5.appreciation
    const spread5Pct = h5.p50 > 0 ? ((h5.spread / h5.p50) * 100) : 0

    let outlook: string
    if (appreciation5 > 10) outlook = "strong growth expected"
    else if (appreciation5 > 3) outlook = "moderate growth expected"
    else if (appreciation5 > -3) outlook = "roughly flat"
    else if (appreciation5 > -10) outlook = "mild decline expected"
    else outlook = "significant decline expected"

    let dispersion: string
    if (spread5Pct < 60) dispersion = "tight"
    else if (spread5Pct < 120) dispersion = "moderate"
    else if (spread5Pct < 150) dispersion = "wide"
    else dispersion = "very wide"

    let locationText = `${neighborhoodName} in ${city}`
    if (neighborhoodName.toLowerCase() === city.toLowerCase() || city.toLowerCase() === 'city') {
        locationText = `${neighborhoodName}`
    } else if (stateAbbr) {
        locationText = `${neighborhoodName} in ${city}, ${stateAbbr}`
    }

    // Build interpretation focused on mechanism + timing (not redundant with Key Takeaway)
    const sentences: string[] = []
    
    // Timing pattern
    if (h1.appreciation > 2 && appreciation5 > h1.appreciation * 3) {
        sentences.push("The forecast implies steady near-term growth accelerating over the longer horizon.")
    } else if (h1.appreciation < -2 && appreciation5 > 0) {
        sentences.push("The forecast implies mild near-term softness followed by longer-term recovery.")
    } else if (h1.appreciation > 0 && appreciation5 < 0) {
        sentences.push("Near-term values look stable, but the model expects gradual softening over the longer horizon.")
    } else if (Math.abs(h1.appreciation) < 2 && Math.abs(appreciation5) < 5) {
        sentences.push("The forecast implies relatively stable values through the full horizon, with modest movements in either direction.")
    }
    
    // Mechanism / drivers
    if (appreciation5 > 10) {
        sentences.push("The base case is supported by strong local demand signals and a positive medium-term appreciation path.")
    } else if (appreciation5 > 3) {
        sentences.push("The base case is supported by stable local demand and a moderate medium-term appreciation path.")
    } else if (appreciation5 < -10) {
        sentences.push("The downside reflects broader macro sensitivity and local demand softening.")
    } else if (appreciation5 < -3) {
        sentences.push("The model sees some downside pressure from rate sensitivity and local supply dynamics.")
    }
    
    // Uncertainty explanation
    if (spread5Pct > 150) {
        sentences.push("The wide range reflects limited transaction history or rapid neighborhood change — outcomes could vary significantly.")
    } else if (spread5Pct > 120) {
        sentences.push("The wider-than-average range suggests more uncertainty about the trajectory — consider both upside and downside scenarios.")
    } else if (spread5Pct < 60) {
        sentences.push("The narrow range suggests relatively high model confidence for this market.")
    }

    const fmtPct = (v: number) => `${v >= 0 ? "+" : ""}${v.toFixed(1)}%`

    return (
        <section id="interpretation" className="space-y-4">
            <h2 className="text-xl font-semibold text-foreground">Interpretation</h2>

            <div className="glass-panel rounded-xl p-5 space-y-4">
                {/* Outlook badge */}
                <div className="flex items-center gap-3">
                    <span className={`inline-flex items-center gap-1.5 rounded-full px-3 py-1 text-xs font-medium border ${outlook.includes("growth")
                        ? "bg-chart-high/15 text-chart-high border-chart-high/25"
                        : outlook === "roughly flat"
                            ? "bg-chart-mid/15 text-chart-mid border-chart-mid/25"
                            : "bg-chart-negative/15 text-chart-negative border-chart-negative/25"
                        }`}>
                        Homecastr outlook: {outlook}
                    </span>
                    <span className="text-xs text-muted-foreground">
                        5yr expected: {fmtPct(appreciation5)} · Spread: {spread5Pct.toFixed(0)}%
                    </span>
                </div>

                {/* AI-generated narrative (when available) */}
                {aiNarrative && (
                    <div className="space-y-3 text-sm text-muted-foreground leading-relaxed border-b border-border pb-4 mb-2">
                        {aiNarrative.trend_analysis && (
                            <p>{aiNarrative.trend_analysis}</p>
                        )}
                        {aiNarrative.uncertainty_interpretation && (
                            <p>{aiNarrative.uncertainty_interpretation}</p>
                        )}
                    </div>
                )}

                {/* Deterministic narrative — compact bullet list */}
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
