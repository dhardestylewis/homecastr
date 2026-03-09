"use client"

import Link from "next/link"
import type { ComparableTract } from "@/lib/publishing/forecast-data"

interface Props {
    similar: ComparableTract[]
    higherUpside: ComparableTract[]
    lowerRisk: ComparableTract[]
    currentTractGeoid: string
    stateSlug: string
    citySlug: string
    aiComparableNarrative?: string | null
}

export function ComparablesTable({
    similar,
    higherUpside,
    lowerRisk,
    currentTractGeoid,
    stateSlug,
    citySlug,
    aiComparableNarrative,
}: Props) {
    const fmtVal = (v: number) => {
        if (v >= 1_000_000) return `$${(v / 1_000_000).toFixed(2)}M`
        if (v >= 1_000) return `$${(v / 1_000).toFixed(0)}K`
        return `$${v.toFixed(0)}`
    }

    const fmtPct = (v: number) => `${v >= 0 ? "+" : ""}${v.toFixed(1)}%`

    const buildUrl = (tractGeoid: string) => {
        const tractSuffix = tractGeoid.substring(5)
        return `/forecasts/${stateSlug}/${citySlug}/tract-${tractSuffix}/home-price-forecast`
    }

    const TableSection = ({
        title,
        subtitle,
        tracts,
        highlightCol,
    }: {
        title: string
        subtitle: string
        tracts: ComparableTract[]
        highlightCol: "appreciation" | "spread"
    }) => {
        if (tracts.length === 0) return null
        return (
            <div className="space-y-3">
                <div>
                    <h3 className="text-sm font-medium text-foreground/80">{title}</h3>
                    <p className="text-xs text-muted-foreground">{subtitle}</p>
                </div>
                <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                        <thead>
                            <tr className="text-xs text-muted-foreground uppercase tracking-wider border-b border-border">
                                <th className="text-left py-2 pr-4">Area</th>
                                <th className="text-right py-2 px-3">Current Value</th>
                                <th className="text-right py-2 px-3">5yr Expected</th>
                                <th className="text-right py-2 pl-3">
                                    {highlightCol === "appreciation" ? "5yr Change" : "Spread"}
                                </th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-border">
                            {tracts.map((t) => (
                                <tr key={t.tractGeoid} className="hover:bg-accent/50 transition-colors">
                                    <td className="py-2.5 pr-4">
                                        <Link
                                            href={buildUrl(t.tractGeoid)}
                                            className="text-primary hover:underline transition-colors font-medium"
                                        >
                                            {t.name}
                                        </Link>
                                    </td>
                                    <td className="text-right py-2.5 px-3 font-mono text-muted-foreground">
                                        {fmtVal(t.p50_12m)}
                                    </td>
                                    <td className="text-right py-2.5 px-3 font-mono text-foreground/80">
                                        {fmtVal(t.p50_60m)}
                                    </td>
                                    <td className={`text-right py-2.5 pl-3 font-mono font-medium ${highlightCol === "appreciation"
                                        ? (t.appreciation_60m > 0 ? "text-chart-high" : "text-chart-negative")
                                        : "text-chart-mid"
                                        }`}>
                                        {highlightCol === "appreciation"
                                            ? fmtPct(t.appreciation_60m)
                                            : fmtVal(t.spread_60m)}
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            </div>
        )
    }

    const isEmpty = similar.length === 0 && higherUpside.length === 0 && lowerRisk.length === 0

    return (
        <section id="comparables" className="space-y-6">
            <h2 className="text-xl font-semibold text-foreground">Comparable Alternatives</h2>

            {isEmpty ? (
                <div className="glass-panel rounded-xl p-5">
                    <p className="text-sm text-muted-foreground italic">
                        {aiComparableNarrative || "No comparable areas found for this neighborhood. This usually happens when only a small number of tracts in the county have forecast coverage."}
                    </p>
                </div>
            ) : (
                <div className="glass-panel rounded-xl p-5 space-y-8">
                    <TableSection
                        title="Most Similar Neighborhoods"
                        subtitle="Areas with the most similar current median values"
                        tracts={similar}
                        highlightCol="appreciation"
                    />
                    <TableSection
                        title="Stronger Appreciation Potential"
                        subtitle="Nearby areas where the model expects higher 5-year price growth"
                        tracts={higherUpside}
                        highlightCol="appreciation"
                    />
                    <TableSection
                        title="Lower Risk Alternatives"
                        subtitle="Nearby areas with narrower forecast ranges (less uncertainty)"
                        tracts={lowerRisk}
                        highlightCol="spread"
                    />
                </div>
            )}
        </section>
    )
}
