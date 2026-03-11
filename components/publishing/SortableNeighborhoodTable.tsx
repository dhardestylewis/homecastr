"use client"

import { useState, useMemo } from "react"
import Link from "next/link"
import { ChevronUp, ChevronDown, ChevronsUpDown, Search } from "lucide-react"

interface TractRow {
    tractGeoid: string
    neighborhoodName: string
    neighborhoodSlug: string
    p50_current: number
    appreciation_5yr: number
    vsMetro: number
}

type SortKey = "name" | "value" | "outlook" | "vsMetro"
type SortDir = "asc" | "desc"

interface Props {
    rows: TractRow[]
    state: string
    city: string
    avgAppreciation: number
}

const fmtVal = (v: number) => {
    if (v >= 1_000_000) return `$${(v / 1_000_000).toFixed(2)}M`
    if (v >= 1_000) return `$${(v / 1_000).toFixed(0)}K`
    return `$${v.toFixed(0)}`
}

const fmtPct = (v: number) => `${v >= 0 ? "+" : ""}${v.toFixed(1)}%`

export function SortableNeighborhoodTable({ rows, state, city, avgAppreciation }: Props) {
    const [sortKey, setSortKey] = useState<SortKey>("outlook")
    const [sortDir, setSortDir] = useState<SortDir>("desc")
    const [searchQuery, setSearchQuery] = useState("")

    const sorted = useMemo(() => {
        const filtered = rows.filter(r => 
            r.neighborhoodName.toLowerCase().includes(searchQuery.toLowerCase())
        )

        const comparators: Record<SortKey, (a: TractRow, b: TractRow) => number> = {
            name: (a, b) => a.neighborhoodName.localeCompare(b.neighborhoodName),
            value: (a, b) => a.p50_current - b.p50_current,
            outlook: (a, b) => a.appreciation_5yr - b.appreciation_5yr,
            vsMetro: (a, b) => a.vsMetro - b.vsMetro,
        }
        const cmp = comparators[sortKey]
        const dir = sortDir === "asc" ? 1 : -1
        return filtered.sort((a, b) => cmp(a, b) * dir)
    }, [rows, sortKey, sortDir, searchQuery])

    const toggleSort = (key: SortKey) => {
        if (sortKey === key) {
            setSortDir(d => d === "asc" ? "desc" : "asc")
        } else {
            setSortKey(key)
            setSortDir(key === "name" ? "asc" : "desc")
        }
    }

    const SortIcon = ({ col }: { col: SortKey }) => {
        if (sortKey !== col) return <ChevronsUpDown className="w-3 h-3 opacity-30" />
        return sortDir === "asc"
            ? <ChevronUp className="w-3 h-3" />
            : <ChevronDown className="w-3 h-3" />
    }

    return (
        <div className="space-y-4">
            <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
                <h2 className="text-xl font-semibold text-foreground">Browse Markets</h2>
                <div className="relative w-full sm:w-72">
                    <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                    <input 
                        type="text" 
                        placeholder="Search neighborhoods..." 
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                        className="w-full bg-background border border-border rounded-lg pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary/50 text-foreground placeholder:text-muted-foreground"
                    />
                </div>
            </div>
            <div className="glass-panel rounded-xl overflow-hidden">
                <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                    <thead>
                        <tr className="text-xs text-muted-foreground uppercase tracking-wider border-b border-border bg-secondary/30">
                            <th className="text-left py-3 px-4 w-10">#</th>
                            <th
                                className="text-left py-3 px-3 cursor-pointer hover:text-foreground transition-colors select-none"
                                onClick={() => toggleSort("name")}
                            >
                                <span className="inline-flex items-center gap-1">
                                    Neighborhood <SortIcon col="name" />
                                </span>
                            </th>
                            <th
                                className="text-right py-3 px-3 cursor-pointer hover:text-foreground transition-colors select-none"
                                onClick={() => toggleSort("value")}
                            >
                                <span className="inline-flex items-center gap-1 justify-end">
                                    Current Value <SortIcon col="value" />
                                </span>
                            </th>
                            <th
                                className="text-right py-3 px-3 cursor-pointer hover:text-foreground transition-colors select-none"
                                onClick={() => toggleSort("outlook")}
                            >
                                <span className="inline-flex items-center gap-1 justify-end">
                                    5yr Outlook <SortIcon col="outlook" />
                                </span>
                            </th>
                            <th
                                className="text-right py-3 px-3 cursor-pointer hover:text-foreground transition-colors select-none"
                                onClick={() => toggleSort("vsMetro")}
                            >
                                <span className="inline-flex items-center gap-1 justify-end">
                                    vs. Metro Avg <SortIcon col="vsMetro" />
                                </span>
                            </th>
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-border">
                        {sorted.map((t, i) => {
                            const vsMetroLabel = t.vsMetro >= 0
                                ? `+${t.vsMetro.toFixed(0)} above avg`
                                : `${t.vsMetro.toFixed(0)} below avg`

                            return (
                                <tr key={t.tractGeoid} className="hover:bg-accent/30 transition-colors">
                                    <td className="py-3 px-4 text-muted-foreground/70 font-mono text-xs">{i + 1}</td>
                                    <td className="py-3 px-3">
                                        <Link
                                            href={`/forecasts/${state}/${city}/${t.neighborhoodSlug}/home-price-forecast`}
                                            className="text-primary hover:underline transition-colors font-medium"
                                        >
                                            {t.neighborhoodName}
                                        </Link>
                                    </td>
                                    <td className="text-right py-3 px-3 font-mono text-muted-foreground">
                                        {fmtVal(t.p50_current)}
                                    </td>
                                    <td className={`text-right py-3 px-3 font-mono font-medium ${t.appreciation_5yr > 0 ? "text-chart-high" : "text-chart-negative"}`}>
                                        {fmtPct(t.appreciation_5yr)}
                                    </td>
                                    <td className={`text-right py-3 px-3 text-xs whitespace-nowrap ${t.vsMetro > 0 ? "text-chart-high" : "text-chart-negative"}`}>
                                        {vsMetroLabel}
                                    </td>
                                </tr>
                            )
                        })}
                    </tbody>
                </table>
                </div>
                {sorted.length === 0 && (
                    <div className="p-8 text-center text-muted-foreground text-sm">
                        No markets found matching "{searchQuery}"
                    </div>
                )}
            </div>
        </div>
    )
}
