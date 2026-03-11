"use client"

import { useState, useMemo } from "react"
import Link from "next/link"
import { ChevronUp, ChevronDown, ChevronsUpDown, Search } from "lucide-react"

export interface CountyRow {
    city: string
    citySlug: string
    tractCount: number
    medianValue: number | null
    medianAppreciation: number | null
    highestUpside: number | null
}

type SortKey = "name" | "neighborhoods" | "value" | "outlook" | "upside"
type SortDir = "asc" | "desc"

const fmtVal = (v: number) => {
    if (v >= 1_000_000) return `$${(v / 1_000_000).toFixed(2)}M`
    if (v >= 1_000) return `$${(v / 1_000).toFixed(0)}K`
    return `$${v.toFixed(0)}`
}

const fmtPct = (v: number) => `${v >= 0 ? "+" : ""}${v.toFixed(1)}%`

// Display cap: values beyond ±100% shown as >+100%
const DISPLAY_CAP = 100
const fmtPctCapped = (v: number) => {
    if (Math.abs(v) > DISPLAY_CAP) return `>${v >= 0 ? "+" : "-"}${DISPLAY_CAP}%`
    return fmtPct(v)
}

export function SortableCountyTable({ rows, state }: { rows: CountyRow[]; state: string }) {
    const [sortKey, setSortKey] = useState<SortKey>("outlook")
    const [sortDir, setSortDir] = useState<SortDir>("desc")
    const [searchQuery, setSearchQuery] = useState("")

    const sorted = useMemo(() => {
        const filtered = rows.filter(r => 
            r.city.toLowerCase().includes(searchQuery.toLowerCase())
        )

        const nullLast = (a: number | null, b: number | null) => {
            if (a === null && b === null) return 0
            if (a === null) return 1
            if (b === null) return -1
            return a - b
        }

        const comparators: Record<SortKey, (a: CountyRow, b: CountyRow) => number> = {
            name: (a, b) => a.city.localeCompare(b.city),
            neighborhoods: (a, b) => a.tractCount - b.tractCount,
            value: (a, b) => nullLast(a.medianValue, b.medianValue),
            outlook: (a, b) => nullLast(a.medianAppreciation, b.medianAppreciation),
            upside: (a, b) => nullLast(a.highestUpside, b.highestUpside),
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

    const thClass = "py-3 px-3 cursor-pointer hover:text-foreground transition-colors select-none"

    return (
        <div className="space-y-4">
            <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
                <h2 className="text-xl font-semibold text-foreground">Browse Markets</h2>
                <div className="relative w-full sm:w-72">
                    <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                    <input 
                        type="text" 
                        placeholder="Search counties or cities..." 
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
                            <th className={`text-left ${thClass}`} onClick={() => toggleSort("name")}>
                                <span className="inline-flex items-center gap-1">
                                    County / City <SortIcon col="name" />
                                </span>
                            </th>
                            <th className={`text-right ${thClass}`} onClick={() => toggleSort("neighborhoods")}>
                                <span className="inline-flex items-center gap-1 justify-end">
                                    Neighborhoods <SortIcon col="neighborhoods" />
                                </span>
                            </th>
                            <th className={`text-right ${thClass}`} onClick={() => toggleSort("value")}>
                                <span className="inline-flex items-center gap-1 justify-end">
                                    Median Value <SortIcon col="value" />
                                </span>
                            </th>
                            <th className={`text-right ${thClass}`} onClick={() => toggleSort("outlook")}>
                                <span className="inline-flex items-center gap-1 justify-end">
                                    5yr Outlook <SortIcon col="outlook" />
                                </span>
                            </th>
                            <th className={`text-right ${thClass}`} onClick={() => toggleSort("upside")}>
                                <span className="inline-flex items-center gap-1 justify-end">
                                    Top Upside <SortIcon col="upside" />
                                </span>
                            </th>
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-border">
                        {sorted.map((c, i) => (
                            <tr key={`${c.citySlug}-${i}`} className="hover:bg-accent/30 transition-colors">
                                <td className="py-3 px-4 text-muted-foreground/70 font-mono text-xs">{i + 1}</td>
                                <td className="py-3 px-3">
                                    <Link
                                        href={`/forecasts/${state}/${c.citySlug}`}
                                        className="text-primary hover:underline transition-colors font-medium"
                                    >
                                        {c.city}
                                    </Link>
                                </td>
                                <td className="text-right py-3 px-3 font-mono text-muted-foreground tabular-nums">
                                    {c.tractCount.toLocaleString()}
                                </td>
                                <td className="text-right py-3 px-3 font-mono text-muted-foreground tabular-nums">
                                    {c.medianValue !== null ? fmtVal(c.medianValue) : "—"}
                                </td>
                                <td className={`text-right py-3 px-3 font-mono font-medium tabular-nums ${c.medianAppreciation !== null
                                    ? c.medianAppreciation >= 0 ? "text-chart-high" : "text-chart-negative"
                                    : "text-muted-foreground"
                                    }`}>
                                    {c.medianAppreciation !== null ? fmtPctCapped(c.medianAppreciation) : "—"}
                                </td>
                                <td className={`text-right py-3 px-3 font-mono tabular-nums ${c.highestUpside !== null
                                    ? c.highestUpside >= 0 ? "text-chart-high/70" : "text-chart-negative/70"
                                    : "text-muted-foreground"
                                    }`}>
                                    {c.highestUpside !== null ? fmtPctCapped(c.highestUpside) : "—"}
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </table>
                {sorted.length === 0 && (
                    <div className="p-8 text-center text-muted-foreground text-sm">
                        No markets found matching "{searchQuery}"
                    </div>
                )}
            </div>
        </div>
    )
}
