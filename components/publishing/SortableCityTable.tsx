"use client"

import { useState } from "react"
import Link from "next/link"
import { Search } from "lucide-react"

interface CityRow {
    city: string
    citySlug: string
    tractCount: number
    href: string
}

type SortKey = "city" | "tractCount"
type SortDir = "asc" | "desc"

export function SortableCityTable({ rows }: { rows: CityRow[] }) {
    const [sortKey, setSortKey] = useState<SortKey>("tractCount")
    const [sortDir, setSortDir] = useState<SortDir>("desc")
    const [searchQuery, setSearchQuery] = useState("")

    const toggle = (key: SortKey) => {
        if (sortKey === key) {
            setSortDir(d => d === "asc" ? "desc" : "asc")
        } else {
            setSortKey(key)
            setSortDir(key === "city" ? "asc" : "desc")
        }
    }

    const sorted = [...rows]
        .filter(r => r.city.toLowerCase().includes(searchQuery.toLowerCase()))
        .sort((a, b) => {
            const dir = sortDir === "asc" ? 1 : -1
            if (sortKey === "city") return dir * a.city.localeCompare(b.city)
            return dir * (a.tractCount - b.tractCount)
        })

    const arrow = (key: SortKey) => {
        if (sortKey !== key) return ""
        return sortDir === "asc" ? " ↑" : " ↓"
    }

    return (
        <div className="space-y-4">
            <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
                <h2 className="text-xl font-semibold text-foreground">Browse Markets</h2>
                <div className="relative w-full sm:w-72">
                    <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                    <input 
                        type="text" 
                        placeholder="Search cities..." 
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                        className="w-full bg-background border border-border rounded-lg pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary/50 text-foreground placeholder:text-muted-foreground"
                    />
                </div>
            </div>
            <div className="glass-panel rounded-xl overflow-hidden">
                <table className="w-full text-sm">
                <thead>
                    <tr className="border-b border-border/50 bg-muted/30 text-muted-foreground text-xs uppercase tracking-wider">
                        <th className="py-3 px-4 text-left w-12">#</th>
                        <th
                            className="py-3 px-4 text-left cursor-pointer hover:text-foreground transition-colors select-none"
                            onClick={() => toggle("city")}
                        >
                            County / City{arrow("city")}
                        </th>
                        <th
                            className="py-3 px-4 text-right cursor-pointer hover:text-foreground transition-colors select-none"
                            onClick={() => toggle("tractCount")}
                        >
                            Neighborhoods{arrow("tractCount")}
                        </th>
                    </tr>
                </thead>
                <tbody>
                    {sorted.map((row, i) => (
                        <tr
                            key={row.citySlug}
                            className="border-b border-border/30 hover:bg-muted/20 transition-colors"
                        >
                            <td className="py-2.5 px-4 text-muted-foreground text-xs">{i + 1}</td>
                            <td className="py-2.5 px-4">
                                <Link
                                    href={row.href}
                                    className="text-foreground hover:text-primary font-medium transition-colors"
                                >
                                    {row.city}
                                </Link>
                            </td>
                            <td className="py-2.5 px-4 text-right text-muted-foreground">
                                {row.tractCount.toLocaleString()}
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
    )
}
