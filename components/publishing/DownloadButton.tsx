"use client"

import { Download } from "lucide-react"

interface DownloadButtonProps {
    state?: string
    city?: string
    label?: string
}

export function DownloadButton({ state, city, label }: DownloadButtonProps) {
    const params = new URLSearchParams()
    if (state) params.set("state", state)
    if (city) params.set("city", city)
    const href = `/api/forecasts/download?${params.toString()}`

    // Build a descriptive filename hint
    let filename = "homecastr-forecasts.csv"
    if (state && city) {
        filename = `${state}-${city}-neighborhood-forecasts.csv`
    } else if (state) {
        filename = `${state}-county-forecasts.csv`
    } else {
        filename = "homecastr-state-forecasts.csv"
    }

    return (
        <a
            href={href}
            download={filename}
            className="inline-flex items-center gap-2 px-4 py-2 rounded-lg border border-border bg-secondary hover:bg-accent text-sm font-medium text-foreground transition-all hover:shadow-md"
        >
            <Download className="w-4 h-4" />
            {label || "Download CSV"}
        </a>
    )
}
