"use client"

/**
 * ForecastMapEmbed
 *
 * Renders an embedded /app iframe pre-centered on a geographic area.
 * Mirrors the pattern used on the landing page hero.
 */
interface ForecastMapEmbedProps {
    lat: number
    lng: number
    zoom?: number
    bbox?: [number, number, number, number] | null
    selectedId?: string
    label?: string
    height?: number
}

export function ForecastMapEmbed({ lat, lng, zoom = 11, bbox, label, height = 420 }: ForecastMapEmbedProps) {
    let src = `/app?lat=${lat.toFixed(5)}&lng=${lng.toFixed(5)}&zoom=${zoom}&embedded=true`
    let fullUrl = `/app?lat=${lat.toFixed(5)}&lng=${lng.toFixed(5)}&zoom=${zoom}`

    if (bbox) {
        const bboxStr = bbox.map(v => v.toFixed(5)).join(",")
        src += `&bbox=${bboxStr}`
        fullUrl += `&bbox=${bboxStr}`
    }

    if (selectedId) {
        src += `&selectedId=${encodeURIComponent(selectedId)}`
        fullUrl += `&selectedId=${encodeURIComponent(selectedId)}`
    }

    return (
        <div
            className="w-full rounded-xl overflow-hidden glass-panel border border-border/50 relative shadow-xl"
            style={{ height }}
        >
            {/* Live badge */}
            <div className="absolute top-3 left-3 z-10 bg-background/90 backdrop-blur-md px-3 py-1.5 rounded-full text-xs font-bold shadow-sm border border-border flex items-center gap-2 pointer-events-none">
                <div className="w-2 h-2 rounded-full bg-lime-500 animate-pulse" />
                {label ?? "Live Forecast Map"}
            </div>

            {/* Full-screen link overlay — opens /app in full */}
            <a
                href={fullUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="absolute top-3 right-3 z-10 bg-background/90 backdrop-blur-md px-3 py-1.5 rounded-full text-xs font-semibold shadow-sm border border-border hover:bg-primary hover:text-primary-foreground hover:border-primary transition-all"
                aria-label="Open full map"
            >
                Open Full Map ↗
            </a>

            <iframe
                src={src}
                className="w-full h-full border-0 pointer-events-auto"
                title={label ?? "Homecastr Interactive Forecast Map"}
                loading="lazy"
            />
        </div>
    )
}
