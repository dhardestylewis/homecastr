"use client"

import React, { useEffect, useState, useCallback, useRef } from "react"
import useEmblaCarousel from "embla-carousel-react"
import { ChevronLeft, ChevronRight, MapPin, ImageOff } from "lucide-react"
import { cn } from "@/lib/utils"
import { getRepresentativeProperties, getSignedStreetViewUrl, checkStreetViewAvailability, type PropertyLocation, type StreetViewMeta } from "@/lib/utils/street-view"

interface StreetViewCarouselProps {
    h3Ids: string[]
    apiKey: string
    className?: string
    coordinates?: [number, number]
    /** Only render at this zoom or higher (default 15 = block scale) */
    minZoom?: number
    mapZoom?: number
}

export function StreetViewCarousel({ h3Ids, apiKey, className, coordinates, minZoom = 15, mapZoom }: StreetViewCarouselProps) {
    const [emblaRef, emblaApi] = useEmblaCarousel({ loop: true, align: "start" })
    const [locations, setLocations] = useState<PropertyLocation[]>([])
    const [selectedIndex, setSelectedIndex] = useState(0)
    const [canScrollPrev, setCanScrollPrev] = useState(false)
    const [canScrollNext, setCanScrollNext] = useState(false)
    // Only populated for slides that have been requested
    const [signedUrls, setSignedUrls] = useState<Record<string, string>>({})
    const [loadedIdxs, setLoadedIdxs] = useState<Set<number>>(new Set())
    const locationsRef = useRef<PropertyLocation[]>([])
    const loadedIdxsRef = useRef<Set<number>>(new Set())

    // Track which slides have valid Street View imagery (free metadata check)
    const [unavailableSlides, setUnavailableSlides] = useState<Set<number>>(new Set())
    const availabilityCheckedRef = useRef(false)

    // Skip rendering below min zoom — street view is meaningless at county/city scale
    if (mapZoom != null && mapZoom < minZoom) return null

    // Stabilize h3Ids and coordinates references to prevent dependency loops
    const h3Key = h3Ids.join(',')
    const coordKey = coordinates ? `${coordinates[0]},${coordinates[1]}` : ''

    useEffect(() => {
        const locs = coordinates
            ? [
                { lat: coordinates[0], lng: coordinates[1], label: "Center" },
                { lat: coordinates[0] + 0.0004, lng: coordinates[1] + 0.0004, label: "North East" },
                { lat: coordinates[0] - 0.0004, lng: coordinates[1] - 0.0004, label: "South West" },
                { lat: coordinates[0] + 0.0004, lng: coordinates[1] - 0.0004, label: "North West" },
                { lat: coordinates[0] - 0.0004, lng: coordinates[1] + 0.0004, label: "South East" },
            ]
            : getRepresentativeProperties(h3Ids)
        locationsRef.current = locs
        loadedIdxsRef.current = new Set()
        availabilityCheckedRef.current = false
        setLocations(locs)
        setSignedUrls({})
        setLoadedIdxs(new Set())
        setUnavailableSlides(new Set())
    }, [h3Key, coordKey]) // eslint-disable-line react-hooks/exhaustive-deps

    /** Fetch signed URL for a single slide index (no-op if already loaded) */
    const loadSlide = useCallback(async (idx: number) => {
        const locs = locationsRef.current
        if (idx < 0 || idx >= locs.length) return
        if (loadedIdxsRef.current.has(idx)) return // already loaded
        loadedIdxsRef.current.add(idx)
        const loc = locs[idx]
        const key = `${loc.lat}-${loc.lng}`
        setLoadedIdxs(prev => new Set(prev).add(idx))
        const url = await getSignedStreetViewUrl(loc.lat, loc.lng)
        setSignedUrls(prev => ({ ...prev, [key]: url }))
    }, []) // stable — reads from refs

    // Check availability for all slides in parallel (free metadata API).
    // Road-snap coordinates to nearest panorama, dedup, then load.
    useEffect(() => {
        if (locations.length === 0) return
        if (availabilityCheckedRef.current) return
        availabilityCheckedRef.current = true

        const locs = locationsRef.current
        Promise.all(locs.map(loc => checkStreetViewAvailability(loc.lat, loc.lng)))
            .then(results => {
                const unavailable = new Set<number>()
                const seenPanos = new Set<string>()

                // Road-snap coordinates and deduplicate
                results.forEach((meta: StreetViewMeta, idx: number) => {
                    if (!meta.available) {
                        unavailable.add(idx)
                        return
                    }
                    // Snap to actual panorama location
                    if (meta.snappedLat != null && meta.snappedLng != null) {
                        const panoKey = `${meta.snappedLat.toFixed(5)},${meta.snappedLng.toFixed(5)}`
                        if (seenPanos.has(panoKey)) {
                            // Duplicate panorama — same road location from different probe
                            unavailable.add(idx)
                            return
                        }
                        seenPanos.add(panoKey)
                        // Update the location to the road-snapped coordinates
                        locs[idx] = { ...locs[idx], lat: meta.snappedLat, lng: meta.snappedLng }
                    }
                })

                // Commit snapped locations
                locationsRef.current = [...locs]
                setLocations([...locs])
                setUnavailableSlides(unavailable)

                // Find the first available slide
                const firstValid = locs.findIndex((_, idx) => !unavailable.has(idx))
                const target = firstValid >= 0 ? firstValid : 0

                // Load the target slide (+ prefetch next available)
                loadSlide(target)
                let next = (target + 1) % locs.length
                let attempts = 0
                while (unavailable.has(next) && attempts < locs.length) {
                    next = (next + 1) % locs.length
                    attempts++
                }
                if (!unavailable.has(next)) loadSlide(next)

                // Auto-scroll to the first valid slide
                if (target > 0 && emblaApi) {
                    setTimeout(() => emblaApi.scrollTo(target, true), 50)
                }
            })
            .catch(() => {
                // Fallback: just load slide 0 if metadata checks fail
                loadSlide(0)
            })
    }, [locations.length, loadSlide, emblaApi]) // eslint-disable-line react-hooks/exhaustive-deps

    // Lazy-load on carousel navigation, prefetch +1
    const onSelect = useCallback(() => {
        if (!emblaApi) return
        const idx = emblaApi.selectedScrollSnap()
        setSelectedIndex(idx)
        setCanScrollPrev(emblaApi.canScrollPrev())
        setCanScrollNext(emblaApi.canScrollNext())
        loadSlide(idx)
        const next = (idx + 1) % locationsRef.current.length
        loadSlide(next)
    }, [emblaApi, loadSlide])

    useEffect(() => {
        if (!emblaApi) return
        onSelect()
        emblaApi.on("select", onSelect)
        emblaApi.on("reInit", onSelect)
        return () => {
            emblaApi.off("select", onSelect)
            emblaApi.off("reInit", onSelect)
        }
    }, [emblaApi, onSelect])

    if (locations.length === 0) return null

    return (
        <div className={cn("relative group h-full", className)}>
            <div className="overflow-hidden rounded-t-lg h-full" ref={emblaRef}>
                <div className="flex h-full">
                    {locations.map((loc, index) => {
                        const key = `${loc.lat}-${loc.lng}`
                        const url = signedUrls[key]
                        const isUnavailable = unavailableSlides.has(index)
                        return (
                            <div key={`${key}-${index}`} className="flex-[0_0_100%] min-w-0 relative h-full bg-zinc-900">
                                {isUnavailable ? (
                                    /* No Street View coverage or duplicate panorama — show clean placeholder */
                                    <div className="w-full h-full flex flex-col items-center justify-center gap-2 text-white/30">
                                        <ImageOff className="w-8 h-8" />
                                        <span className="text-[10px] uppercase tracking-wider">No Street View</span>
                                    </div>
                                ) : url ? (
                                    /* eslint-disable-next-line @next/next/no-img-element */
                                    <img src={url} alt={loc.label || "Property view"} className="w-full h-full object-cover" />
                                ) : (
                                    <div className="w-full h-full flex items-center justify-center">
                                        <div className="w-5 h-5 border-2 border-white/20 border-t-white/70 rounded-full animate-spin" />
                                    </div>
                                )}
                                <div className="absolute bottom-0 left-0 right-0 bg-gradient-to-t from-black/70 to-transparent pt-6 pb-2 px-2">
                                    <div className="flex items-center gap-1 text-white text-xs font-semibold drop-shadow">
                                        <MapPin className="w-3 h-3 shrink-0" />
                                        <span>{loc.label}</span>
                                    </div>
                                </div>
                            </div>
                        )
                    })}
                </div>
            </div>

            {locations.length > 1 && (
                <div className="absolute top-2 right-2 z-10 bg-black/40 backdrop-blur-sm text-white text-[10px] font-medium px-1.5 py-0.5 rounded-full">
                    {selectedIndex + 1} / {locations.length}
                </div>
            )}
            {locations.length > 1 && (
                <div className="absolute bottom-2 right-2 flex gap-1 z-10">
                    {locations.map((_, idx) => (
                        <div key={idx} className={cn("w-1.5 h-1.5 rounded-full transition-all", selectedIndex === idx ? "bg-white w-3" : "bg-white/40")} />
                    ))}
                </div>
            )}
            {locations.length > 1 && (
                <>
                    <button onClick={() => emblaApi?.scrollPrev()} disabled={!canScrollPrev}
                        className={cn("absolute left-2 top-1/2 -translate-y-1/2 w-8 h-8 rounded-full bg-black/30 backdrop-blur-sm border border-white/10 flex items-center justify-center text-white transition-opacity opacity-0 group-hover:opacity-100 disabled:opacity-0", !canScrollPrev && "pointer-events-none")}
                        aria-label="Previous image">
                        <ChevronLeft className="w-5 h-5" />
                    </button>
                    <button onClick={() => emblaApi?.scrollNext()} disabled={!canScrollNext}
                        className={cn("absolute right-2 top-1/2 -translate-y-1/2 w-8 h-8 rounded-full bg-black/30 backdrop-blur-sm border border-white/10 flex items-center justify-center text-white transition-opacity opacity-0 group-hover:opacity-100 disabled:opacity-0", !canScrollNext && "pointer-events-none")}
                        aria-label="Next image">
                        <ChevronRight className="w-5 h-5" />
                    </button>
                </>
            )}
        </div>
    )
}
