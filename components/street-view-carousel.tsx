"use client"

import React, { useEffect, useState } from "react"
import useEmblaCarousel from "embla-carousel-react"
import { ChevronLeft, ChevronRight, MapPin } from "lucide-react"
import { cn } from "@/lib/utils"
import { getRepresentativeProperties, getStreetViewImageUrl, type PropertyLocation } from "@/lib/utils/street-view"

interface StreetViewCarouselProps {
    h3Ids: string[]
    apiKey: string
    className?: string
}

export function StreetViewCarousel({ h3Ids, apiKey, className }: StreetViewCarouselProps) {
    const [emblaRef, emblaApi] = useEmblaCarousel({ loop: true, align: "start" })
    const [locations, setLocations] = useState<PropertyLocation[]>([])
    const [canScrollPrev, setCanScrollPrev] = useState(false)
    const [canScrollNext, setCanScrollNext] = useState(false)
    const [selectedIndex, setSelectedIndex] = useState(0)

    useEffect(() => {
        if (h3Ids.length > 0) {
            setLocations(getRepresentativeProperties(h3Ids))
        }
    }, [h3Ids])

    const onSelect = React.useCallback(() => {
        if (!emblaApi) return
        setSelectedIndex(emblaApi.selectedScrollSnap())
        setCanScrollPrev(emblaApi.canScrollPrev())
        setCanScrollNext(emblaApi.canScrollNext())
    }, [emblaApi])

    useEffect(() => {
        if (!emblaApi) return
        onSelect()
        emblaApi.on("select", onSelect)
        emblaApi.on("reInit", onSelect)
    }, [emblaApi, onSelect])

    if (locations.length === 0) return null

    return (
        <div className={cn("relative group", className)}>
            <div className="overflow-hidden rounded-t-lg" ref={emblaRef}>
                <div className="flex">
                    {locations.map((loc, index) => (
                        <div key={`${loc.lat}-${loc.lng}-${index}`} className="flex-[0_0_100%] min-w-0 relative h-[180px]">
                            {/* eslint-disable-next-line @next/next/no-img-element */}
                            <img
                                src={getStreetViewImageUrl(loc.lat, loc.lng, apiKey)}
                                alt={loc.label || "Property View"}
                                className="w-full h-full object-cover"
                                loading="lazy"
                            />
                            <div className="absolute bottom-0 left-0 right-0 bg-gradient-to-t from-black/60 to-transparent p-2">
                                <div className="flex items-center gap-1 text-white text-[10px] font-medium">
                                    <MapPin className="w-3 h-3" />
                                    <span>{loc.label}</span>
                                </div>
                            </div>
                        </div>
                    ))}
                </div>
            </div>

            {/* Navigation Dots */}
            {locations.length > 1 && (
                <div className="absolute bottom-2 right-2 flex gap-1 z-10">
                    {locations.map((_, idx) => (
                        <div
                            key={idx}
                            className={cn(
                                "w-1.5 h-1.5 rounded-full transition-all",
                                selectedIndex === idx ? "bg-white w-3" : "bg-white/40"
                            )}
                        />
                    ))}
                </div>
            )}

            {/* Navigation Buttons */}
            {locations.length > 1 && (
                <>
                    <button
                        onClick={() => emblaApi?.scrollPrev()}
                        className={cn(
                            "absolute left-2 top-1/2 -translate-y-1/2 w-8 h-8 rounded-full bg-black/30 backdrop-blur-sm border border-white/10 flex items-center justify-center text-white transition-opacity opacity-0 group-hover:opacity-100 disabled:opacity-0",
                            !canScrollPrev && "pointer-events-none"
                        )}
                        disabled={!canScrollPrev}
                    >
                        <ChevronLeft className="w-5 h-5" />
                    </button>
                    <button
                        onClick={() => emblaApi?.scrollNext()}
                        className={cn(
                            "absolute right-2 top-1/2 -translate-y-1/2 w-8 h-8 rounded-full bg-black/30 backdrop-blur-sm border border-white/10 flex items-center justify-center text-white transition-opacity opacity-0 group-hover:opacity-100 disabled:opacity-0",
                            !canScrollNext && "pointer-events-none"
                        )}
                        disabled={!canScrollNext}
                    >
                        <ChevronRight className="w-5 h-5" />
                    </button>
                </>
            )}
        </div>
    )
}
