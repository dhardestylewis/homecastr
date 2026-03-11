"use client"

import * as React from "react"
import { Slider } from "@/components/ui/slider"
import { Button } from "@/components/ui/button"
import { Play, Pause, ChevronLeft, ChevronRight, Calendar } from "lucide-react"
import { cn } from "@/lib/utils"

interface TimeControlsProps {
    minYear: number
    maxYear: number
    currentYear: number
    onChange: (year: number) => void
    onPlayStart?: () => void  // Called when user starts playback
    className?: string
}

export function TimeControls({
    minYear,
    maxYear,
    currentYear,
    onChange,
    onPlayStart,
    className,
}: TimeControlsProps) {
    const [isPlaying, setIsPlaying] = React.useState(false)
    const hasTriggeredPrefetchRef = React.useRef(false)

    // Auto-play functionality
    React.useEffect(() => {
        let interval: NodeJS.Timeout

        if (isPlaying) {
            // Trigger prefetch callback ONCE when play starts
            if (!hasTriggeredPrefetchRef.current && onPlayStart) {
                hasTriggeredPrefetchRef.current = true
                onPlayStart()
            }

            interval = setInterval(() => {
                onChange(currentYear >= maxYear ? minYear : currentYear + 1)
            }, 1500) // 1.5s per year
        } else {
            // Reset prefetch flag when stopped
            hasTriggeredPrefetchRef.current = false
        }

        return () => clearInterval(interval)
    }, [isPlaying, currentYear, maxYear, minYear, onChange]) // onPlayStart not in deps, accessed via ref pattern

    const handlePrevious = () => {
        if (currentYear > minYear) onChange(currentYear - 1)
    }

    const handleNext = () => {
        if (currentYear < maxYear) onChange(currentYear + 1)
    }

    const handleSliderChange = (value: number[]) => {
        onChange(value[0])
        // Pause if user interacts manually
        if (isPlaying) setIsPlaying(false)
    }

    return (
        <div className={cn("glass-panel rounded-lg shadow-lg w-full md:w-[320px] transition-all h-12 flex items-center px-3 gap-3", className)}>
            <Button
                variant="ghost"
                size="icon"
                className="h-8 w-8 shrink-0 text-primary hover:bg-primary/10"
                onClick={() => setIsPlaying(!isPlaying)}
                aria-label={isPlaying ? "Pause timeline" : "Play timeline"}
            >
                {isPlaying ? <Pause className="h-4 w-4" /> : <Play className="h-4 w-4 ml-0.5" />}
            </Button>

            <div className="flex-1 flex flex-col justify-center gap-0.5 relative top-0.5 min-w-0">
                    <Slider
                        value={[currentYear]}
                        min={minYear}
                        max={maxYear}
                        step={1}
                        onValueChange={handleSliderChange}
                        className="cursor-pointer"
                        aria-label="Forecast year"
                    />
                    <div className="relative w-full h-3 px-0.5">
                        <span className="absolute left-0 text-[9px] text-muted-foreground font-mono">{minYear}</span>
                        <span
                            className="absolute text-[9px] text-primary font-mono font-bold tabular-nums -translate-x-1/2"
                            style={{ left: `${((currentYear - minYear) / (maxYear - minYear)) * 100}%` }}
                        >
                            {currentYear}
                        </span>
                        <span className="absolute right-0 text-[9px] text-muted-foreground font-mono">{maxYear}</span>
                    </div>
                </div>
        </div>
    )
}
