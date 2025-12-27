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

    // Historical years are ≤ 2025, Forecast years are > 2025
    const isHistorical = currentYear <= 2025
    const yearLabel = isHistorical ? "Historical Year" : "Forecast Year"

    return (
        <div className={cn("flex flex-col gap-2 p-3 bg-card/95 backdrop-blur-sm border border-border rounded-lg shadow-lg w-[320px]", className)}>
            <div className="flex items-center justify-between mb-1">
                <div className="flex items-center gap-1.5 text-sm font-semibold">
                    <Calendar className="h-4 w-4 text-muted-foreground" />
                    <span>{yearLabel}</span>
                </div>
                <span className="text-xl font-mono font-bold text-primary">{currentYear}</span>
            </div>

            <div className="flex items-center gap-2">
                <Button
                    variant="outline"
                    size="icon"
                    className="h-8 w-8 shrink-0"
                    onClick={() => setIsPlaying(!isPlaying)}
                >
                    {isPlaying ? <Pause className="h-3.5 w-3.5" /> : <Play className="h-3.5 w-3.5 ml-0.5" />}
                </Button>

                <div className="flex-1 px-1">
                    <Slider
                        value={[currentYear]}
                        min={minYear}
                        max={maxYear}
                        step={1}
                        onValueChange={handleSliderChange}
                        className="cursor-pointer"
                    />
                </div>
            </div>

            <div className="flex justify-between text-[10px] text-muted-foreground font-mono px-0.5">
                <span>{minYear}</span>
                <span>{maxYear}</span>
            </div>

            {/* Optional: Step controls if slider is too fiddly */}
            <div className="grid grid-cols-2 gap-2 mt-1">
                <Button
                    variant="ghost"
                    size="sm"
                    onClick={handlePrevious}
                    disabled={currentYear <= minYear}
                    className="h-6 text-xs"
                >
                    <ChevronLeft className="h-3 w-3 mr-1" />
                    Prev Year
                </Button>
                <Button
                    variant="ghost"
                    size="sm"
                    onClick={handleNext}
                    disabled={currentYear >= maxYear}
                    className="h-6 text-xs"
                >
                    Next Year
                    <ChevronRight className="h-3 w-3 ml-1" />
                </Button>
            </div>
        </div>
    )
}
