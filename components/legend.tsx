"use client"

import { useState, useEffect } from "react"
import { Info } from "lucide-react"
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip"
import { cn } from "@/lib/utils"

// Value mode gradient (unchanged)
const VALUE_GRADIENT = "linear-gradient(to right, oklch(0.25 0.10 280), oklch(0.60 0.20 30), oklch(0.95 0.15 80))"
const VALUE_LABELS = ["$150k", "$525k", "$1M+"]

interface LegendProps {
  className?: string
  colorMode?: "growth" | "value" | "growth_dollar"
  onColorModeChange?: (mode: "growth" | "value" | "growth_dollar") => void
  year?: number
  originYear?: number
}

export function Legend({ className, colorMode = "growth", onColorModeChange, year = 2026, originYear = 2024 }: LegendProps) {
  // Data-driven growth labels + gradient stops from API
  const [growthLabels, setGrowthLabels] = useState<[string, string, string]>(["-20%", "0%", "+100%+"])
  const [growthGradient, setGrowthGradient] = useState<string>(
    "linear-gradient(to right, #3b82f6, #93c5fd 30%, #f8f8f8 50%, #f59e0b 70%, #ef4444)"
  )
  const presentYear = originYear + 2
  const yrsFromPresent = Math.max(Math.abs(year - presentYear), 1)
  const growthDollarMin = -10000 * yrsFromPresent
  const growthDollarMax = 30000 * yrsFromPresent
  const growthDollarRange = growthDollarMax - growthDollarMin
  const zeroPct = growthDollarRange > 0 ? Math.round((0 - growthDollarMin) / growthDollarRange * 100) : 50

  const GROWTH_DOLLAR_GRADIENT = `linear-gradient(to right, #3b82f6, #f8f8f8 ${zeroPct}%, #ef4444)`
  const GROWTH_DOLLAR_LABELS = [
    `-$${Math.abs(growthDollarMin) / 1000}k`,
    "$0",
    `+$${growthDollarMax / 1000}k+`
  ]
  const horizonM = (year - originYear) * 12

  useEffect(() => {
    if (colorMode !== "growth" || horizonM === 0) return
    fetch(`/api/forecast-stats?mode=growth&originYear=${originYear}&horizonM=${horizonM}`)
      .then(r => r.ok ? r.json() : null)
      .then(json => {
        if (json?.levels?.tract) {
          const s = json.levels.tract
          const fmt = (n: number) => {
            const v = Math.round(n)
            return v >= 0 ? `+${v}%` : `${v}%`
          }
          setGrowthLabels([
            fmt(s.p5),
            fmt(s.p50),
            fmt(s.p95),
          ])
          // Compute proportional CSS gradient stops matching map's interpolation
          // Map uses: p5 → blue, p25 → light blue, p50 → white, p75 → amber, p95 → red
          const range = s.p95 - s.p5
          if (range > 0) {
            const pct = (v: number) => Math.round(((v - s.p5) / range) * 100)
            setGrowthGradient(
              `linear-gradient(to right, #3b82f6 ${pct(s.p5)}%, #93c5fd ${pct(s.p25)}%, #f8f8f8 ${pct(s.p50)}%, #f59e0b ${pct(s.p75)}%, #ef4444 ${pct(s.p95)}%)`
            )
          }
        }
      })
      .catch(() => { /* keep defaults */ })
  }, [colorMode, horizonM, originYear])

  return (
    <div className={cn("glass-panel rounded-lg p-3 space-y-1 text-xs", className)}>
      {/* Color Scale */}
      <div className="space-y-1.5">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-1.5 text-foreground font-medium">
            <span>{colorMode === "value" ? "Property Value" : colorMode === "growth_dollar" ? "Growth ($)" : "Growth (%)"}</span>
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Info className="h-3 w-3 text-muted-foreground cursor-help" />
                </TooltipTrigger>
                <TooltipContent side="right" className="max-w-48">
                  <p>
                    {colorMode === "value"
                      ? "Estimated median property value ($)."
                      : colorMode === "growth_dollar"
                        ? "Absolute 5-year dollar growth vs current value."
                        : "Relative growth vs median. White = average."}
                  </p>
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
          </div>
          {/* Color Mode Toggle */}
          {onColorModeChange && (
            <div className="grid grid-cols-3 gap-1 p-0.5 bg-secondary/50 rounded-md shrink-0">
              <button
                onClick={() => onColorModeChange("growth")}
                className={cn(
                  "px-2 py-1 text-[10px] font-medium rounded transition-all",
                  colorMode === "growth"
                    ? "bg-background text-foreground shadow-sm"
                    : "text-muted-foreground hover:text-foreground"
                )}
              >
                Growth %
              </button>
              <button
                onClick={() => onColorModeChange("growth_dollar")}
                className={cn(
                  "px-2 py-1 text-[10px] font-medium rounded transition-all",
                  colorMode === "growth_dollar"
                    ? "bg-background text-foreground shadow-sm"
                    : "text-muted-foreground hover:text-foreground"
                )}
              >
                Growth $
              </button>
              <button
                onClick={() => onColorModeChange("value")}
                className={cn(
                  "px-2 py-1 text-[10px] font-medium rounded transition-all",
                  colorMode === "value"
                    ? "bg-background text-foreground shadow-sm"
                    : "text-muted-foreground hover:text-foreground"
                )}
              >
                Value
              </button>
            </div>
          )}
        </div>
        <div className="flex flex-col gap-1">
          <div
            className="h-3 w-full rounded-sm"
            style={{ background: colorMode === "value" ? VALUE_GRADIENT : colorMode === "growth_dollar" ? GROWTH_DOLLAR_GRADIENT : growthGradient }}
          />
          <div className="flex justify-between text-[9px] text-muted-foreground font-mono px-0.5">
            {colorMode === "value" ? (
              <>
                <span>{VALUE_LABELS[0]}</span>
                <span>{VALUE_LABELS[1]}</span>
                <span>{VALUE_LABELS[2]}</span>
              </>
            ) : colorMode === "growth_dollar" ? (
              <>
                <span>{GROWTH_DOLLAR_LABELS[0]}</span>
                <span>{GROWTH_DOLLAR_LABELS[1]}</span>
                <span>{GROWTH_DOLLAR_LABELS[2]}</span>
              </>
            ) : (
              <>
                <span>{growthLabels[0]}</span>
                <span>{growthLabels[1]}</span>
                <span>{growthLabels[2]}</span>
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
