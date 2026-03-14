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
  compareMode?: boolean
  onCompareModeChange?: (compare: boolean) => void
  pinnedCount?: number
  zoom?: number
}

export function Legend({ className, colorMode = "growth", onColorModeChange, year = 2026, originYear = 2024, compareMode, onCompareModeChange, pinnedCount, zoom = 10 }: LegendProps) {
  const presentYear = originYear + 2
  const yrsFromPresent = Math.max(Math.abs(year - presentYear), 1)
  
  // Default values
  const defaultGrowthLabels: [string, string, string] = ["-20%", "0%", "+100%+"]
  const defaultGrowthGradient = "linear-gradient(to right, #3b82f6, #93c5fd 30%, #f8f8f8 50%, #f59e0b 70%, #ef4444)"
  
  const growthDollarMin = -10000 * yrsFromPresent
  const growthDollarMax = 30000 * yrsFromPresent
  const growthDollarRange = growthDollarMax - growthDollarMin
  const zeroPct = growthDollarRange > 0 ? Math.round((0 - growthDollarMin) / growthDollarRange * 100) : 50
  
  const defaultGrowthDollarGradient = `linear-gradient(to right, #3b82f6, #f8f8f8 ${zeroPct}%, #ef4444)`
  const defaultGrowthDollarLabels: [string, string, string] = [
    `-$${Math.abs(growthDollarMin) / 1000}k`,
    "$0",
    `+$${growthDollarMax / 1000}k+`
  ]

  // Data-driven state
  const [apiGrowthLabels, setApiGrowthLabels] = useState<[string, string, string] | null>(null)
  const [apiGrowthGradient, setApiGrowthGradient] = useState<string | null>(null)
  
  const [apiGrowthDollarLabels, setApiGrowthDollarLabels] = useState<[string, string, string] | null>(null)
  const [apiGrowthDollarGradient, setApiGrowthDollarGradient] = useState<string | null>(null)

  const horizonM = (year - originYear) * 12
  const aggregationLevel = zoom < 5 ? "state" : zoom < 8 ? "zcta" : zoom < 12 ? "tract" : "tabblock"

  useEffect(() => {
    // Note: Do not clear old data explicitly to prevent flashing; let the new data override when fetched.
    if ((colorMode !== "growth" && colorMode !== "growth_dollar") || horizonM === 0) return
    fetch(`/api/forecast-stats?mode=${colorMode}&originYear=${originYear}&horizonM=${horizonM}`)
      .then(r => r.ok ? r.json() : null)
      .then(json => {
        if (json?.levels && json.levels[aggregationLevel]) {
          const s = json.levels[aggregationLevel]
          const range = s.p95 - s.p5

          if (colorMode === "growth") {
            const fmt = (n: number) => {
              const v = Math.round(n)
              return v >= 0 ? `+${v}%` : `${v}%`
            }
            setApiGrowthLabels([
              fmt(s.p5),
              fmt(s.p50),
              fmt(s.p95),
            ])
            if (range > 0) {
              const pct = (v: number) => Math.round(((v - s.p5) / range) * 100)
              setApiGrowthGradient(
                `linear-gradient(to right, #3b82f6 ${pct(s.p5)}%, #93c5fd ${pct(s.p25)}%, #f8f8f8 ${pct(s.p50)}%, #f59e0b ${pct(s.p75)}%, #ef4444 ${pct(s.p95)}%)`
              )
            }
          } else if (colorMode === "growth_dollar") {
            const fmt = (n: number) => {
              const v = Math.round(n / 1000)
              return v >= 0 ? `+$${v}k` : `-$${Math.abs(v)}k`
            }
            setApiGrowthDollarLabels([
              fmt(s.p5),
              "$0",
              fmt(s.p95),
            ])
            if (range > 0) {
              // Map uses blue -> white -> red with 0 at white
              // Determine where 0 falls on this scale
              const zeroPctCalc = s.p5 < 0 && s.p95 > 0 ? Math.round(((0 - s.p5) / range) * 100) : (s.p95 <= 0 ? 100 : 0)
              setApiGrowthDollarGradient(
                `linear-gradient(to right, #3b82f6, #f8f8f8 ${zeroPctCalc}%, #ef4444)`
              )
            }
          }
        }
      })
      .catch(() => { /* keep defaults */ })
  }, [colorMode, horizonM, originYear, aggregationLevel])
  
  const currentGrowthLabels = apiGrowthLabels || defaultGrowthLabels
  const currentGrowthGradient = apiGrowthGradient || defaultGrowthGradient
  const currentGrowthDollarLabels = apiGrowthDollarLabels || defaultGrowthDollarLabels
  const currentGrowthDollarGradient = apiGrowthDollarGradient || defaultGrowthDollarGradient

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
          {/* Single/Compare toggle — same row */}
          {onCompareModeChange && (
            <div className="grid grid-cols-2 gap-1 p-0.5 bg-secondary/50 rounded-md shrink-0">
              <button
                onClick={() => onCompareModeChange(false)}
                className={cn(
                  "px-2 py-1 text-[10px] font-medium rounded transition-all",
                  !compareMode
                    ? "bg-background text-foreground shadow-sm"
                    : "text-muted-foreground hover:text-foreground"
                )}
              >
                Single
              </button>
              <button
                onClick={() => onCompareModeChange(true)}
                className={cn(
                  "px-2 py-1 text-[10px] font-medium rounded transition-all relative",
                  compareMode
                    ? "bg-lime-500/80 text-black shadow-sm"
                    : "text-muted-foreground hover:text-foreground"
                )}
              >
                Compare
                {(pinnedCount ?? 0) > 0 && (
                  <span className="absolute -top-1 -right-1 w-3.5 h-3.5 bg-lime-400 text-black text-[7px] font-bold rounded-full flex items-center justify-center">
                    {pinnedCount}
                  </span>
                )}
              </button>
            </div>
          )}
        </div>
        <div className="flex flex-col gap-1">
          <div
            className="h-3 w-full rounded-sm"
            style={{ background: colorMode === "value" ? VALUE_GRADIENT : colorMode === "growth_dollar" ? currentGrowthDollarGradient : currentGrowthGradient }}
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
                <span>{currentGrowthDollarLabels[0]}</span>
                <span>{currentGrowthDollarLabels[1]}</span>
                <span>{currentGrowthDollarLabels[2]}</span>
              </>
            ) : (
              <>
                <span>{currentGrowthLabels[0]}</span>
                <span>{currentGrowthLabels[1]}</span>
                <span>{currentGrowthLabels[2]}</span>
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
