"use client"

import { Info } from "lucide-react"
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip"
import { cn } from "@/lib/utils"

interface LegendProps {
  className?: string
}

// Continuous legend gradient
// Purple (Hue 300) -> White (Purple Hue) -> White (Blue Hue) -> Blue (Hue 240)
const OPPORTUNITY_GRADIENT = "linear-gradient(to right, oklch(0.55 0.22 300), oklch(0.97 0 300) 50%, oklch(0.97 0 240) 50%, oklch(0.55 0.22 240))"

// Magma-like: Deep Purple -> Red -> Orange -> Yellow
const VALUE_GRADIENT = "linear-gradient(to right, oklch(0.25 0.10 280), oklch(0.60 0.20 30), oklch(0.95 0.15 80))"

const OPPORTUNITY_LABELS = ["-50%", "0%", "+50%"]
const VALUE_LABELS = ["$100k", "$800k", "$1.5M+"]

interface LegendProps {
  className?: string
  colorMode?: "growth" | "value"
}

export function Legend({ className, colorMode = "growth" }: LegendProps) {
  return (
    <div className={cn("glass-panel rounded-lg p-3 space-y-3 text-xs", className)}>
      {/* Color Scale */}
      <div className="space-y-1.5">
        <div className="flex items-center gap-1.5 text-foreground font-medium">
          <span>{colorMode === "value" ? "Property Value" : "Projected Growth"}</span>
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger asChild>
                <Info className="h-3 w-3 text-muted-foreground cursor-help" />
              </TooltipTrigger>
              <TooltipContent side="right" className="max-w-48">
                <p>
                  {colorMode === "value"
                    ? "Estimated median property value ($)."
                    : "Estimated compound annual growth rate (CAGR)."}
                </p>
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </div>
        <div className="flex flex-col gap-1">
          <div
            className="h-3 w-full rounded-sm"
            style={{ background: colorMode === "value" ? VALUE_GRADIENT : OPPORTUNITY_GRADIENT }}
          />
          <div className="flex justify-between text-[9px] text-muted-foreground font-mono px-0.5">
            {colorMode === "value" ? (
              <>
                <span>{VALUE_LABELS[0]}</span>
                <span>{VALUE_LABELS[1]}</span>
                <span>{VALUE_LABELS[2]}</span>
              </>
            ) : (
              <>
                <span>{OPPORTUNITY_LABELS[0]}</span>
                <span>{OPPORTUNITY_LABELS[1]}</span>
                <span>{OPPORTUNITY_LABELS[2]}</span>
              </>
            )}
          </div>
        </div>
      </div>



      {/* No Data Indicator */}
      <div className="flex items-center gap-1.5">
        <div className="w-4 h-4 rounded border border-[#888888] bg-[#888888]/10" />
        <span className="text-muted-foreground">No Data</span>
      </div>

      {/* Warning indicators */}
      <div className="space-y-1.5">
        <span className="text-foreground font-medium">Warnings</span>
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-1.5">
            <div className="w-4 h-4 rounded border-2 border-dashed border-warning bg-transparent" />
            <span className="text-muted-foreground">Stability/Robustness</span>
          </div>
        </div>
      </div>
    </div>
  )
}
