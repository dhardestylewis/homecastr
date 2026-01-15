"use client"

import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip"
import { cn } from "@/lib/utils"
import type { ReliabilityComponents } from "@/lib/types"

interface DecompositionBarProps {
  components: ReliabilityComponents
}

// Layperson-friendly labels with tooltips explaining technical meaning
const COMPONENT_CONFIG = {
  accuracy_term: { label: "Precision", color: "bg-[oklch(0.65_0.18_180)]" },       // How close predictions are to actual values
  confidence_term: { label: "Sample Size", color: "bg-[oklch(0.70_0.15_145)]" },   // Number of properties in this area
  stability_term: { label: "Consistency", color: "bg-[oklch(0.75_0.15_120)]" },    // How stable predictions are over time
  robustness_term: { label: "Outlier Resistance", color: "bg-[oklch(0.75_0.15_85)]" }, // Not affected by extreme values
  support_term: { label: "Data Coverage", color: "bg-[oklch(0.70_0.12_60)]" },     // Amount of historical data available
}

/**
 * Check if a value is a finite number (not null, undefined, NaN, or Infinity)
 */
function isFiniteNumber(value: number | null | undefined): value is number {
  return value != null && Number.isFinite(value)
}

export function DecompositionBar({ components }: DecompositionBarProps) {
  // Filter to only valid (finite) components
  const validEntries = (Object.entries(components) as [keyof ReliabilityComponents, number | null][])
    .filter(([_, value]) => isFiniteNumber(value)) as [keyof ReliabilityComponents, number][]

  const total = validEntries.reduce((sum, [_, v]) => sum + v, 0)
  const average = validEntries.length > 0 ? total / validEntries.length : 0

  return (
    <div className="space-y-3">
      {/* Horizontal stacked bar - only show valid components */}
      <div className="h-6 rounded-md overflow-hidden flex bg-secondary/30">
        <TooltipProvider>
          {validEntries.map(([key, value]) => {
            const config = COMPONENT_CONFIG[key]
            const widthPercent = (value / 5) * 100 // Each can contribute up to 1, 5 total

            return (
              <Tooltip key={key}>
                <TooltipTrigger asChild>
                  <div
                    className={cn("h-full transition-all hover:brightness-110", config.color)}
                    style={{ width: `${widthPercent}%` }}
                  />
                </TooltipTrigger>
                <TooltipContent>
                  <p className="font-medium">{config.label}</p>
                  <p className="text-xs text-muted-foreground">{(value * 100).toFixed(0)}%</p>
                </TooltipContent>
              </Tooltip>
            )
          })}
        </TooltipProvider>
      </div>

      {/* Individual bars - show all, with N/A for missing */}
      <div className="space-y-1.5">
        {(Object.entries(components) as [keyof ReliabilityComponents, number | null][]).map(([key, value]) => {
          const config = COMPONENT_CONFIG[key]
          const isValid = isFiniteNumber(value)

          return (
            <div key={key} className="flex items-center gap-2">
              <span className="text-[10px] text-muted-foreground w-20 truncate">{config.label}</span>
              <div className="flex-1 h-1.5 bg-secondary rounded-full overflow-hidden">
                {isValid ? (
                  <div className={cn("h-full rounded-full", config.color)} style={{ width: `${value * 100}%` }} />
                ) : (
                  <div className="h-full rounded-full bg-muted/50" style={{ width: "0%" }} />
                )}
              </div>
              <span className="text-[10px] font-mono text-muted-foreground w-8 text-right">
                {isValid ? `${(value * 100).toFixed(0)}%` : "N/A"}
              </span>
            </div>
          )
        })}
      </div>
    </div>
  )
}
