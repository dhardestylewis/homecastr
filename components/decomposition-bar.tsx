"use client"

import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip"
import { cn } from "@/lib/utils"
import type { ReliabilityComponents } from "@/lib/types"

interface DecompositionBarProps {
  components: ReliabilityComponents
}

const COMPONENT_CONFIG = {
  accuracy_term: { label: "Accuracy", color: "bg-[oklch(0.65_0.18_180)]" },
  confidence_term: { label: "Confidence", color: "bg-[oklch(0.70_0.15_145)]" },
  stability_term: { label: "Stability", color: "bg-[oklch(0.75_0.15_120)]" },
  robustness_term: { label: "Robustness", color: "bg-[oklch(0.75_0.15_85)]" },
  support_term: { label: "Support", color: "bg-[oklch(0.70_0.12_60)]" },
}

export function DecompositionBar({ components }: DecompositionBarProps) {
  const total = Object.values(components).reduce((sum, v) => sum + v, 0)
  const average = total / Object.keys(components).length

  return (
    <div className="space-y-3">
      {/* Horizontal stacked bar */}
      <div className="h-6 rounded-md overflow-hidden flex">
        <TooltipProvider>
          {(Object.entries(components) as [keyof ReliabilityComponents, number][]).map(([key, value]) => {
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

      {/* Individual bars */}
      <div className="space-y-1.5">
        {(Object.entries(components) as [keyof ReliabilityComponents, number][]).map(([key, value]) => {
          const config = COMPONENT_CONFIG[key]

          return (
            <div key={key} className="flex items-center gap-2">
              <span className="text-[10px] text-muted-foreground w-20 truncate">{config.label}</span>
              <div className="flex-1 h-1.5 bg-secondary rounded-full overflow-hidden">
                <div className={cn("h-full rounded-full", config.color)} style={{ width: `${value * 100}%` }} />
              </div>
              <span className="text-[10px] font-mono text-muted-foreground w-8 text-right">
                {(value * 100).toFixed(0)}%
              </span>
            </div>
          )
        })}
      </div>
    </div>
  )
}
