"use client"

import { Info } from "lucide-react"
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip"
import { cn } from "@/lib/utils"

interface LegendProps {
  className?: string
}

const OPPORTUNITY_SCALE = [
  { label: "< 0%", color: "bg-[oklch(0.55_0.2_25)]" },
  { label: "0-3%", color: "bg-[oklch(0.75_0.15_85)]" },
  { label: "3-6%", color: "bg-[oklch(0.75_0.15_120)]" },
  { label: "6-10%", color: "bg-[oklch(0.70_0.18_145)]" },
  { label: "> 10%", color: "bg-[oklch(0.65_0.15_180)]" },
]

const RELIABILITY_BINS = [
  { label: "Very Low", opacity: "opacity-25" },
  { label: "Low", opacity: "opacity-45" },
  { label: "Moderate", opacity: "opacity-65" },
  { label: "High", opacity: "opacity-85" },
  { label: "Very High", opacity: "opacity-100" },
]

export function Legend({ className }: LegendProps) {
  return (
    <div className={cn("glass-panel rounded-lg p-3 space-y-3 text-xs", className)}>
      {/* Opportunity Color */}
      <div className="space-y-1.5">
        <div className="flex items-center gap-1.5 text-foreground font-medium">
          <span>Opportunity</span>
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger asChild>
                <Info className="h-3 w-3 text-muted-foreground cursor-help" />
              </TooltipTrigger>
              <TooltipContent side="right" className="max-w-48">
                <p>Expected financial upside (CAGR). Higher values indicate better investment opportunities.</p>
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </div>
        <div className="flex gap-0.5">
          {OPPORTUNITY_SCALE.map((item, i) => (
            <div key={i} className="flex-1 text-center">
              <div className={cn("h-3 rounded-sm", item.color)} />
              <span className="text-[9px] text-muted-foreground mt-0.5 block">{item.label}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Reliability Opacity */}
      <div className="space-y-1.5">
        <div className="flex items-center gap-1.5 text-foreground font-medium">
          <span>Reliability</span>
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger asChild>
                <Info className="h-3 w-3 text-muted-foreground cursor-help" />
              </TooltipTrigger>
              <TooltipContent side="right" className="max-w-48">
                <p>Trustworthiness of the opportunity score. Lower reliability appears more transparent.</p>
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </div>
        <div className="flex gap-0.5">
          {RELIABILITY_BINS.map((item, i) => (
            <div key={i} className="flex-1 text-center">
              <div className={cn("h-3 rounded-sm bg-primary", item.opacity)} />
              <span className="text-[9px] text-muted-foreground mt-0.5 block">{item.label}</span>
            </div>
          ))}
        </div>
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
