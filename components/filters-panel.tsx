"use client"

import { useCallback } from "react"
import { SlidersHorizontal, RotateCcw, ChevronLeft, ChevronRight } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Slider } from "@/components/ui/slider"
import { Switch } from "@/components/ui/switch"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { cn } from "@/lib/utils"
import type { FilterState } from "@/lib/types"

interface FiltersPanelProps {
  filters: FilterState
  onFiltersChange: (updates: Partial<FilterState>) => void
  onReset: () => void
  isOpen: boolean
  onToggle: () => void
  currentZoom: number // New prop
}

const RELIABILITY_BINS = [
  { value: 0, label: "All", description: "Show all reliability levels" },
  { value: 0.2, label: "≥20%", description: "Low and above" },
  { value: 0.4, label: "≥40%", description: "Moderate and above" },
  { value: 0.6, label: "≥60%", description: "High and above" },
  { value: 0.8, label: "≥80%", description: "Very high only" },
]

function getH3Resolution(zoom: number, override?: number): number {
  if (override) return override
  if (zoom < 10.5) return 7 // District
  if (zoom < 12.0) return 8 // Neighborhood
  if (zoom < 13.5) return 9 // Block
  if (zoom < 15.0) return 10
  return 11
}

export function FiltersPanel({ filters, onFiltersChange, onReset, isOpen, onToggle, currentZoom }: FiltersPanelProps) {
  const currentRes = getH3Resolution(currentZoom, filters.layerOverride)

  // Dynamic Slider Max based on View Level
  const getMaxAccounts = (res: number) => {
    if (res <= 6) return 500 // City: Lots of aggregation
    if (res === 7) return 100 // District
    if (res >= 8) return 25   // Neighborhood/Block
    return 100
  }

  const maxAccounts = getMaxAccounts(currentRes)

  // Ensure value doesn't get "stuck" above max visually (though logic handles it)
  // We won't force-change the filter state to avoid unexpected side effects, 
  // but we clamp the slider display.

  const handleReliabilityChange = useCallback(
    (value: number[]) => {
      onFiltersChange({ reliabilityMin: value[0] })
    },
    [onFiltersChange],
  )

  const handleNAcctsChange = useCallback(
    (value: number[]) => {
      onFiltersChange({ nAcctsMin: value[0] })
    },
    [onFiltersChange],
  )

  const handleMedNYearsChange = useCallback(
    (value: number[]) => {
      onFiltersChange({ medNYearsMin: value[0] })
    },
    [onFiltersChange],
  )

  return (
    <aside
      className={cn(
        "h-full border-r border-border bg-sidebar transition-all duration-300 flex flex-col",
        isOpen ? "w-72" : "w-12",
      )}
    >
      {/* Header */}
      <div className="p-3 border-b border-sidebar-border flex items-center justify-between">
        {isOpen && (
          <div className="flex items-center gap-2">
            <SlidersHorizontal className="h-4 w-4 text-sidebar-foreground" />
            <span className="font-medium text-sm text-sidebar-foreground">Filters</span>
          </div>
        )}
        <Button
          variant="ghost"
          size="icon"
          onClick={onToggle}
          className="h-7 w-7 text-sidebar-foreground hover:bg-sidebar-accent"
          aria-label={isOpen ? "Collapse filters" : "Expand filters"}
        >
          {isOpen ? <ChevronLeft className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
        </Button>
      </div>

      {/* Content */}
      {isOpen && (
        <div className="flex-1 overflow-y-auto custom-scrollbar p-4 space-y-6">
          {/* Reliability Filter - Moved to Advanced */}

          {/* Map Color Mode */}
          <div className="space-y-3 pt-2 border-t border-border/50">
            <Label className="text-sm font-medium text-sidebar-foreground">Map Color Mode</Label>
            <div className="grid grid-cols-2 gap-2 p-1 bg-secondary/50 rounded-lg">
              <button
                onClick={() => onFiltersChange({ colorMode: "growth" })}
                className={cn(
                  "px-2 py-1.5 text-xs font-medium rounded-md transition-all",
                  filters.colorMode === "growth"
                    ? "bg-background text-foreground shadow-sm"
                    : "text-muted-foreground hover:text-foreground"
                )}
              >
                Δ vs 2025
              </button>
              <button
                onClick={() => onFiltersChange({ colorMode: "value" })}
                className={cn(
                  "px-2 py-1.5 text-xs font-medium rounded-md transition-all",
                  filters.colorMode === "value"
                    ? "bg-background text-foreground shadow-sm"
                    : "text-muted-foreground hover:text-foreground"
                )}
              >
                Prop. Value $
              </button>
            </div>
            <p className="text-[10px] text-muted-foreground">
              {filters.colorMode === "growth"
                ? "Change in value relative to 2025 baseline"
                : "Colors represent raw property values"}
            </p>
          </div>

          {/* Advanced Filters (Collapsible) */}
          <details className="group">
            <summary className="text-xs font-medium text-muted-foreground cursor-pointer hover:text-foreground list-none flex items-center gap-2 mb-3">
              <ChevronRight className="h-3 w-3 transition-transform group-open:rotate-90" />
              Advanced Filters
            </summary>

            <div className="pl-2 space-y-6 border-l border-border/50 ml-1.5 pt-2">


              {/* Layer Override */}
              <div className="space-y-2">
                <Label className="text-sm font-medium text-sidebar-foreground">Layer Override</Label>
                <Select
                  value={filters.layerOverride?.toString() || "auto"}
                  onValueChange={(value) =>
                    onFiltersChange({
                      layerOverride: value === "auto" ? undefined : Number.parseInt(value, 10),
                    })
                  }
                >
                  <SelectTrigger className="w-full h-8 text-xs bg-sidebar-accent border-sidebar-border">
                    <SelectValue placeholder="Auto (by zoom)" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="auto">Auto (by zoom)</SelectItem>
                    <SelectItem value="6">H3 Resolution 6</SelectItem>
                    <SelectItem value="7">H3 Resolution 7</SelectItem>
                    <SelectItem value="8">H3 Resolution 8</SelectItem>
                    <SelectItem value="9">H3 Resolution 9</SelectItem>
                  </SelectContent>
                </Select>
                <p className="text-[10px] text-muted-foreground">Override automatic zoom-based layer switching</p>
              </div>
            </div>


          </details>
        </div>
      )}

      {/* Reset Button */}
      {isOpen && (
        <div className="p-3 border-t border-sidebar-border">
          <Button variant="outline" size="sm" onClick={onReset} className="w-full text-xs bg-transparent">
            <RotateCcw className="h-3 w-3 mr-2" />
            Reset Filters
          </Button>
        </div>
      )}
    </aside>
  )
}
