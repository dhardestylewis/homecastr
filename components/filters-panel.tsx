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
}

const RELIABILITY_BINS = [
  { value: 0, label: "All", description: "Show all reliability levels" },
  { value: 0.2, label: "≥20%", description: "Low and above" },
  { value: 0.4, label: "≥40%", description: "Moderate and above" },
  { value: 0.6, label: "≥60%", description: "High and above" },
  { value: 0.8, label: "≥80%", description: "Very high only" },
]

export function FiltersPanel({ filters, onFiltersChange, onReset, isOpen, onToggle }: FiltersPanelProps) {
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
          {/* Reliability Filter */}
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <Label className="text-sm font-medium text-sidebar-foreground">
                Reliability Minimum
              </Label>
              <span className="text-xs text-muted-foreground font-mono">
                {filters.reliabilityMin > 0 ? `≥${(filters.reliabilityMin * 100).toFixed(0)}%` : "All"}
              </span>
            </div>
            <Slider
              id="reliability-min"
              name="reliability-min"
              value={[filters.reliabilityMin]}
              onValueChange={handleReliabilityChange}
              min={0}
              max={0.8}
              step={0.2}
              className="w-full"
              aria-label="Reliability minimum"
            />
            <div className="flex justify-between text-[10px] text-muted-foreground px-0.5">
              {RELIABILITY_BINS.slice(0, -1).map((bin) => (
                <span key={bin.value}>{bin.label}</span>
              ))}
            </div>
          </div>

          {/* Support Filters */}
          <div className="space-y-4">
            <Label className="text-sm font-medium text-sidebar-foreground">Support Filters</Label>

            {/* Accounts minimum */}
            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <span className="text-xs text-muted-foreground">Min Accounts</span>
                <span className="text-xs text-muted-foreground font-mono">{filters.nAcctsMin || "None"}</span>
              </div>
              <Slider
                id="n-accts-min"
                name="n-accts-min"
                value={[filters.nAcctsMin]}
                onValueChange={handleNAcctsChange}
                min={0}
                max={100}
                step={5}
                className="w-full"
                aria-label="Minimum accounts"
              />
            </div>

            {/* HIDDEN: Min Med Years filter - No med_years column exists in database
            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <span className="text-xs text-muted-foreground">Min Med Years</span>
                <span className="text-xs text-muted-foreground font-mono">
                  {filters.medNYearsMin > 0 ? `≥${filters.medNYearsMin}` : "None"}
                </span>
              </div>
              <Slider
                id="med-n-years-min"
                name="med-n-years-min"
                value={[filters.medNYearsMin]}
                onValueChange={handleMedNYearsChange}
                min={0}
                max={10}
                step={0.5}
                className="w-full"
                aria-label="Minimum median years"
              />
            </div>
            */}
          </div>

          {/* Toggles */}
          <div className="space-y-4">
            <Label className="text-sm font-medium text-sidebar-foreground">Display Options</Label>

            <div className="flex items-center justify-between">
              <Label htmlFor="show-underperformers" className="text-xs text-muted-foreground cursor-pointer">
                Show underperformers
              </Label>
              <Switch
                id="show-underperformers"
                checked={filters.showUnderperformers}
                onCheckedChange={(checked) => onFiltersChange({ showUnderperformers: checked })}
              />
            </div>

            <div className="flex items-center justify-between">
              <Label htmlFor="highlight-warnings" className="text-xs text-muted-foreground cursor-pointer">
                Highlight warnings
              </Label>
              <Switch
                id="highlight-warnings"
                checked={filters.highlightWarnings}
                onCheckedChange={(checked) => onFiltersChange({ highlightWarnings: checked })}
              />
            </div>
          </div>

          {/* Layer Override (Advanced) */}
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
