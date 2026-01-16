"use client"

import { Building2, Menu, X } from "lucide-react"
import { Button } from "@/components/ui/button"
import { SearchBox } from "./search-box"
import type { FilterState } from "@/lib/types"

interface TopBarProps {
  filters: FilterState
  isFiltersPanelOpen: boolean
  onToggleFiltersPanel: () => void
  onSearch: (query: string) => void
  onTogglePMTiles: () => void
}

export function TopBar({ filters, isFiltersPanelOpen, onToggleFiltersPanel, onSearch, onTogglePMTiles }: TopBarProps) {
  // Count active filters
  const activeFilterCount = [
    filters.reliabilityMin > 0,
    filters.nAcctsMin > 0,
    filters.medNYearsMin > 0,
    !filters.showUnderperformers,
    filters.layerOverride !== undefined,
  ].filter(Boolean).length

  return (
    <header className="h-14 border-b border-border bg-background/95 backdrop-blur-sm flex items-center px-4 gap-4 z-50">
      {/* Logo and Brand */}
      <div className="flex items-center gap-2">
        <Button
          variant="ghost"
          size="icon"
          className="md:hidden"
          onClick={onToggleFiltersPanel}
          aria-label={isFiltersPanelOpen ? "Close filters" : "Open filters"}
        >
          {isFiltersPanelOpen ? <X className="h-5 w-5" /> : <Menu className="h-5 w-5" />}
        </Button>
        <Building2 className="h-6 w-6 text-primary" />
        <span className="font-semibold text-lg hidden sm:inline">InvestMap</span>
      </div>

      {/* Search */}
      <div className="flex-1 max-w-md">
        <SearchBox onSearch={onSearch} />
      </div>

      {/* Filter Summary */}
      <div className="hidden md:flex items-center gap-2 text-sm text-muted-foreground">
        <div className="flex items-center gap-2 mr-4 border-r border-border pr-4">
          <span className="text-xs font-medium">Map Mode:</span>
          <Button
            variant={filters.usePMTiles ? "default" : "outline"}
            size="sm"
            className="h-6 text-xs px-2"
            onClick={onTogglePMTiles}
          >
            {filters.usePMTiles ? "PMTiles (New)" : "Legacy (DB)"}
          </Button>
        </div>
        {activeFilterCount > 0 && (
          <span className="px-2 py-1 bg-primary/10 text-primary rounded-md text-xs font-medium">
            {activeFilterCount} filter{activeFilterCount !== 1 ? "s" : ""} active
          </span>
        )}
        {filters.reliabilityMin > 0 && (
          <span className="px-2 py-0.5 bg-secondary rounded text-xs">
            R ≥ {(filters.reliabilityMin * 100).toFixed(0)}%
          </span>
        )}
        {filters.nAcctsMin > 0 && (
          <span className="px-2 py-0.5 bg-secondary rounded text-xs">n ≥ {filters.nAcctsMin}</span>
        )}
      </div>
    </header>
  )
}
