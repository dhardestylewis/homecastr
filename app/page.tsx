"use client"

import { useState, useCallback, Suspense } from "react"
import { TopBar } from "@/components/top-bar"
import { FiltersPanel } from "@/components/filters-panel"
import { MapView } from "@/components/map-view"
import { Legend } from "@/components/legend"
import { InspectorDrawer } from "@/components/inspector-drawer"
import { useFilters } from "@/hooks/use-filters"
import { useMapState } from "@/hooks/use-map-state"

function DashboardContent() {
  const { filters, setFilters, resetFilters } = useFilters()
  const { mapState, selectFeature, hoverFeature } = useMapState()
  const [isFiltersPanelOpen, setIsFiltersPanelOpen] = useState(true)

  const handleSearch = useCallback((query: string) => {
    // TODO: Implement search - geocode address or lookup by ID
    console.log("Search:", query)
  }, [])

  const handleToggleFiltersPanel = useCallback(() => {
    setIsFiltersPanelOpen((prev) => !prev)
  }, [])

  const handleCloseInspector = useCallback(() => {
    selectFeature(null)
  }, [selectFeature])

  return (
    <div className="h-screen flex flex-col">
      {/* Top Bar */}
      <TopBar
        filters={filters}
        isFiltersPanelOpen={isFiltersPanelOpen}
        onToggleFiltersPanel={handleToggleFiltersPanel}
        onSearch={handleSearch}
      />

      {/* Main Content */}
      <div className="flex-1 flex overflow-hidden">
        {/* Filters Panel */}
        <FiltersPanel
          filters={filters}
          onFiltersChange={setFilters}
          onReset={resetFilters}
          isOpen={isFiltersPanelOpen}
          onToggle={handleToggleFiltersPanel}
        />

        {/* Map Container */}
        <main className="flex-1 relative">
          <MapView
            filters={filters}
            mapState={mapState}
            onFeatureSelect={selectFeature}
            onFeatureHover={hoverFeature}
          />

          {/* Legend Overlay */}
          <Legend className="absolute bottom-4 left-4 z-10" />

          {/* Zoom Level Indicator */}
          <div className="absolute top-4 left-4 z-10 glass-panel rounded-md px-2 py-1 text-xs font-mono text-muted-foreground">
            Zoom: {mapState.zoom.toFixed(1)}
          </div>
        </main>

        {/* Inspector Drawer */}
        <InspectorDrawer selectedId={mapState.selectedId} onClose={handleCloseInspector} />
      </div>
    </div>
  )
}

export default function Page() {
  return (
    <Suspense
      fallback={
        <div className="h-screen flex items-center justify-center bg-background">
          <div className="animate-pulse text-muted-foreground">Loading dashboard...</div>
        </div>
      }
    >
      <DashboardContent />
    </Suspense>
  )
}
