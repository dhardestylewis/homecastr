"use client"

import { useState, useCallback, Suspense } from "react"
import { TopBar } from "@/components/top-bar"
import { FiltersPanel } from "@/components/filters-panel"
import { MapView } from "@/components/map-view"
import { Legend } from "@/components/legend"
import { InspectorDrawer } from "@/components/inspector-drawer"
import { ForecastChart } from "@/components/forecast-chart"
import { useFilters } from "@/hooks/use-filters"
import { useMapState } from "@/hooks/use-map-state"
import { useToast } from "@/hooks/use-toast"
import type { PropertyForecast } from "@/app/actions/property-forecast"
import { TimeControls } from "@/components/time-controls"
import { geocodeAddress } from "@/app/actions/geocode"

function DashboardContent() {
  const { filters, setFilters, resetFilters } = useFilters()
  const { mapState, setMapState, selectFeature, hoverFeature } = useMapState()
  const [isFiltersPanelOpen, setIsFiltersPanelOpen] = useState(true)
  const [forecastData, setForecastData] = useState<{ acct: string; data: PropertyForecast[] } | null>(null)
  const [currentYear, setCurrentYear] = useState(2026)
  const { toast } = useToast()

  const handleForecastLoaded = useCallback(
    (acct: string, forecast: PropertyForecast[]) => {
      setForecastData({ acct, data: forecast })
      toast({
        title: "Forecast loaded",
        description: `Found ${forecast.length} years of data for account ${acct}`,
      })
    },
    [toast],
  )

  const handleSearchError = useCallback(
    (error: string) => {
      toast({
        title: "Search failed",
        description: error,
        variant: "destructive",
      })
    },
    [toast],
  )

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
        onSearch={async (query) => {
          try {
            const result = await geocodeAddress(query)
            if (result) {
              setMapState({
                center: [result.lng, result.lat],
                zoom: 14 // Good zoom for neighborhood context
              })
              toast({ title: "Found Address", description: result.displayName })
            } else {
              handleSearchError(`Address not found: ${query}`)
            }
          } catch (e) {
            handleSearchError("Search failed")
          }
        }}
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
        <main className="flex-1 relative flex flex-col">
          <div className="p-4 border-b border-border bg-card/50 backdrop-blur-sm">
            {forecastData && (
              <div className="mt-4">
                <ForecastChart acct={forecastData.acct} data={forecastData.data} />
              </div>
            )}
          </div>

          <div className="flex-1 relative">
            <MapView
              filters={filters}
              mapState={mapState}
              onFeatureSelect={selectFeature}
              onFeatureHover={hoverFeature}
              year={currentYear}
            />

            {/* Legend Overlay */}
            <Legend className="absolute bottom-4 left-4 z-50" />

            {/* Time Controls Overlay */}
            <div className="absolute top-4 right-4 z-50">
              <TimeControls
                minYear={2019}
                maxYear={2032}
                currentYear={currentYear}
                onChange={setCurrentYear}
                onPlayStart={() => {
                  console.log('[PAGE] Play started - prefetch all years triggered')
                  // Note: actual prefetch happens in MapView's cache
                  // This is just a notification that playback has begun
                }}
              />
            </div>
          </div>
        </main>

        {/* Inspector Drawer */}
        <InspectorDrawer
          selectedId={mapState.selectedId}
          onClose={handleCloseInspector}
          year={currentYear}
        />
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
