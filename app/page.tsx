"use client"

import { useState, useCallback, Suspense } from "react"
import { TopBar } from "@/components/top-bar"
import { FiltersPanel } from "@/components/filters-panel"
import { MapView } from "@/components/map-view"
import H3Map from "@/components/h3-map"
import { Legend } from "@/components/legend"
import { InspectorDrawer } from "@/components/inspector-drawer"
import { ForecastChart } from "@/components/forecast-chart"
import { useFilters } from "@/hooks/use-filters"
import { useMapState } from "@/hooks/use-map-state"
import { useToast } from "@/hooks/use-toast"
import type { PropertyForecast } from "@/app/actions/property-forecast"
import { TimeControls } from "@/components/time-controls"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { AlertCircle } from "lucide-react"
import { geocodeAddress } from "@/app/actions/geocode"

function DashboardContent() {
  const { filters, setFilters, resetFilters } = useFilters()
  const { mapState, setMapState, selectFeature, hoverFeature } = useMapState()
  const [isFiltersPanelOpen, setIsFiltersPanelOpen] = useState(true)
  const [forecastData, setForecastData] = useState<{ acct: string; data: PropertyForecast[] } | null>(null)
  const [currentYear, setCurrentYear] = useState(2026)
  const [isUsingMockData, setIsUsingMockData] = useState(false)
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

  const handleMockDataDetected = useCallback(() => {
    if (!isUsingMockData) {
      setIsUsingMockData(true)
      toast({
        title: "Database Quota Exceeded",
        description: "Displaying mock data. Please upgrade your Supabase plan or contact support.",
        variant: "destructive",
        duration: 10000,
      })
    }
  }, [isUsingMockData, toast])

  return (
    <div className="h-screen flex flex-col">
      {/* Top Bar */}
      <TopBar
        filters={filters}
        isFiltersPanelOpen={isFiltersPanelOpen}
        onToggleFiltersPanel={handleToggleFiltersPanel}
        onTogglePMTiles={() => setFilters({ usePMTiles: !filters.usePMTiles })}
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
          currentZoom={mapState.zoom}
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
            {isUsingMockData && (
              <Alert
                variant="destructive"
                className="absolute top-4 left-1/2 -translate-x-1/2 z-50 w-auto max-w-2xl shadow-lg"
              >
                <AlertCircle className="h-4 w-4" />
                <AlertTitle>Database Quota Exceeded</AlertTitle>
                <AlertDescription>
                  Displaying mock data. Contact Supabase support at{" "}
                  <a href="https://supabase.help" target="_blank" rel="noopener noreferrer" className="underline">
                    supabase.help
                  </a>
                </AlertDescription>
              </Alert>
            )}

            {filters.usePMTiles ? (
              <div className="absolute inset-0 z-0">
                <H3Map year={currentYear} colorMode={filters.colorMode} />
                <div className="absolute bottom-4 left-4 bg-card/95 backdrop-blur-sm border border-border rounded-lg px-3 py-2 text-xs text-muted-foreground z-40 shadow-lg font-mono">
                  <div className="font-semibold text-foreground mb-1">PMTiles Mode</div>
                  <div>Rendering from local MVT</div>
                </div>
              </div>
            ) : (
              <MapView
                filters={filters}
                mapState={mapState}
                onFeatureSelect={selectFeature}
                onFeatureHover={hoverFeature}
                year={currentYear}
                onMockDataDetected={handleMockDataDetected}
              />
            )}

            {/* Legend Overlay */}
            <Legend
              className="absolute bottom-4 left-4 z-50"
              colorMode={filters.colorMode}
            />

            {/* Time Controls Overlay */}
            <div className="absolute top-4 right-4 z-50">
              <TimeControls
                minYear={2019}
                maxYear={2030}
                currentYear={currentYear}
                onChange={setCurrentYear}
                onPlayStart={() => {
                  console.log("[PAGE] Play started - prefetch all years triggered")
                }}
              />
            </div>
          </div>
        </main>

        {/* Inspector Drawer */}
        <InspectorDrawer selectedId={mapState.selectedId} onClose={handleCloseInspector} year={currentYear} />
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
