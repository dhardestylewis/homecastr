"use client"

import { useState, useCallback, Suspense, useEffect } from "react"
import { MapView } from "@/components/map-view"
import H3Map from "@/components/h3-map"
import { Legend } from "@/components/legend"

import { SearchBox } from "@/components/search-box"
import { useFilters } from "@/hooks/use-filters"
import { useMapState } from "@/hooks/use-map-state"
import { useToast } from "@/hooks/use-toast"
import type { PropertyForecast } from "@/app/actions/property-forecast"
import { TimeControls } from "@/components/time-controls"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { AlertCircle, Plus, Minus, RotateCcw, ArrowLeftRight, Copy } from "lucide-react"
import { geocodeAddress, reverseGeocode } from "@/app/actions/geocode"

import { cellToLatLng } from "h3-js"
import { ExplainerPopup } from "@/components/explainer-popup"

function DashboardContent() {
  const { filters, setFilters, resetFilters } = useFilters()
  const { mapState, setMapState, selectFeature, hoverFeature } = useMapState()
  const [forecastData, setForecastData] = useState<{ acct: string; data: PropertyForecast[] } | null>(null)
  const [currentYear, setCurrentYear] = useState(2026)
  const [isUsingMockData, setIsUsingMockData] = useState(false)
  const [searchBarValue, setSearchBarValue] = useState<string>("")
  const [mobileSelectionMode, setMobileSelectionMode] = useState<'replace' | 'add' | 'range'>('replace')
  const { toast } = useToast()

  // Reverse Geocode Effect
  useEffect(() => {
    if (!mapState.selectedId) {
      setSearchBarValue("")
      return
    }

    // Do NOT show raw ID. Show "..." or nothing while loading.
    setSearchBarValue("Loading location...")

    const fetchAddress = async () => {
      try {
        const [lat, lng] = cellToLatLng(mapState.selectedId!)
        const address = await reverseGeocode(lat, lng)
        if (address) {
          setSearchBarValue(address)
        } else {
          setSearchBarValue("Unknown Location")
        }
      } catch (e) {
        console.error("Reverse geocode failed", e)
        setSearchBarValue("")
      }
    }
    fetchAddress()
  }, [mapState.selectedId])

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

  const handleSearch = useCallback(async (query: string) => {
    try {
      const result = await geocodeAddress(query)
      if (result) {
        setMapState({
          center: [result.lng, result.lat],
          zoom: 14
        })
        toast({ title: "Found Address", description: result.displayName })
      } else {
        handleSearchError(`Address not found: ${query}`)
      }
    } catch (e) {
      handleSearchError("Search failed")
    }
  }, [setMapState, toast, handleSearchError])



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

  const handleColorModeChange = useCallback((mode: "growth" | "value") => {
    setFilters({ colorMode: mode })
  }, [setFilters])

  return (
    <div className="h-dvh flex flex-col">
      {/* Full-screen Map Container */}
      <main className="flex-1 relative h-full w-full">
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
          </div>
        ) : (
          <MapView
            filters={filters}
            mapState={mapState}
            onFeatureSelect={selectFeature}
            onFeatureHover={hoverFeature}
            year={currentYear}
            onMockDataDetected={handleMockDataDetected}
            onYearChange={setCurrentYear}
            mobileSelectionMode={mobileSelectionMode}
            onMobileSelectionModeChange={setMobileSelectionMode}
          />
        )}

        {/* Unified Sidebar Container - Top Left */}
        <div className="absolute top-4 left-4 z-[60] flex flex-col gap-3 w-full max-w-[calc(100vw-32px)] md:w-[320px]">
          {/* ... (SearchBox and TimeControls remain same) */}
          <SearchBox
            onSearch={handleSearch}
            placeholder="Search address or ID..."
            value={searchBarValue}
          />

          <TimeControls
            minYear={2019}
            maxYear={2030}
            currentYear={currentYear}
            onChange={setCurrentYear}
            onPlayStart={() => {
              console.log("[PAGE] Play started - prefetch all years triggered")
            }}
            className="w-full"
          />

          {/* 3. Legend & (Selection Buttons + Vertical Zoom Controls) Row */}
          <div className="flex flex-row gap-2 items-stretch h-full">
            {/* Legend - Takes up available space */}
            <Legend
              className="flex-1"
              colorMode={filters.colorMode}
              onColorModeChange={handleColorModeChange}
            />

            {/* Controls Column: Grid on Mobile, Flex Col on Desktop */}
            <div className="grid grid-cols-2 gap-2 md:flex md:flex-col md:w-10 shrink-0">

              {/* Mobile Selection Buttons (Hidden on Desktop) */}
              <div className="flex flex-col gap-2 md:hidden w-10">
                <button
                  onClick={() => setMobileSelectionMode('replace')}
                  className={`w-10 h-10 rounded-lg flex items-center justify-center transition-colors shadow-sm font-bold text-xs ${mobileSelectionMode === 'replace' ? "bg-primary text-primary-foreground" : "glass-panel text-foreground"}`}
                  title="Single Select"
                >
                  1
                </button>
                <button
                  onClick={() => setMobileSelectionMode('add')}
                  className={`w-10 h-10 rounded-lg flex items-center justify-center transition-colors shadow-sm font-bold text-xs ${mobileSelectionMode === 'add' ? "bg-primary text-primary-foreground" : "glass-panel text-foreground"}`}
                  title="Multi Select (Add)"
                >
                  <Copy className="h-4 w-4" />
                </button>
                <button
                  onClick={() => setMobileSelectionMode('range')}
                  className={`w-10 h-10 rounded-lg flex items-center justify-center transition-colors shadow-sm font-bold text-xs ${mobileSelectionMode === 'range' ? "bg-primary text-primary-foreground" : "glass-panel text-foreground"}`}
                  title="Range Select"
                >
                  <ArrowLeftRight className="h-4 w-4" />
                </button>
              </div>

              {/* Vertical Zoom Controls (Always Visible, Col 2 on Mobile) */}
              <div className="flex flex-col gap-2 w-10">
                <button
                  onClick={() => {
                    setMapState({ zoom: Math.min(18, mapState.zoom + 1) })
                  }}
                  className="w-10 h-10 glass-panel rounded-lg flex items-center justify-center text-foreground hover:bg-accent transition-colors shadow-sm active:scale-95"
                  aria-label="Zoom In"
                >
                  <Plus className="h-5 w-5" />
                </button>
                <button
                  onClick={() => {
                    setMapState({ zoom: Math.max(9, mapState.zoom - 1) })
                  }}
                  className="w-10 h-10 glass-panel rounded-lg flex items-center justify-center text-foreground hover:bg-accent transition-colors shadow-sm active:scale-95"
                  aria-label="Zoom Out"
                >
                  <Minus className="h-5 w-5" />
                </button>
                <button
                  onClick={() => {
                    setMapState({
                      center: [-95.3698, 29.7604],
                      zoom: 11,
                      selectedId: null
                    })
                    resetFilters()
                  }}
                  className="w-10 h-10 glass-panel rounded-lg flex items-center justify-center text-foreground hover:bg-accent transition-colors shadow-sm active:scale-95"
                  aria-label="Reset Map"
                  title="Reset Map"
                >
                  <RotateCcw className="h-4 w-4" />
                </button>
              </div>
            </div>
          </div>
        </div>




        <ExplainerPopup />
      </main>
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
