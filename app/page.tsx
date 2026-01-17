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
import { AlertCircle } from "lucide-react"
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
          />
        )}

        {/* Floating Search Bar - Top Left */}
        <div className="absolute top-2 left-4 right-4 md:right-auto md:w-auto z-[60]">
          <SearchBox
            onSearch={handleSearch}
            placeholder="Search address or ID..."
            value={searchBarValue}
          />
        </div>

        {/* Time Controls - Top Right */}
        <div className="absolute top-[56px] left-4 right-4 md:top-4 md:right-4 md:left-auto md:w-auto z-50">
          <TimeControls
            minYear={2019}
            maxYear={2030}
            currentYear={currentYear}
            onChange={setCurrentYear}
            onPlayStart={() => {
              console.log("[PAGE] Play started - prefetch all years triggered")
            }}
            className="w-full md:w-[320px]"
          />
        </div>

        <Legend
          className="absolute top-[104px] left-4 md:top-auto md:bottom-4 z-50"
          colorMode={filters.colorMode}
          onColorModeChange={handleColorModeChange}
        />


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
