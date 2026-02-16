"use client"

import React, { useState, useCallback, Suspense, useEffect } from "react"
import { MapView } from "@/components/map-view"
import { VectorMap } from "@/components/vector-map"
import H3Map from "@/components/h3-map"
import { Legend } from "@/components/legend"
import { cn } from "@/lib/utils"

import { SearchBox } from "@/components/search-box"
import { useFilters } from "@/hooks/use-filters"
import { useMapState } from "@/hooks/use-map-state"
import { useToast } from "@/hooks/use-toast"
import type { PropertyForecast } from "@/app/actions/property-forecast"
import { TimeControls } from "@/components/time-controls"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { AlertCircle, Plus, Minus, RotateCcw, ArrowLeftRight, Copy, Bot } from "lucide-react"
import { geocodeAddress, reverseGeocode } from "@/app/actions/geocode"

import { cellToLatLng, latLngToCell } from "h3-js"
import { getH3CellDetails } from "@/app/actions/h3-details"
import { ExplainerPopup } from "@/components/explainer-popup"
import { ChatPanel, type MapAction } from "@/components/chat-panel"
import { MessageSquare } from "lucide-react"
import { createTavusConversation } from "@/app/actions/tavus"
import dynamic from "next/dynamic"

// Dynamic import with SSR disabled — daily-js needs browser APIs
const TavusMiniWindow = dynamic(
  () => import("@/components/tavus-mini-window").then((mod) => mod.TavusMiniWindow),
  { ssr: false }
) as React.ComponentType<{ conversationUrl: string; onClose: () => void; chatOpen?: boolean }>

function DashboardContent() {
  const { filters, setFilters, resetFilters } = useFilters()
  const { mapState, setMapState, selectFeature, hoverFeature } = useMapState()
  const [forecastData, setForecastData] = useState<{ acct: string; data: PropertyForecast[] } | null>(null)
  const [currentYear, setCurrentYear] = useState(2026)
  const [isUsingMockData, setIsUsingMockData] = useState(false)
  const [searchBarValue, setSearchBarValue] = useState<string>("")
  const [mobileSelectionMode, setMobileSelectionMode] = useState<'replace' | 'add' | 'range'>('replace')
  const [isChatOpen, setIsChatOpen] = useState(false)
  const { toast } = useToast()

  // Tavus Homecastr state
  const [tavusConversationUrl, setTavusConversationUrl] = useState<string | null>(null)
  const [isTavusLoading, setIsTavusLoading] = useState(false)

  // Handle map actions from chat (smooth fly-to)
  const handleChatMapAction = useCallback((action: MapAction) => {
    setMapState({
      center: [action.lng, action.lat],
      zoom: action.zoom,
      ...(action.select_hex_id ? { selectedId: action.select_hex_id } : {}),
      ...(action.highlighted_hex_ids ? { highlightedIds: action.highlighted_hex_ids } : {}),
    })
    toast({
      title: "Map updated",
      description: `Navigating to ${action.lat.toFixed(4)}, ${action.lng.toFixed(4)}`,
      duration: 2000,
    })
  }, [setMapState, toast])

  // Listen for Tavus tool events (dispatched from window by TavusMiniWindow)
  useEffect(() => {
    const handleTavusAction = (e: Event) => {
      const detail = (e as CustomEvent).detail
      if (detail.type === "fly_to") {
        setMapState({
          center: [detail.lng, detail.lat],
          zoom: detail.zoom,
          ...(detail.select_hex_id ? { selectedId: detail.select_hex_id } : {}),
        })
        toast({ title: "Homecastr Agent", description: "Moving map..." })
      }
      // Add 'rank' logic here if needed (uses search tool under the hood)
    }
    window.addEventListener("tavus-map-action", handleTavusAction)
    return () => window.removeEventListener("tavus-map-action", handleTavusAction)
  }, [setMapState, toast])

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

  /* Homecastr handler */
  const handleConsultAI = useCallback(async (details: {
    predictedValue: number | null
    opportunityScore: number | null
    capRate: number | null
  }) => {
    if (isTavusLoading) return

    setIsTavusLoading(true)
    try {
      const result = await createTavusConversation({
        predictedValue: details.predictedValue,
        opportunityScore: details.opportunityScore,
        capRate: details.capRate,
        address: searchBarValue && !searchBarValue.includes("Loading") ? searchBarValue : "this neighborhood",
      })

      if (result.error || !result.conversation_url) {
        throw new Error(result.error || "Failed to create conversation")
      }

      setTavusConversationUrl(result.conversation_url)
    } catch (err) {
      console.error("[TAVUS] Failed to create conversation:", err)
      toast({
        title: "Homecastr Unavailable",
        description: err instanceof Error ? err.message : "Could not connect to Homecastr agent.",
        variant: "destructive",
      })
    } finally {
      setIsTavusLoading(false)
    }
  }, [isTavusLoading, toast])

  /* Floating button handler */
  const handleFloatingConsultAI = useCallback(async () => {
    if (isTavusLoading) return

    setIsTavusLoading(true)
    try {
      // Try to use selected hex first, otherwise derive from map center
      let h3Id = mapState.selectedId
      if (!h3Id) {
        const [lng, lat] = mapState.center
        h3Id = latLngToCell(lat, lng, 8)
      }

      // Fetch details from Supabase
      const details = await getH3CellDetails(h3Id, currentYear)

      const result = await createTavusConversation({
        predictedValue: details?.proforma?.predicted_value ?? null,
        opportunityScore: details?.opportunity?.value ?? null,
        capRate: details?.proforma?.cap_rate ?? null,
        address: searchBarValue && !searchBarValue.includes("Loading") && !searchBarValue.startsWith("8") ? searchBarValue : "this neighborhood",
      })

      if (result.error || !result.conversation_url) {
        throw new Error(result.error || "Failed to create conversation")
      }

      setTavusConversationUrl(result.conversation_url)
    } catch (err) {
      console.error("[TAVUS] Failed to create conversation:", err)
      toast({
        title: "Homecastr Unavailable",
        description: err instanceof Error ? err.message : "Could not connect to Homecastr agent.",
        variant: "destructive",
      })
    } finally {
      setIsTavusLoading(false)
    }
  }, [isTavusLoading, toast, mapState.selectedId, mapState.center, currentYear])

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

        {filters.useVectorMap ? (
          <VectorMap
            filters={filters}
            mapState={mapState}
            onFeatureSelect={selectFeature}
            onFeatureHover={hoverFeature}
            year={currentYear}
            className="absolute inset-0 z-0"
            onConsultAI={handleConsultAI}
          />
        ) : filters.usePMTiles ? (
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
            onConsultAI={handleConsultAI}
          />
        )}

        {/* Chat Panel Overlay */}
        <ChatPanel
          isOpen={isChatOpen}
          onClose={() => setIsChatOpen(false)}
          onMapAction={handleChatMapAction}
        />

        {/* Unified Sidebar Container - Top Left */}
        <div className={`absolute top-4 left-4 z-[60] flex flex-col gap-3 w-full max-w-[calc(100vw-32px)] md:w-[320px] transition-all duration-300 ${isChatOpen ? 'md:left-[416px]' : ''}`}>
          {/* Search + Chat Toggle Row */}
          <div className="flex items-center gap-2">
            <SearchBox
              onSearch={handleSearch}
              placeholder="Search address or ID..."
              value={searchBarValue}
            />
            <button
              onClick={() => setIsChatOpen(!isChatOpen)}
              className={cn(
                "w-10 h-10 rounded-lg flex items-center justify-center transition-all shadow-sm shrink-0",
                isChatOpen
                  ? "bg-primary text-primary-foreground"
                  : "glass-panel text-foreground hover:bg-accent"
              )}
              aria-label={isChatOpen ? "Close chat" : "Open AI chat"}
              title="Chat with Homecastr AI"
            >
              <MessageSquare className="h-4 w-4" />
            </button>
          </div>

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

          {/* Migration Toggle */}
          <button
            onClick={() => setFilters({ useVectorMap: !filters.useVectorMap })}
            className={cn(
              "w-full py-1.5 px-3 rounded-lg border text-[10px] font-bold uppercase tracking-wider transition-all shadow-sm flex items-center justify-between",
              filters.useVectorMap
                ? "bg-primary/20 border-primary/50 text-primary"
                : "bg-muted/30 border-border/50 text-muted-foreground hover:bg-muted/50"
            )}
          >
            <span>Tile Engine: {filters.useVectorMap ? "New (Vector)" : "Classic (H3)"}</span>
            <div className={cn(
              "w-2 h-2 rounded-full",
              filters.useVectorMap ? "bg-primary animate-pulse" : "bg-muted-foreground/30"
            )} />
          </button>

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

        {/* Floating Homecastr Live Agent Button — always visible, bottom-left, shifts when chat open */}
        {!tavusConversationUrl && !isTavusLoading && (
          <button
            onClick={handleFloatingConsultAI}
            className={cn(
              "fixed bottom-5 z-[9999] flex items-center gap-2.5 px-5 py-3 rounded-2xl bg-[#16161e] hover:bg-[#1e1e2a] border border-white/15 hover:border-primary/40 text-white shadow-2xl transition-all duration-300 hover:scale-105 active:scale-95 group",
              isChatOpen ? "left-[420px]" : "left-5"
            )}
          >
            <div className="w-8 h-8 rounded-full bg-primary/20 flex items-center justify-center group-hover:bg-primary/30 transition-colors">
              <Bot className="w-4 h-4 text-primary" />
            </div>
            <div className="flex flex-col items-start">
              <span className="text-xs font-semibold text-white/90">Talk to Homecastr Live Agent</span>
              <span className="text-[10px] text-white/40">Powered by Tavus</span>
            </div>
          </button>
        )}

        {/* Homecastr Loading Indicator */}
        {isTavusLoading && (
          <div className={cn(
            "fixed bottom-5 z-[10000] bg-[#16161e] text-white/90 rounded-2xl px-5 py-4 shadow-2xl border border-white/10 flex items-center gap-3 transition-all duration-300",
            isChatOpen ? "left-[420px]" : "left-5"
          )}>
            <div className="w-5 h-5 border-2 border-primary/30 border-t-primary rounded-full animate-spin" />
            <span className="text-xs font-medium">Connecting to Homecastr...</span>
          </div>
        )}

        {/* Tavus AI Analyst Mini Window */}
        {tavusConversationUrl && !isTavusLoading && (
          <TavusMiniWindow
            conversationUrl={tavusConversationUrl}
            onClose={() => setTavusConversationUrl(null)}
            chatOpen={isChatOpen}
          />
        )}
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
