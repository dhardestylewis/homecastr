"use client"

import React, { useState, useCallback, Suspense, useEffect, useRef, useMemo } from "react"
import { MapView } from "@/components/map-view"
import { VectorMap } from "@/components/vector-map"
import { ForecastMap } from "@/components/forecast-map"
import H3Map from "@/components/h3-map"
import { Legend } from "@/components/legend"
import { cn, getZoomForRes } from "@/lib/utils"

import { SearchBox } from "@/components/search-box"
import { useFilters } from "@/hooks/use-filters"
import { useMapState } from "@/hooks/use-map-state"
import { useToast } from "@/hooks/use-toast"
import type { PropertyForecast } from "@/app/actions/property-forecast"
import { TimeControls } from "@/components/time-controls"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { AlertCircle, Plus, Minus, RotateCcw, ArrowLeftRight, Copy, Terminal, Activity, MessageSquare, Mic, CalendarDays, Link2, FileDown, Check, Search, ChevronUp, ChevronDown, X } from "lucide-react"
import { useSearchParams, useRouter } from "next/navigation"
import { geocodeAddress, reverseGeocode } from "@/app/actions/geocode"

import { cellToLatLng, latLngToCell } from "h3-js"
import { getH3CellDetails } from "@/app/actions/h3-details"
// import { ExplainerPopup } from "@/components/explainer-popup"  // Deactivated — replaced by OnboardingIntro
import { OnboardingIntro } from "@/components/onboarding-intro"
import { ChatPanel, type MapAction, type ChatPanelHandle } from "@/components/chat-panel"

import { createTavusConversation } from "@/app/actions/tavus"
import dynamic from "next/dynamic"
import { HomecastrLogo } from "@/components/homecastr-logo"
import { ContactModal } from "@/components/contact-modal"
import { generateForecastPDF } from "@/lib/generate-pdf"

// Dynamic import with SSR disabled — daily-js needs browser APIs
const TavusMiniWindow = dynamic(
  () => import("@/components/tavus-mini-window").then((mod) => mod.TavusMiniWindow),
  { ssr: false }
) as React.ComponentType<{ conversationUrl: string; onClose: () => void; chatOpen?: boolean; forecastMode?: boolean }>



function DashboardContent() {
  const router = useRouter()
  const searchParams = useSearchParams()
  const { filters, setFilters, resetFilters } = useFilters()
  const { mapState, setMapState, selectFeature, hoverFeature } = useMapState()
  const [forecastData, setForecastData] = useState<{ acct: string; data: PropertyForecast[] } | null>(null)
  const [currentYear, setCurrentYear] = useState(() => {
    const yrParam = searchParams.get("yr")
    return yrParam ? Math.max(2019, Math.min(2030, parseInt(yrParam, 10))) : 2027
  })
  const [hasManuallySetYear, setHasManuallySetYear] = useState(false)
  const [isUsingMockData, setIsUsingMockData] = useState(false)
  const [searchBarValue, setSearchBarValue] = useState<string>("")
  const [mobileSelectionMode, setMobileSelectionMode] = useState<'replace' | 'add' | 'range'>('replace')
  const [mobileFiltersOpen, setMobileFiltersOpen] = useState(false)
  const [mobileActionsOpen, setMobileActionsOpen] = useState(false)
  const [isMobileViewport, setIsMobileViewport] = useState(false)
  const chatPanelRef = useRef<ChatPanelHandle>(null)
  const [chatInput, setChatInput] = useState("")

  // Track mobile viewport
  useEffect(() => {
    const checkMobile = () => setIsMobileViewport(window.innerWidth < 768)
    checkMobile()
    window.addEventListener('resize', checkMobile)
    return () => window.removeEventListener('resize', checkMobile)
  }, [])
  const [compareMode, setCompareMode] = useState(false)
  const [pinnedCount, setPinnedCount] = useState(0)
  const [isChatOpen, setIsChatOpen] = useState(false)
  const [isContactOpen, setIsContactOpen] = useState(false)
  const [linkCopied, setLinkCopied] = useState(false)
  const { toast } = useToast()

  // Auto-open contact form and/or enable compare mode from URL params (post-hydration)
  useEffect(() => {
    const params = new URLSearchParams(window.location.search)
    if (params.get("contact") === "1") {
      setIsContactOpen(true)
      params.delete("contact")
      const newUrl = `${window.location.pathname}${params.toString() ? `?${params.toString()}` : ""}${window.location.hash}`
      window.history.replaceState({}, "", newUrl)
    }
    if (params.has("compare")) {
      setCompareMode(true)
    }
  }, [])

  // Sync currentYear to URL
  const prevYearRef = useRef(currentYear)
  useEffect(() => {
    if (prevYearRef.current === currentYear) return
    prevYearRef.current = currentYear
    const params = new URLSearchParams(window.location.search)
    if (currentYear !== 2027) {
      params.set("yr", currentYear.toString())
    } else {
      params.delete("yr")
    }
    router.replace(`?${params.toString()}`, { scroll: false })
  }, [currentYear, router])

  // Derive origin year from map viewport — same Harris County check as forecast-map.tsx
  // HCAD temporarily disabled — using ACS-only (origin_year=2024) everywhere
  // When re-enabled, HCAD will use schema=forecast_hcad (separate DB schema)
  // instead of toggling origin_year, to prevent accidental data source mixing.
  const [mapLng, mapLat] = mapState.center
  // const isHarrisCounty = mapLat >= 29.4 && mapLat <= 30.2 && mapLng >= -95.9 && mapLng <= -94.9
  const isHarrisCounty = false // HCAD off — ACS only
  const pageOriginYear = 2024 // ACS: origin_year=2024; HCAD (when re-enabled): forecast_hcad schema

  // Tavus Homecastr state
  const [tavusConversationUrl, setTavusConversationUrl] = useState<string | null>(null)
  const [isTavusLoading, setIsTavusLoading] = useState(false)

  // Handle map actions from chat (smooth fly-to)
  const handleChatMapAction = useCallback((action: MapAction) => {
    // Handle end_session action
    if ((action as any).action === 'end_session') {
      console.log('[PAGE] end_session from chat')
      setIsChatOpen(false)
      toast({ title: "Chat closed", duration: 2000 })
      return
    }

    // Handle clear_selection action
    if ((action as any).action === 'clear_selection') {
      console.log('[PAGE] clear_selection from chat')
      setMapState(prev => ({
        ...prev,
        selectedId: null,
        highlightedIds: undefined
      }))
      // Dispatch window event so forecast-map clears MapLibre feature state (border/highlight)
      window.dispatchEvent(new CustomEvent("tavus-map-action", {
        detail: { action: "clear_selection" }
      }))
      toast({ title: "Selection cleared", duration: 2000 })
      return
    }

    // Handle clear_comparison action (keep primary selection, clear comparison overlay)
    if ((action as any).action === 'clear_comparison') {
      console.log('[PAGE] clear_comparison from chat')
      window.dispatchEvent(new CustomEvent("tavus-map-action", {
        detail: { action: "clear_comparison" }
      }))
      toast({ title: "Comparison cleared", duration: 2000 })
      return
    }

    // Handle set_color_mode — switch between value and growth views
    if ((action as any).action === 'set_color_mode') {
      const mode = (action as any).mode === 'growth' ? 'growth' : 'value'
      console.log('[PAGE] set_color_mode from chat:', mode)
      setFilters({ colorMode: mode })
      toast({ title: `Map view: ${mode}`, duration: 2000 })
      return
    }

    // Handle set_forecast_year — change the timeline year
    if ((action as any).action === 'set_forecast_year') {
      const yr = Math.max(2019, Math.min(2030, (action as any).year || 2029))
      console.log('[PAGE] set_forecast_year from chat:', yr)
      setCurrentYear(yr)
      toast({ title: `Timeline set to ${yr}`, duration: 2000 })
      return
    }

    // Handle add_location_to_selection — keep primary selection, add comparison overlay
    if ((action as any).action === 'add_location_to_selection') {
      console.log('[PAGE] add_location_to_selection from chat', { lat: action.lat, lng: action.lng })
      window.dispatchEvent(new CustomEvent("tavus-map-action", {
        detail: {
          action: "add_location_to_selection",
          params: { lat: action.lat, lng: action.lng, zoom: action.zoom },
          result: {
            chosen: { lat: action.lat, lng: action.lng, label: "" },
            area: { id: (action as any).area_id, level: (action as any).level || "zcta" }
          }
        }
      }))
      toast({
        title: "Comparison added",
        description: `Overlaying comparison at ${action.lat.toFixed(4)}, ${action.lng.toFixed(4)}`,
        duration: 2000,
      })
      return
    }

    // Use area_id for forecast mode, select_hex_id for H3 mode
    const selectedId = action.area_id || action.select_hex_id || undefined
    setMapState({
      center: [action.lng, action.lat],
      zoom: action.zoom,
      ...(selectedId ? { selectedId } : {}),
      ...(action.highlighted_hex_ids ? { highlightedIds: action.highlighted_hex_ids } : {}),
    })

    // Always dispatch tavus-map-action so the forecast map auto-selects on idle
    if (action.area_id) {
      window.dispatchEvent(new CustomEvent("tavus-map-action", {
        detail: {
          action: "location_to_area",
          params: { lat: action.lat, lng: action.lng, zoom: action.zoom },
          result: {
            chosen: { lat: action.lat, lng: action.lng, label: "" },
            area: { id: action.area_id, level: action.level || "zcta" }
          }
        }
      }))
    } else {
      // Fallback: fly_to_location also auto-selects the feature at center on idle
      window.dispatchEvent(new CustomEvent("tavus-map-action", {
        detail: {
          action: "fly_to_location",
          params: { lat: action.lat, lng: action.lng, zoom: action.zoom }
        }
      }))
    }

    toast({
      title: "Map updated",
      description: `Navigating to ${action.lat.toFixed(4)}, ${action.lng.toFixed(4)}`,
      duration: 2000,
    })
  }, [setMapState, toast])

  // Listen for Tavus tool events (dispatched from window by TavusMiniWindow)
  useEffect(() => {
    const handleTavusAction = (e: Event) => {
      const { action, params, result } = (e as CustomEvent).detail

      console.log(`[PAGE] Received Tavus action: ${action}`, { params, result })

      if (action === "fly_to_location") {
        setMapState(prev => {
          // If we have highlightedIds (e.g. from location_to_hex), preserve them unless new ones are provided.
          const nextHighlightedIds = params.selected_hex_ids || prev.highlightedIds

          // ZOOM SAFETY: If we have highlighted IDs (neighborhood mode), don't let AI force a zoom that hides them (e.g. Zoom 12 is too far out for Res 9 hexes? No, Res 9 needs ~13. Zoom 12 might be okay but let's check).
          // Actually, Res 9 hexes are rendered at Zoom 12?
          // getZoomForRes(9) -> 13.2.
          // If AI says Zoom 12, and we have Res 9 hexes, we should probably prefer 13.
          // Let's rely on the AI's zoom mostly, but if we have IDs and no specific selection, ensure we can see them.
          let nextZoom = params.zoom || 12
          if (nextHighlightedIds && nextHighlightedIds.length > 0 && nextZoom < 13) {
            nextZoom = 13 // Force at least 13 if we are highlighting things
          }

          return {
            center: [params.lng, params.lat],
            zoom: nextZoom,
            selectedId: params.select_hex_id || prev.selectedId, // Preserve selectedId if not overwriting
            highlightedIds: nextHighlightedIds
          }
        })
        toast({ title: "Homecastr Agent", description: "Moving map..." })
      } else if (action === "inspect_location") {
        setMapState({
          center: [params.lng, params.lat],
          zoom: params.zoom || 15,
          selectedId: params.h3_id
        })
        toast({ title: "Homecastr Agent", description: "Inspecting property..." })
      } else if (action === "inspect_neighborhood") {
        setMapState(prev => {
          // If we already have a selectedId and it's in the new set, keep it.
          // Otherwise, default to the first one to ensure tooltip appears.
          const newHighlights = params.h3_ids || []
          const keepSelected = prev.selectedId && newHighlights.includes(prev.selectedId)

          return {
            ...prev,
            center: [params.lng, params.lat],
            zoom: params.zoom || 13,
            highlightedIds: newHighlights,
            selectedId: keepSelected ? prev.selectedId : (newHighlights[0] || null)
          }
        })
        toast({ title: "Homecastr Agent", description: "Inspecting neighborhood..." })
      } else if (action === "location_to_hex") {
        if (result?.h3?.h3_id) {
          const isNeighborhood = result.h3.context === "neighborhood_average" || (result.h3.neighbors && result.h3.neighbors.length > 1)
          const targetRes = result.h3.h3_res || 9
          const targetZoom = getZoomForRes(targetRes)

          setMapState(prev => ({
            ...prev,
            center: [result.chosen.lng, result.chosen.lat],
            zoom: targetZoom,
            selectedId: result.h3.h3_id,
            // If we have neighbors (neighborhood context), highlight them all
            highlightedIds: result.h3.neighbors || undefined
          }))
          toast({ title: "Homecastr Agent", description: `Found ${result.chosen.label}` })
        }
      } else if (action === "add_location_to_selection") {
        const resultIds = result?.h3?.h3_ids || (result?.h3?.h3_id ? [result.h3.h3_id] : [])
        // Fallback for neighborhood context from resolveLocationToHex
        const neighborIds = result?.h3?.neighbors || []

        const idsToAdd = [...resultIds, ...neighborIds]

        if (idsToAdd.length > 0) {
          // If we have a single new location with lat/lng, maybe zoom/pan? 
          // But for "Compare top 3", we likely just want to highlight them.
          // Let's decide zoom based on the FIRST added item if we don't have a bounding box.

          setMapState(prev => {
            const currentHighlights = prev.highlightedIds || (prev.selectedId ? [prev.selectedId] : [])
            const combined = Array.from(new Set([...currentHighlights, ...idsToAdd]))

            // If we have an H3 ID but no explicit lat/lng (e.g. adding by ID), derive it
            let targetCenter = result.chosen?.lat ? [result.chosen.lng, result.chosen.lat] : undefined
            if (!targetCenter && result.h3?.h3_id) {
              const [lat, lng] = cellToLatLng(result.h3.h3_id)
              targetCenter = [lng, lat]
            }

            return {
              ...prev,
              highlightedIds: combined,
              ...(targetCenter && result.h3?.h3_id ? {
                center: targetCenter as [number, number],
                zoom: getZoomForRes(result.h3.h3_res || 9),
                selectedId: result.h3.h3_id // Select the new location so the tooltip appears!
              } : {})
            }
          })
          toast({ title: "Homecastr Agent", description: `Added ${result.chosen?.label || idsToAdd.length + " locations"} to comparison` })
        }
      } else if (action === "clear_selection") {
        console.log("[PAGE] clear_selection fired")
        setMapState(prev => ({
          ...prev,
          selectedId: null,
          highlightedIds: undefined
        }))
        toast({ title: "Homecastr Agent", description: "Selection cleared" })
      } else if (action === "rank_h3_hexes") {
        if (result?.hexes?.length > 0) {
          const topHex = result.hexes[0]
          setMapState({
            center: [topHex.location.lng, topHex.location.lat],
            zoom: 12,
            highlightedIds: result.hexes.map((h: any) => h.h3_id),
            selectedId: topHex.h3_id
          })
          toast({ title: "Homecastr Agent", description: "Ranking locations..." })
        }
      }
      // ── Forecast-map geography-level actions ──
      else if (action === "location_to_area" || action === "get_forecast_area") {
        if (result?.chosen?.lat) {
          setMapState(prev => ({
            ...prev,
            center: [result.chosen.lng, result.chosen.lat],
            zoom: 13,
            selectedId: result.area?.id || prev.selectedId,
          }))
          toast({ title: "Homecastr Agent", description: `Found ${result.chosen?.label || "area"}` })
        } else if (result?.area) {
          toast({ title: "Homecastr Agent", description: `Forecast data loaded for ${result.area.id}` })
        }
      } else if (action === "rank_forecast_areas") {
        if (result?.areas?.length > 0) {
          toast({ title: "Homecastr Agent", description: `Found top ${result.areas.length} areas` })
        }
      }
    }
    window.addEventListener("tavus-map-action", handleTavusAction)
    return () => window.removeEventListener("tavus-map-action", handleTavusAction)
  }, [setMapState, toast])

  // Click coordinates from ForecastMap (actual feature location, not viewport center)
  const [clickCoords, setClickCoords] = useState<[number, number] | null>(null)

  // Reverse Geocode Effect — only for non-forecast-map modes
  // In forecast map mode, ForecastMap's onGeocodedName callback sets the search bar directly
  useEffect(() => {
    if (!mapState.selectedId) {
      setSearchBarValue("")
      return
    }

    // In forecast map mode, the search bar is set by onGeocodedName callback
    if (filters.useForecastMap) return

    // Do NOT show raw ID. Show "..." or nothing while loading.
    setSearchBarValue("Loading location...")

    const fetchAddress = async () => {
      try {
        const [lat, lng] = cellToLatLng(mapState.selectedId!)
        const address = await reverseGeocode(lat, lng, 18)
        if (address) {
          setSearchBarValue(address)
        } else {
          setSearchBarValue("")
        }
      } catch (e) {
        console.error("Reverse geocode failed", e)
        setSearchBarValue("")
      }
    }
    fetchAddress()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mapState.selectedId, filters.useForecastMap])

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

  // Listen for Tavus-dispatched map actions that affect page-level state
  // (set_forecast_year, set_color_mode come directly from Tavus, not through chat)
  useEffect(() => {
    const handler = (e: Event) => {
      const detail = (e as CustomEvent).detail
      const action = detail?.action
      const params = detail?.params

      if (action === "set_forecast_year" && params?.year) {
        const yr = Math.max(2019, Math.min(2030, params.year))
        console.log('[PAGE] set_forecast_year from Tavus:', yr)
        setCurrentYear(yr)
        toast({ title: `Timeline set to ${yr}`, duration: 2000 })
      } else if (action === "set_color_mode" && params?.mode) {
        const mode = params.mode === 'growth' ? 'growth' : 'value'
        console.log('[PAGE] set_color_mode from Tavus:', mode)
        setFilters({ colorMode: mode })
        toast({ title: `Map view: ${mode}`, duration: 2000 })
      }
    }
    window.addEventListener("tavus-map-action", handler)
    return () => window.removeEventListener("tavus-map-action", handler)
  }, [toast, setFilters])

  const handleSearch = useCallback(async (query: string) => {
    try {
      const result = await geocodeAddress(query)
      if (result) {
        // Adaptive zoom based on Nominatim result type/class
        const t = result.resultType?.toLowerCase() || ""
        const c = result.resultClass?.toLowerCase() || ""
        let zoom = 14 // default
        if (t === "house" || t === "building" || t === "apartments" || c === "building") {
          zoom = 18  // full address → parcel scale
        } else if (t === "road" || t === "street" || t === "residential" || c === "highway") {
          zoom = 16  // street → block scale
        } else if (t === "suburb" || t === "neighbourhood" || t === "neighborhood" || t === "quarter") {
          zoom = 14  // neighborhood
        } else if (t === "postcode" || t === "postal_code") {
          zoom = 14  // zip code
        } else if (t === "city" || t === "town" || t === "village") {
          zoom = 12  // city scale
        } else if (t === "county" || t === "state" || t === "country") {
          zoom = 10
        }

        // Clear any existing selection so fly_to_location will auto-select at destination
        if (mapState.selectedId) {
          window.dispatchEvent(new CustomEvent("tavus-map-action", {
            detail: { action: "clear_selection" }
          }))
        }

        setMapState({
          center: [result.lng, result.lat],
          zoom,
        })
        // Dispatch fly_to_location so forecast-map auto-selects the center feature
        window.dispatchEvent(new CustomEvent("tavus-map-action", {
          detail: {
            action: "fly_to_location",
            params: { lat: result.lat, lng: result.lng, zoom }
          }
        }))
        toast({ title: "Found Address", description: result.displayName })
      } else {
        handleSearchError(`Address not found: ${query}`)
      }
    } catch (e) {
      handleSearchError("Search failed")
    }
  }, [setMapState, toast, handleSearchError, mapState.selectedId])



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

  const handleColorModeChange = useCallback((mode: "growth" | "value" | "growth_dollar") => {
    setFilters({ colorMode: mode })
    // First time the user clicks Growth and hasn't touched the timeline — animate to 2027
    if (mode === "growth" && !hasManuallySetYear && currentYear < 2027) {
      try {
        const alreadyShown = localStorage.getItem("properlytic_growth_intro_shown")
        if (!alreadyShown) {
          localStorage.setItem("properlytic_growth_intro_shown", "1")
          // Animate to 2027 by stepping through years
          let yr = currentYear + 1
          const step = () => {
            if (yr <= 2027) {
              setCurrentYear(yr)
              yr++
              setTimeout(step, 80)
            }
          }
          setTimeout(step, 200)
        }
      } catch { /* localStorage unavailable (SSR/private) */ }
    }
  }, [setFilters, hasManuallySetYear, currentYear])

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
        forecastMode: filters.useForecastMap ?? false,
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
  }, [isTavusLoading, toast, filters.useForecastMap])

  /* Floating button handler */
  const handleFloatingConsultAI = useCallback(async () => {
    if (isTavusLoading) return

    setIsTavusLoading(true)
    try {
      let predictedValue: number | null = null
      let opportunityScore: number | null = null
      let capRate: number | null = null

      if (filters.useForecastMap) {
        // Forecast map mode: query forecast-detail API if we have a selectedId
        if (mapState.selectedId) {
          try {
            const res = await fetch(`/api/forecast-detail?level=zcta&id=${encodeURIComponent(mapState.selectedId)}&originYear=${pageOriginYear}`)
            if (res.ok) {
              const json = await res.json()
              if (json.p50 && json.p50.length > 0) {
                predictedValue = json.p50[json.p50.length - 1] // Last horizon
              }
            }
          } catch { }
        }
      } else {
        // Classic H3 mode
        let h3Id = mapState.selectedId
        if (!h3Id) {
          const [lng, lat] = mapState.center
          h3Id = latLngToCell(lat, lng, 8)
        }
        const details = await getH3CellDetails(h3Id, currentYear)
        predictedValue = details?.proforma?.predicted_value ?? null
        opportunityScore = details?.opportunity?.value ?? null
        capRate = details?.proforma?.cap_rate ?? null
      }

      const result = await createTavusConversation({
        predictedValue,
        opportunityScore,
        capRate,
        address: searchBarValue && !searchBarValue.includes("Loading") && !searchBarValue.startsWith("8") ? searchBarValue : "this neighborhood",
        forecastMode: filters.useForecastMap ?? false,
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
  }, [isTavusLoading, toast, mapState.selectedId, mapState.center, currentYear, filters.useForecastMap])
  // ═══ Shared bottom bar row — used in BOTH tooltip and standalone contexts ═══
  const mobileBottomBarRow = isMobileViewport ? (
    <div className="px-3 py-2 flex items-center gap-2">
      <div className="flex-1 min-w-0">
        <div className={cn("flex items-center gap-1.5 rounded-lg px-2.5 py-1.5 border transition-colors", isChatOpen ? "bg-muted/30 border-primary/30" : "bg-muted/20 border-border/50")}>
          {isChatOpen && <MessageSquare size={14} className="text-primary shrink-0" />}
          {!isChatOpen && <Search size={14} className="text-muted-foreground/60 shrink-0" />}
          <input
            value={chatInput}
            onChange={(e) => setChatInput(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter' && chatInput.trim()) {
                const text = chatInput.trim()
                // Chat vs Search heuristic:
                // Route to SEARCH if: starts with digit, looks like zip, has state abbrev, OR is a short phrase (1-3 words) that doesn't start with conversational intent.
                // Route to CHAT if: it's a longer sentence, or starts with greetings/questions (hi, hello, what, why, show me, tell me).
                const isShortPhrase = text.split(/\s+/).length <= 4;
                const isConversational = /^(hi|hello|hey|what|why|how|who|where|when|can|could|would|please|show|tell|explain)\b/i.test(text);
                const isExplicitAddress = /^\d/.test(text) || /^\d{5}(-\d{4})?$/.test(text) || /,\s*[A-Z]{2}\b/i.test(text);
                const isSearch = isExplicitAddress || (isShortPhrase && !isConversational);

                if (!isChatOpen && isSearch) {
                  handleSearch(text)
                } else {
                  if (!isChatOpen) setIsChatOpen(true)
                  setTimeout(() => chatPanelRef.current?.sendExternalMessage(text), isChatOpen ? 0 : 100)
                }
                setChatInput('')
              }
              if (e.key === 'Escape' && isChatOpen) {
                setIsChatOpen(false)
                setChatInput('')
              }
            }}
            placeholder={isChatOpen ? "Ask about this area..." : "Search or ask a question..."}
            className="flex-1 bg-transparent text-sm outline-none placeholder:text-muted-foreground/50"
          />
          {!tavusConversationUrl && !isTavusLoading && (
            <button onClick={handleFloatingConsultAI} className="shrink-0 w-6 h-6 rounded-full flex items-center justify-center hover:bg-muted/50 active:scale-90 transition-transform" aria-label="Voice agent">
              <Mic size={14} className="text-muted-foreground/70" />
            </button>
          )}
          {isChatOpen && (
            <button onClick={() => { setIsChatOpen(false); setChatInput('') }} className="shrink-0 w-5 h-5 rounded flex items-center justify-center hover:bg-muted/50">
              <X size={12} />
            </button>
          )}
        </div>
      </div>
      {/* FAB — PDF and Analysis */}
      <div className="relative shrink-0">
        {mobileActionsOpen && (
          <div className="absolute bottom-12 right-0 flex flex-col gap-2 animate-in fade-in slide-in-from-bottom-2 duration-150 z-[70]">
            <button
              onClick={async () => {
                setMobileActionsOpen(false)
                try {
                  toast({ title: "Generating PDF…", duration: 2000 })
                  const captureMap = (window as any).__captureMapImage
                  const mapImageDataUrl: string | undefined = captureMap ? await captureMap() : undefined
                  const selectedId = mapState.selectedId
                  let historicalValues: (number | null)[] = []
                  let p50: number[] = [], p10: number[] = [], p90: number[] = [], years: number[] = []
                  if (selectedId) {
                    const level = selectedId.length === 3 ? "zip3" : selectedId.length === 5 ? "zcta" : selectedId.length === 11 ? "tract" : selectedId.length === 15 ? "tabblock" : "parcel"
                    const res = await fetch(`/api/forecast-detail?level=${level}&id=${encodeURIComponent(selectedId)}&originYear=${pageOriginYear}`)
                    if (res.ok) { const json = await res.json(); historicalValues = json.historicalValues || []; p50 = json.p50 || []; p10 = json.p10 || []; p90 = json.p90 || []; years = json.years || [] }
                  }
                  const shareUrl = new URL(window.location.href)
                  if (currentYear !== 2027) shareUrl.searchParams.set("yr", currentYear.toString())
                  const pdfPinnedIds = (window as any).__getPinnedIds?.() as string[] | undefined
                  if (pdfPinnedIds?.length) shareUrl.searchParams.set("compare", pdfPinnedIds.join(","))
                  await generateForecastPDF({
                    locationName: searchBarValue && !searchBarValue.includes("Loading") ? searchBarValue : selectedId ? selectedId : "Map Overview",
                    locationId: selectedId || "—", currentYear, historicalValues, p50, p10, p90, years, mapImageDataUrl,
                    shareUrl: shareUrl.toString(), pinnedComparisons: (window as any).__getPinnedComparisons?.() || undefined,
                  })
                } catch (err) { console.error("PDF generation failed:", err); toast({ title: "PDF failed", description: "Could not generate report", variant: "destructive" }) }
              }}
              className="h-10 px-3 rounded-full glass-panel flex items-center gap-2 text-foreground shadow-xl active:scale-95 transition-transform" aria-label="PDF"
            >
              <FileDown size={16} /><span className="text-xs font-medium">PDF</span>
            </button>
            <button onClick={() => { setIsContactOpen(true); setMobileActionsOpen(false) }} className="h-10 px-3 rounded-full glass-panel flex items-center gap-2 text-foreground shadow-xl active:scale-95 transition-transform border border-[hsl(var(--primary))]/30" aria-label="Analysis">
              <CalendarDays size={16} className="text-[hsl(45,80%,45%)]" /><span className="text-xs font-medium">Analysis</span>
            </button>
          </div>
        )}
        <button onClick={() => setMobileActionsOpen(!mobileActionsOpen)} className={cn("w-9 h-9 rounded-xl glass-panel flex items-center justify-center text-foreground shadow-lg active:scale-90 transition-all duration-200", mobileActionsOpen && "rotate-45 bg-primary text-primary-foreground")}><Plus size={18} /></button>
      </div>
      <div className="shrink-0 flex flex-col items-center justify-center w-9 h-9 cursor-pointer active:scale-95 transition-transform" onClick={() => { setMobileActionsOpen(false); setMobileFiltersOpen(!mobileFiltersOpen) }}>
        <div className="w-6 h-1 rounded-full bg-muted-foreground/40 mb-0.5" />
        <div className="w-4 h-1 rounded-full bg-muted-foreground/25" />
      </div>
    </div>
  ) : null

  return (
    <main className="h-dvh flex flex-col">
      {/* Full-screen Map Container */}
      <div className="flex-1 relative h-full w-full">
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

        {filters.useForecastMap ? (
          <ForecastMap
            filters={filters}
            mapState={mapState}
            onFeatureSelect={(id) => {
              if (mobileFiltersOpen) setMobileFiltersOpen(false)
              selectFeature(id)
            }}
            onFeatureHover={(id) => {
              if (id && mobileFiltersOpen) setMobileFiltersOpen(false)
              hoverFeature(id)
            }}
            onCoordsChange={setClickCoords}
            onGeocodedName={(name) => setSearchBarValue(name || "")}
            year={currentYear}
            className="absolute inset-0 z-0"
            onConsultAI={handleConsultAI}
            isChatOpen={isChatOpen}
            isTavusOpen={!!tavusConversationUrl && !isTavusLoading}
            compareMode={compareMode}
            onPinnedCountChange={setPinnedCount}
            onMobileClose={() => {
              if (isChatOpen) setIsChatOpen(false)
              else if (tavusConversationUrl) { setTavusConversationUrl(null); setIsTavusLoading(false) }
              else if (isContactOpen) setIsContactOpen(false)
              else selectFeature(null)
            }}
            mobileContentOverride={isMobileViewport && isChatOpen ? (
              <ChatPanel
                ref={chatPanelRef}
                isOpen={true}
                embedded={true}
                onClose={() => setIsChatOpen(false)}
                onMapAction={handleChatMapAction}
                forecastMode={filters.useForecastMap ?? false}
                tooltipVisible={!!(mapState.selectedId || mapState.hoveredId)}
                mapViewport={{ center: mapState.center, zoom: mapState.zoom, selectedId: mapState.selectedId }}
              />
            ) : isMobileViewport && tavusConversationUrl && !isTavusLoading ? (
              <TavusMiniWindow
                conversationUrl={tavusConversationUrl}
                onClose={() => { setTavusConversationUrl(null); setIsTavusLoading(false) }}
                forecastMode={filters.useForecastMap ?? false}
                embedded={true}
              />
            ) : isMobileViewport && isContactOpen ? (
              <ContactModal
                isOpen={true}
                embedded={true}
                onClose={() => setIsContactOpen(false)}
              />
            ) : undefined}
          />
        ) : filters.useVectorMap ? (
          <VectorMap
            filters={filters}
            mapState={mapState}
            onFeatureSelect={(id) => {
              if (mobileFiltersOpen) setMobileFiltersOpen(false)
              selectFeature(id)
            }}
            onFeatureHover={(id) => {
              if (id && mobileFiltersOpen) setMobileFiltersOpen(false)
              hoverFeature(id)
            }}
            year={currentYear}
            className="absolute inset-0 z-0"
            onConsultAI={handleConsultAI}
            isEmbedded={searchParams.has("embedded")}
          />
        ) : filters.usePMTiles ? (
          <div className="absolute inset-0 z-0">
            <H3Map year={currentYear} colorMode={filters.colorMode} mapState={mapState} />
          </div>
        ) : (
          <MapView
            filters={filters}
            mapState={mapState}
            onFeatureSelect={(id) => {
              if (mobileFiltersOpen) setMobileFiltersOpen(false)
              selectFeature(id)
            }}
            onFeatureHover={(id) => {
              if (id && mobileFiltersOpen) setMobileFiltersOpen(false)
              hoverFeature(id)
            }}
            year={currentYear}
            onMockDataDetected={handleMockDataDetected}
            onYearChange={setCurrentYear}
            mobileSelectionMode={mobileSelectionMode}
            onMobileSelectionModeChange={setMobileSelectionMode}
            onConsultAI={handleConsultAI}
            isEmbedded={searchParams.has("embedded")}
          />
        )}

        {/* Chat Panel Overlay — desktop only (on mobile, chat is embedded in unified bottom sheet) */}
        {!isMobileViewport && (
          <ChatPanel
            isOpen={isChatOpen}
            onClose={() => setIsChatOpen(false)}
            onMapAction={handleChatMapAction}
            forecastMode={filters.useForecastMap ?? false}
            onTavusRequest={handleFloatingConsultAI}
            tooltipVisible={!!(mapState.selectedId || mapState.hoveredId)}
            mapViewport={{ center: mapState.center, zoom: mapState.zoom, selectedId: mapState.selectedId }}
          />
        )}

        {/* Desktop Sidebar Container - Top Left (Hidden on mobile + embedded) */}
        {!searchParams.has("embedded") && !isMobileViewport && (
          <div className={`absolute top-4 left-4 z-[60] flex flex-col gap-1.5 w-fit min-w-[320px] transition-all duration-300`}>
            {/* Search Row */}
            <div className="flex items-center gap-2 w-full">
              <SearchBox
                onSearch={handleSearch}
                placeholder="Search address or ID..."
                value={searchBarValue}
              />
            </div>

            {/* TimeControls + Help Button Row */}
            <div className="flex items-center gap-2">
              <TimeControls
                minYear={2019}
                maxYear={2030}
                currentYear={currentYear}
                onChange={(yr) => { setHasManuallySetYear(true); setCurrentYear(yr) }}
                onPlayStart={() => {
                  console.log("[PAGE] Play started - prefetch all years triggered")
                }}
                className="flex-1"
              />
            </div>

            {/* Legend & Selection Buttons + Zoom Controls Row */}
            <div className="flex flex-row gap-1.5 items-stretch">
              {/* Legend - Takes up available space */}
              <Legend
                className="flex-1"
                colorMode={filters.colorMode}
                onColorModeChange={handleColorModeChange}
                year={currentYear}
                originYear={pageOriginYear}
              />

              {/* Controls: 2x2 Grid */}
              <div className="grid grid-cols-2 grid-rows-2 gap-1 shrink-0 self-stretch w-[4.5rem]">

                {/* Single Select — labelled */}
                <button
                  onClick={() => { setCompareMode(false); setMobileSelectionMode('replace') }}
                  className={`aspect-square rounded-md flex items-center justify-center transition-colors shadow-sm font-bold text-[10px] ${!compareMode ? "bg-primary text-primary-foreground" : "glass-panel text-foreground"}`}
                  title="Single Select"
                >
                  Single
                </button>

                {/* Compare Mode — labelled */}
                <button
                  onClick={() => setCompareMode(!compareMode)}
                  className={`aspect-square rounded-md flex items-center justify-center transition-colors shadow-sm relative font-bold text-[9px] ${compareMode ? "bg-lime-500 text-black" : "glass-panel text-foreground"}`}
                  title={compareMode ? "Compare Mode ON — click areas to pin" : "Compare Mode — pin areas to compare"}
                >
                  Comp.
                  {pinnedCount > 0 && (
                    <span className="absolute -top-1 -right-1 w-3.5 h-3.5 bg-lime-400 text-black text-[8px] font-bold rounded-full flex items-center justify-center">
                      {pinnedCount}
                    </span>
                  )}
                </button>

                {/* Zoom In */}
                <button
                  onClick={() => {
                    setMapState({ zoom: Math.min(18, mapState.zoom + 1) })
                  }}
                  className="aspect-square glass-panel rounded-md flex items-center justify-center text-foreground hover:bg-accent transition-colors shadow-sm active:scale-95"
                  aria-label="Zoom In"
                >
                  <Plus className="h-3.5 w-3.5" />
                </button>

                {/* Zoom Out */}
                <button
                  onClick={() => {
                    setMapState({ zoom: Math.max(9, mapState.zoom - 1) })
                  }}
                  className="aspect-square glass-panel rounded-md flex items-center justify-center text-foreground hover:bg-accent transition-colors shadow-sm active:scale-95"
                  aria-label="Zoom Out"
                >
                  <Minus className="h-3.5 w-3.5" />
                </button>
              </div>
            </div>

            {/* API Documentation + Version */}
            <div className="flex justify-between items-center px-1">
              <div className="flex items-center gap-2">
                <a
                  href="/api-docs"
                  className="text-[10px] text-muted-foreground hover:text-primary transition-colors flex items-center gap-1 font-medium"
                  target="_blank"
                >
                  <Terminal className="w-3 h-3" />
                  API Documentation
                </a>

                {/* Dev Only Schema Toggle */}
                {process.env.NODE_ENV === "development" && (
                  <button
                    onClick={() => {
                      const urlArgs = new URLSearchParams(window.location.search)
                      const currentSchema = urlArgs.get("schema") || ""
                      const newSchema = prompt("Switch inference schema (leave blank for PROD):", currentSchema ? currentSchema : "forecast_queue")
                      if (newSchema !== null) {
                        if (newSchema.trim() === "") {
                          urlArgs.delete("schema")
                        } else {
                          urlArgs.set("schema", newSchema.trim())
                        }
                        window.location.search = urlArgs.toString()
                      }
                    }}
                    className="text-[10px] px-1.5 py-0.5 rounded flex items-center gap-1 font-mono transition-colors bg-amber-500/10 hover:bg-amber-500/20 text-amber-500 border border-amber-500/20"
                  >
                    <Activity className="w-2.5 h-2.5" />
                    Schema
                  </button>
                )}
              </div>
              <div className="text-[10px] text-muted-foreground/50 font-mono">v1.4.0-beige</div>
            </div>
          </div>
        )}

        {/* ═══ MOBILE BOTTOM BAR — always fixed at bottom, ONE element ═══ */}
        {!searchParams.has("embedded") && isMobileViewport && (
          <div className="fixed left-0 right-0 bottom-0 z-[10000] flex flex-col pointer-events-none">
            {/* Filters sheet — slides up above the bar */}
            {mobileFiltersOpen && (
              <div className="glass-panel border-t border-border/50 px-4 py-3 space-y-3 animate-in slide-in-from-bottom-4 duration-200 pointer-events-auto shadow-[0_-8px_30px_rgba(0,0,0,0.12)] bg-background/95 backdrop-blur-xl">
                <TimeControls
                  minYear={2019} maxYear={2030} currentYear={currentYear}
                  onChange={(yr) => { setHasManuallySetYear(true); setCurrentYear(yr) }}
                  onPlayStart={() => console.log("[PAGE] Play started")}
                  className="w-full"
                />
                <Legend
                  className="w-full" colorMode={filters.colorMode}
                  onColorModeChange={handleColorModeChange} year={currentYear} originYear={pageOriginYear}
                  compareMode={compareMode}
                  onCompareModeChange={(c) => { setCompareMode(c); if (!c) setMobileSelectionMode('replace') }}
                  pinnedCount={pinnedCount}
                />
              </div>
            )}

            {/* The ONE bar row — shared via mobileBottomBarRow */}
            <div className="glass-panel border-t border-border/50 pointer-events-auto">
              {mobileBottomBarRow}
            </div>
          </div>
        )}

        {/* Desktop floating action buttons — 2-column layout (hidden on mobile) */}
        {!searchParams.has("embedded") && !isMobileViewport && (
          <div className={cn(
            "fixed z-[9999] flex items-stretch gap-2 transition-all duration-300 max-w-[calc(100vw-24px)]",
            tavusConversationUrl || isChatOpen ? "left-[365px]" : "left-5",
            (mapState.selectedId || mapState.hoveredId) && filters.useForecastMap ? "bottom-5" : "bottom-5"
          )}>
            {/* Left column — Agent buttons stacked vertically */}
            <div className="flex flex-col gap-1.5">
              {/* Chat button — visible when chat panel is closed */}
              {!isChatOpen && (
                <button
                  onClick={() => setIsChatOpen(true)}
                  className="flex-1 flex items-center gap-2.5 px-4 py-2.5 rounded-2xl glass-panel hover:bg-accent/50 text-foreground shadow-2xl transition-all duration-300 hover:scale-105 active:scale-95 min-w-0"
                >
                  <MessageSquare size={18} className="shrink-0" />
                  <div className="flex flex-col items-start min-w-0">
                    <span className="text-xs font-semibold whitespace-nowrap">Chat with live agent</span>
                    <span className="text-[10px] text-muted-foreground whitespace-nowrap">Powered by OpenAI</span>
                  </div>
                </button>
              )}

              {/* Tavus button — visible when not in a Tavus call */}
              {!tavusConversationUrl && !isTavusLoading && (
                <button
                  onClick={handleFloatingConsultAI}
                  className="flex-1 flex items-center gap-2.5 px-4 py-2.5 rounded-2xl glass-panel hover:bg-accent/50 text-foreground shadow-2xl transition-all duration-300 hover:scale-105 active:scale-95 min-w-0"
                >
                  <Mic size={18} className="shrink-0" />
                  <div className="flex flex-col items-start min-w-0">
                    <span className="text-xs font-semibold whitespace-nowrap">Talk to live agent</span>
                    <span className="text-[10px] text-muted-foreground whitespace-nowrap">Powered by Tavus</span>
                  </div>
                </button>
              )}
            </div>

            {/* Right column — Utility buttons stacked vertically */}
            <div className="flex flex-col gap-1.5">
              {/* Share View — copy link with full state */}
              <button
                onClick={() => {
                  const url = new URL(window.location.href)
                  if (currentYear !== 2027) url.searchParams.set("yr", currentYear.toString())
                  const pinnedIds = (window as any).__getPinnedIds?.() as string[] | undefined
                  if (pinnedIds?.length) {
                    url.searchParams.set("compare", pinnedIds.join(","))
                  } else {
                    url.searchParams.delete("compare")
                  }
                  navigator.clipboard.writeText(url.toString()).then(() => {
                    setLinkCopied(true)
                    toast({ title: "Link copied", description: pinnedIds?.length ? `Shared with ${pinnedIds.length} comparison(s)` : "Share this URL to show the same view", duration: 2500 })
                    setTimeout(() => setLinkCopied(false), 2500)
                  })
                }}
                className="flex-1 flex items-center gap-2 px-4 py-2 rounded-2xl glass-panel hover:bg-accent/50 text-foreground shadow-2xl transition-all duration-300 hover:scale-105 active:scale-95 min-w-0"
              >
                {linkCopied ? <Check size={16} className="shrink-0 text-green-500" /> : <Link2 size={16} className="shrink-0" />}
                <span className="text-xs font-semibold whitespace-nowrap">{linkCopied ? "Copied!" : "Share"}</span>
              </button>

              {/* Download PDF — branded forecast report */}
              <button
                onClick={async () => {
                  try {
                    toast({ title: "Generating PDF…", duration: 2000 })
                    const captureMap = (window as any).__captureMapImage
                    const mapImageDataUrl: string | undefined = captureMap ? await captureMap() : undefined
                    const selectedId = mapState.selectedId
                    let historicalValues: (number | null)[] = []
                    let p50: number[] = [], p10: number[] = [], p90: number[] = [], years: number[] = []
                    if (selectedId) {
                      const level = selectedId.length === 3 ? "zip3" : selectedId.length === 5 ? "zcta" : selectedId.length === 11 ? "tract" : selectedId.length === 15 ? "tabblock" : "parcel"
                      const res = await fetch(`/api/forecast-detail?level=${level}&id=${encodeURIComponent(selectedId)}&originYear=${pageOriginYear}`)
                      if (res.ok) { const json = await res.json(); historicalValues = json.historicalValues || []; p50 = json.p50 || []; p10 = json.p10 || []; p90 = json.p90 || []; years = json.years || [] }
                    }
                    const shareUrl = new URL(window.location.href)
                    if (currentYear !== 2027) shareUrl.searchParams.set("yr", currentYear.toString())
                    const pdfPinnedIds = (window as any).__getPinnedIds?.() as string[] | undefined
                    if (pdfPinnedIds?.length) shareUrl.searchParams.set("compare", pdfPinnedIds.join(","))
                    await generateForecastPDF({
                      locationName: searchBarValue && !searchBarValue.includes("Loading") ? searchBarValue : selectedId ? selectedId : (() => { const params = new URLSearchParams(window.location.search); const lat = parseFloat(params.get("lat") || "0"); const lng = parseFloat(params.get("lng") || "0"); return lat ? `Map View — ${Math.abs(lat).toFixed(2)}°${lat >= 0 ? "N" : "S"}, ${Math.abs(lng).toFixed(2)}°${lng >= 0 ? "E" : "W"}` : "Map Overview" })(),
                      locationId: selectedId || "—", currentYear, historicalValues, p50, p10, p90, years, mapImageDataUrl,
                      shareUrl: shareUrl.toString(),
                      coords: (() => { const params = new URLSearchParams(window.location.search); const lat = parseFloat(params.get("lat") || "0"); const lng = parseFloat(params.get("lng") || "0"); return lat ? [lat, lng] as [number, number] : undefined })(),
                      pinnedComparisons: (window as any).__getPinnedComparisons?.() || undefined,
                    })
                  } catch (err) { console.error("PDF generation failed:", err); toast({ title: "PDF failed", description: "Could not generate report", variant: "destructive" }) }
                }}
                className="flex-1 flex items-center gap-2 px-4 py-2 rounded-2xl glass-panel hover:bg-accent/50 text-foreground shadow-2xl transition-all duration-300 hover:scale-105 active:scale-95 min-w-0"
              >
                <FileDown size={16} className="shrink-0" />
                <span className="text-xs font-semibold whitespace-nowrap">PDF</span>
              </button>

              {/* CTA: Request custom analysis */}
              <button
                onClick={() => setIsContactOpen(true)}
                className="flex-1 flex items-center gap-2 px-4 py-2 rounded-2xl glass-panel hover:bg-accent/50 text-foreground shadow-2xl transition-all duration-300 hover:scale-105 active:scale-95 border border-[hsl(var(--primary))]/30 min-w-0"
              >
                <CalendarDays size={16} className="text-[hsl(45,80%,45%)] shrink-0" />
                <span className="text-xs font-semibold whitespace-nowrap">Request Analysis</span>
              </button>
            </div>
          </div>
        )}

        {/* Homecastr Loading Indicator */}
        {
          isTavusLoading && (
            <div className={cn(
              "fixed z-[10000] glass-panel text-foreground rounded-2xl px-5 py-4 shadow-2xl flex items-center gap-3 transition-all duration-300",
              "bottom-3 left-3",
              "md:bottom-5",
              isChatOpen ? "md:left-[365px]" : "md:left-5"
            )}>
              <div className="w-5 h-5 border-2 border-primary/30 border-t-primary rounded-full animate-spin" />
              <span className="text-xs font-medium">Connecting to Homecastr...</span>
            </div>
          )
        }

        {/* Tavus AI Analyst Mini Window — desktop only (on mobile, embedded in unified bottom sheet) */}
        {
          !isMobileViewport && tavusConversationUrl && !isTavusLoading && (
            <TavusMiniWindow
              conversationUrl={tavusConversationUrl}
              onClose={() => setTavusConversationUrl(null)}
              chatOpen={isChatOpen}
              forecastMode={filters.useForecastMap ?? false}
            />
          )
        }
      </div >

      {/* Cinematic onboarding intro — first-visit only */}
      <OnboardingIntro />
      {!isMobileViewport && <ContactModal isOpen={isContactOpen} onClose={() => setIsContactOpen(false)} />}
    </main >
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
