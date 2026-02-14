"use client"

import { useEffect, useState, useRef } from "react"
import {
  X,
  TrendingUp,
  TrendingDown,
  Minus,
  Copy,
  Download,
  AlertTriangle,
  CheckCircle2,
  DollarSign,
  ShieldAlert,
  Bot,
} from "lucide-react"
import { Button } from "@/components/ui/button"
import { Separator } from "@/components/ui/separator"
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip"
import { cn } from "@/lib/utils"
import { DecompositionBar } from "./decomposition-bar"
import { FanChart } from "./fan-chart"
import type { DetailsResponse } from "@/lib/types"
import { getH3CellDetails } from "@/app/actions/h3-details"

interface InspectorDrawerProps {
  selectedId: string | null
  onClose: () => void
  year?: number
  className?: string
  onConsultAI?: (details: { predictedValue: number | null; opportunityScore: number | null; capRate: number | null }) => void
}

async function fetchDetails(id: string, year: number): Promise<DetailsResponse | null> {
  const details = await getH3CellDetails(id, year)
  return details
}

// =============================================================================
// Safe Formatting Utilities - Prevent NaN display
// =============================================================================

/**
 * Check if a value is a finite number (not null, undefined, NaN, or Infinity)
 */
function isFiniteNumber(value: number | null | undefined): value is number {
  return value != null && Number.isFinite(value)
}

/**
 * Format currency with null-safety - returns "N/A" for invalid values
 */
function safeFormatCurrency(value: number | null | undefined): string {
  if (!isFiniteNumber(value)) return "N/A"
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(value)
}

/**
 * Format percentage with null-safety - returns "N/A" for invalid values
 * Expects value in 0-1 range, multiplies by 100
 */
function safeFormatPercent(value: number | null | undefined, decimals = 0): string {
  if (!isFiniteNumber(value)) return "N/A"
  return `${(value * 100).toFixed(decimals)}%`
}

/**
 * Format percentage that's already scaled (0-100 range)
 */
function safeFormatPercentScaled(value: number | null | undefined, decimals = 0): string {
  if (!isFiniteNumber(value)) return "N/A"
  return `${value.toFixed(decimals)}%`
}

/**
 * Format fixed decimal with null-safety - returns "N/A" for invalid values
 */
function safeFormatFixed(value: number | null | undefined, decimals = 1, suffix = ""): string {
  if (!isFiniteNumber(value)) return "N/A"
  return `${value.toFixed(decimals)}${suffix}`
}

/**
 * Format integer with null-safety
 */
function safeFormatInt(value: number | null | undefined): string {
  if (!isFiniteNumber(value)) return "N/A"
  return Math.round(value).toString()
}

// =============================================================================
// Component
// =============================================================================

export function InspectorDrawer({ selectedId, onClose, year = 2026, className, onConsultAI }: InspectorDrawerProps) {
  const [details, setDetails] = useState<DetailsResponse | null>(null)
  const [isLoading, setIsLoading] = useState(false)
  const scrollContainerRef = useRef<HTMLDivElement>(null)
  const lastSelectedIdRef = useRef<string | null>(null)

  useEffect(() => {
    if (!selectedId) {
      setDetails(null)
      return
    }

    // Save scroll position before refetch
    const scrollTop = scrollContainerRef.current?.scrollTop ?? 0

    // Only show loading spinner if selecting a NEW cell, not for year changes
    const isNewCell = selectedId !== lastSelectedIdRef.current
    if (isNewCell) {
      setIsLoading(true)
    }
    lastSelectedIdRef.current = selectedId

    console.log(`[INSPECTOR] Fetching details for ${selectedId} year ${year} (isNewCell: ${isNewCell})`)
    fetchDetails(selectedId, year)
      .then((data) => {
        setDetails(data)
        // Debug logging for NaN investigation
        if (data) {
          console.log('[DEBUG] API payload for', selectedId, ':', JSON.stringify(data, null, 2))
        }
        if (!data) {
          console.warn(`No details found for ID: ${selectedId}`)
        }

        // Restore scroll position after data loads (for year changes)
        if (!isNewCell && scrollContainerRef.current) {
          requestAnimationFrame(() => {
            if (scrollContainerRef.current) {
              scrollContainerRef.current.scrollTop = scrollTop
            }
          })
        }
      })
      .catch((err) => {
        console.error(err)
        setDetails(null)
      })
      .finally(() => setIsLoading(false))
  }, [selectedId, year])

  const handleCopyLink = () => {
    const url = new URL(window.location.href)
    url.searchParams.set("id", selectedId || "")
    navigator.clipboard.writeText(url.toString())
  }

  const handleDownload = () => {
    if (!details) return
    const blob = new Blob([JSON.stringify(details, null, 2)], {
      type: "application/json",
    })
    const url = URL.createObjectURL(blob)
    const a = document.createElement("a")
    a.href = url
    a.download = `${selectedId}.json`
    a.click()
    URL.revokeObjectURL(url)
  }

  const isOpen = selectedId !== null

  // Helper to get trend icon
  const getTrendIcon = (trend: "up" | "down" | "stable" | undefined) => {
    if (trend === "up") return <TrendingUp className="h-4 w-4 text-green-500" />
    if (trend === "down") return <TrendingDown className="h-4 w-4 text-red-500" />
    return <Minus className="h-4 w-4 text-muted-foreground" />
  }

  return (
    <aside
      className={cn(
        "h-full border-l border-border bg-card transition-all duration-300 overflow-hidden flex flex-col",
        isOpen ? "w-80 lg:w-96" : "w-0",
        className,
      )}
    >
      {isOpen && (
        <>
          {/* Header */}
          <div className="p-4 border-b border-border flex items-start justify-between gap-3">
            <div className="min-w-0 flex-1">
              <h2 className="font-semibold text-sm truncate">
                {details?.locationLabel || (isLoading ? "Loading..." : "No Data Available")}
              </h2>
              <p className="text-xs text-muted-foreground font-mono truncate">{selectedId?.slice(0, 16)}...</p>
            </div>
            <Button
              variant="ghost"
              size="icon"
              onClick={onClose}
              className="h-7 w-7 shrink-0"
              aria-label="Close inspector"
            >
              <X className="h-4 w-4" />
            </Button>
          </div>

          {/* Content */}
          <div ref={scrollContainerRef} className="flex-1 overflow-y-auto custom-scrollbar">
            {isLoading ? (
              <div className="p-4 space-y-4">
                <div className="h-20 bg-muted animate-pulse rounded-lg" />
                <div className="h-32 bg-muted animate-pulse rounded-lg" />
                <div className="h-48 bg-muted animate-pulse rounded-lg" />
              </div>
            ) : details ? (
              <div className="p-4 space-y-5">
                {/* Headline Metrics */}
                <div className="grid grid-cols-2 gap-3">
                  <div className="bg-secondary/50 rounded-lg p-3">
                    <div className="flex justify-between items-baseline mb-1">
                      <div className="text-xs text-muted-foreground">
                        {year > 2025 ? "Predicted Value" : "Historical Value"} ({year})
                      </div>
                    </div>

                    <div className="flex flex-col gap-1">
                      <span className="text-2xl font-bold text-primary">
                        {details.proforma?.predicted_value ? safeFormatCurrency(details.proforma.predicted_value) : "N/A"}
                      </span>

                      {/* Current Value (2025) Display */}
                      {details.historicalValues && details.historicalValues.length > 0 && (
                        <div className="text-xs text-muted-foreground flex items-center gap-1">
                          <span>Current:</span>
                          <span className="font-mono text-foreground">
                            {safeFormatCurrency(details.historicalValues[details.historicalValues.length - 1])}
                          </span>
                          <span className="text-[10px] text-muted-foreground/70">(2025)</span>
                        </div>
                      )}
                    </div>
                  </div>

                  <div className="bg-secondary/50 rounded-lg p-3">
                    <div className="text-xs text-muted-foreground mb-1">
                      {year > 2025 ? "Projected Growth" : "Change vs Current"}
                    </div>
                    <div className="flex items-center gap-1.5">
                      {(() => {
                        const predicted = details.proforma?.predicted_value
                        const current = details.historicalValues?.[details.historicalValues.length - 1]

                        if (isFiniteNumber(predicted) && isFiniteNumber(current) && current !== 0) {
                          const growth = ((predicted - current) / current)
                          return (
                            <>
                              <span className={cn(
                                "text-2xl font-bold",
                                growth > 0 ? "text-green-500" : growth < 0 ? "text-red-500" : "text-primary"
                              )}>
                                {growth > 0 ? "+" : ""}{safeFormatPercentScaled(growth * 100, 1)}
                              </span>
                              <span className="text-xs text-muted-foreground">%</span>
                              {getTrendIcon(growth > 0.05 ? "up" : growth < -0.02 ? "down" : "stable")}
                            </>
                          )
                        } else if (isFiniteNumber(details.opportunity.value)) {
                          // Fallback to original if calc fails (e.g. missing historical)
                          return (
                            <>
                              <span className="text-2xl font-bold text-green-500">
                                {details.opportunity.value > 0 ? "+" : ""}{details.opportunity.value.toFixed(1)}
                              </span>
                              <span className="text-xs text-muted-foreground">{details.opportunity.unit}</span>
                              {getTrendIcon(details.opportunity.trend)}
                            </>
                          )
                        } else {
                          return <span className="text-2xl font-bold text-muted-foreground">N/A</span>
                        }
                      })()}
                    </div>
                  </div>
                </div>

                {/* Value Timeline Chart */}
                {details.fanChart && (
                  <div className="space-y-2">
                    <h3 className="text-xs font-medium text-muted-foreground uppercase tracking-wide">
                      Value Timeline
                    </h3>
                    <FanChart
                      data={details.fanChart}
                      currentYear={year}
                      historicalValues={details.historicalValues}
                    />
                  </div>
                )}

                <Separator />

                {/* Data Quality & Validation */}
                <div className="space-y-2">
                  <h3 className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Data Factors</h3>
                  <div className="bg-secondary/20 rounded p-2">
                    <div className="text-xs text-muted-foreground">Properties</div>
                    <div className="font-medium">{safeFormatInt(details.metrics.n_accts)}</div>
                  </div>
                </div>


              </div>
            ) : (
              <div className="p-8 text-center space-y-3">
                <div className="mx-auto w-12 h-12 rounded-full bg-muted flex items-center justify-center">
                  <AlertTriangle className="h-6 w-6 text-muted-foreground" />
                </div>
                <div>
                  <h3 className="text-sm font-medium">Data Unavailable</h3>
                  <p className="text-xs text-muted-foreground mt-1">
                    No detailed analytics found for this location.
                  </p>
                </div>
              </div>
            )}
          </div>

          {/* Actions */}
          <div className="p-3 border-t border-border space-y-2">
            {/* Talk to Homecastr Live Agent */}
            {onConsultAI && details && (
              <Button
                size="sm"
                className="w-full bg-primary/15 hover:bg-primary/25 border border-primary/30 text-primary font-semibold"
                variant="outline"
                onClick={() =>
                  onConsultAI({
                    predictedValue: details.proforma?.predicted_value ?? null,
                    opportunityScore: details.opportunity?.value ?? null,
                    capRate: details.proforma?.cap_rate ?? null,
                  })
                }
              >
                <Bot className="h-3.5 w-3.5 mr-1.5" />
                Talk to Homecastr
              </Button>
            )}
            <div className="flex gap-2">
              <TooltipProvider>
                <Tooltip>
                  <TooltipTrigger asChild>
                    <Button variant="outline" size="sm" className="flex-1 bg-transparent" onClick={handleCopyLink}>
                      <Copy className="h-3.5 w-3.5 mr-1.5" />
                      Copy Link
                    </Button>
                  </TooltipTrigger>
                  <TooltipContent>Copy shareable link</TooltipContent>
                </Tooltip>
              </TooltipProvider>
              <TooltipProvider>
                <Tooltip>
                  <TooltipTrigger asChild>
                    <Button variant="outline" size="sm" className="flex-1 bg-transparent" onClick={handleDownload}>
                      <Download className="h-3.5 w-3.5 mr-1.5" />
                      Download
                    </Button>
                  </TooltipTrigger>
                  <TooltipContent>Download as JSON</TooltipContent>
                </Tooltip>
              </TooltipProvider>
            </div>
          </div>
        </>
      )
      }
    </aside >
  )
}
