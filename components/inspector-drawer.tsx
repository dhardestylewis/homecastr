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

export function InspectorDrawer({ selectedId, onClose, year = 2026, className }: InspectorDrawerProps) {
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
              <h2 className="font-semibold text-sm truncate">{details?.locationLabel || "Loading..."}</h2>
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
                    <div className="text-xs text-muted-foreground mb-1">Predicted Value ({year})</div>
                    <div className="flex items-center gap-1.5">
                      <span className="text-2xl font-bold text-primary">
                        {details.proforma?.predicted_value ? safeFormatCurrency(details.proforma.predicted_value) : "N/A"}
                      </span>
                    </div>
                  </div>
                  <div className="bg-secondary/50 rounded-lg p-3">
                    <div className="text-xs text-muted-foreground mb-1">Projected Growth</div>
                    <div className="flex items-center gap-1.5">
                      <span className="text-2xl font-bold text-green-500">
                        {isFiniteNumber(details.opportunity.value)
                          ? `${details.opportunity.value > 0 ? "+" : ""}${details.opportunity.value.toFixed(1)}`
                          : "N/A"}
                      </span>
                      <span className="text-xs text-muted-foreground">{details.opportunity.unit}</span>
                      {isFiniteNumber(details.opportunity.value) && getTrendIcon(details.opportunity.trend)}
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
                  <div className="grid grid-cols-2 gap-2 text-sm">
                    <div className="bg-secondary/20 rounded p-2">
                      <div className="text-xs text-muted-foreground">Confidence</div>
                      <div className="font-medium">
                        {!isFiniteNumber(details.reliability.value) || details.reliability.value === 0
                          ? "N/A"
                          : details.reliability.value >= 0.7
                            ? "High"
                            : details.reliability.value >= 0.4
                              ? "Medium"
                              : "Low"}
                      </div>
                    </div>
                    <div className="bg-secondary/20 rounded p-2">
                      <div className="text-xs text-muted-foreground">Properties</div>
                      <div className="font-medium">{safeFormatInt(details.metrics.n_accts)}</div>
                    </div>
                  </div>
                </div>

                {/* Technical / Hidden by default details */}
                <details className="text-xs text-muted-foreground cursor-pointer group">
                  <summary className="hover:text-foreground transition-colors mb-2">View Technical Details</summary>
                  <div className="space-y-4 pt-2 pl-2 border-l-2 border-border/50 ml-1">

                    {/* Confidence Factors */}
                    <div className="space-y-2">
                      <h3 className="text-[10px] font-medium uppercase tracking-wide">Confidence Breakdown</h3>
                      <DecompositionBar components={details.reliability.components} />
                    </div>

                    {/* Data Quality Metrics */}
                    <div className="space-y-2">
                      <div className="flex justify-between">
                        <span>Data History</span>
                        <span className="font-mono text-foreground">{safeFormatFixed(details.metrics.med_n_years, 1)} yrs</span>
                      </div>
                      <div className="flex justify-between">
                        <span>Avg Error</span>
                        <span className="font-mono text-foreground">
                          {safeFormatPercentScaled(details.metrics.med_mean_ape_pct, 1)}
                        </span>
                      </div>
                      <div className="flex justify-between">
                        <span>Value Spread</span>
                        <span className="font-mono text-foreground">
                          {safeFormatPercentScaled(details.metrics.med_mean_pred_cv_pct, 1)}
                        </span>
                      </div>
                    </div>

                    {/* Stress Tests */}
                    {details.stressTests && (
                      <div className="space-y-2">
                        <h3 className="text-[10px] font-medium uppercase tracking-wide">Stress Tests</h3>
                        <div className="grid grid-cols-1 gap-2">
                          {Object.entries(details.stressTests).map(([key, test]) => (
                            <div key={key} className="flex justify-between items-center text-[10px]">
                              <span className="capitalize">{key.replace(/_/g, " ")}</span>
                              <span className={cn(
                                "font-mono px-1.5 py-0.5 rounded",
                                test.status === "pass" ? "bg-green-500/10 text-green-500" : "bg-destructive/10 text-destructive"
                              )}>
                                {test.status === "pass" ? "PASS" : "WARN"}
                              </span>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                </details>
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
          <div className="p-3 border-t border-border flex gap-2">
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
        </>
      )
      }
    </aside >
  )
}
