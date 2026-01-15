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
                    <div className="text-xs text-muted-foreground mb-1">Projected Growth</div>
                    <div className="flex items-center gap-1.5">
                      <span className="text-2xl font-bold text-primary">
                        {isFiniteNumber(details.opportunity.value)
                          ? `${details.opportunity.value > 0 ? "+" : ""}${details.opportunity.value.toFixed(1)}`
                          : "N/A"}
                      </span>
                      <span className="text-xs text-muted-foreground">{details.opportunity.unit}</span>
                      {isFiniteNumber(details.opportunity.value) && getTrendIcon(details.opportunity.trend)}
                    </div>
                  </div>
                  <div className="bg-secondary/50 rounded-lg p-3">
                    <div className="text-xs text-muted-foreground mb-1">Data Confidence</div>
                    <div className="flex items-baseline gap-1">
                      <span className="text-2xl font-bold">
                        {!isFiniteNumber(details.reliability.value) || details.reliability.value === 0
                          ? "N/A"
                          : details.reliability.value >= 0.7
                            ? "High"
                            : details.reliability.value >= 0.4
                              ? "Medium"
                              : "Low"}
                      </span>
                    </div>
                  </div>
                </div>

                {/* Reliability Decomposition */}
                <div className="space-y-2">
                  <h3 className="text-xs font-medium text-muted-foreground uppercase tracking-wide">
                    Confidence Factors
                  </h3>
                  <DecompositionBar components={details.reliability.components} />
                </div>

                <Separator />

                {/* Predicted Value - Keep visible for homeowners */}
                {details.proforma?.predicted_value && (
                  <>
                    <div className="space-y-2">
                      <div className="bg-secondary/30 rounded-lg p-3">
                        <div className="flex justify-between items-center">
                          <span className="text-sm text-muted-foreground">Predicted Value ({year})</span>
                          <span className="font-mono font-semibold text-primary text-lg">
                            {safeFormatCurrency(details.proforma.predicted_value)}
                          </span>
                        </div>
                      </div>
                    </div>
                    <Separator />
                  </>
                )}

                {/* HIDDEN: Proforma section - Investor metrics hidden for homeowner-focused view
                    Kept in code per user request (not removed). Contains:
                    - NOI, Monthly Rent, DSCR, Cap Rate, Breakeven, Liquidity
                {details.proforma && (
                  <>
                    <div className="space-y-2">
                      <h3 className="text-xs font-medium text-muted-foreground uppercase tracking-wide flex items-center gap-1.5">
                        <DollarSign className="h-3.5 w-3.5" />
                        Proforma
                      </h3>
                      <div className="bg-secondary/30 rounded-lg p-3 space-y-2">
                        <div className="flex justify-between items-center">
                          <span className="text-sm text-muted-foreground">NOI</span>
                          <span className="font-mono">{safeFormatCurrency(details.proforma.noi)}</span>
                        </div>
                        <div className="flex justify-between items-center">
                          <span className="text-sm text-muted-foreground">Monthly Rent</span>
                          <span className="font-mono">{safeFormatCurrency(details.proforma.monthly_rent)}</span>
                        </div>
                        <Separator className="my-2" />
                        <div className="grid grid-cols-2 gap-2 text-sm">
                          <div className="flex justify-between">
                            <span className="text-muted-foreground">DSCR</span>
                            <span className="font-mono">{safeFormatFixed(details.proforma.dscr, 2, "x")}</span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-muted-foreground">Cap Rate</span>
                            <span className="font-mono">{safeFormatPercent(details.proforma.cap_rate, 1)}</span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-muted-foreground">Breakeven</span>
                            <span className="font-mono">{safeFormatPercent(details.proforma.breakeven_occ, 0)}</span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-muted-foreground">Liquidity</span>
                            <span className="font-mono">{safeFormatPercentScaled(details.proforma.liquidity_rank, 0)}</span>
                          </div>
                        </div>
                      </div>
                    </div>
                    <Separator />
                  </>
                )}
                */}

                {/* HIDDEN: Risk Scoring section - Technical metrics hidden for homeowner-focused view
                    Kept in code per user request (not removed). Contains:
                    - Investment Score (10.0/10 format)
                    - Risk Factor, Price Deviation (σ), Prediction Error (σ), Debt Stress (σ)
                */}
                {false && details.riskScoring && (
                  <>
                    <div className="space-y-2">
                      <h3 className="text-xs font-medium text-muted-foreground uppercase tracking-wide flex items-center gap-1.5">
                        <ShieldAlert className="h-3.5 w-3.5" />
                        Risk Scoring
                      </h3>
                      <div
                        className={cn(
                          "rounded-lg p-3 border",
                          details.riskScoring.alert_triggered
                            ? "bg-destructive/10 border-destructive/30"
                            : "bg-secondary/30 border-transparent",
                        )}
                      >
                        <div className="flex items-center justify-between mb-3">
                          <div className="flex items-center gap-2">
                            <span className="text-sm font-medium">Investment Score</span>
                            <span className="text-[10px] text-muted-foreground ml-1">(0-10)</span>
                            {details.riskScoring.alert_triggered && (
                              <span className="text-[10px] px-1.5 py-0.5 rounded bg-destructive/20 text-destructive font-medium">
                                ALERT
                              </span>
                            )}
                          </div>
                          <span
                            className={cn(
                              "text-2xl font-bold",
                              isFiniteNumber(details.riskScoring.score)
                                ? details.riskScoring.score >= 7
                                  ? "text-green-500"
                                  : details.riskScoring.score >= 4
                                    ? "text-warning"
                                    : "text-destructive"
                                : "text-muted-foreground",
                            )}
                          >
                            {isFiniteNumber(details.riskScoring.score)
                              ? `${details.riskScoring.score.toFixed(1)}/10`
                              : "N/A"}
                          </span>
                        </div>
                        <div className="space-y-1.5 text-xs">
                          <div className="flex justify-between">
                            <span className="text-muted-foreground">Risk Factor</span>
                            <span
                              className={cn(
                                "font-mono",
                                isFiniteNumber(details.riskScoring.R)
                                  ? Math.abs(details.riskScoring.R) <= 1
                                    ? "text-green-500"
                                    : Math.abs(details.riskScoring.R) <= 2
                                      ? "text-warning"
                                      : "text-destructive"
                                  : "text-muted-foreground",
                              )}
                            >
                              {isFiniteNumber(details.riskScoring.R)
                                ? `${details.riskScoring.R >= 0 ? "+" : ""}${details.riskScoring.R.toFixed(2)}`
                                : "N/A"}
                            </span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-muted-foreground">Price Deviation</span>
                            <span className="font-mono">
                              {isFiniteNumber(details.riskScoring.tail_gap_z)
                                ? `${details.riskScoring.tail_gap_z.toFixed(2)}σ`
                                : "N/A"}
                            </span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-muted-foreground">Prediction Error</span>
                            <span className="font-mono">
                              {isFiniteNumber(details.riskScoring.medAE_z)
                                ? `${details.riskScoring.medAE_z.toFixed(2)}σ`
                                : "N/A"}
                            </span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-muted-foreground">Debt Stress</span>
                            <span className="font-mono">
                              {isFiniteNumber(details.riskScoring.inv_dscr_z)
                                ? `${details.riskScoring.inv_dscr_z.toFixed(2)}σ`
                                : "N/A"}
                            </span>
                          </div>
                        </div>
                      </div>
                    </div>
                    <Separator />
                  </>
                )}

                {/* Value Timeline Chart - Shows historical + forecast together */}
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

                {/* Stress Tests */}
                {details.stressTests && (
                  <div className="space-y-2">
                    <h3 className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Stress Tests</h3>
                    <div className="grid grid-cols-2 gap-2">
                      {Object.entries(details.stressTests).map(([key, test]) => (
                        <div
                          key={key}
                          className={cn(
                            "rounded-lg p-2.5 border",
                            test.status === "pass"
                              ? "bg-green-500/5 border-green-500/20"
                              : test.status === "warn"
                                ? "bg-warning/10 border-warning/30"
                                : "bg-destructive/10 border-destructive/30",
                          )}
                        >
                          <div className="flex items-center gap-1.5 mb-1">
                            {test.status === "pass" ? (
                              <CheckCircle2 className="h-3.5 w-3.5 text-green-500" />
                            ) : (
                              <AlertTriangle
                                className={cn(
                                  "h-3.5 w-3.5",
                                  test.status === "warn" ? "text-warning" : "text-destructive",
                                )}
                              />
                            )}
                            <span className="text-[10px] font-medium capitalize">{key.replace(/_/g, " ")}</span>
                          </div>
                          <div className="text-xs font-mono">
                            {safeFormatFixed(test.value, 2)} / {safeFormatFixed(test.threshold, 2)}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                <Separator />

                {/* Data Quality Metrics - renamed for clarity */}
                <div className="space-y-2">
                  <h3 className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Data Quality</h3>
                  <div className="grid grid-cols-2 gap-2 text-sm">
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Properties</span>
                      <span className="font-mono">{safeFormatInt(details.metrics.n_accts)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Data History</span>
                      <span className="font-mono">{safeFormatFixed(details.metrics.med_n_years, 1)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Avg Error %</span>
                      <span className="font-mono">
                        {safeFormatPercentScaled(details.metrics.med_mean_ape_pct, 1)}
                      </span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Value Spread</span>
                      <span className="font-mono">
                        {safeFormatPercentScaled(details.metrics.med_mean_pred_cv_pct, 1)}
                      </span>
                    </div>
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
      )}
    </aside>
  )
}
