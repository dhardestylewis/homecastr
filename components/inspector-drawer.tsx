"use client"

import { useEffect, useState } from "react"
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
import { generateMockDetailsResponse } from "@/lib/mock-data"

interface InspectorDrawerProps {
  selectedId: string | null
  onClose: () => void
  className?: string
}

async function fetchDetails(id: string): Promise<DetailsResponse> {
  await new Promise((resolve) => setTimeout(resolve, 300))
  return generateMockDetailsResponse(id)
}

function formatCurrency(value: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(value)
}

export function InspectorDrawer({ selectedId, onClose, className }: InspectorDrawerProps) {
  const [details, setDetails] = useState<DetailsResponse | null>(null)
  const [isLoading, setIsLoading] = useState(false)

  useEffect(() => {
    if (!selectedId) {
      setDetails(null)
      return
    }

    setIsLoading(true)
    fetchDetails(selectedId)
      .then(setDetails)
      .finally(() => setIsLoading(false))
  }, [selectedId])

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
          <div className="flex-1 overflow-y-auto custom-scrollbar">
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
                    <div className="text-xs text-muted-foreground mb-1">Opportunity</div>
                    <div className="flex items-center gap-1.5">
                      <span className="text-2xl font-bold text-primary">
                        {details.opportunity.value > 0 ? "+" : ""}
                        {details.opportunity.value.toFixed(1)}
                      </span>
                      <span className="text-xs text-muted-foreground">{details.opportunity.unit}</span>
                      {details.opportunity.trend === "up" && <TrendingUp className="h-4 w-4 text-green-500" />}
                      {details.opportunity.trend === "down" && <TrendingDown className="h-4 w-4 text-red-500" />}
                      {details.opportunity.trend === "stable" && <Minus className="h-4 w-4 text-muted-foreground" />}
                    </div>
                  </div>
                  <div className="bg-secondary/50 rounded-lg p-3">
                    <div className="text-xs text-muted-foreground mb-1">Reliability</div>
                    <div className="flex items-baseline gap-1">
                      <span className="text-2xl font-bold">{(details.reliability.value * 100).toFixed(0)}</span>
                      <span className="text-sm text-muted-foreground">%</span>
                    </div>
                  </div>
                </div>

                {/* Reliability Decomposition */}
                <div className="space-y-2">
                  <h3 className="text-xs font-medium text-muted-foreground uppercase tracking-wide">
                    Reliability Decomposition
                  </h3>
                  <DecompositionBar components={details.reliability.components} />
                </div>

                <Separator />

                {details.proforma && (
                  <>
                    <div className="space-y-2">
                      <h3 className="text-xs font-medium text-muted-foreground uppercase tracking-wide flex items-center gap-1.5">
                        <DollarSign className="h-3.5 w-3.5" />
                        Proforma
                      </h3>
                      <div className="bg-secondary/30 rounded-lg p-3 space-y-2">
                        <div className="flex justify-between items-center">
                          <span className="text-sm text-muted-foreground">Predicted Value</span>
                          <span className="font-mono font-semibold text-primary">
                            {formatCurrency(details.proforma.predicted_value)}
                          </span>
                        </div>
                        <div className="flex justify-between items-center">
                          <span className="text-sm text-muted-foreground">NOI</span>
                          <span className="font-mono">{formatCurrency(details.proforma.noi)}</span>
                        </div>
                        <div className="flex justify-between items-center">
                          <span className="text-sm text-muted-foreground">Monthly Rent</span>
                          <span className="font-mono">{formatCurrency(details.proforma.monthly_rent)}</span>
                        </div>
                        <Separator className="my-2" />
                        <div className="grid grid-cols-2 gap-2 text-sm">
                          <div className="flex justify-between">
                            <span className="text-muted-foreground">DSCR</span>
                            <span
                              className={cn(
                                "font-mono font-medium",
                                details.proforma.dscr >= 1.25
                                  ? "text-green-500"
                                  : details.proforma.dscr >= 1.05
                                    ? "text-warning"
                                    : "text-destructive",
                              )}
                            >
                              {details.proforma.dscr.toFixed(2)}x
                            </span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-muted-foreground">Cap Rate</span>
                            <span className="font-mono">{(details.proforma.cap_rate * 100).toFixed(1)}%</span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-muted-foreground">Breakeven</span>
                            <span
                              className={cn(
                                "font-mono",
                                details.proforma.breakeven_occ <= 0.85
                                  ? "text-green-500"
                                  : details.proforma.breakeven_occ <= 0.92
                                    ? "text-warning"
                                    : "text-destructive",
                              )}
                            >
                              {(details.proforma.breakeven_occ * 100).toFixed(0)}%
                            </span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-muted-foreground">Liquidity</span>
                            <span className="font-mono">{details.proforma.liquidity_rank.toFixed(0)}%</span>
                          </div>
                        </div>
                      </div>
                    </div>
                    <Separator />
                  </>
                )}

                {details.riskScoring && (
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
                            {details.riskScoring.alert_triggered && (
                              <span className="text-[10px] px-1.5 py-0.5 rounded bg-destructive/20 text-destructive font-medium">
                                ALERT
                              </span>
                            )}
                          </div>
                          <span
                            className={cn(
                              "text-2xl font-bold",
                              details.riskScoring.score >= 7
                                ? "text-green-500"
                                : details.riskScoring.score >= 4
                                  ? "text-warning"
                                  : "text-destructive",
                            )}
                          >
                            {details.riskScoring.score.toFixed(1)}
                          </span>
                        </div>
                        <div className="space-y-1.5 text-xs">
                          <div className="flex justify-between">
                            <span className="text-muted-foreground">Risk Score (R)</span>
                            <span
                              className={cn(
                                "font-mono",
                                Math.abs(details.riskScoring.R) <= 1
                                  ? "text-green-500"
                                  : Math.abs(details.riskScoring.R) <= 2
                                    ? "text-warning"
                                    : "text-destructive",
                              )}
                            >
                              {details.riskScoring.R >= 0 ? "+" : ""}
                              {details.riskScoring.R.toFixed(2)}
                            </span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-muted-foreground">Tail Gap (z)</span>
                            <span className="font-mono">{details.riskScoring.tail_gap_z.toFixed(2)}</span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-muted-foreground">MedAE (z)</span>
                            <span className="font-mono">{details.riskScoring.medAE_z.toFixed(2)}</span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-muted-foreground">Inv DSCR (z)</span>
                            <span className="font-mono">{details.riskScoring.inv_dscr_z.toFixed(2)}</span>
                          </div>
                        </div>
                      </div>
                    </div>
                    <Separator />
                  </>
                )}

                {/* Fan Chart */}
                {details.fanChart && (
                  <div className="space-y-2">
                    <h3 className="text-xs font-medium text-muted-foreground uppercase tracking-wide">
                      Projection Fan Chart
                    </h3>
                    <FanChart data={details.fanChart} />
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
                            {test.value.toFixed(2)} / {test.threshold.toFixed(2)}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                <Separator />

                {/* Metrics */}
                <div className="space-y-2">
                  <h3 className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Support Metrics</h3>
                  <div className="grid grid-cols-2 gap-2 text-sm">
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Accounts</span>
                      <span className="font-mono">{details.metrics.n_accts}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Med Years</span>
                      <span className="font-mono">{details.metrics.med_n_years.toFixed(1)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">APE</span>
                      <span className="font-mono">{details.metrics.med_mean_ape_pct.toFixed(1)}%</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Pred CV</span>
                      <span className="font-mono">{details.metrics.med_mean_pred_cv_pct.toFixed(1)}%</span>
                    </div>
                  </div>
                </div>
              </div>
            ) : null}
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
