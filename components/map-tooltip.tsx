"use client"

import { useMemo } from "react"
import { createPortal } from "react-dom"
import Link from "next/link"
import { TrendingUp, TrendingDown, Minus, Bot, Map } from "lucide-react"
import { HomecastrLogo } from "@/components/homecastr-logo"
import { cn } from "@/lib/utils"
import { FanChart } from "@/components/fan-chart"
import { StreetViewCarousel } from "@/components/street-view-carousel"
import type { DetailsResponse, FeatureProperties, FanChartData } from "@/lib/types"

// Helper functions (extracted from MapView)
function formatCurrency(val: number): string {
    if (val >= 1_000_000) return `$${(val / 1_000_000).toFixed(1)}M`
    if (val >= 1_000) return `$${(val / 1_000).toFixed(0)}k`
    return `$${val.toFixed(0)}`
}

function formatOpportunity(val: number): string {
    const sign = val > 0 ? "+" : ""
    return `${sign}${val.toFixed(1)}%`
}

function formatReliability(val: number): string {
    if (val >= 0.8) return "High"
    if (val >= 0.6) return "Good"
    if (val >= 0.4) return "Fair"
    return "Low"
}

// H3 Helper (basic version sufficient for tooltip display)
function cellToLatLng(h3Index: string): [number, number] {
    // Note: We don't have the h3-js library imported directly here, 
    // but the parent component usually passes resolved properties.
    // If we need lat/lng display in the header, we might need it passed in props
    // or we assume the parent handles the coordinate logic if needed.
    // For now, let's accept coordinates as a prop or omit the specific header detail 
    // if it requires heavy library imports, to keep this lightweight.
    return [0, 0]
}

interface MapTooltipProps {
    x: number
    y: number
    globalX?: number
    globalY?: number
    displayProps: FeatureProperties
    displayDetails: DetailsResponse | null
    primaryDetails: DetailsResponse | null
    selectionDetails: DetailsResponse | null
    comparisonDetails: DetailsResponse | null
    previewDetails: DetailsResponse | null
    selectedHexes: string[]
    hoveredDetails: DetailsResponse | null
    year: number
    h3Resolution: number
    isLoadingDetails: boolean
    isMobile?: boolean
    isMinimized?: boolean
    lockedMode?: boolean
    showDragHint?: boolean
    dragOffset?: number
    touchStart?: number | null
    onYearChange?: (year: number) => void
    onMouseDown?: (e: React.MouseEvent) => void
    onTouchStart?: (e: React.TouchEvent) => void
    onTouchMove?: (e: React.TouchEvent) => void
    onTouchEnd?: (e: React.TouchEvent) => void
    // Optional coord overrides if not importing h3-js
    coordinates?: [number, number]
    // Tavus AI Analyst callback
    onConsultAI?: () => void
    // Google Maps API Key
    googleMapsApiKey?: string
    // Link back to the /forecasts hub page for this geography
    forecastsHref?: string
    // Whether this is rendered inside an embedded widget
    isEmbedded?: boolean
}

export function MapTooltip({
    x, y, globalX, globalY,
    displayProps,
    displayDetails,
    primaryDetails,
    selectionDetails,
    comparisonDetails,
    previewDetails,
    selectedHexes,
    hoveredDetails,
    year,
    h3Resolution,
    isLoadingDetails,
    isMobile = false,
    isMinimized = false,
    lockedMode = false,
    showDragHint = false,
    dragOffset = 0,
    touchStart = null,
    onYearChange,
    onMouseDown,
    onTouchStart,
    onTouchMove,
    onTouchEnd,
    coordinates,
    onConsultAI,
    googleMapsApiKey,
    forecastsHref,
    isEmbedded = false,
}: MapTooltipProps) {

    const getTrendIcon = (trend: "up" | "down" | "stable" | undefined) => {
        if (trend === "up") return <TrendingUp className="h-3 w-3 text-green-500" />
        if (trend === "down") return <TrendingDown className="h-3 w-3 text-red-500" />
        return <Minus className="h-3 w-3 text-muted-foreground" />
    }

    // Determine position style
    const style = isMobile ? {
        transform: `translateY(calc(${isMinimized ? '100% - 24px' : '0px'} + ${dragOffset}px))`,
        transition: touchStart === null ? 'transform 0.3s ease-out' : 'none'
    } : isEmbedded ? {
        // Embedded mode uses CSS positioning entirely (right/top/bottom via classes)
    } : {
        left: globalX ?? x,
        top: globalY ?? y,
    }

    const content = (
        <div
            className={cn(
                "z-[9999] glass-panel shadow-2xl",
                // Mobile layout
                isMobile
                    ? "fixed bottom-0 left-0 right-0 w-full rounded-t-xl rounded-b-none border-t border-x-0 border-b-0 pointer-events-auto transition-transform duration-300 ease-out touch-none overflow-hidden"
                    // Embedded layouts
                    : isEmbedded
                        ? "fixed right-4 top-4 bottom-4 w-[320px] rounded-xl pointer-events-auto overflow-y-auto"
                        // Default floating desktop layout
                        : "fixed rounded-xl w-[320px] overflow-hidden",
                lockedMode && !isMobile && !isEmbedded ? "pointer-events-auto cursor-move" : "pointer-events-none",
                showDragHint && "animate-pulse"
            )}
            style={style}
            onMouseDown={onMouseDown}
            onTouchStart={onTouchStart}
            onTouchMove={onTouchMove}
            onTouchEnd={onTouchEnd}
        >
            {displayProps.has_data ? (
                <div className="flex flex-col">
                    {/* Mobile Drag Handle */}
                    {isMobile && (
                        <div className="w-full flex justify-center py-2 bg-muted/40 backdrop-blur-md border-b-0 cursor-grab active:cursor-grabbing">
                            <div className="w-10 h-1 bg-muted-foreground/30 rounded-full" />
                        </div>
                    )}

                    {/* Desktop Headers (Hidden on Mobile) */}
                    {!isMobile && (
                        <>
                            <div className="flex items-center justify-between px-3 py-2 border-b border-border/50 bg-muted/40 backdrop-blur-md">
                                <div className="flex items-center gap-2">
                                    <HomecastrLogo variant="horizontal" size={18} />
                                    {lockedMode && (
                                        <span className="px-1.5 py-0.5 bg-yellow-500/20 text-yellow-600 dark:text-yellow-400 text-[8px] font-semibold uppercase tracking-wider rounded">Locked</span>
                                    )}
                                </div>
                                <div className="flex items-center gap-2">
                                    {lockedMode && <span className="text-[9px] text-muted-foreground">ESC to exit</span>}
                                </div>
                            </div>
                            <div className="p-3 border-b border-border/50 bg-muted/30">
                                <div className="flex justify-between items-start">
                                    <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold mb-0.5">
                                        {h3Resolution <= 7 ? "District Scale" : h3Resolution <= 9 ? "Neighborhood Scale" : h3Resolution <= 10 ? "Block Scale" : "Property Scale"} (Res {h3Resolution})
                                    </div>
                                    <div className="text-[9px] px-1.5 py-0.5 bg-primary/10 text-primary rounded-full font-bold">
                                        {displayProps.n_accts} Prop
                                    </div>
                                </div>
                                {(displayDetails?.locationLabel || coordinates) && (
                                    <div className="text-xs text-foreground font-medium truncate mt-0.5">
                                        {displayDetails?.locationLabel || (coordinates ? coordinates.map((n: number) => n.toFixed(5)).join(", ") : null)}
                                    </div>
                                )}
                            </div>
                        </>
                    )}

                    {/* Street View Carousel — ONLY in locked/click mode, never on hover
                       This prevents expensive Google API calls on every mouse flyover.
                       Street view is already shown in the forecast-map inspector panel on click. */}
                    {lockedMode && displayProps.has_data && googleMapsApiKey && (
                        <StreetViewCarousel
                            h3Ids={selectedHexes.length > 0 ? selectedHexes : hoveredDetails?.id ? [hoveredDetails.id] : []}
                            apiKey={googleMapsApiKey}
                        />
                    )}


                    {/* Content Body */}
                    {isMobile ? (
                        /* Mobile Layout: 2x2 Grid + Chart Side-by-Side */
                        <div className="p-3 flex gap-2 items-stretch h-[140px]">
                            {/* Left: Stats Grid (45%) - Centered */}
                            <div className="flex flex-col justify-center items-center w-[45%] shrink-0">
                                <div className="grid grid-cols-2 gap-x-2 gap-y-1 w-full text-center">
                                    <div>
                                        <div className="text-[9px] uppercase tracking-wider text-muted-foreground font-semibold truncate">Value</div>
                                        <div className="text-sm font-bold text-foreground tracking-tight truncate">
                                            {displayProps.med_predicted_value ? formatCurrency(displayProps.med_predicted_value) : "N/A"}
                                        </div>
                                    </div>
                                    <div>
                                        <div className="text-[9px] uppercase tracking-wider text-muted-foreground font-semibold truncate">Growth %</div>
                                        <div className={cn("text-sm font-bold tracking-tight truncate", displayProps.O >= 0 ? "text-green-500" : "text-destructive")}>
                                            {formatOpportunity(displayProps.O)}
                                        </div>
                                    </div>
                                    <div>
                                        <div className="text-[9px] uppercase tracking-wider text-muted-foreground font-semibold truncate">Growth $</div>
                                        <div className={cn("text-sm font-bold tracking-tight truncate",
                                            (displayProps.med_predicted_value ?? displayProps.p50) && (displayProps.current_value ?? displayProps.baseline_value ?? displayProps.value) &&
                                                ((displayProps.med_predicted_value ?? displayProps.p50) - (displayProps.current_value ?? displayProps.baseline_value ?? displayProps.value)) >= 0
                                                ? "text-green-500" : "text-destructive"
                                        )}>
                                            {(displayProps.med_predicted_value ?? displayProps.p50) && (displayProps.current_value ?? displayProps.baseline_value ?? displayProps.value)
                                                ? formatCurrency((displayProps.med_predicted_value ?? displayProps.p50) - (displayProps.current_value ?? displayProps.baseline_value ?? displayProps.value))
                                                : "N/A"}
                                        </div>
                                    </div>
                                    <div className="col-span-2">
                                        <div className="text-[9px] uppercase tracking-wider text-muted-foreground font-semibold truncate">Properties</div>
                                        <div className="text-sm font-medium text-foreground truncate">{displayProps.n_accts}</div>
                                    </div>
                                </div>
                            </div>

                            {/* Right: Fan Chart (Remaining) */}
                            <div className="flex-1 min-w-0 h-full relative">
                                {displayDetails?.fanChart ? (
                                    <div className="h-full w-full">
                                        <FanChart
                                            data={displayDetails.fanChart}
                                            currentYear={year}
                                            height={130}
                                            historicalValues={displayDetails.historicalValues}
                                            comparisonData={selectedHexes.length > 1 ? previewDetails?.fanChart : comparisonDetails?.fanChart}
                                            comparisonHistoricalValues={selectedHexes.length > 1 ? previewDetails?.historicalValues : comparisonDetails?.historicalValues}
                                            onYearChange={onYearChange}
                                        />
                                    </div>
                                ) : (
                                    <div className="h-full flex items-center justify-center">
                                        {isLoadingDetails && <div className="w-4 h-4 border-2 border-primary/30 border-t-primary rounded-full animate-spin" />}
                                    </div>
                                )}
                            </div>
                        </div>
                    ) : (
                        /* Desktop Layout: Stacked */
                        <div className="p-4 space-y-5">
                            <div className="grid grid-cols-2 gap-3">
                                <div className="text-center pl-6">
                                    <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold mb-1">
                                        {year <= 2025 ? "Actual" : "Predicted"} ({year})
                                    </div>
                                    <div className="text-xl font-bold text-foreground tracking-tight">
                                        {displayProps.med_predicted_value ? formatCurrency(displayProps.med_predicted_value) : "N/A"}
                                    </div>
                                    {displayDetails?.historicalValues && (
                                        <div className="text-[10px] text-muted-foreground mt-1">
                                            Current (2025): <span className="text-foreground">{formatCurrency(displayDetails.historicalValues[displayDetails.historicalValues.length - 1])}</span>
                                        </div>
                                    )}
                                </div>
                                <div className="text-center">
                                    <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold mb-1">Growth %</div>
                                    <div className={cn("text-xl font-bold tracking-tight flex items-center justify-center gap-1", displayProps.O >= 0 ? "text-green-500" : "text-destructive")}>
                                        {formatOpportunity(displayProps.O)}
                                        {getTrendIcon(displayProps.O >= 0 ? "up" : "down")}
                                    </div>
                                </div>
                                <div className="text-center pr-6">
                                    <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold mb-1">Growth $</div>
                                    <div className={cn("text-xl font-bold tracking-tight flex items-center justify-center gap-1",
                                        (displayProps.med_predicted_value ?? displayProps.p50) && (displayProps.current_value ?? displayProps.baseline_value ?? displayProps.value) &&
                                            ((displayProps.med_predicted_value ?? displayProps.p50) - (displayProps.current_value ?? displayProps.baseline_value ?? displayProps.value)) >= 0
                                            ? "text-green-500" : "text-destructive"
                                    )}>
                                        {(displayProps.med_predicted_value ?? displayProps.p50) && (displayProps.current_value ?? displayProps.baseline_value ?? displayProps.value)
                                            ? formatCurrency((displayProps.med_predicted_value ?? displayProps.p50) - (displayProps.current_value ?? displayProps.baseline_value ?? displayProps.value))
                                            : "N/A"}
                                        {getTrendIcon((displayProps.med_predicted_value ?? displayProps.p50) && (displayProps.current_value ?? displayProps.baseline_value ?? displayProps.value) &&
                                            ((displayProps.med_predicted_value ?? displayProps.p50) - (displayProps.current_value ?? displayProps.baseline_value ?? displayProps.value)) >= 0
                                            ? "up" : "down")}
                                    </div>
                                </div>
                            </div>

                            {(displayDetails?.fanChart || primaryDetails?.fanChart) ? (
                                <div className="space-y-2">
                                    <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold flex justify-between">
                                        <span>Value Timeline</span>
                                        <span className="text-primary/70">{year}</span>
                                    </div>
                                    <div className="h-44 -mx-2 mb-2">
                                        <FanChart
                                            // Line 1: Primary (Anchor) - always first hex's data
                                            // Fallback to displayDetails (hovered) if no primary (i.e. no selection)
                                            data={(primaryDetails?.fanChart || displayDetails?.fanChart)!}
                                            currentYear={year}
                                            height={160}
                                            historicalValues={primaryDetails?.historicalValues || displayDetails?.historicalValues}

                                            // Line 2: Selection Aggregate (Orange)
                                            comparisonData={selectedHexes.length > 1 ? selectionDetails?.fanChart : null}
                                            comparisonHistoricalValues={selectedHexes.length > 1 ? selectionDetails?.historicalValues : null}

                                            // Line 3: Preview (Fuchsia) - Aggregate of Selection + Candidate
                                            // Show hoveredDetails explicitly as Candidate only if it's NOT already selected
                                            previewData={previewDetails?.fanChart || (selectedHexes.length > 0 && hoveredDetails && !selectedHexes.includes(hoveredDetails.id) ? hoveredDetails?.fanChart : null)}
                                            previewHistoricalValues={previewDetails?.historicalValues || (selectedHexes.length > 0 && hoveredDetails && !selectedHexes.includes(hoveredDetails.id) ? hoveredDetails?.historicalValues : null)}

                                            onYearChange={onYearChange}
                                        />
                                    </div>
                                </div>
                            ) : (
                                isLoadingDetails && (
                                    <div className="h-32 flex items-center justify-center">
                                        <div className="w-5 h-5 border-2 border-primary/30 border-t-primary rounded-full animate-spin" />
                                    </div>
                                )
                            )}

                            <div className="pt-1 mt-0 border-t border-border/50 text-center">
                                <div className="text-[9px] text-muted-foreground flex justify-center items-center gap-1.5">
                                    <Bot className="w-3 h-3 text-primary/50" />
                                    <span>AI Predictive Confidence: <span className="text-foreground font-medium">{formatReliability(displayProps.R)}</span></span>
                                </div>
                            </div>

                            {lockedMode && onConsultAI && (
                                <div className="pt-2 mt-1 border-t border-border/50">
                                    <button
                                        onClick={(e) => {
                                            e.stopPropagation()
                                            onConsultAI()
                                        }}
                                        className="w-full flex items-center justify-center gap-2 px-3 py-2 rounded-lg bg-primary/15 hover:bg-primary/25 border border-primary/30 text-primary text-xs font-semibold transition-all hover:scale-[1.02] active:scale-[0.98]"
                                    >
                                        <HomecastrLogo variant="horizontal" size={14} />
                                        <span>Talk to live agent</span>
                                    </button>
                                </div>
                            )}

                            {/* View Forecast Hub link — appears when forecast URL is resolved */}
                            {lockedMode && forecastsHref && (
                                <div className={onConsultAI ? "pt-1" : "pt-2 mt-1 border-t border-border/50"}>
                                    <Link
                                        href={forecastsHref}
                                        target="_blank"
                                        rel="noopener noreferrer"
                                        onClick={(e) => e.stopPropagation()}
                                        className="w-full flex items-center justify-center gap-2 px-3 py-2 rounded-lg bg-muted/30 hover:bg-muted/60 border border-border/50 text-muted-foreground hover:text-foreground text-xs font-medium transition-all hover:scale-[1.01]"
                                    >
                                        <Map className="w-3 h-3 text-primary" />
                                        <span>View Forecast Hub →</span>
                                    </Link>
                                </div>
                            )}
                        </div>
                    )
                    }
                </div>
            ) : (
                <div className="p-3">
                    <div className="font-medium text-muted-foreground text-xs">No Residential Properties</div>
                </div>
            )
            }
        </div >
    )

    // Render Portal
    return createPortal(content, document.body)
}
