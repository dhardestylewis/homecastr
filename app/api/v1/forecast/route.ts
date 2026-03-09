import { NextRequest, NextResponse } from "next/server"
import { requireApiKey } from "@/lib/api-auth"
import { forwardGeocode } from "@/lib/forward-geocode"
import { getH3CellDetails } from "@/app/actions/h3-details"
import { latLngToCell } from "h3-js"

const H3_RESOLUTION = 8 // neighbourhood-level hex

export async function GET(req: NextRequest) {
    // ── Auth ──
    const authError = await requireApiKey(req)
    if (authError) return authError

    // ── Params ──
    const { searchParams } = new URL(req.url)
    const address = searchParams.get("address")
    const yearParam = searchParams.get("year")
    const forecastYear = yearParam ? parseInt(yearParam, 10) : 2030

    if (!address) {
        return NextResponse.json(
            {
                error: "Query parameter 'address' is required.",
                example: "GET /api/v1/forecast?address=123+Main+St+Houston+TX",
            },
            { status: 400 }
        )
    }

    try {
        // ── 1. Forward geocode ──
        const geo = await forwardGeocode(address)
        if (!geo) {
            return NextResponse.json(
                {
                    error: `Could not geocode address: "${address}". Try a more specific US address.`,
                    tip: "Include street number, city, and state. Example: 123 Main St Houston TX",
                },
                { status: 404 }
            )
        }

        // ── 2. Address → H3 cell ──
        const h3Cell = latLngToCell(geo.lat, geo.lng, H3_RESOLUTION)

        // ── 3. Fetch forecast data ──
        const details = await getH3CellDetails(h3Cell, forecastYear)

        if (!details) {
            return NextResponse.json(
                {
                    error: `No forecast data available for this location yet.`,
                    address: geo.displayName,
                    coordinates: { lat: geo.lat, lng: geo.lng },
                    h3_cell: h3Cell,
                },
                { status: 404 }
            )
        }

        // ── 4. Build clean developer-friendly response ──
        const currentValue = details.proforma?.predicted_value ?? null
        const opportunityPct = details.opportunity?.value ?? null

        // Build fan chart with absolute years if available
        const baseYear = 2025
        let fanChart: any = null
        if (details.fanChart) {
            fanChart = {
                years: details.fanChart.years.map((y: number) => baseYear + y),
                p10: details.fanChart.p10,
                p50: details.fanChart.p50,
                p90: details.fanChart.p90,
            }
        }

        // Extract the specific horizon forecast from fan chart
        const horizonIndex = Math.min(forecastYear - baseYear, 5) - 1
        let forecasts: any = null
        if (fanChart && horizonIndex >= 0 && horizonIndex < fanChart.p50.length) {
            forecasts = {
                p10: Math.round(fanChart.p10[horizonIndex]),
                p50: Math.round(fanChart.p50[horizonIndex]),
                p90: Math.round(fanChart.p90[horizonIndex]),
            }
        }

        return NextResponse.json({
            address: geo.displayName,
            coordinates: { lat: geo.lat, lng: geo.lng },
            h3_cell: h3Cell,
            forecast_year: forecastYear,
            current_value: currentValue ? Math.round(currentValue) : null,
            appreciation_pct: opportunityPct != null ? Math.round(opportunityPct * 10) / 10 : null,
            forecasts,
            horizon_years: forecastYear - baseYear,
            reliability: details.reliability?.value ?? null,
            fan_chart: fanChart
                ? {
                    years: fanChart.years,
                    p10: fanChart.p10.map((v: number) => Math.round(v)),
                    p50: fanChart.p50.map((v: number) => Math.round(v)),
                    p90: fanChart.p90.map((v: number) => Math.round(v)),
                }
                : null,
            property_count: details.metrics?.n_accts ?? null,
            _links: {
                self: `/api/v1/forecast?address=${encodeURIComponent(address)}&year=${forecastYear}`,
                hex: `/api/v1/forecast/hex?h3_id=${h3Cell}&year=${forecastYear}`,
                docs: "/api-docs",
            },
        })
    } catch (error: any) {
        console.error("[API] Address forecast error:", error)
        return NextResponse.json(
            { error: error.message || "Internal server error" },
            { status: 500 }
        )
    }
}
