
import { NextRequest, NextResponse } from "next/server"
import { getH3CellDetails } from "@/app/actions/h3-details"
import { requireApiKey } from "@/lib/api-auth"

export async function GET(req: NextRequest) {
    const authError = await requireApiKey(req)
    if (authError) return authError

    const { searchParams } = new URL(req.url)
    const h3_id = searchParams.get("h3_id")
    const year = parseInt(searchParams.get("year") || "2026")

    if (!h3_id) {
        return NextResponse.json(
            { error: "Query parameter 'h3_id' is required." },
            { status: 400 }
        )
    }

    try {
        const details = await getH3CellDetails(h3_id, year)

        if (!details) {
            return NextResponse.json(
                { error: `No data found for H3 cell ${h3_id} in year ${year}.` },
                { status: 404 }
            )
        }

        // Standardize the response to include the requested scenarios and bands
        return NextResponse.json({
            h3_id: details.id,
            forecast_year: year,
            location: details.locationLabel,
            coordinates: details.coordinates,
            opportunity: details.opportunity,
            reliability: details.reliability,
            metrics: details.metrics,
            proforma: details.proforma,
            scenarios: {
                baseline: details.proforma?.predicted_value || null,
                stress_test: details.riskScoring || null,
            },
            bands: details.fanChart ? {
                p10: details.fanChart.p10,
                p50: details.fanChart.p50,
                p90: details.fanChart.p90,
                years_horizon: details.fanChart.years
            } : null
        })
    } catch (error: any) {
        console.error("[API] Hex forecast error:", error)
        return NextResponse.json(
            { error: error.message || "Internal server error" },
            { status: 500 }
        )
    }
}
