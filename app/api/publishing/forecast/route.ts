import { NextResponse } from "next/server"
import { fetchForecastPageData } from "@/lib/publishing/forecast-data"
import { parseTractGeoid, enrichWithNeighborhood } from "@/lib/publishing/geo-crosswalk"

const SCHEMA = process.env.FORECAST_SCHEMA || "forecast_queue"

/**
 * GET /api/publishing/forecast?level=tract&id=48201240100&originYear=2025
 *
 * Returns the complete data package for a forecast publishing page.
 */
export async function GET(request: Request) {
    const { searchParams } = new URL(request.url)
    const id = searchParams.get("id")
    const originYear = parseInt(searchParams.get("originYear") || "2025")
    const schema = searchParams.get("schema") || SCHEMA

    if (!id) {
        return NextResponse.json({ error: "id is required" }, { status: 400 })
    }

    try {
        const data = await fetchForecastPageData(id, originYear, schema)
        if (!data) {
            return NextResponse.json({ error: "No forecast data found" }, { status: 404 })
        }

        // Quality gate: minimum unique data tokens
        if (data.uniqueDataTokens < 150) {
            return NextResponse.json(
                { error: "Insufficient data density", uniqueDataTokens: data.uniqueDataTokens },
                { status: 404 }
            )
        }

        // Enrich with geography info
        let geo = parseTractGeoid(id)
        geo = await enrichWithNeighborhood(geo)

        return NextResponse.json({
            ...data,
            meta: {
                tractGeoid: geo.tractGeoid,
                neighborhoodName: geo.neighborhoodName,
                city: geo.city,
                stateAbbr: geo.stateAbbr,
                stateName: geo.stateName,
                zcta5: geo.zcta5,
                schemaVersion: schema,
                originYear: data.forecast.originYear,
            },
        }, {
            headers: { "Cache-Control": "public, max-age=3600, stale-while-revalidate=86400" },
        })
    } catch (e: any) {
        console.error("[PUBLISHING] Error:", e)
        return NextResponse.json({ error: e.message }, { status: 500 })
    }
}
