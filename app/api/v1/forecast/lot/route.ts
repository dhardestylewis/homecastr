
import { NextRequest, NextResponse } from "next/server"
import { getPropertyForecast } from "@/app/actions/property-forecast"
import { withApiLogging } from "@/lib/api-auth"

export const GET = withApiLogging(async (req: NextRequest) => {
    const { searchParams } = new URL(req.url)
    const acct = searchParams.get("acct")

    if (!acct) {
        return NextResponse.json(
            { error: "Query parameter 'acct' is required." },
            { status: 400 }
        )
    }

    const forecasts = await getPropertyForecast(acct)

    if (!forecasts || forecasts.length === 0) {
        return NextResponse.json(
            { error: `No forecasts found for account ${acct}.` },
            { status: 404 }
        )
    }

    return NextResponse.json({
        acct,
        forecasts: forecasts.map(f => ({
            year: f.yr,
            valuation: f.valuation
        }))
    })
})
