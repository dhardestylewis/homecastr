import { NextResponse } from "next/server"
import { getSupabaseAdmin } from "@/lib/supabase/admin"

/**
 * Forecast choropleth MVT tile endpoint.
 *
 * Calls forecast_20260220_7f31c6e4.mvt_choropleth_forecast(z,x,y,origin_year,horizon_m)
 * which auto-routes by zoom:
 *   z <= 7  → ZCTA
 *   z <= 11 → Tract
 *   z <= 16 → Tabblock
 *   z >= 17 → Parcel (capped at 3500)
 *
 * Query params:
 *   originYear  (default 2025)
 *   horizonM    (default 12)
 *   seriesKind  (default 'forecast')
 *   variantId   (default '__forecast__')
 *   level       (optional override: zcta/tract/tabblock/parcel/unsd/neighborhood)
 */
export async function GET(
    request: Request,
    { params }: { params: Promise<{ z: string; x: string; y: string }> }
) {
    const { z: zStr, x: xStr, y: yStr } = await params

    const z = parseInt(zStr)
    const x = parseInt(xStr)
    const y = parseInt(yStr)

    const { searchParams } = new URL(request.url)
    const originYear = parseInt(searchParams.get("originYear") || "2025")
    const horizonM = parseInt(searchParams.get("horizonM") || "12")
    const seriesKind = searchParams.get("seriesKind") || "forecast"
    const variantId = searchParams.get("variantId") || "__forecast__"
    const levelOverride = searchParams.get("level") || null

    const supabase = getSupabaseAdmin()

    try {
        // Call the schema-qualified RPC via the admin client
        // The function lives in forecast_20260220_7f31c6e4 schema
        const { data, error } = await supabase
            .schema("forecast_20260220_7f31c6e4" as any)
            .rpc("mvt_choropleth_forecast", {
                z,
                x,
                y,
                p_origin_year: originYear,
                p_horizon_m: horizonM,
                p_level_override: levelOverride,
                p_series_kind: seriesKind,
                p_variant_id: variantId,
                p_run_id: null,
                p_backtest_id: null,
                p_parcel_limit: 3500,
            })

        if (error) {
            console.error("[FORECAST-TILE] RPC Error:", {
                message: error.message,
                details: error.details,
                hint: error.hint,
                code: error.code,
                params: { z, x, y, originYear, horizonM, seriesKind, variantId, levelOverride },
            })
            return NextResponse.json({ error: error.message }, { status: 500 })
        }

        if (!data) {
            return new NextResponse(null, { status: 204 })
        }

        // Supabase bytea → Buffer
        let buffer: Buffer
        if (typeof data === "string") {
            if (data.startsWith("\\x")) {
                buffer = Buffer.from(data.substring(2), "hex")
            } else {
                buffer = Buffer.from(data, "base64")
            }
        } else {
            buffer = Buffer.from(data)
        }

        return new NextResponse(new Uint8Array(buffer), {
            status: 200,
            headers: {
                "Content-Type": "application/vnd.mapbox-vector-tile",
                "Cache-Control": "public, max-age=3600",
                "Access-Control-Allow-Origin": "*",
            },
        })
    } catch (e: any) {
        console.error("[FORECAST-TILE] Error:", e)
        return NextResponse.json({ error: e.message }, { status: 500 })
    }
}
