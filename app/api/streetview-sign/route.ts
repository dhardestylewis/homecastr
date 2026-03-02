import { NextRequest, NextResponse } from "next/server"
import crypto from "crypto"
import { getGcsBucket, gcsPublicUrl } from "@/lib/gcs"
import { getSupabaseAdmin } from "@/lib/supabase/admin"

/**
 * GET /api/streetview-sign?lat=29.76&lng=-95.36&w=400&h=300
 *
 * Cache lookup order (fastest → slowest, cheapest → most expensive):
 *   1. Supabase  – check street_view_cache for a stored GCS path → free
 *   2. GCS       – if path found, return public GCS URL             → free
 *   3. Google API – fetch image bytes, upload to GCS, upsert Supabase → $0.007
 */
export async function GET(req: NextRequest) {
    const { searchParams } = req.nextUrl
    const lat = searchParams.get("lat")
    const lng = searchParams.get("lng")
    const w = parseInt(searchParams.get("w") || "400")
    const h = parseInt(searchParams.get("h") || "300")

    if (!lat || !lng) {
        return NextResponse.json({ error: "Missing lat/lng" }, { status: 400 })
    }

    const latF = parseFloat(lat)
    const lngF = parseFloat(lng)
    const lat5 = latF.toFixed(5)
    const lng5 = lngF.toFixed(5)

    const bucketName = process.env.GCS_STREETVIEW_BUCKET

    // ── 1 & 2. Supabase cache lookup ─────────────────────────────────────────
    if (bucketName) {
        try {
            const supabase = getSupabaseAdmin()
            const { data } = await supabase
                .from("street_view_cache")
                .select("gcs_path")
                .eq("lat5", lat5)
                .eq("lng5", lng5)
                .eq("w", w)
                .eq("h", h)
                .maybeSingle()

            if (data?.gcs_path) {
                console.log(`[SV-CACHE] HIT  ${lat5},${lng5} ${w}x${h}`)
                return NextResponse.json(
                    { url: gcsPublicUrl(bucketName, data.gcs_path) },
                    { headers: { "Cache-Control": "public, max-age=86400" } }
                )
            }
        } catch (err) {
            // Non-fatal: fall through to Google API on any cache error
            console.warn("[SV-CACHE] Supabase lookup failed:", err)
        }
    }

    // ── 3. Build the Google Street View URL ──────────────────────────────────
    const apiKey = process.env.NEXT_PUBLIC_GOOGLE_MAPS_KEY
    const signingSecret = process.env.GOOGLE_MAPS_SIGNING_SECRET

    if (!apiKey) {
        return NextResponse.json({ error: "Missing NEXT_PUBLIC_GOOGLE_MAPS_KEY" }, { status: 500 })
    }

    const path = `/maps/api/streetview?size=${w}x${h}&location=${lat},${lng}&key=${apiKey}`

    let googleUrl: string
    if (signingSecret) {
        const decodedKey = Buffer.from(
            signingSecret.replace(/-/g, "+").replace(/_/g, "/"),
            "base64"
        )
        const signature = crypto
            .createHmac("sha1", decodedKey)
            .update(path)
            .digest("base64")
            .replace(/\+/g, "-")
            .replace(/\//g, "_")
        googleUrl = `https://maps.googleapis.com${path}&signature=${signature}`
    } else {
        googleUrl = `https://maps.googleapis.com${path}`
    }

    // ── 4. Cache miss — fetch image from Google ───────────────────────────────
    if (!bucketName) {
        // GCS not configured — return the signed Google URL directly (old behaviour)
        return NextResponse.json({ url: googleUrl })
    }

    console.log(`[SV-CACHE] MISS ${lat5},${lng5} ${w}x${h} — fetching from Google`)

    let imageBytes: Buffer
    try {
        const res = await fetch(googleUrl)
        if (!res.ok) {
            console.warn(`[SV-CACHE] Google API returned ${res.status} — returning URL directly`)
            return NextResponse.json({ url: googleUrl })
        }
        imageBytes = Buffer.from(await res.arrayBuffer())
    } catch (err) {
        console.warn("[SV-CACHE] Google fetch failed:", err)
        return NextResponse.json({ url: googleUrl })
    }

    // ── 5. Upload to GCS ──────────────────────────────────────────────────────
    const gcsPath = `streetview/${lat5},${lng5}_${w}x${h}.jpg`
    try {
        const bucket = getGcsBucket(bucketName)
        const file = bucket.file(gcsPath)
        await file.save(imageBytes, {
            contentType: "image/jpeg",
            predefinedAcl: "publicRead",
            resumable: false,
        })
        console.log(`[SV-CACHE] Uploaded → gs://${bucketName}/${gcsPath}`)
    } catch (err) {
        console.warn("[SV-CACHE] GCS upload failed:", err)
        // Return the signed Google URL as fallback — still usable, just not cached
        return NextResponse.json({ url: googleUrl })
    }

    // ── 6. Upsert into Supabase ───────────────────────────────────────────────
    try {
        const supabase = getSupabaseAdmin()
        await supabase
            .from("street_view_cache")
            .upsert({ lat5, lng5, w, h, gcs_path: gcsPath })
        console.log(`[SV-CACHE] Supabase upsert OK`)
    } catch (err) {
        // Non-fatal: image is in GCS, just not indexed yet
        console.warn("[SV-CACHE] Supabase upsert failed:", err)
    }

    return NextResponse.json(
        { url: gcsPublicUrl(bucketName, gcsPath) },
        { headers: { "Cache-Control": "public, max-age=86400" } }
    )
}
