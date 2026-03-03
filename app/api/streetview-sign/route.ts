import { NextRequest, NextResponse } from "next/server"
import crypto from "crypto"
import { getGcsBucket, gcsPublicUrl } from "@/lib/gcs"

/**
 * GET /api/streetview-sign?lat=29.76&lng=-95.36&w=400&h=300
 *
 * GCS-only cache — no Supabase table needed.
 *   1. Check if gs://<bucket>/streetview/<lat5>,<lng5>_<w>x<h>.jpg exists
 *   2. HIT  → return public GCS URL (free)
 *   3. MISS → fetch from Google ($0.007), upload to GCS, return GCS URL
 *
 * If GCS_STREETVIEW_BUCKET is not set, falls back to direct Google URL (old behaviour).
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

    const lat5 = parseFloat(lat).toFixed(5)
    const lng5 = parseFloat(lng).toFixed(5)
    const bucketName = process.env.GCS_STREETVIEW_BUCKET
    const gcsPath = `streetview/${lat5},${lng5}_${w}x${h}.jpg`

    // ── 1. GCS cache lookup ──────────────────────────────────────────────────
    if (bucketName) {
        try {
            const bucket = getGcsBucket(bucketName)
            const [exists] = await bucket.file(gcsPath).exists()
            if (exists) {
                console.log(`[SV-CACHE] HIT  ${gcsPath}`)
                return NextResponse.json(
                    { url: gcsPublicUrl(bucketName, gcsPath) },
                    { headers: { "Cache-Control": "public, max-age=86400" } }
                )
            }
        } catch (err) {
            console.warn("[SV-CACHE] GCS lookup failed:", err)
        }
    }

    // ── 2. Build the Google Street View URL ──────────────────────────────────
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

    // ── 3. No GCS configured — old behaviour ─────────────────────────────────
    if (!bucketName) {
        return NextResponse.json({ url: googleUrl })
    }

    // ── 4. Cache miss — fetch from Google & upload to GCS ────────────────────
    console.log(`[SV-CACHE] MISS ${gcsPath} — fetching from Google`)

    let imageBytes: Buffer
    try {
        const res = await fetch(googleUrl)
        if (!res.ok) {
            console.warn(`[SV-CACHE] Google API ${res.status}, returning URL directly`)
            return NextResponse.json({ url: googleUrl })
        }
        imageBytes = Buffer.from(await res.arrayBuffer())
    } catch (err) {
        console.warn("[SV-CACHE] Google fetch failed:", err)
        return NextResponse.json({ url: googleUrl })
    }

    try {
        const bucket = getGcsBucket(bucketName)
        await bucket.file(gcsPath).save(imageBytes, {
            contentType: "image/jpeg",
            predefinedAcl: "publicRead",
            resumable: false,
        })
        console.log(`[SV-CACHE] Uploaded → gs://${bucketName}/${gcsPath}`)
    } catch (err) {
        console.warn("[SV-CACHE] GCS upload failed:", err)
        return NextResponse.json({ url: googleUrl })
    }

    return NextResponse.json(
        { url: gcsPublicUrl(bucketName, gcsPath) },
        { headers: { "Cache-Control": "public, max-age=86400" } }
    )
}
