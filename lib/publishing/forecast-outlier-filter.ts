/**
 * Centralized outlier filter for forecast tract data.
 *
 * Replaces ad-hoc inline guards scattered across /forecasts pages with
 * consistent thresholds and structured tagging for later inspection.
 */
import { getSupabaseAdmin } from "@/lib/supabase/admin"

// ── Thresholds ──────────────────────────────────────────────────────────────
/** Minimum plausible h12 median home value ($) */
export const P50_MIN = 20_000
/** Maximum plausible h12 median home value ($) — caps runaway valuations */
export const P50_MAX = 5_000_000
/** Floor for 5-year appreciation (%) — anything below is likely model blow-up */
export const APPR_MIN = -95
/** Ceiling for 5-year appreciation (%) */
export const APPR_MAX = 100
/** Maximum ratio of h60/h12 — catches exponential runaway even when individual
 *  values stay under P50_MAX (e.g. h12=$300K → h60=$30M) */
export const GROWTH_RATIO_MAX = 10

// ── Types ───────────────────────────────────────────────────────────────────
export interface OutlierTag {
    tractGeoid: string
    reason: string
    h12: number
    h60: number | null
    schema: string
    timestamp: string
}

export interface OutlierResult {
    outlier: boolean
    reason?: string
}

// ── Core filter ─────────────────────────────────────────────────────────────
/**
 * Determine whether a tract's h12/h60 forecast values are outliers.
 *
 * Returns `{ outlier: false }` for clean data, or
 * `{ outlier: true, reason: "..." }` for flagged data.
 */
export function isOutlierTract(h12: number, h60: number | null | undefined): OutlierResult {
    if (h12 < P50_MIN) return { outlier: true, reason: `h12 below floor ($${h12.toFixed(0)} < $${P50_MIN})` }
    if (h12 >= P50_MAX) return { outlier: true, reason: `h12 above cap ($${h12.toFixed(0)} >= $${P50_MAX})` }

    if (h60 != null) {
        if (h60 < 0) return { outlier: true, reason: `h60 negative ($${h60.toFixed(0)})` }

        const appr = ((h60 - h12) / h12) * 100
        if (appr <= APPR_MIN) return { outlier: true, reason: `appreciation ${appr.toFixed(1)}% <= ${APPR_MIN}%` }
        if (appr > APPR_MAX) return { outlier: true, reason: `appreciation ${appr.toFixed(1)}% > ${APPR_MAX}%` }

        const ratio = h60 / h12
        if (ratio > GROWTH_RATIO_MAX) return { outlier: true, reason: `h60/h12 ratio ${ratio.toFixed(1)}x > ${GROWTH_RATIO_MAX}x` }
    }

    return { outlier: false }
}

// ── Tagging / Logging ───────────────────────────────────────────────────────
/**
 * Emit structured JSON log lines for flagged tracts.
 *
 * These lines are captured by Vercel's log pipeline and can be queried later
 * for inspection, replacement batches, or alerting.  Format:
 *
 *   [OUTLIER_FLAG] { tractGeoid, reason, h12, h60, schema, timestamp }
 */
export async function logFlaggedOutliers(flagged: OutlierTag[]): Promise<void> {
    if (flagged.length === 0) return
    console.warn(
        `[OUTLIER_FLAG] ${flagged.length} tracts flagged as outliers:`,
        JSON.stringify(flagged.slice(0, 20)),  // cap log line size
        flagged.length > 20 ? `... and ${flagged.length - 20} more` : "",
    )

    try {
        const supabase = getSupabaseAdmin()
        const rows = flagged.map(f => ({
            tract_geoid: f.tractGeoid,
            reason: f.reason,
            h12: f.h12,
            h60: f.h60,
            schema_name: f.schema,
        }))

        // Upsert on tract_geoid + schema_name to prevent duplicating records on multiple runs
        const { error } = await supabase
            .from("outlier_flags")
            .upsert(rows, { onConflict: "tract_geoid,schema_name" })

        if (error) {
            console.error("[OUTLIER_FLAG] Failed to execute Supabase upsert:", error.message)
        }
    } catch (e: any) {
        console.error("[OUTLIER_FLAG] Supabase execution exception:", e?.message || e)
    }
}

/**
 * Helper to create a tag entry.
 */
export function createOutlierTag(
    tractGeoid: string,
    reason: string,
    h12: number,
    h60: number | null,
    schema: string,
): OutlierTag {
    return { tractGeoid, reason, h12, h60, schema, timestamp: new Date().toISOString() }
}
