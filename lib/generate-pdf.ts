"use client"

import { jsPDF } from "jspdf"

interface PinnedComparisonPDF {
    label: string
    historicalValues?: number[]
    p50?: number[]
    p10?: number[]
    p90?: number[]
    years?: number[]
}

interface ForecastPDFData {
    locationName: string
    locationId: string
    currentYear: number
    historicalValues: (number | null)[]
    p50: number[]
    p10: number[]
    p90: number[]
    years: number[]
    mapImageDataUrl?: string
    shareUrl: string
    coords?: [number, number] // [lat, lng] for street view
    pinnedComparisons?: PinnedComparisonPDF[]
}

const HOMECASTR_URL = "https://homecastr.com"
const CONTACT_EMAIL = "daniel@homecastr.com"

function fmt$(v: number | null | undefined): string {
    if (v == null || !Number.isFinite(v)) return "N/A"
    return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(v)
}

function fmtPct(v: number): string {
    return `${v > 0 ? "+" : ""}${(v * 100).toFixed(1)}%`
}

function hexRgb(hex: string): [number, number, number] {
    return [parseInt(hex.slice(1, 3), 16), parseInt(hex.slice(3, 5), 16), parseInt(hex.slice(5, 7), 16)]
}

export async function generateForecastPDF(data: ForecastPDFData): Promise<void> {
    const doc = new jsPDF({ orientation: "portrait", unit: "mm", format: "letter" })
    const pageW = doc.internal.pageSize.getWidth()
    const pageH = doc.internal.pageSize.getHeight()
    const margin = 16
    const contentW = pageW - margin * 2
    let y = margin

    const dark: [number, number, number] = [40, 40, 40]
    const muted: [number, number, number] = [120, 120, 120]
    const orange = hexRgb("#fb923c")
    const orangeLight = hexRgb("#fed7aa")
    const gold: [number, number, number] = [180, 140, 50]
    const green: [number, number, number] = [34, 139, 34]
    const red: [number, number, number] = [200, 50, 50]
    const bgCard: [number, number, number] = [245, 243, 238]
    const baselineTint: [number, number, number] = [235, 238, 245]

    // ═══════════════════════════════════════════════
    // HEADER
    // ═══════════════════════════════════════════════
    doc.setFillColor(...gold)
    doc.rect(0, 0, pageW, 3, "F")

    y = 10
    const logoW = 40, logoH = 8
    try {
        const logoRes = await fetch("/homecastr-logo-horizontal-clean.png")
        const logoBlob = await logoRes.blob()
        const logoDataUrl = await new Promise<string>((resolve) => {
            const reader = new FileReader()
            reader.onloadend = () => resolve(reader.result as string)
            reader.readAsDataURL(logoBlob)
        })
        doc.addImage(logoDataUrl, "PNG", margin, y - 5, logoW, logoH)
    } catch {
        doc.setFont("helvetica", "bold")
        doc.setFontSize(22)
        doc.setTextColor(...dark)
        doc.text("homecastr", margin, y)
    }
    doc.link(margin, y - 5, logoW, logoH, { url: HOMECASTR_URL })

    doc.setFont("helvetica", "normal")
    doc.setFontSize(9)
    doc.setTextColor(...muted)
    doc.text("Forecast Report", margin + 44, y)

    const dateStr = new Date().toLocaleDateString("en-US", { year: "numeric", month: "long", day: "numeric" })
    doc.setFontSize(8)
    doc.text(dateStr, pageW - margin, y, { align: "right" })

    y += 6
    doc.setDrawColor(220, 218, 212)
    doc.setLineWidth(0.3)
    doc.line(margin, y, pageW - margin, y)

    // ═══════════════════════════════════════════════
    // COMPUTE METRICS
    // ═══════════════════════════════════════════════
    const currentValue = data.historicalValues?.[data.historicalValues.length - 1]
    // 2026 P50 value — find exact 2026 in data.years (may start at 2024/2025)
    const _idx2026 = (() => {
        if (!data.years?.length) return -1
        // Try exact match first
        const exact = data.years.findIndex(yr => yr === 2026)
        if (exact >= 0) return exact
        // Fallback: first year >= 2026
        return data.years.findIndex(yr => yr >= 2026)
    })()
    const p50_2026 = _idx2026 >= 0 && data.p50?.length > _idx2026 ? data.p50[_idx2026] : null

    // Growth to SELECTED year
    const selectedIdx = data.years?.findIndex(yr => yr >= data.currentYear)
    const selectedForecast = selectedIdx != null && selectedIdx >= 0 ? data.p50[selectedIdx] : null
    const baseValue = currentValue ?? p50_2026 // use best available baseline
    const growthSelected = baseValue && selectedForecast && baseValue !== 0
        ? (selectedForecast - baseValue) / baseValue : null

    // Growth to last year (2030)
    const lastForecast = data.p50?.[data.p50.length - 1]
    const lastYear = data.years?.[data.years.length - 1]
    const growthLast = baseValue && lastForecast && baseValue !== 0 && lastYear && lastYear > data.currentYear
        ? (lastForecast - baseValue) / baseValue : null

    const hasData = currentValue != null || selectedForecast != null

    // ═══════════════════════════════════════════════
    // LOCATION
    // ═══════════════════════════════════════════════
    y += 8
    doc.setFont("helvetica", "bold")
    doc.setFontSize(16)
    doc.setTextColor(...dark)
    doc.text(data.locationName || "Map Overview", margin, y)

    y += 5
    doc.setFont("helvetica", "normal")
    doc.setFontSize(8)
    doc.setTextColor(...muted)
    const idLine = data.locationId && data.locationId !== "—"
        ? `ID: ${data.locationId}  ·  Forecast Year: ${data.currentYear}`
        : `Forecast Year: ${data.currentYear}`
    doc.text(idLine, margin, y)

    // ═══════════════════════════════════════════════
    // MAP + STREET VIEW (side by side)
    // ═══════════════════════════════════════════════
    y += 8
    const imgRowH = hasData ? 55 : 80

    // Fetch street view image if we have coordinates
    let streetViewDataUrl: string | undefined
    if (data.coords && hasData) {
        try {
            const [lat, lng] = data.coords
            const svRes = await fetch(`/api/streetview-sign?lat=${lat}&lng=${lng}&w=400&h=300&base64=1`)
            if (svRes.ok) {
                const svJson = await svRes.json()
                if (svJson.dataUrl) {
                    streetViewDataUrl = svJson.dataUrl
                } else if (svJson.url) {
                    // Fallback: try fetching the URL directly (may fail due to CORS)
                    try {
                        const imgRes = await fetch(svJson.url)
                        const imgBlob = await imgRes.blob()
                        streetViewDataUrl = await new Promise<string>((resolve) => {
                            const reader = new FileReader()
                            reader.onloadend = () => resolve(reader.result as string)
                            reader.readAsDataURL(imgBlob)
                        })
                    } catch { /* CORS blocked, skip */ }
                }
            }
        } catch { /* non-fatal */ }
    }

    if (data.mapImageDataUrl && streetViewDataUrl && hasData) {
        // Side by side: map left, street view right
        const halfW = (contentW - 4) / 2

        // Map (left) — maintain aspect ratio
        let mX = margin; let mW = halfW; let mH = 55;
        try {
            const dims = await new Promise<{ w: number; h: number }>((resolve) => {
                const img = new Image()
                img.onload = () => resolve({ w: img.width, h: img.height })
                img.onerror = () => resolve({ w: 4, h: 3 })
                img.src = data.mapImageDataUrl!
            })
            const aspect = dims.w / dims.h
            mW = halfW; mH = halfW / aspect
            if (mH > imgRowH) { mH = imgRowH; mW = imgRowH * aspect }
            if (mW > halfW) { mW = halfW; mH = halfW / aspect }
            mX = margin // Left-aligned
            doc.addImage(data.mapImageDataUrl, "PNG", mX, y, mW, mH)
            doc.setDrawColor(220, 218, 212)
            doc.setLineWidth(0.3)
            doc.rect(mX, y, mW, mH)
        } catch { /* skip */ }

        // Street view (right next to map)
        try {
            const svW = halfW, svH = halfW * 0.75 // 4:3 aspect
            const finalH = Math.min(svH, imgRowH)
            const finalW = finalH / 0.75
            const svX = mX + mW + 3 // Immediately next to Map with 3mm gap
            doc.addImage(streetViewDataUrl, "JPEG", svX, y, finalW, finalH)
            doc.setDrawColor(220, 218, 212)
            doc.setLineWidth(0.3)
            doc.rect(svX, y, finalW, finalH)
        } catch { /* skip */ }

        y += imgRowH + 6
    } else if (data.mapImageDataUrl) {
        // Map only — maintain aspect ratio
        try {
            const dims = await new Promise<{ w: number; h: number }>((resolve) => {
                const img = new Image()
                img.onload = () => resolve({ w: img.width, h: img.height })
                img.onerror = () => resolve({ w: 16, h: 9 })
                img.src = data.mapImageDataUrl!
            })
            const aspect = dims.w / dims.h
            let mW = contentW, mH = contentW / aspect
            if (mH > imgRowH) { mH = imgRowH; mW = imgRowH * aspect }
            if (mW > contentW) { mW = contentW; mH = contentW / aspect }
            const mX = margin + (contentW - mW) / 2
            doc.addImage(data.mapImageDataUrl, "PNG", mX, y, mW, mH)
            doc.setDrawColor(220, 218, 212)
            doc.setLineWidth(0.3)
            doc.rect(mX, y, mW, mH)
            y += mH + 6
        } catch {
            y += 2
        }
    }

    // ═══════════════════════════════════════════════
    // FAN CHART (triangle: converges at 2026 P50, fans out)
    // ═══════════════════════════════════════════════
    if (data.years?.length && data.p50?.length && hasData) {
        doc.setFont("helvetica", "bold")
        doc.setFontSize(8)
        doc.setTextColor(...dark)
        doc.text("FORECAST TRAJECTORY", margin, y)
        y += 4

        const cX = margin + 12
        const cW = contentW - 12
        const cH = 38

        const allVals = [
            ...(data.historicalValues?.filter(v => v != null) as number[] || []),
            ...data.p10.filter(v => Number.isFinite(v)),
            ...data.p90.filter(v => Number.isFinite(v)),
            // Include pinned comparison data in Y range
            ...(data.pinnedComparisons || []).flatMap(pc => [
                ...(pc.historicalValues?.filter(v => v != null && Number.isFinite(v)) as number[] || []),
                ...(pc.p50?.filter(v => Number.isFinite(v)) || []),
            ]),
        ]

        if (allVals.length > 0) {
            const vMin = Math.min(...allVals) * 0.95
            const vMax = Math.max(...allVals) * 1.05
            const vR = vMax - vMin || 1

            const histYrs = [2019, 2020, 2021, 2022, 2023, 2024, 2025]
            const allYrs = [...histYrs, ...data.years]
            const yrMin = Math.min(...allYrs)
            const yrMax = Math.max(...allYrs)
            const yrR = yrMax - yrMin || 1

            const xOf = (yr: number) => cX + ((yr - yrMin) / yrR) * cW
            const yOf = (v: number) => y + cH - ((v - vMin) / vR) * cH

            // Historical shading
            const nowX = xOf(2026)
            doc.setFillColor(...baselineTint)
            doc.rect(cX, y, nowX - cX, cH, "F")

            // "Now" dashed line
            doc.setDrawColor(180, 195, 220)
            doc.setLineWidth(0.4)
            for (let d = 0; d < cH; d += 3) doc.line(nowX, y + d, nowX, Math.min(y + d + 1.5, y + cH))
            doc.setFont("helvetica", "normal")
            doc.setFontSize(5)
            doc.setTextColor(140, 160, 190)
            doc.text("Now", nowX, y - 1, { align: "center" })

            // ── Pinned Comparison Lines (drawn FIRST so primary appears on top) ──
            const COMP_COLORS: [number, number, number][] = [
                [163, 230, 53],  // lime #a3e635
                [56, 189, 248],  // sky  #38bdf8
                [244, 114, 182], // pink #f472b6
                [250, 204, 21],  // amber #facc15
            ]
            const histYrsComp = [2019, 2020, 2021, 2022, 2023, 2024, 2025]
            if (data.pinnedComparisons?.length) {
                for (let ci = 0; ci < data.pinnedComparisons.length; ci++) {
                    const pc = data.pinnedComparisons[ci]
                    const color = COMP_COLORS[ci % COMP_COLORS.length]

                    // Fan band (P10–P90 shaded area) — lighter tint
                    if (pc.p10?.length && pc.p90?.length && pc.p50?.length && pc.years?.length) {
                        const lightColor: [number, number, number] = [
                            Math.min(255, color[0] + Math.round((255 - color[0]) * 0.7)),
                            Math.min(255, color[1] + Math.round((255 - color[1]) * 0.7)),
                            Math.min(255, color[2] + Math.round((255 - color[2]) * 0.7)),
                        ]
                        doc.setFillColor(...lightColor)
                        const pcIdx2026 = pc.years.findIndex(yr => yr >= 2026)
                        const pcIdx2027 = pc.years.findIndex(yr => yr >= 2027)
                        const pcFanP50 = pcIdx2026 >= 0 ? pc.p50[pcIdx2026] : pc.p50[0]

                        if (pcIdx2027 >= 0) {
                            const pcInterp = (yr: number, arr: number[], yrs: number[]) => {
                                let lo = 0
                                for (let i = 0; i < yrs.length; i++) { if (yrs[i] <= yr) lo = i }
                                const hi = Math.min(lo + 1, yrs.length - 1)
                                if (lo === hi) return arr[lo] || 0
                                const f = (yr - yrs[lo]) / (yrs[hi] - yrs[lo])
                                return (arr[lo] || 0) + f * ((arr[hi] || 0) - (arr[lo] || 0))
                            }
                            const pcStartYr = 2026
                            const pcEndYr = pc.years[pc.years.length - 1]
                            const steps = 300
                            for (let s = 0; s < steps; s++) {
                                const t1 = s / steps, t2 = (s + 1) / steps
                                const yr1 = pcStartYr + t1 * (pcEndYr - pcStartYr)
                                const yr2 = pcStartYr + t2 * (pcEndYr - pcStartYr)
                                const sf1 = Math.min(Math.max((yr1 - pcStartYr) / (2027 - pcStartYr), 0), 1)
                                const sf2 = Math.min(Math.max((yr2 - pcStartYr) / (2027 - pcStartYr), 0), 1)
                                const p501 = pcInterp(yr1, pc.p50, pc.years)
                                const p502 = pcInterp(yr2, pc.p50, pc.years)
                                const top1 = p501 + sf1 * (pcInterp(yr1, pc.p90, pc.years) - p501)
                                const bot1 = p501 + sf1 * (pcInterp(yr1, pc.p10, pc.years) - p501)
                                const top2 = p502 + sf2 * (pcInterp(yr2, pc.p90, pc.years) - p502)
                                const bot2 = p502 + sf2 * (pcInterp(yr2, pc.p10, pc.years) - p502)
                                const yTop = Math.min(yOf(top1), yOf(top2))
                                const yBot = Math.max(yOf(bot1), yOf(bot2))
                                doc.rect(xOf(yr1), yTop, xOf(yr2) - xOf(yr1), Math.max(yBot - yTop, 0.1), "F")
                            }

                            // P10/P90 boundary lines
                            doc.setDrawColor(...color)
                            doc.setLineWidth(0.25)
                            doc.line(xOf(2026), yOf(pcFanP50), xOf(pc.years[pcIdx2027]), yOf(pc.p10[pcIdx2027]))
                            doc.line(xOf(2026), yOf(pcFanP50), xOf(pc.years[pcIdx2027]), yOf(pc.p90[pcIdx2027]))
                            for (let i = pcIdx2027; i < pc.years.length - 1; i++) {
                                doc.line(xOf(pc.years[i]), yOf(pc.p10[i]), xOf(pc.years[i + 1]), yOf(pc.p10[i + 1]))
                                doc.line(xOf(pc.years[i]), yOf(pc.p90[i]), xOf(pc.years[i + 1]), yOf(pc.p90[i + 1]))
                            }
                        }
                    }

                    doc.setDrawColor(...color)
                    doc.setLineWidth(0.7)

                    // Historical solid line
                    if (pc.historicalValues?.length) {
                        const pcH = histYrsComp.map((yr, i) => ({ yr, v: pc.historicalValues![i] }))
                            .filter(d => d.v != null && Number.isFinite(d.v)) as { yr: number; v: number }[]
                        for (let i = 0; i < pcH.length - 1; i++) {
                            doc.line(xOf(pcH[i].yr), yOf(pcH[i].v), xOf(pcH[i + 1].yr), yOf(pcH[i + 1].v))
                        }
                        // Connector to 2026 P50
                        const pcIdx2026c = pc.years?.findIndex(yr => yr >= 2026) ?? -1
                        if (pcH.length > 0 && pcIdx2026c >= 0 && pc.p50?.[pcIdx2026c] != null) {
                            doc.line(xOf(pcH[pcH.length - 1].yr), yOf(pcH[pcH.length - 1].v), xOf(2026), yOf(pc.p50[pcIdx2026c]))
                        }
                    }

                    // P50 dashed forecast line (from 2026 onward)
                    if (pc.p50?.length && pc.years?.length) {
                        for (let i = 0; i < pc.years.length - 1; i++) {
                            if (pc.years[i] < 2026) continue
                            if (!Number.isFinite(pc.p50[i]) || !Number.isFinite(pc.p50[i + 1])) continue
                            const x1 = xOf(pc.years[i]), y1 = yOf(pc.p50[i])
                            const x2 = xOf(pc.years[i + 1]), y2 = yOf(pc.p50[i + 1])
                            const len = Math.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)
                            let d = 0
                            while (d < len) {
                                const a = d / len, b = Math.min((d + 1.5) / len, 1)
                                doc.line(x1 + (x2 - x1) * a, y1 + (y2 - y1) * a, x1 + (x2 - x1) * b, y1 + (y2 - y1) * b)
                                d += 2.5
                            }
                        }
                    }
                }
            }

            // Fan band: starts at 2026 P50 (point), fans out to 2027+ P10/P90
            const idx2026 = data.years.findIndex(yr => yr >= 2026)
            const idx2027 = data.years.findIndex(yr => yr >= 2027)
            const fanP50_2026 = idx2026 >= 0 ? data.p50[idx2026] : data.p50[0]
            doc.setFillColor(...orangeLight)
            if (idx2026 >= 0 && idx2027 >= 0) {
                const interp = (yr: number, arr: number[], yrs: number[]) => {
                    let lo = 0
                    for (let i = 0; i < yrs.length; i++) { if (yrs[i] <= yr) lo = i }
                    const hi = Math.min(lo + 1, yrs.length - 1)
                    if (lo === hi) return arr[lo] || 0
                    const f = (yr - yrs[lo]) / (yrs[hi] - yrs[lo])
                    return (arr[lo] || 0) + f * ((arr[hi] || 0) - (arr[lo] || 0))
                }

                // Draw filled triangle: from 2026 P50 (point) fanning out to 2027+ P10/P90
                const fanStartYr = 2026
                const fanEndYr = data.years[data.years.length - 1]
                const steps = 500
                for (let s = 0; s < steps; s++) {
                    const t1 = s / steps, t2 = (s + 1) / steps
                    const yr1 = fanStartYr + t1 * (fanEndYr - fanStartYr)
                    const yr2 = fanStartYr + t2 * (fanEndYr - fanStartYr)

                    // At 2026: spread=0 (P50 only). At 2027+: actual P10/P90.
                    const spreadFrac1 = Math.min(Math.max((yr1 - fanStartYr) / (2027 - fanStartYr), 0), 1)
                    const spreadFrac2 = Math.min(Math.max((yr2 - fanStartYr) / (2027 - fanStartYr), 0), 1)

                    const p50_1 = interp(yr1, data.p50, data.years)
                    const p50_2 = interp(yr2, data.p50, data.years)
                    const p90_1 = interp(yr1, data.p90, data.years)
                    const p10_1 = interp(yr1, data.p10, data.years)
                    const p90_2 = interp(yr2, data.p90, data.years)
                    const p10_2 = interp(yr2, data.p10, data.years)

                    const top1 = p50_1 + spreadFrac1 * (p90_1 - p50_1)
                    const bot1 = p50_1 + spreadFrac1 * (p10_1 - p50_1)
                    const top2 = p50_2 + spreadFrac2 * (p90_2 - p50_2)
                    const bot2 = p50_2 + spreadFrac2 * (p10_2 - p50_2)

                    const yTop = Math.min(yOf(top1), yOf(top2))
                    const yBot = Math.max(yOf(bot1), yOf(bot2))
                    doc.rect(xOf(yr1), yTop, xOf(yr2) - xOf(yr1), Math.max(yBot - yTop, 0.1), "F")
                }
            }

            // P10/P90 boundary lines (from 2026 P50 → 2027 P10/P90, then actual onward)
            doc.setDrawColor(...orange)
            doc.setLineWidth(0.3)
            if (idx2026 >= 0 && idx2027 >= 0) {
                // Line from 2026 P50 → 2027 P10 and 2026 P50 → 2027 P90
                doc.line(xOf(2026), yOf(fanP50_2026), xOf(data.years[idx2027]), yOf(data.p10[idx2027]))
                doc.line(xOf(2026), yOf(fanP50_2026), xOf(data.years[idx2027]), yOf(data.p90[idx2027]))
                // Actual P10/P90 lines from 2027 onward
                for (let i = idx2027; i < data.years.length - 1; i++) {
                    doc.line(xOf(data.years[i]), yOf(data.p10[i]), xOf(data.years[i + 1]), yOf(data.p10[i + 1]))
                    doc.line(xOf(data.years[i]), yOf(data.p90[i]), xOf(data.years[i + 1]), yOf(data.p90[i + 1]))
                }
            }

            // Historical solid line
            doc.setDrawColor(...orange)
            doc.setLineWidth(0.8)
            const vh = histYrs.map((yr, i) => ({ yr, v: data.historicalValues?.[i] }))
                .filter(d => d.v != null) as { yr: number; v: number }[]
            for (let i = 0; i < vh.length - 1; i++) {
                doc.line(xOf(vh[i].yr), yOf(vh[i].v), xOf(vh[i + 1].yr), yOf(vh[i + 1].v))
            }

            // Solid connector from last historical (2025) → 2026 (current year, treated as known)
            if (vh.length > 0 && idx2026 >= 0 && fanP50_2026 != null) {
                doc.line(xOf(vh[vh.length - 1].yr), yOf(vh[vh.length - 1].v), xOf(2026), yOf(fanP50_2026))
            }

            // Selected year marker
            if (data.currentYear >= yrMin && data.currentYear <= yrMax) {
                const mx = xOf(data.currentYear)
                doc.setDrawColor(200, 100, 50)
                doc.setLineWidth(0.6)
                doc.line(mx, y, mx, y + cH)
                doc.setFillColor(200, 100, 50)
                doc.circle(mx, y + 1, 0.8, "F")
            }

            // Y-axis
            doc.setFont("helvetica", "normal")
            doc.setFontSize(5)
            doc.setTextColor(...muted)
            for (let i = 0; i <= 4; i++) {
                const tv = vMin + (vR * i) / 4
                const lbl = tv >= 1e6 ? `$${(tv / 1e6).toFixed(1)}M` : `$${Math.round(tv / 1000)}K`
                doc.text(lbl, cX - 1, yOf(tv) + 1, { align: "right" })
            }

            // (Comparisons drawn earlier so primary appears on top)

            // Clip mask: hide any chart content that overflows the plot area
            doc.setFillColor(255, 255, 255)
            doc.rect(cX, y + cH, cW + 1, 6, "F")      // below chart (before labels)

            // X-axis labels (drawn on top of clip mask)
            for (const yr of [2019, 2022, 2025, 2027, 2030]) {
                if (yr >= yrMin && yr <= yrMax) doc.text(yr.toString(), xOf(yr), y + cH + 3.5, { align: "center" })
            }

            // P50 dashed forecast line — drawn LAST so it's always visible on top
            doc.setDrawColor(...orange)
            doc.setLineWidth(1.0)
            for (let i = 0; i < data.years.length - 1; i++) {
                if (data.years[i] < 2026) continue
                const x1 = xOf(data.years[i]), y1 = yOf(data.p50[i])
                const x2 = xOf(data.years[i + 1]), y2 = yOf(data.p50[i + 1])
                const len = Math.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)
                let d = 0
                while (d < len) {
                    const a = d / len, b = Math.min((d + 1.5) / len, 1)
                    doc.line(x1 + (x2 - x1) * a, y1 + (y2 - y1) * a, x1 + (x2 - x1) * b, y1 + (y2 - y1) * b)
                    d += 2.5
                }
            }

            // Legend
            const lY = y + cH + 7
            doc.setFontSize(5)
            doc.setDrawColor(...orange)
            doc.setLineWidth(0.8)
            doc.line(margin, lY - 1, margin + 5, lY - 1)
            doc.text("Historical", margin + 7, lY)
            doc.line(margin + 24, lY - 1, margin + 26, lY - 1); doc.line(margin + 27.5, lY - 1, margin + 29.5, lY - 1)
            doc.text("Forecast (P50)", margin + 32, lY)
            doc.setFillColor(...orangeLight)
            doc.rect(margin + 60, lY - 2.5, 5, 3, "F")
            doc.text("P10–P90 Range", margin + 67, lY)

            // Comparison legend entries
            if (data.pinnedComparisons?.length) {
                let legX = margin + 95
                for (let ci = 0; ci < data.pinnedComparisons.length; ci++) {
                    const pc = data.pinnedComparisons[ci]
                    const color = COMP_COLORS[ci % COMP_COLORS.length]
                    doc.setDrawColor(...color)
                    doc.setLineWidth(0.8)
                    doc.line(legX, lY - 1, legX + 4, lY - 1)
                    doc.setTextColor(...color)
                    const label = (pc.label || `Comp ${ci + 1}`).substring(0, 18)
                    doc.text(label, legX + 6, lY)
                    legX += doc.getTextWidth(label) + 10
                }
                doc.setTextColor(...muted) // reset
            }

            y += cH + 15
        }
    }

    // ═══════════════════════════════════════════════
    // HEADLINE METRIC CARDS (3 cards, forecast+growth combined)
    // ═══════════════════════════════════════════════
    if (hasData) {
        const cardH = 26
        const cardGap = 4
        const showBoth = growthLast != null && lastYear && lastYear > data.currentYear
        const numCards = showBoth ? 3 : 2
        const cardW = (contentW - cardGap * (numCards - 1)) / numCards

        // Card 1: Current Value
        const cx0 = margin
        doc.setFillColor(...bgCard)
        doc.roundedRect(cx0, y, cardW, cardH, 2, 2, "F")
        doc.setFont("helvetica", "normal")
        doc.setFontSize(6.5)
        doc.setTextColor(...muted)
        doc.text("Current Value (2025)", cx0 + 3, y + 5)
        doc.setFont("helvetica", "bold")
        doc.setFontSize(14)
        doc.setTextColor(...dark)
        doc.text(fmt$(currentValue), cx0 + 3, y + 14)
        if (p50_2026 != null) {
            doc.setFont("helvetica", "normal")
            doc.setFontSize(6)
            doc.setTextColor(...muted)
            doc.text(`2026 est: ${fmt$(p50_2026)}`, cx0 + 3, y + 20)
        }

        // Card 2: Forecast + Growth to selected year
        const cx1 = margin + (cardW + cardGap)
        doc.setFillColor(...bgCard)
        doc.roundedRect(cx1, y, cardW, cardH, 2, 2, "F")
        doc.setFont("helvetica", "normal")
        doc.setFontSize(6.5)
        doc.setTextColor(...muted)
        doc.text(`Forecast ${data.currentYear}`, cx1 + 3, y + 5)
        doc.setFont("helvetica", "bold")
        doc.setFontSize(14)
        doc.setTextColor(...dark)
        doc.text(fmt$(selectedForecast), cx1 + 3, y + 14)
        if (growthSelected != null) {
            doc.setFont("helvetica", "bold")
            doc.setFontSize(9)
            doc.setTextColor(...(growthSelected > 0 ? green : growthSelected < 0 ? red : dark))
            doc.text(fmtPct(growthSelected), cx1 + 3, y + 21)
        }

        // Card 3: Forecast + Growth to 2030 (if different from selected)
        if (showBoth) {
            const cx2 = margin + 2 * (cardW + cardGap)
            doc.setFillColor(...bgCard)
            doc.roundedRect(cx2, y, cardW, cardH, 2, 2, "F")
            doc.setFont("helvetica", "normal")
            doc.setFontSize(6.5)
            doc.setTextColor(...muted)
            doc.text(`Forecast ${lastYear}`, cx2 + 3, y + 5)
            doc.setFont("helvetica", "bold")
            doc.setFontSize(14)
            doc.setTextColor(...dark)
            doc.text(fmt$(lastForecast), cx2 + 3, y + 14)
            doc.setFont("helvetica", "bold")
            doc.setFontSize(9)
            doc.setTextColor(...(growthLast! > 0 ? green : growthLast! < 0 ? red : dark))
            doc.text(fmtPct(growthLast!), cx2 + 3, y + 21)
        }

        y += cardH + 7
    } else {
        doc.setFillColor(...bgCard)
        doc.roundedRect(margin, y, contentW, 18, 3, 3, "F")
        doc.setFont("helvetica", "bold")
        doc.setFontSize(9)
        doc.setTextColor(...dark)
        doc.text("How to use this report", margin + 8, y + 7)
        doc.setFont("helvetica", "normal")
        doc.setFontSize(7.5)
        doc.setTextColor(...muted)
        doc.text("Click any area on the map, then download the PDF to see forecast metrics and charts.", margin + 8, y + 13)
        y += 24
    }

    // ═══════════════════════════════════════════════
    // FORECAST TABLE (2026–2030)
    // ═══════════════════════════════════════════════
    if (data.years?.length && data.p50?.length && hasData) {
        const fIdxs: number[] = []
        const fYrs: number[] = []
        for (let i = 0; i < data.years.length; i++) {
            if (data.years[i] >= 2027) { fIdxs.push(i); fYrs.push(data.years[i]) }
        }

        if (fYrs.length > 0) {
            doc.setFont("helvetica", "bold")
            doc.setFontSize(8)
            doc.setTextColor(...dark)
            doc.text("FORECAST BY YEAR", margin, y)
            y += 4

            const cols = fYrs.length
            const labelColW = 22
            const dataColW = (contentW - labelColW) / cols
            const rowH = 5.5

            // Header
            doc.setFillColor(...bgCard)
            doc.rect(margin, y, contentW, rowH, "F")
            doc.setFont("helvetica", "bold")
            doc.setFontSize(6)
            doc.setTextColor(...muted)
            for (let i = 0; i < cols; i++) {
                doc.text(fYrs[i].toString(), margin + labelColW + i * dataColW + dataColW / 2, y + 3.8, { align: "center" })
            }
            y += rowH

            // P90
            doc.setFont("helvetica", "normal")
            doc.setFontSize(6)
            doc.setTextColor(...muted)
            doc.text("High (P90)", margin + 2, y + 3.8)
            doc.setTextColor(...dark)
            for (let i = 0; i < cols; i++) {
                doc.text(fmt$(data.p90?.[fIdxs[i]]), margin + labelColW + i * dataColW + dataColW / 2, y + 3.8, { align: "center" })
            }
            y += rowH

            // P50 — highlighted
            doc.setFillColor(...orange)
            doc.rect(margin, y, contentW, rowH, "F")
            doc.setFont("helvetica", "bold")
            doc.setFontSize(6)
            doc.setTextColor(255, 255, 255)
            doc.text("Median (P50)", margin + 2, y + 3.8)
            for (let i = 0; i < cols; i++) {
                doc.text(fmt$(data.p50?.[fIdxs[i]]), margin + labelColW + i * dataColW + dataColW / 2, y + 3.8, { align: "center" })
            }
            y += rowH

            // P10
            doc.setFont("helvetica", "normal")
            doc.setFontSize(6)
            doc.setTextColor(...muted)
            doc.text("Low (P10)", margin + 2, y + 3.8)
            doc.setTextColor(...dark)
            for (let i = 0; i < cols; i++) {
                doc.text(fmt$(data.p10?.[fIdxs[i]]), margin + labelColW + i * dataColW + dataColW / 2, y + 3.8, { align: "center" })
            }
            y += rowH + 7
        }
    }

    // ═══════════════════════════════════════════════
    // HISTORICAL VALUES TABLE (2019–2026)
    // ═══════════════════════════════════════════════
    if (data.historicalValues?.some(v => v != null) || p50_2026 != null) {
        doc.setFont("helvetica", "bold")
        doc.setFontSize(8)
        doc.setTextColor(...dark)
        doc.text("HISTORICAL VALUES", margin, y)
        y += 4

        // 2019–2026 (2026 shows P50 as current estimate)
        const histYears = [2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
        const histVals = [...(data.historicalValues || [null, null, null, null, null, null, null]), p50_2026]
        const colW = contentW / histYears.length

        doc.setFillColor(...bgCard)
        doc.rect(margin, y, contentW, 5.5, "F")
        doc.setFont("helvetica", "bold")
        doc.setFontSize(6)
        doc.setTextColor(...muted)
        for (let i = 0; i < histYears.length; i++) {
            doc.text(histYears[i].toString(), margin + i * colW + colW / 2, y + 3.8, { align: "center" })
        }
        y += 5.5

        doc.setFont("helvetica", "normal")
        doc.setFontSize(6)
        doc.setTextColor(...dark)
        for (let i = 0; i < histYears.length; i++) {
            doc.text(histVals[i] != null ? fmt$(histVals[i]) : "—", margin + i * colW + colW / 2, y + 3.8, { align: "center" })
        }
        y += 12
    }

    // ═══════════════════════════════════════════════
    // FOOTER
    // ═══════════════════════════════════════════════
    const fY = pageH - 26
    doc.setDrawColor(220, 218, 212)
    doc.setLineWidth(0.3)
    doc.line(margin, fY, pageW - margin, fY)

    doc.setFont("helvetica", "italic")
    doc.setFontSize(6)
    doc.setTextColor(...muted)
    doc.text("This report is generated by homecastr's World Model and is for informational purposes only. It does not constitute investment advice.", margin, fY + 4)
    doc.text("Forecasts are probabilistic estimates based on historical data and machine learning models. Actual values may differ materially.", margin, fY + 8)

    // Contact — clickable
    doc.setFont("helvetica", "normal")
    doc.setFontSize(6)
    doc.setTextColor(...dark)
    doc.text(`Contact: ${CONTACT_EMAIL}`, margin, fY + 13)
    doc.link(margin, fY + 10.5, doc.getTextWidth(`Contact: ${CONTACT_EMAIL}`), 3, { url: `mailto:${CONTACT_EMAIL}` })

    // Request Analysis CTA — clickable (opens contact form on the live view)
    const contactTextW = doc.getTextWidth(`Contact: ${CONTACT_EMAIL}`)
    const ctaLabel = ">> Request Custom Analysis"
    doc.setFont("helvetica", "bold")
    doc.setFontSize(6)
    doc.setTextColor(...gold)
    const ctaX = margin + contactTextW + 8
    doc.text(ctaLabel, ctaX, fY + 13)
    // Link to live view URL with ?contact=1 to auto-open the form
    const ctaUrl = new URL(data.shareUrl)
    ctaUrl.searchParams.set("contact", "1")
    doc.link(ctaX, fY + 10.5, doc.getTextWidth(ctaLabel), 3, { url: ctaUrl.toString() })

    // Share URL — clickable
    doc.setFont("helvetica", "normal")
    doc.setTextColor(...gold)
    const shareLabel = `View live: ${data.shareUrl}`
    doc.text(shareLabel, margin, fY + 18)
    doc.link(margin, fY + 15.5, doc.getTextWidth(shareLabel), 3, { url: data.shareUrl })

    // homecastr.com — clickable, right-aligned
    const siteLabel = "homecastr.com"
    doc.text(siteLabel, pageW - margin, fY + 18, { align: "right" })
    doc.link(pageW - margin - doc.getTextWidth(siteLabel), fY + 15.5, doc.getTextWidth(siteLabel), 3, { url: HOMECASTR_URL })

    // Bottom gold bar
    doc.setFillColor(...gold)
    doc.rect(0, pageH - 3, pageW, 3, "F")

    // ═══════════════════════════════════════════════
    // SAVE
    // ═══════════════════════════════════════════════
    const safeName = (data.locationName || "overview").replace(/[^a-zA-Z0-9]/g, "_").replace(/_+/g, "_").substring(0, 50)
    const dateStamp = new Date().toISOString().slice(0, 10).replace(/-/g, "")
    doc.save(`homecastr_${safeName}_Forecast_${data.currentYear}_${dateStamp}.pdf`)
}
