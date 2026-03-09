import { NextRequest, NextResponse } from "next/server"
import { toCsv, type CsvColumn } from "@/lib/publishing/csv"
import {
    getStatesWithData,
    getCitiesForState,
    getTractsForCity,
    batchEnrichTracts,
} from "@/lib/publishing/geo-crosswalk"
import { withRedisCache } from "@/lib/redis"
import { getSupabaseAdmin } from "@/lib/supabase/admin"

const SCHEMA = process.env.FORECAST_SCHEMA || "forecast_queue"

const STATE_NAMES: Record<string, string> = {
    al: "Alabama", ak: "Alaska", az: "Arizona", ar: "Arkansas", ca: "California",
    co: "Colorado", ct: "Connecticut", de: "Delaware", dc: "District of Columbia",
    fl: "Florida", ga: "Georgia", hi: "Hawaii", id: "Idaho", il: "Illinois",
    in: "Indiana", ia: "Iowa", ks: "Kansas", ky: "Kentucky", la: "Louisiana",
    me: "Maine", md: "Maryland", ma: "Massachusetts", mi: "Michigan", mn: "Minnesota",
    ms: "Mississippi", mo: "Missouri", mt: "Montana", ne: "Nebraska", nv: "Nevada",
    nh: "New Hampshire", nj: "New Jersey", nm: "New Mexico", ny: "New York",
    nc: "North Carolina", nd: "North Dakota", oh: "Ohio", ok: "Oklahoma", or: "Oregon",
    pa: "Pennsylvania", ri: "Rhode Island", sc: "South Carolina", sd: "South Dakota",
    tn: "Tennessee", tx: "Texas", ut: "Utah", vt: "Vermont", va: "Virginia",
    wa: "Washington", wv: "West Virginia", wi: "Wisconsin", wy: "Wyoming", pr: "Puerto Rico",
}

const SLUG_TO_FIPS: Record<string, string> = {
    al: "01", ak: "02", az: "04", ar: "05", ca: "06", co: "08", ct: "09",
    de: "10", dc: "11", fl: "12", ga: "13", hi: "15", id: "16", il: "17",
    in: "18", ia: "19", ks: "20", ky: "21", la: "22", me: "23", md: "24",
    ma: "25", mi: "26", mn: "27", ms: "28", mo: "29", mt: "30", ne: "31",
    nv: "32", nh: "33", nj: "34", nm: "35", ny: "36", nc: "37", nd: "38",
    oh: "39", ok: "40", or: "41", pa: "42", pr: "72", ri: "44", sc: "45",
    sd: "46", tn: "47", tx: "48", ut: "49", vt: "50", va: "51", wa: "53",
    wv: "54", wi: "55", wy: "56",
}

// ---------------------------------------------------------------------------
// Shared data-fetching helpers (mirrors page-level logic)
// ---------------------------------------------------------------------------

async function fetchAllRows(queryBuilder: () => any, pageSize = 1000) {
    const all: any[] = []
    let offset = 0
    while (true) {
        const { data, error } = await queryBuilder().range(offset, offset + pageSize - 1)
        if (error || !data || data.length === 0) break
        all.push(...data)
        if (data.length < pageSize) break
        offset += pageSize
    }
    return all
}

/** State-level outlook (mirrors /forecasts/page.tsx getStateOutlooksFast) */
async function getStateOutlook(stateFips: string) {
    return withRedisCache(`state_outlooks_v2:${stateFips}:${SCHEMA}`, async () => {
        const supabase = getSupabaseAdmin()
        try {
            const { data, error } = await supabase.rpc("get_state_outlooks", {
                target_schema: SCHEMA,
                state_fips: stateFips,
            })
            if (!error && data && data.length > 0) {
                const row = data[0]
                return {
                    countyCount: Number(row.county_count || 0),
                    neighborhoodCount: Number(row.neighborhood_count || 0),
                    medianValue: row.median_value !== null ? Number(row.median_value) : null,
                    medianAppreciation: row.median_appreciation !== null ? Number(row.median_appreciation) : null,
                    highestUpside: row.highest_upside !== null ? Math.min(Number(row.highest_upside), 500) : null,
                }
            }
        } catch { /* fall through */ }

        const nextFips = String(Number(stateFips) + 1).padStart(2, "0")
        const [h12Rows, h60Rows] = await Promise.all([
            fetchAllRows(() =>
                supabase.schema(SCHEMA as any).from("metrics_tract_forecast")
                    .select("tract_geoid20, p50").gte("tract_geoid20", stateFips)
                    .lt("tract_geoid20", nextFips).eq("horizon_m", 12)
                    .eq("series_kind", "forecast").not("p50", "is", null).order("tract_geoid20")
            ),
            fetchAllRows(() =>
                supabase.schema(SCHEMA as any).from("metrics_tract_forecast")
                    .select("tract_geoid20, p50").gte("tract_geoid20", stateFips)
                    .lt("tract_geoid20", nextFips).eq("horizon_m", 60)
                    .eq("series_kind", "forecast").not("p50", "is", null).order("tract_geoid20")
            ),
        ])

        const counties = new Set<string>()
        const tracts = new Set<string>()
        for (const row of h12Rows) { counties.add(row.tract_geoid20.substring(0, 5)); tracts.add(row.tract_geoid20) }
        if (h12Rows.length === 0) return null

        const h12Map = new Map<string, number>(); for (const r of h12Rows) h12Map.set(r.tract_geoid20, r.p50)
        const h60Map = new Map<string, number>(); for (const r of h60Rows) h60Map.set(r.tract_geoid20, r.p50)

        const appreciations: number[] = []; const values: number[] = []
        for (const [tid, h12] of h12Map) {
            const h60 = h60Map.get(tid)
            if (h12 >= 20_000 && h60 && h12 < 5_000_000) {
                const appr = ((h60 - h12) / h12) * 100
                if (appr > -95 && appr <= 500) { appreciations.push(appr); values.push(h12) }
            }
        }
        if (appreciations.length === 0) return { countyCount: counties.size, neighborhoodCount: tracts.size, medianValue: null, medianAppreciation: null, highestUpside: null }

        appreciations.sort((a, b) => a - b); values.sort((a, b) => a - b)
        return {
            countyCount: counties.size,
            neighborhoodCount: tracts.size,
            medianValue: values[Math.floor(values.length / 2)],
            medianAppreciation: appreciations[Math.floor(appreciations.length / 2)],
            highestUpside: appreciations[Math.floor(appreciations.length * 0.99)],
        }
    })
}

/** County-level outlook (mirrors /forecasts/[state]/page.tsx getCountyOutlooks) */
async function getCountyOutlooks(stateFips: string) {
    return withRedisCache(`county_outlooks:${stateFips}:${SCHEMA}`, async () => {
        const supabase = getSupabaseAdmin()
        const [h12Rows, h60Rows] = await Promise.all([
            fetchAllRows(() =>
                supabase.schema(SCHEMA as any).from("metrics_tract_forecast")
                    .select("tract_geoid20, p50").gte("tract_geoid20", stateFips)
                    .lt("tract_geoid20", stateFips + "z").eq("horizon_m", 12)
                    .eq("series_kind", "forecast").not("p50", "is", null).order("tract_geoid20")
            ),
            fetchAllRows(() =>
                supabase.schema(SCHEMA as any).from("metrics_tract_forecast")
                    .select("tract_geoid20, p50").gte("tract_geoid20", stateFips)
                    .lt("tract_geoid20", stateFips + "z").eq("horizon_m", 60)
                    .eq("series_kind", "forecast").not("p50", "is", null).order("tract_geoid20")
            ),
        ])

        const h12Map = new Map<string, number>(); for (const r of h12Rows) h12Map.set(r.tract_geoid20, r.p50)
        const h60Map = new Map<string, number>(); for (const r of h60Rows) h60Map.set(r.tract_geoid20, r.p50)

        const countyData = new Map<string, { appreciations: number[]; values: number[] }>()
        for (const [tid, h12] of h12Map) {
            const h60 = h60Map.get(tid)
            const countyFips = tid.substring(0, 5)
            if (h12 >= 20_000 && h60 && h12 < 5_000_000) {
                const appr = ((h60 - h12) / h12) * 100
                if (appr > -95 && appr <= 100) {
                    if (!countyData.has(countyFips)) countyData.set(countyFips, { appreciations: [], values: [] })
                    const cd = countyData.get(countyFips)!
                    cd.appreciations.push(appr); cd.values.push(h12)
                }
            }
        }

        const result: Record<string, { medianAppreciation: number; highestUpside: number; medianValue: number }> = {}
        for (const [countyFips, data] of countyData) {
            if (data.appreciations.length === 0) continue
            data.appreciations.sort((a, b) => a - b); data.values.sort((a, b) => a - b)
            const p99Idx = Math.min(Math.floor(data.appreciations.length * 0.99), data.appreciations.length - 1)
            result[countyFips] = {
                medianAppreciation: data.appreciations[Math.floor(data.appreciations.length / 2)],
                highestUpside: data.appreciations[p99Idx],
                medianValue: data.values[Math.floor(data.values.length / 2)],
            }
        }
        return result
    })
}

// ---------------------------------------------------------------------------
// Formatters for CSV values
// ---------------------------------------------------------------------------
const fmtDollar = (v: number | null) => v != null ? v.toFixed(0) : ""
const fmtPct = (v: number | null) => v != null ? v.toFixed(1) : ""

function buildCommentBlock(geography: string, pageUrl: string) {
    const today = new Date().toISOString().split("T")[0]
    return [
        "Homecastr Home Price Forecasts",
        `Geography: ${geography}`,
        "Model: Homecastr World Model v11",
        `Generated: ${today}`,
        "License: CC BY 4.0",
        `Source: ${pageUrl}`,
    ].join("\n")
}

// ---------------------------------------------------------------------------
// GET handler
// ---------------------------------------------------------------------------

export async function GET(req: NextRequest) {
    const { searchParams } = req.nextUrl
    const state = searchParams.get("state")
    const city = searchParams.get("city")

    try {
        if (state && city) {
            return await buildCityCsv(state, city)
        } else if (state) {
            return await buildStateCsv(state)
        } else {
            return await buildNationalCsv()
        }
    } catch (err) {
        console.error("[CSV download] error:", err)
        return NextResponse.json({ error: "Failed to generate CSV" }, { status: 500 })
    }
}

// ---------------------------------------------------------------------------
// National CSV
// ---------------------------------------------------------------------------

type NationalRow = {
    state: string
    stateAbbr: string
    counties: string
    neighborhoods: string
    medianValue: string
    medianOutlook: string
    topUpside: string
}

async function buildNationalCsv() {
    const states = await getStatesWithData(SCHEMA)

    const BATCH = 6
    const rows: NationalRow[] = []

    for (let i = 0; i < states.length; i += BATCH) {
        const batch = states.slice(i, i + BATCH)
        const results = await Promise.all(
            batch.map(async (s) => {
                const fips = SLUG_TO_FIPS[s.stateSlug] || "00"
                const outlook = await getStateOutlook(fips)
                return {
                    state: s.stateName,
                    stateAbbr: s.stateAbbr,
                    counties: String(outlook?.countyCount ?? ""),
                    neighborhoods: String(outlook?.neighborhoodCount ?? ""),
                    medianValue: fmtDollar(outlook?.medianValue ?? null),
                    medianOutlook: fmtPct(outlook?.medianAppreciation ?? null),
                    topUpside: fmtPct(outlook?.highestUpside ?? null),
                }
            })
        )
        rows.push(...results)
    }

    const columns: CsvColumn<NationalRow>[] = [
        { key: "state", header: "State" },
        { key: "stateAbbr", header: "Abbreviation" },
        { key: "counties", header: "Counties" },
        { key: "neighborhoods", header: "Neighborhoods" },
        { key: "medianValue", header: "Median Value ($)" },
        { key: "medianOutlook", header: "Median 5yr Outlook (%)" },
        { key: "topUpside", header: "Top Upside (%)" },
    ]

    const csv = toCsv(columns, rows, buildCommentBlock("United States (all states)", "https://www.homecastr.com/forecasts"))
    return new NextResponse(csv, {
        headers: {
            "Content-Type": "text/csv; charset=utf-8",
            "Content-Disposition": 'attachment; filename="homecastr-state-forecasts.csv"',
            "Cache-Control": "public, s-maxage=3600, stale-while-revalidate=86400",
        },
    })
}

// ---------------------------------------------------------------------------
// State CSV (county-level)
// ---------------------------------------------------------------------------

type StateRow = {
    county: string
    neighborhoods: string
    medianValue: string
    medianOutlook: string
    topUpside: string
}

async function buildStateCsv(state: string) {
    const stateFips = SLUG_TO_FIPS[state]
    if (!stateFips) return NextResponse.json({ error: "Unknown state" }, { status: 404 })

    const stateName = STATE_NAMES[state] || state.toUpperCase()
    const [cities, countyOutlooks] = await Promise.all([
        getCitiesForState(state, SCHEMA),
        getCountyOutlooks(stateFips),
    ])

    const rows: StateRow[] = cities.map(c => {
        const outlook = countyOutlooks[c.countyFips]
        return {
            county: c.city,
            neighborhoods: String(c.tractCount),
            medianValue: fmtDollar(outlook?.medianValue ?? null),
            medianOutlook: fmtPct(outlook?.medianAppreciation ?? null),
            topUpside: fmtPct(outlook?.highestUpside ?? null),
        }
    })

    const columns: CsvColumn<StateRow>[] = [
        { key: "county", header: "County / City" },
        { key: "neighborhoods", header: "Neighborhoods" },
        { key: "medianValue", header: "Median Value ($)" },
        { key: "medianOutlook", header: "Median 5yr Outlook (%)" },
        { key: "topUpside", header: "Top Upside (%)" },
    ]

    const csv = toCsv(columns, rows, buildCommentBlock(stateName, `https://www.homecastr.com/forecasts/${state}`))
    return new NextResponse(csv, {
        headers: {
            "Content-Type": "text/csv; charset=utf-8",
            "Content-Disposition": `attachment; filename="${state}-county-forecasts.csv"`,
            "Cache-Control": "public, s-maxage=3600, stale-while-revalidate=86400",
        },
    })
}

// ---------------------------------------------------------------------------
// City CSV (neighborhood-level)
// ---------------------------------------------------------------------------

type CityRow = {
    neighborhood: string
    tractGeoid: string
    currentValue: string
    outlook5yr: string
    vsMetro: string
}

async function buildCityCsv(state: string, city: string) {
    const stateName = STATE_NAMES[state] || state.toUpperCase()
    const tracts = await getTractsForCity(state, city, SCHEMA)
    if (tracts.length === 0) return NextResponse.json({ error: "No data" }, { status: 404 })

    const cityName = tracts[0]?.city || city.split("-").map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(" ")
    const supabase = getSupabaseAdmin()
    const tractIds = tracts.map(t => t.tractGeoid)

    const [{ data: forecastRows }, enrichedNames] = await Promise.all([
        supabase
            .schema(SCHEMA as any)
            .from("metrics_tract_forecast")
            .select("tract_geoid20, horizon_m, p50, p10, p90")
            .in("tract_geoid20", tractIds)
            .in("horizon_m", [12, 60])
            .eq("series_kind", "forecast")
            .not("p50", "is", null),
        batchEnrichTracts(tractIds),
    ])

    // Build per-tract lookup
    const tractLookup = new Map<string, { p50_current: number; appreciation_5yr: number }>()
    if (forecastRows) {
        const grouped = new Map<string, typeof forecastRows>()
        for (const row of forecastRows) {
            if (!grouped.has(row.tract_geoid20)) grouped.set(row.tract_geoid20, [])
            grouped.get(row.tract_geoid20)!.push(row)
        }
        for (const [tid, rows] of grouped) {
            const h12 = rows.find((r: any) => r.horizon_m === 12)
            const h60 = rows.find((r: any) => r.horizon_m === 60)
            if (h12) {
                tractLookup.set(tid, {
                    p50_current: h12.p50,
                    appreciation_5yr: h60 ? ((h60.p50 - h12.p50) / h12.p50 * 100) : 0,
                })
            }
        }
    }

    // Compute metro average
    const allAppr = [...tractLookup.values()]
        .filter(v => v.p50_current >= 20_000 && v.p50_current < 5_000_000 && v.appreciation_5yr > -95 && v.appreciation_5yr <= 100)
    const avgAppreciation = allAppr.length > 0 ? allAppr.reduce((s, v) => s + v.appreciation_5yr, 0) / allAppr.length : 0

    const rows: CityRow[] = tracts
        .map(t => {
            const enriched = enrichedNames.get(t.tractGeoid)
            const data = tractLookup.get(t.tractGeoid)
            if (!data || data.p50_current < 20_000 || data.p50_current >= 5_000_000) return null
            if (data.appreciation_5yr <= -95 || data.appreciation_5yr > 100) return null
            return {
                neighborhood: enriched?.name || t.neighborhoodName,
                tractGeoid: t.tractGeoid,
                currentValue: fmtDollar(data.p50_current),
                outlook5yr: fmtPct(data.appreciation_5yr),
                vsMetro: fmtPct(data.appreciation_5yr - avgAppreciation),
            }
        })
        .filter(Boolean) as CityRow[]

    rows.sort((a, b) => parseFloat(b.outlook5yr) - parseFloat(a.outlook5yr))

    const columns: CsvColumn<CityRow>[] = [
        { key: "neighborhood", header: "Neighborhood" },
        { key: "tractGeoid", header: "Tract GEOID" },
        { key: "currentValue", header: "Current Value ($)" },
        { key: "outlook5yr", header: "5yr Outlook (%)" },
        { key: "vsMetro", header: "vs Metro Avg (pp)" },
    ]

    const csv = toCsv(columns, rows, buildCommentBlock(`${cityName}, ${stateName}`, `https://www.homecastr.com/forecasts/${state}/${city}`))
    return new NextResponse(csv, {
        headers: {
            "Content-Type": "text/csv; charset=utf-8",
            "Content-Disposition": `attachment; filename="${state}-${city}-neighborhood-forecasts.csv"`,
            "Cache-Control": "public, s-maxage=3600, stale-while-revalidate=86400",
        },
    })
}
