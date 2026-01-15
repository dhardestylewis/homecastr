
import { createClient } from '@supabase/supabase-js'

const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL!
const SUPABASE_ANON_KEY = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!

const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY)

async function testQuery() {
    console.log("Testing 2-step spatial fetch (Grid -> Details)...")

    // 1. Fetch from h3_aoi_grid (Simulating Viewport)
    const bounds = {
        minLat: 29.7, maxLat: 29.8,
        minLng: -95.4, maxLng: -95.3
    }
    const res = 9

    console.log(`Step 1: Fetching grid for res=${res} in bounds...`)
    const { data: gridData, error: gridError } = await supabase
        .from("h3_aoi_grid")
        .select("h3_id, lat, lng")
        .eq("h3_res", res)
        .eq("aoi_id", "harris_county")
        .gte("lat", bounds.minLat)
        .lte("lat", bounds.maxLat)
        .gte("lng", bounds.minLng)
        .lte("lng", bounds.maxLng)
        .limit(50)

    if (gridError) {
        console.error("Grid fetch failed:", gridError)
        return
    }

    if (!gridData || gridData.length === 0) {
        console.warn("No grid data found in bounds.")
        return
    }

    console.log(`Found ${gridData.length} grid cells. Example:`, gridData[0])

    // 2. Fetch Details for these IDs
    const ids = gridData.map(g => g.h3_id)
    console.log(`Step 2: Fetching details for ${ids.length} IDs...`)

    const { data: detailsData, error: detailsError } = await supabase
        .from("h3_precomputed_hex_details")
        .select("h3_id, opportunity, reliability")
        .eq("forecast_year", 2026)
        .eq("h3_res", res)
        .in("h3_id", ids)

    if (detailsError) {
        console.error("Details fetch failed:", detailsError)
        return
    }

    console.log(`Found ${detailsData?.length} details rows. Example:`, detailsData?.[0])

    // Merge
    const merged = gridData.map(g => {
        const d = detailsData?.find(d => d.h3_id === g.h3_id)
        return {
            ...g,
            o: d?.opportunity,
            r: d?.reliability
        }
    })

    console.log("Merged Example:", merged[0])
}

testQuery()
