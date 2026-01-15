const { createClient } = require('@supabase/supabase-js');

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL;
const supabaseKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;

if (!supabaseUrl || !supabaseKey) {
    console.error('Missing Supabase env vars');
    process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey);

async function checkDiagnostics() {
    console.log("--- DIAGNOSTICS START ---");

    // 1. Check Spatial Query for User Validated Bounds (Res 8, Zoom 11)
    // Lat: 29.29 -> 30.11
    // Lng: -95.78 -> -95.21
    console.log("\n1. Testing Spatial Query for User Viewport (Res 8)...");
    const bounds = { minLat: 29.29, maxLat: 30.11, minLng: -95.78, maxLng: -95.21 };

    const { data: spatialGrid, error: spatialError } = await supabase
        .from('h3_aoi_grid')
        .select('h3_id')
        .eq('h3_res', 8)
        .eq('aoi_id', 'harris_county')
        .gte('lat', bounds.minLat)
        .lte('lat', bounds.maxLat)
        .gte('lng', bounds.minLng)
        .lte('lng', bounds.maxLng);

    if (spatialError) {
        console.error('Spatial Query Error:', spatialError.message);
    } else {
        console.log(`Spatial Query found ${spatialGrid.length} cells in viewport.`);
    }

    // 4. Systemic Check
    const YEAR_CHECK = 2030;
    console.log(`\n4. Checking Systemic Data Loss for Year ${YEAR_CHECK}...`);

    const { count, error } = await supabase
        .from('h3_precomputed_hex_details')
        .select('*', { count: 'exact', head: true })
        .eq('forecast_year', YEAR_CHECK)
        .not('fan_p50_y1', 'is', null);

    if (error) {
        console.error('Systemic Check Error:', error.message);
    } else {
        console.log(`Rows with valid Fan Chart data for ${YEAR_CHECK}: ${count}`);

        const { count: totalCount } = await supabase
            .from('h3_precomputed_hex_details')
            .select('*', { count: 'exact', head: true })
            .eq('forecast_year', YEAR_CHECK);

        console.log(`Total rows for ${YEAR_CHECK}: ${totalCount}`);
    }
}

checkDiagnostics();
