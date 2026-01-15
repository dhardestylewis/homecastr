const { createClient } = require('@supabase/supabase-js');

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL;
const supabaseKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;

if (!supabaseUrl || !supabaseKey) {
    console.error('Missing Supabase env vars');
    process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey);

async function checkDataIntegrity() {
    const RES = 9;
    const YEAR = 2029;
    const AOI = 'harris_county';

    console.log(`\n--- Checking Integrity for Res ${RES}, Year ${YEAR} ---`);

    // 1. Fetch Grid Sample
    console.log(`1. Fetching first 10 grid cells for ${AOI}...`);
    const { data: gridData, error: gridError } = await supabase
        .from('h3_aoi_grid')
        .select('h3_id, lat, lng')
        .eq('h3_res', RES)
        .eq('aoi_id', AOI)
        .limit(10);

    if (gridError) {
        console.error('Grid Fetch Error:', gridError);
        return;
    }

    if (!gridData || gridData.length === 0) {
        console.log('No grid data found for this resolution/AOI.');
        return;
    }

    console.log(`Found ${gridData.length} grid cells.`);
    console.log('Sample Grid IDs:', gridData.map(g => g.h3_id));

    const ids = gridData.map(g => g.h3_id);

    // 2. Fetch Details for these IDs
    console.log(`\n2. Fetching details for these ${ids.length} IDs...`);
    const { data: detailsData, error: detailsError } = await supabase
        .from('h3_precomputed_hex_details')
        .select('*')
        .eq('forecast_year', YEAR)
        .eq('h3_res', RES)
        .in('h3_id', ids);

    if (detailsError) {
        console.error('Details Fetch Error:', detailsError);
        return;
    }

    console.log(`Found ${detailsData ? detailsData.length : 0} matching detail records.`);

    if (detailsData && detailsData.length > 0) {
        const sample = detailsData[0];
        const nullChecks = {
            opportunity: sample.opportunity === null,
            reliability: sample.reliability === null,
            predicted_value: sample.predicted_value === null
        };

        console.log('\nSample Details Row:', JSON.stringify(sample, null, 2));
        console.log('Null Checks:', nullChecks);

        if (detailsData.length !== ids.length) {
            console.warn(`WARNING: Mismatch! Searched for ${ids.length} IDs, found ${detailsData.length}.`);
            const foundIds = new Set(detailsData.map(d => d.h3_id));
            const missing = ids.filter(id => !foundIds.has(id));
            console.log('Missing IDs:', missing);
        } else {
            console.log('SUCCESS: All grid IDs have corresponding details.');
        }
    } else {
        console.error('CRITICAL: No details found for the grid cells provided.');
    }
}

checkDataIntegrity();
