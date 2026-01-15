const { createClient } = require('@supabase/supabase-js');

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL;
const supabaseKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;

if (!supabaseUrl || !supabaseKey) {
    console.error('Missing Supabase env vars');
    process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey);

async function checkCounts() {
    console.log("Checking h3_aoi_grid counts and samples...");

    const resolutions = [7, 8, 9, 10, 11];

    for (const res of resolutions) {
        // Check Count
        const { count, error } = await supabase
            .from('h3_aoi_grid')
            .select('*', { count: 'exact', head: true })
            .eq('h3_res', res);

        if (error) {
            console.error(`Error fetching res ${res}:`, error.message);
        } else {
            console.log(`Res ${res}: ${count} rows`);

            // Check Sample if count > 0
            if (count > 0) {
                const { data, error: sampleError } = await supabase
                    .from('h3_aoi_grid')
                    .select('h3_id, aoi_id, lat, lng')
                    .eq('h3_res', res)
                    .limit(1);

                if (data && data.length > 0) {
                    console.log(`   Sample Res ${res}:`, JSON.stringify(data[0]));
                }
            }
        }
    }
}

checkCounts();
