
require('dotenv').config({ path: '.env.local' });
const { createClient } = require('@supabase/supabase-js');

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseKey) {
    console.error("Supabase credentials missing from env");
    process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey);

async function findCoordinateColumns() {
    console.log("Searching information_schema for 'gis_lat'...");

    // We try to query information_schema via a postgrest select if exposed.
    // Sometimes people expose it as a view.

    // If not, we'll try to find a table with 'property' in the name.
    const tablesToTry = [
        'property_locations', 'property_coords', 'properties_gis', 'hcad_data', 'hcad_parcels'
    ];

    for (const table of tablesToTry) {
        try {
            const { data, error } = await supabase.from(table).select('*').limit(1);
            if (!error && data && data.length > 0) {
                console.log(`✓ Table found: ${table}`);
                console.log(`  Columns: ${Object.keys(data[0]).join(', ')}`);
            }
        } catch (e) { }
    }

    // Try to see if there is an RPC that lists tables
    const { data: rpcData, error: rpcError } = await supabase.rpc('get_tables');
    if (rpcData) {
        console.log("Found get_tables RPC:", rpcData);
    }
}

findCoordinateColumns();
