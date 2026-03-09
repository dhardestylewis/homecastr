import { loadEnvConfig } from '@next/env';
loadEnvConfig(process.cwd());
import { getSupabaseAdmin } from './lib/supabase/admin';

async function main() {
    const supabase = getSupabaseAdmin();
    let fRes = await supabase.schema('forecast_queue').from('metrics_tract_forecast').select('*').limit(1);
    console.log('Tract forecast keys:', Object.keys(fRes.data[0] || {}));

    fRes = await supabase.schema('forecast_queue').from('metrics_zcta_forecast').select('*').limit(1);
    console.log('ZCTA forecast keys:', Object.keys(fRes.data[0] || {}));
}
main().catch(console.error);
