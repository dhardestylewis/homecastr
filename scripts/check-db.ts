
import { getSupabaseServerClient } from "./lib/supabase/server";

async function check() {
    const supabase = await getSupabaseServerClient();
    const { data, error } = await supabase
        .from("h3_precomputed_hex_details")
        .select("h3_res, count")
        .select("h3_res")
        .limit(1); // Just a placeholder to see if I can write a test script

    // Actually, I'll just write a script that counts hexes per resolution.
    const { data: counts, error: err } = await supabase.rpc('get_h3_stats');
    // If RPC doesn't exist, I'll just do a raw select count group by.

    console.log("Checking resolutions...");
    for (const res of [7, 8, 9, 10, 11]) {
        const { count } = await supabase
            .from("h3_precomputed_hex_details")
            .select("*", { count: 'exact', head: true })
            .eq("h3_res", res);
        console.log(`Res ${res}: ${count} rows`);
    }
}

check();
