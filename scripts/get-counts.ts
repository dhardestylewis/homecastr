import { loadEnvConfig } from "@next/env";
loadEnvConfig(process.cwd());

import { getSupabaseAdmin } from "../lib/supabase/admin";
import fs from "fs";

const SCHEMA = process.env.FORECAST_SCHEMA || "forecast_queue";

async function fetchAllTracts() {
    console.log(`Querying schema: ${SCHEMA}`);
    const supabase = getSupabaseAdmin();
    
    let offset = 0;
    const pageSize = 1000;
    const allTracts = new Set<string>();

    while (true) {
        console.log(`Fetching offset ${offset}...`);
        const { data, error } = await supabase
            .schema(SCHEMA as any)
            .from("metrics_tract_forecast")
            .select("tract_geoid20")
            .eq("origin_year", 2025)
            .eq("horizon_m", 12)
            .eq("series_kind", "forecast")
            .order("tract_geoid20", { ascending: true })
            .range(offset, offset + pageSize - 1);

        if (error) {
            console.error("Error:", error);
            fs.writeFileSync("scripts/counts.json", JSON.stringify({error}));
            return null;
        }

        if (!data || data.length === 0) {
            break;
        }

        for (const row of data) {
            allTracts.add(row.tract_geoid20);
        }

        if (data.length < pageSize) {
            break;
        }

        offset += pageSize;
    }

    return allTracts;
}

async function run() {
    const allTracts = await fetchAllTracts();
    if (!allTracts) return;

    const tracts = new Set<string>();
    const counties = new Set<string>();
    const states = new Set<string>();

    for (const geoid of allTracts) {
        tracts.add(geoid);
        counties.add(geoid.substring(0, 5));
        states.add(geoid.substring(0, 2));
    }

    const result = {
        tracts: tracts.size,
        counties: counties.size,
        states: states.size
    };
    
    console.log("Results:", result);
    fs.writeFileSync("scripts/counts.json", JSON.stringify(result));
    process.exit(0);
}

run();
