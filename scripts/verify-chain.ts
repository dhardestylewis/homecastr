
import { executeTopLevelTool } from "../app/actions/tools";

async function verifyChain() {
    console.log("--- Granular Chain Verification Start ---");

    // 1. Resolve 'The Heights'
    console.log("\n1. Resolving 'The Heights'...");
    const resolveRes = await executeTopLevelTool("resolve_place", { query: "The Heights", city_hint: "Houston", max_candidates: 1 });
    const { candidates } = JSON.parse(resolveRes);
    const top = candidates[0];
    console.log(`   Found: ${top.label} at (${top.lat}, ${top.lng})`);

    // 2. Map to Hex
    console.log("\n2. Mapping to H3 at res 9...");
    const hexRes = await executeTopLevelTool("point_to_hex", { lat: top.lat, lng: top.lng, h3_id: 9 });
    const { h3_id } = JSON.parse(hexRes);
    console.log(`   H3 ID: ${h3_id}`);

    // 3. Get Metrics (2030)
    console.log("\n3. Fetching metrics for 2030 (Robust check)...");
    const metricsRes = await executeTopLevelTool("get_h3_hex", { h3_id, forecast_year: 2030 });
    const { hex } = JSON.parse(metricsRes);
    console.log(`   Location: ${hex.location.name}`);
    console.log(`   Opportunity (2030): ${hex.metrics.annual_change_pct}%`);
    console.log(`   Context: ${hex.context}`);

    console.log("\n--- Granular Chain Verification End ---");
}

verifyChain();
