
import { executeTopLevelTool } from "../app/actions/tools";

async function verify() {
    console.log("--- Verification Start ---");

    // 1. Test "Pasadena" geocoding and metrics
    console.log("\nTesting Pasadena (City search):");
    const pasadena = await executeTopLevelTool("location_to_hex", { query: "Pasadena" });
    const pData = JSON.parse(pasadena);
    console.log("Label:", pData.chosen.label);
    console.log("Metrics:", pData.h3.metrics ? "FOUND" : "NULL");
    if (pData.h3.metrics) console.log("Property Count:", pData.h3.metrics.property_count);
    console.log("Context:", pData.h3.context);

    // 2. Test "Montrose" baseline (2026)
    console.log("\nTesting Montrose 2026 (Baseline check):");
    const montrose2026 = await executeTopLevelTool("location_to_hex", { query: "Montrose", forecast_year: 2026 });
    const m26Data = JSON.parse(montrose2026);
    console.log("Annual Change (Expected 0):", m26Data.h3.metrics?.annual_change_pct);

    // 3. Test "Montrose" 2029
    console.log("\nTesting Montrose 2029:");
    const montrose2029 = await executeTopLevelTool("location_to_hex", { query: "Montrose", forecast_year: 2029 });
    const m29Data = JSON.parse(montrose2029);
    console.log("Annual Change:", m29Data.h3.metrics?.annual_change_pct);

    // 4. Test missing metrics fallback (find a known park or empty area)
    console.log("\nTesting Neighborhood Fallback (Memorial Park):");
    const park = await executeTopLevelTool("location_to_hex", { query: "Memorial Park" });
    const parkData = JSON.parse(park);
    console.log("Label:", parkData.chosen.label);
    console.log("Context (Expected neighborhood_average):", parkData.h3.context);
    console.log("Metrics:", parkData.h3.metrics ? "FOUND" : "NULL");

    console.log("\n--- Verification End ---");
}

verify();
