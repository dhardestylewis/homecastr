import { loadEnvConfig } from "@next/env"
loadEnvConfig(process.cwd())

import { getDynamicBounds } from "./lib/publishing/geo-bounds"

async function run() {
    const stateBounds = await getDynamicBounds("state", "23")
    console.log("State ME:", stateBounds)

    const countyBounds = await getDynamicBounds("county", "48201")
    console.log("County Harris TX:", countyBounds)

    const countyME = await getDynamicBounds("county", "23005")
    console.log("County Cumberland ME:", countyME)

    const tractBounds = await getDynamicBounds("tract", "48201100100", "tx")
    console.log("Tract 48201100100:", tractBounds)
}
run()
