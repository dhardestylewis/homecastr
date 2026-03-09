import { loadEnvConfig } from '@next/env'
loadEnvConfig(process.cwd())

import { fetchForecastPageData } from "./lib/publishing/forecast-data"

async function main() {
    // 06085510801 is a known standard tract with history
    const data = await fetchForecastPageData("06085510801")
    console.log("CACHE RESULT FOR 06085510801:")
    console.log(`History length: ${data?.history?.length}`)
    if (data?.history && data.history.length > 0) {
        console.log(`First item:`, data.history[0])
    }
}

main().catch(console.error)
