import { loadEnvConfig } from '@next/env'
loadEnvConfig(process.cwd())

import { fetchForecastPageData } from "./lib/publishing/forecast-data"
import * as fs from "fs"

async function main() {
    let out = ""
    const syntheticTract = "0611570025R"
    const standardTract = "06085510801" // known to exist in history table

    out += `\n=== Testing synthetic tract: ${syntheticTract} ===\n`
    const synthData = await fetchForecastPageData(syntheticTract)
    out += `History length: ${synthData?.history.length}\n`
    if (synthData?.history && synthData.history.length > 0) {
        out += `First history point: ${JSON.stringify(synthData.history[0])}\n`
    } else {
        out += "No history found.\n"
    }

    out += `\n=== Testing standard tract: ${standardTract} ===\n`
    const stdData = await fetchForecastPageData(standardTract)
    out += `History length: ${stdData?.history.length}\n`
    if (stdData?.history && stdData.history.length > 0) {
        out += `First history point: ${JSON.stringify(stdData.history[0])}\n`
    } else {
        out += "No history found.\n"
    }

    fs.writeFileSync("test_fetch_history.txt", out, "utf8")
    console.log("Wrote test_fetch_history.txt")
}

main().catch(console.error)
