/**
 * Build script: regenerate zip-city-names.json with full national coverage.
 * 
 * Uses the `zipcodes` npm package (41K+ US ZIP codes with city names)
 * as the comprehensive baseline, then merges with the existing file
 * to preserve any manually-added entries.
 * 
 * Usage:  npx tsx scripts/build_zip_city_names.ts
 * Output: lib/publishing/zip-city-names.json
 */

import * as fs from "fs"
import * as path from "path"

// @ts-ignore — no types for this package
import zipcodes from "zipcodes"

const OUT_FILE = path.join(__dirname, "..", "lib", "publishing", "zip-city-names.json")

function titleCase(s: string): string {
    return s
        .split(" ")
        .map(w => {
            // Preserve common abbreviations
            if (["AFB", "JFK", "NAS", "NE", "NW", "SE", "SW"].includes(w.toUpperCase())) {
                return w.toUpperCase()
            }
            return w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()
        })
        .join(" ")
}

function main() {
    console.log("Building zip-city-names.json...")

    const result: Record<string, string> = {}

    // Step 1: Extract all ZIP codes from the zipcodes package
    // The package stores data keyed by ZIP code string
    const allCodes = Object.keys(zipcodes.codes || {})

    // If .codes doesn't exist, try iterating through lookup
    if (allCodes.length === 0) {
        // Try brute-force lookup for all 5-digit codes
        // Actually, let's just scan the package's internal data
        console.log("  Using lookup scan approach...")
        for (let i = 0; i <= 99999; i++) {
            const zip = String(i).padStart(5, "0")
            const info = zipcodes.lookup(zip)
            if (info && info.city) {
                result[zip] = titleCase(info.city)
            }
        }
    } else {
        console.log(`  Found ${allCodes.length} ZIP codes in package...`)
        for (const zip of allCodes) {
            const info = zipcodes.lookup(zip)
            if (info && info.city) {
                result[zip] = titleCase(info.city)
            }
        }
    }

    console.log(`  From zipcodes package: ${Object.keys(result).length} entries`)

    // Step 2: Merge with existing file to preserve any entries not in the package
    if (fs.existsSync(OUT_FILE)) {
        const existing: Record<string, string> = JSON.parse(fs.readFileSync(OUT_FILE, "utf-8"))
        let added = 0
        for (const [zip, city] of Object.entries(existing)) {
            if (!result[zip] && city) {
                result[zip] = city
                added++
            }
        }
        console.log(`  Merged ${added} additional entries from existing file`)
    }

    // Sort by ZIP code for deterministic output
    const sorted: Record<string, string> = {}
    for (const key of Object.keys(result).sort()) {
        sorted[key] = result[key]
    }

    const total = Object.keys(sorted).length
    console.log(`\nFinal: ${total} ZIP-to-city mappings`)

    // Coverage report by state prefix
    const prefixes: Record<string, number> = {}
    for (const zip of Object.keys(sorted)) {
        const pfx = zip.substring(0, 3)
        prefixes[pfx] = (prefixes[pfx] || 0) + 1
    }

    // Check NH coverage specifically
    const nhZips = Object.keys(sorted).filter(z => z.startsWith("03"))
    console.log(`NH coverage (03xxx): ${nhZips.length} ZIPs`)
    console.log(`  Samples: ${nhZips.slice(0, 5).map(z => `${z}=${sorted[z]}`).join(", ")}`)

    // Check a few other states
    const prefixSamples = [
        { label: "TX (75-79)", filter: (z: string) => z >= "75000" && z <= "79999" },
        { label: "CA (90-96)", filter: (z: string) => z >= "90000" && z <= "96999" },
        { label: "FL (32-34)", filter: (z: string) => z >= "32000" && z <= "34999" },
        { label: "NY (10-14)", filter: (z: string) => z >= "10000" && z <= "14999" },
    ]
    for (const { label, filter } of prefixSamples) {
        const count = Object.keys(sorted).filter(filter).length
        console.log(`${label}: ${count} ZIPs`)
    }

    // Write output
    fs.writeFileSync(OUT_FILE, JSON.stringify(sorted, null, 2) + "\n")
    console.log(`\nWritten to ${OUT_FILE}`)
}

main()
