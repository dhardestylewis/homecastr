/**
 * Build script: populate tract-name-cache.json using Census tract gazetteer.
 * 
 * Reads the Census Bureau tract gazetteer (pre-downloaded to C:\tmp\tracts_gaz)
 * and uses the `zipcodes` npm package to find the nearest city for each tract
 * that doesn't already have a ZCTA-based name.
 * 
 * Prerequisites:
 *   [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
 *   Invoke-WebRequest -Uri "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2020_Gazetteer/2020_Gaz_tracts_national.zip" -OutFile "C:\tmp\tracts_gaz.zip" -UseBasicParsing
 *   Expand-Archive -Path "C:\tmp\tracts_gaz.zip" -DestinationPath "C:\tmp\tracts_gaz" -Force
 * 
 * Usage:  npx tsx scripts/build_tract_name_cache.ts
 * Output: lib/publishing/tract-name-cache.json
 */

import * as fs from "fs"
import * as path from "path"

// @ts-ignore
import zipcodes from "zipcodes"

const OUT_FILE = path.join(__dirname, "..", "lib", "publishing", "tract-name-cache.json")
const ZCTA_XWALK_FILE = path.join(__dirname, "..", "lib", "publishing", "tract-zcta-crosswalk.json")
const ZIP_NAMES_FILE = path.join(__dirname, "..", "lib", "publishing", "zip-city-names.json")
const GAZ_DIR = "C:\\tmp\\tracts_gaz"

function haversine(lat1: number, lon1: number, lat2: number, lon2: number): number {
    const R = 6371
    const dLat = (lat2 - lat1) * Math.PI / 180
    const dLon = (lon2 - lon1) * Math.PI / 180
    const a = Math.sin(dLat / 2) ** 2 +
        Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
        Math.sin(dLon / 2) ** 2
    return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
}

function titleCase(s: string): string {
    return s.split(" ").map(w => {
        if (w.length <= 2 && w === w.toUpperCase()) return w
        return w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()
    }).join(" ")
}

function main() {
    console.log("Building tract-name-cache.json from Census gazetteer...")

    // Step 1: Load existing name sources
    const zctaXwalk: Record<string, string> = JSON.parse(fs.readFileSync(ZCTA_XWALK_FILE, "utf-8"))
    const zipNames: Record<string, string> = JSON.parse(fs.readFileSync(ZIP_NAMES_FILE, "utf-8"))

    const tractsWithName = new Set<string>()
    for (const [tract, zcta] of Object.entries(zctaXwalk)) {
        if (zipNames[zcta]) tractsWithName.add(tract)
    }
    console.log(`  Tracts with ZCTA+name: ${tractsWithName.size}`)

    // Step 2: Build ZIP spatial index
    console.log("  Building ZIP spatial index...")
    interface ZipEntry { lat: number; lng: number; city: string }
    const zipEntries: ZipEntry[] = []
    for (let i = 0; i <= 99999; i++) {
        const zip = String(i).padStart(5, "0")
        const info = zipcodes.lookup(zip)
        if (info?.city && info?.latitude && info?.longitude) {
            zipEntries.push({ lat: info.latitude, lng: info.longitude, city: titleCase(info.city) })
        }
    }
    console.log(`  ZIP entries: ${zipEntries.length}`)

    const grid = new Map<string, ZipEntry[]>()
    for (const e of zipEntries) {
        const key = `${Math.floor(e.lat)},${Math.floor(e.lng)}`
        if (!grid.has(key)) grid.set(key, [])
        grid.get(key)!.push(e)
    }

    function findNearest(lat: number, lng: number): string | null {
        const cLat = Math.floor(lat), cLng = Math.floor(lng)
        let bestCity = "", bestDist = Infinity
        for (let dl = -3; dl <= 3; dl++) {
            for (let dn = -3; dn <= 3; dn++) {
                const entries = grid.get(`${cLat + dl},${cLng + dn}`)
                if (!entries) continue
                for (const e of entries) {
                    const d = haversine(lat, lng, e.lat, e.lng)
                    if (d < bestDist) { bestDist = d; bestCity = e.city }
                }
            }
        }
        return bestDist < 500 ? bestCity : null // 500km for remote Alaska
    }

    // Step 3: Read Census tract gazetteer
    console.log("  Loading Census tract gazetteer...")
    const gazFiles = fs.readdirSync(GAZ_DIR).filter(f => f.endsWith(".txt"))
    if (gazFiles.length === 0) {
        console.error("No .txt file in C:\\tmp\\tracts_gaz. See script header for download instructions.")
        process.exit(1)
    }

    const gazText = fs.readFileSync(path.join(GAZ_DIR, gazFiles[0]), "utf-8")
    const lines = gazText.split("\n")
    const header = lines[0].split("\t").map(h => h.trim().toUpperCase())
    const geoidIdx = header.findIndex(h => h === "GEOID")
    const latIdx = header.findIndex(h => h.includes("INTPTLAT"))
    const lngIdx = header.findIndex(h => h.includes("INTPTLONG"))
    console.log(`  Columns: GEOID=${geoidIdx}, LAT=${latIdx}, LNG=${lngIdx}`)
    console.log(`  ${lines.length - 1} tracts in gazetteer`)

    // Step 4: Process ALL tracts (not just ones missing from ZCTA)
    // This builds the comprehensive cache that the runtime can use
    const result: Record<string, string> = {}
    let processed = 0, named = 0, alreadyHasZcta = 0

    for (let i = 1; i < lines.length; i++) {
        const cols = lines[i].split("\t").map(c => c.trim())
        if (cols.length <= Math.max(geoidIdx, latIdx, lngIdx)) continue

        const geoid = cols[geoidIdx]
        const lat = parseFloat(cols[latIdx])
        const lng = parseFloat(cols[lngIdx])
        if (!geoid || isNaN(lat) || isNaN(lng)) continue
        processed++

        // Skip tracts that already have a ZCTA-based name
        if (tractsWithName.has(geoid)) { alreadyHasZcta++; continue }

        const city = findNearest(lat, lng)
        if (city) {
            result[geoid] = city
            named++
        }
    }

    console.log(`  Processed: ${processed}, have ZCTA name: ${alreadyHasZcta}, newly named: ${named}`)

    // Sort and write
    const sorted: Record<string, string> = {}
    for (const key of Object.keys(result).sort()) sorted[key] = result[key]

    console.log(`\nFinal: ${Object.keys(sorted).length} tract name mappings`)

    const stateGroups: Record<string, number> = {}
    for (const geoid of Object.keys(sorted)) {
        const st = geoid.substring(0, 2)
        stateGroups[st] = (stateGroups[st] || 0) + 1
    }
    const topStates = Object.entries(stateGroups).sort((a, b) => b[1] - a[1]).slice(0, 10)
    console.log("Top 10 states by newly named tracts:")
    for (const [st, count] of topStates) console.log(`  FIPS ${st}: ${count}`)

    const akTracts = Object.keys(sorted).filter(k => k.startsWith("02"))
    console.log(`\nAlaska: ${akTracts.length} tracts`)
    if (akTracts.length > 0)
        console.log(`  Samples: ${akTracts.slice(0, 8).map(k => `${k}=${sorted[k]}`).join(", ")}`)

    fs.writeFileSync(OUT_FILE, JSON.stringify(sorted, null, 2) + "\n")
    console.log(`\nWritten to ${OUT_FILE}`)
}

main()
