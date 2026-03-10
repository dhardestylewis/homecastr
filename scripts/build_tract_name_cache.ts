/**
 * Build script: populate tract-name-cache.json for tracts without ZCTA-based names.
 * 
 * Strategy: Comprehensive lookup using multiple centroid sources.
 * 1. Queries DB geometry tables (geo_tract20_us, geo_tract20_tx) for exact tract centroids used in active forecasts.
 * 2. Falls back to Census Bureau tract gazetteer (pre-downloaded to C:\tmp\tracts_gaz) for missing tracts.
 * 3. Uses `zipcodes` npm package to find the nearest city for the centroid.
 * 
 * Usage:  $env:NODE_TLS_REJECT_UNAUTHORIZED="0"; npx tsx scripts/build_tract_name_cache.ts
 * Output: lib/publishing/tract-name-cache.json
 */

import * as fs from "fs"
import * as path from "path"
import { Client } from "pg"
import { loadEnvConfig } from "@next/env"

// @ts-ignore
import zipcodes from "zipcodes"

loadEnvConfig(process.cwd())

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

async function main() {
    console.log("Building tract-name-cache.json (DB + Gazetteer approach)...")

    // Step 1: Load existing name sources
    const zctaXwalk: Record<string, string> = JSON.parse(fs.readFileSync(ZCTA_XWALK_FILE, "utf-8"))
    const zipNames: Record<string, string> = JSON.parse(fs.readFileSync(ZIP_NAMES_FILE, "utf-8"))

    const tractsWithZctaName = new Set<string>()
    for (const [tract, zcta] of Object.entries(zctaXwalk)) {
        if (zipNames[zcta]) tractsWithZctaName.add(tract)
    }
    console.log(`  Tracts already named via ZCTA: ${tractsWithZctaName.size}`)

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
        return bestDist < 500 ? bestCity : null
    }

    const allCentroids = new Map<string, { lat: number; lng: number; source: string }>()

    // Step 3: Fetch exact centroids from DB geometry tables
    console.log("  Fetching tract centroids from DB geometry tables...")
    const dbUrl = process.env.POSTGRES_URL_NON_POOLING || process.env.POSTGRES_URL
    if (dbUrl) {
        const client = new Client({ connectionString: dbUrl })
        try {
            await client.connect()
            const tables = ["geo_tract20_us", "geo_tract20_tx"]
            for (const table of tables) {
                try {
                    const { rows } = await client.query(`
                        SELECT geoid, ST_Y(ST_Centroid(geom)) as lat, ST_X(ST_Centroid(geom)) as lng
                        FROM public.${table}
                    `)
                    for (const row of rows) {
                        if (row.geoid && !isNaN(row.lat) && !isNaN(row.lng)) {
                            allCentroids.set(row.geoid, { lat: row.lat, lng: row.lng, source: "db" })
                        }
                    }
                    console.log(`    Successfully loaded ${rows.length} centroids from ${table}`)
                } catch (e) {
                    console.log(`    Skipped ${table}: ${(e as Error).message?.substring(0, 50)}`)
                }
            }
        } catch (e) {
            console.error(`    DB connection failed: ${e}`)
        } finally {
            await client.end()
        }
    } else {
        console.log("    Skipping DB: POSTGRES_URL not set in environment.")
    }

    // Step 4: Fallback to Census Gazetteers (2010 and 2020)
    console.log("  Loading Census tract gazetteers...")
    const gazDirs = ["C:\\tmp\\tracts_gaz", "C:\\tmp\\tracts_gaz_2010"]
    for (const d of gazDirs) {
        if (fs.existsSync(d)) {
            const gazFiles = fs.readdirSync(d).filter(f => f.endsWith(".txt"))
            for (const file of gazFiles) {
                const gazText = fs.readFileSync(path.join(d, file), "utf-8")
                const lines = gazText.split("\n")
                if (lines.length === 0) continue
                const header = lines[0].split("\t").map(h => h.trim().toUpperCase())
                const geoidIdx = header.findIndex(h => h === "GEOID" || h === "GEOID10")
                const latIdx = header.findIndex(h => h.includes("INTPTLAT"))
                const lngIdx = header.findIndex(h => h.includes("INTPTLONG"))

                let count = 0
                for (let i = 1; i < lines.length; i++) {
                    const cols = lines[i].split("\t").map(c => c.trim())
                    if (cols.length <= Math.max(geoidIdx, latIdx, lngIdx)) continue

                    const geoid = cols[geoidIdx]
                    const lat = parseFloat(cols[latIdx])
                    const lng = parseFloat(cols[lngIdx])

                    if (geoid && !isNaN(lat) && !isNaN(lng)) {
                        // Only add if DB didn't already have it
                        if (!allCentroids.has(geoid)) {
                            allCentroids.set(geoid, { lat, lng, source: "gazetteer" })
                            count++
                        }
                    }
                }
                console.log(`    Added ${count} missing centroids from gazetteer (${path.basename(d)})`)
            }
        } else {
            console.log(`    Gazetteer directory not found: ${d}`)
        }
    }

    // Step 5: Process all collected centroids that don't have a ZCTA name
    const result: Record<string, string> = {}
    let named = 0
    let skipped = 0

    for (const [geoid, { lat, lng }] of allCentroids.entries()) {
        if (tractsWithZctaName.has(geoid)) {
            skipped++
            continue
        }

        const city = findNearest(lat, lng)
        if (city) {
            result[geoid] = city
            named++
        }
    }

    console.log(`  Processed unnamed tracts. Skipped (has ZCTA name): ${skipped}, Newly named: ${named}`)

    // Sort and write
    const sorted: Record<string, string> = {}
    for (const key of Object.keys(result).sort()) sorted[key] = result[key]

    console.log(`\nFinal mapping covers ${Object.keys(sorted).length} tracts`)

    const akTracts = Object.keys(sorted).filter(k => k.startsWith("02"))
    console.log(`Alaska coverage: ${akTracts.length} tracts`)
    if (akTracts.length > 0) {
        console.log(`  Samples: ${akTracts.slice(0, 8).map(k => `${k}=${sorted[k]}`).join(", ")}`)
    }

    fs.writeFileSync(OUT_FILE, JSON.stringify(sorted, null, 2) + "\n")
    console.log(`\nWritten to ${OUT_FILE}`)
}

main().catch(console.error)
