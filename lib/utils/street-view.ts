import { cellToLatLng, cellToParent, getResolution } from "h3-js"

export interface PropertyLocation {
    lat: number
    lng: number
    label?: string
}

/**
 * Generates a list of representative property locations for a given set of H3 hexes.
 * If one hex is selected, it generates offsets around the center.
 * If multiple hexes are selected, it uses the centers of those hexes.
 */
export function getRepresentativeProperties(h3Ids: string[]): PropertyLocation[] {
    if (h3Ids.length === 0) return []

    if (h3Ids.length === 1) {
        const id = h3Ids[0]
        const [lat, lng] = cellToLatLng(id)

        // Generate a few stable offsets so "properties" are consistent for the same hex
        return [
            { lat: lat, lng: lng, label: "Center" },
            { lat: lat + 0.0004, lng: lng + 0.0004, label: "North East" },
            { lat: lat - 0.0004, lng: lng - 0.0004, label: "South West" },
            { lat: lat + 0.0004, lng: lng - 0.0004, label: "North West" },
            { lat: lat - 0.0004, lng: lng + 0.0004, label: "South East" },
        ]
    }

    // For multiple hexes, take the center of up to 5 hexes
    return h3Ids.slice(0, 5).map((id, index) => {
        const [lat, lng] = cellToLatLng(id)
        return { lat, lng, label: `Area ${index + 1}` }
    })
}

/**
 * Constructs an unsigned Google Street View Static API URL.
 */
export function getStreetViewImageUrl(lat: number, lng: number, apiKey: string, width = 400, height = 300): string {
    return `https://maps.googleapis.com/maps/api/streetview?size=${width}x${height}&location=${lat},${lng}&key=${apiKey}`
}

// ── Cache constants ───────────────────────────────────────────────────────────
const LS_KEY = "properlytic_sv_v1"   // localStorage key
const LS_MAX = 2000                   // max entries in localStorage store
const MEM_MAX = 500                    // in-memory LRU mirror size

// In-memory LRU mirror for hot reads (avoids JSON.parse on every call)
const memCache = new Map<string, string>()

// ── localStorage helpers ──────────────────────────────────────────────────────
function lsRead(): Record<string, string> {
    try { return JSON.parse(localStorage.getItem(LS_KEY) ?? "{}") } catch { return {} }
}

function lsGet(key: string): string | null {
    try { return lsRead()[key] ?? null } catch { return null }
}

function lsSet(key: string, url: string): void {
    try {
        const store = lsRead()
        store[key] = url
        const keys = Object.keys(store)
        if (keys.length > LS_MAX) {
            keys.slice(0, keys.length - LS_MAX).forEach(k => delete store[k])
        }
        localStorage.setItem(LS_KEY, JSON.stringify(store))
    } catch { /* ignore quota / SSR errors */ }
}

// ── In-memory LRU helper ──────────────────────────────────────────────────────
function memSet(key: string, url: string) {
    memCache.delete(key)
    memCache.set(key, url)
    if (memCache.size > MEM_MAX) {
        const oldest = memCache.keys().next().value
        if (oldest) memCache.delete(oldest)
    }
}

function svKey(lat: number, lng: number, w: number, h: number) {
    return `${lat.toFixed(5)},${lng.toFixed(5)},${w},${h}`
}

// ── H3 ancestor zoom sharing ──────────────────────────────────────────────────
/**
 * Checks if any ancestor H3 cell (up to `depth` resolutions coarser) already
 * has a cached street view URL. Enables coarser-zoom views to reuse finer-zoom
 * images without an extra API call.
 */
export function getCachedAncestorStreetView(
    h3Id: string,
    width: number,
    height: number,
    depth = 3
): string | null {
    try {
        let cur = h3Id
        const floor = Math.max(0, getResolution(h3Id) - depth)
        while (getResolution(cur) > floor) {
            cur = cellToParent(cur, getResolution(cur) - 1)
            const [lat, lng] = cellToLatLng(cur)
            const key = svKey(lat, lng, width, height)
            const hit = memCache.get(key) ?? lsGet(key)
            if (hit) { memSet(key, hit); return hit }
        }
    } catch { /* h3 errors safe to swallow */ }
    return null
}

// ── Main export ───────────────────────────────────────────────────────────────
/**
 * Returns a signed (or unsigned fallback) Google Street View Static API URL.
 *
 * Cache lookup order — fastest to slowest:
 *   1. In-memory LRU          (instant, current session)
 *   2. localStorage            (persistent across refreshes)
 *   3. H3 ancestor sharing     (reuse a cached parent-cell URL)
 *   4. Google Maps API call    (billed; last resort, then stored in 1+2)
 */
export async function getSignedStreetViewUrl(
    lat: number,
    lng: number,
    width = 400,
    height = 300,
    h3Id?: string   // optional — enables ancestor zoom sharing
): Promise<string> {
    const key = svKey(lat, lng, width, height)

    // 1. In-memory
    const mem = memCache.get(key)
    if (mem) { memCache.delete(key); memCache.set(key, mem); console.log(`[SV] ✅ MEM HIT  ${key}`); return mem }

    // 2. localStorage
    const stored = lsGet(key)
    if (stored) { memSet(key, stored); console.log(`[SV] ✅ LS HIT   ${key}`); return stored }

    // 3. H3 ancestor zoom sharing
    if (h3Id) {
        const ancestor = getCachedAncestorStreetView(h3Id, width, height)
        if (ancestor) { console.log(`[SV] ✅ ANCESTOR ${key} (reused parent cell)`); return ancestor }
    }

    // 4. API call
    console.log(`[SV] 🌐 API CALL ${key} (cache miss — will cost $0.007)`)
    try {
        const res = await fetch(`/api/streetview-sign?lat=${lat}&lng=${lng}&w=${width}&h=${height}`)
        if (res.ok) {
            const { url } = await res.json()
            memSet(key, url)
            lsSet(key, url)
            console.log(`[SV] 💾 CACHED  ${key} (mem + localStorage)`)
            return url
        }
    } catch { /* fall through to unsigned */ }

    const apiKey = process.env.NEXT_PUBLIC_GOOGLE_MAPS_KEY || ""
    return getStreetViewImageUrl(lat, lng, apiKey, width, height)
}
