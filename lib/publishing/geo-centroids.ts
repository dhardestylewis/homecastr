/**
 * geo-centroids.ts
 *
 * Approximate geographic centroids for US states and major counties/cities.
 * Used to pre-center the embedded /app iframe on forecast hub pages.
 *
 * County FIPS → { lat, lng, zoom }
 * State slug  → { lat, lng, zoom }
 */

export interface GeoCenter {
    lat: number
    lng: number
    zoom: number
}

/** State slug → centroid + default zoom */
export const STATE_CENTROIDS: Record<string, GeoCenter> = {
    al: { lat: 32.806671, lng: -86.791130, zoom: 6 },
    ak: { lat: 61.370716, lng: -152.404419, zoom: 3 },
    az: { lat: 33.729759, lng: -111.431221, zoom: 6 },
    ar: { lat: 34.969704, lng: -92.373123, zoom: 6 },
    ca: { lat: 36.116203, lng: -119.681564, zoom: 5 },
    co: { lat: 39.059811, lng: -105.311104, zoom: 6 },
    ct: { lat: 41.597782, lng: -72.755371, zoom: 7 },
    de: { lat: 39.318523, lng: -75.507141, zoom: 7 },
    dc: { lat: 38.897438, lng: -77.026817, zoom: 10 },
    fl: { lat: 27.766279, lng: -81.686783, zoom: 5 },
    ga: { lat: 33.040619, lng: -83.643074, zoom: 6 },
    hi: { lat: 21.094318, lng: -157.498337, zoom: 6 },
    id: { lat: 44.240459, lng: -114.478828, zoom: 5 },
    il: { lat: 40.349457, lng: -88.986137, zoom: 6 },
    in: { lat: 39.849426, lng: -86.258278, zoom: 6 },
    ia: { lat: 42.011539, lng: -93.210526, zoom: 6 },
    ks: { lat: 38.526600, lng: -96.726486, zoom: 6 },
    ky: { lat: 37.668140, lng: -84.670067, zoom: 6 },
    la: { lat: 31.169960, lng: -91.867805, zoom: 6 },
    me: { lat: 44.693947, lng: -69.381927, zoom: 6 },
    md: { lat: 39.063946, lng: -76.802101, zoom: 7 },
    ma: { lat: 42.230171, lng: -71.530106, zoom: 7 },
    mi: { lat: 43.326618, lng: -84.536095, zoom: 6 },
    mn: { lat: 45.694454, lng: -93.900192, zoom: 6 },
    ms: { lat: 32.741646, lng: -89.678696, zoom: 6 },
    mo: { lat: 38.456085, lng: -92.288368, zoom: 6 },
    mt: { lat: 46.921925, lng: -110.454353, zoom: 5 },
    ne: { lat: 41.125370, lng: -98.268082, zoom: 6 },
    nv: { lat: 38.313515, lng: -117.055374, zoom: 5 },
    nh: { lat: 43.452492, lng: -71.563896, zoom: 7 },
    nj: { lat: 40.298904, lng: -74.521011, zoom: 7 },
    nm: { lat: 34.840515, lng: -106.248482, zoom: 5 },
    ny: { lat: 42.165726, lng: -74.948051, zoom: 6 },
    nc: { lat: 35.630066, lng: -79.806419, zoom: 6 },
    nd: { lat: 47.528912, lng: -99.784012, zoom: 5 },
    oh: { lat: 40.388783, lng: -82.764915, zoom: 6 },
    ok: { lat: 35.565342, lng: -96.928917, zoom: 6 },
    or: { lat: 44.572021, lng: -122.070938, zoom: 5 },
    pa: { lat: 40.590752, lng: -77.209755, zoom: 6 },
    pr: { lat: 18.220833, lng: -66.590149, zoom: 7 },
    ri: { lat: 41.680893, lng: -71.511780, zoom: 8 },
    sc: { lat: 33.856892, lng: -80.945007, zoom: 6 },
    sd: { lat: 44.299782, lng: -99.438828, zoom: 5 },
    tn: { lat: 35.747845, lng: -86.692345, zoom: 6 },
    tx: { lat: 31.054487, lng: -97.563461, zoom: 5 },
    ut: { lat: 40.150032, lng: -111.862434, zoom: 6 },
    vt: { lat: 44.045876, lng: -72.710686, zoom: 7 },
    va: { lat: 37.769337, lng: -78.169968, zoom: 6 },
    wa: { lat: 47.400902, lng: -121.490494, zoom: 6 },
    wv: { lat: 38.491226, lng: -80.954453, zoom: 6 },
    wi: { lat: 44.268543, lng: -89.616508, zoom: 6 },
    wy: { lat: 42.755966, lng: -107.302490, zoom: 5 },
}

/** County FIPS → centroid + zoom (major metros only; fallback to state centroid) */
export const COUNTY_CENTROIDS: Record<string, GeoCenter> = {
    // Texas
    "48201": { lat: 29.7604, lng: -95.3698, zoom: 9 },   // Harris (Houston)
    "48113": { lat: 32.7767, lng: -96.7970, zoom: 9 },   // Dallas
    "48029": { lat: 29.4241, lng: -98.4936, zoom: 9 },   // Bexar (San Antonio)
    "48141": { lat: 31.7619, lng: -106.4850, zoom: 9 },  // El Paso
    "48439": { lat: 32.7254, lng: -97.3208, zoom: 9 },   // Tarrant (Fort Worth)
    "48453": { lat: 30.2672, lng: -97.7431, zoom: 9 },   // Travis (Austin)
    "48157": { lat: 29.5293, lng: -95.7639, zoom: 9 },   // Fort Bend (Sugar Land)
    // New York
    "36061": { lat: 40.7831, lng: -73.9712, zoom: 11 },   // Manhattan
    "36047": { lat: 40.6782, lng: -73.9442, zoom: 10 },   // Brooklyn
    "36081": { lat: 40.7282, lng: -73.7949, zoom: 10 },   // Queens
    "36005": { lat: 40.8448, lng: -73.8648, zoom: 10 },   // Bronx
    "36085": { lat: 40.5795, lng: -74.1502, zoom: 10 },   // Staten Island
    "36059": { lat: 40.7282, lng: -73.5973, zoom: 9 },   // Nassau
    "36103": { lat: 40.9176, lng: -72.6673, zoom: 8 },   // Suffolk
    // California
    "06037": { lat: 34.0522, lng: -118.2437, zoom: 9 },  // Los Angeles
    "06073": { lat: 32.7157, lng: -117.1611, zoom: 9 },  // San Diego
    "06075": { lat: 37.7749, lng: -122.4194, zoom: 11 },  // San Francisco
    "06001": { lat: 37.6017, lng: -121.7195, zoom: 9 },  // Alameda (Oakland)
    // Illinois
    "17031": { lat: 41.8781, lng: -87.6298, zoom: 10 },   // Cook (Chicago)
    // Georgia
    "13121": { lat: 33.7490, lng: -84.3880, zoom: 10 },   // Fulton (Atlanta)
    // Florida
    "12086": { lat: 25.7617, lng: -80.1918, zoom: 10 },   // Miami-Dade
    "12011": { lat: 26.1901, lng: -80.3659, zoom: 9 },   // Broward (Fort Lauderdale)
    "12095": { lat: 28.5383, lng: -81.3792, zoom: 9 },   // Orange (Orlando)
    "12057": { lat: 27.9506, lng: -82.4572, zoom: 10 },   // Hillsborough (Tampa)
    // Washington DC
    "11001": { lat: 38.9072, lng: -77.0369, zoom: 11 },   // DC
    // Pennsylvania
    "42101": { lat: 39.9526, lng: -75.1652, zoom: 10 },   // Philadelphia
    "42003": { lat: 40.4406, lng: -79.9959, zoom: 10 },   // Allegheny (Pittsburgh)
    // Massachusetts
    "25025": { lat: 42.3601, lng: -71.0589, zoom: 11 },   // Suffolk (Boston)
    // Colorado
    "08031": { lat: 39.7392, lng: -104.9903, zoom: 10 },  // Denver
    // Washington
    "53033": { lat: 47.6062, lng: -122.3321, zoom: 10 },  // King (Seattle)
    // Arizona
    "04013": { lat: 33.4484, lng: -112.0740, zoom: 9 },  // Maricopa (Phoenix)
    // Michigan
    "26163": { lat: 42.3314, lng: -83.0458, zoom: 10 },   // Wayne (Detroit)
    // Minnesota
    "27053": { lat: 44.9778, lng: -93.2650, zoom: 10 },   // Hennepin (Minneapolis)
    // Oregon
    "41051": { lat: 45.5051, lng: -122.6750, zoom: 10 },  // Multnomah (Portland)
    // Tennessee
    "47037": { lat: 36.1627, lng: -86.7816, zoom: 10 },   // Davidson (Nashville)
    "47157": { lat: 35.1495, lng: -90.0490, zoom: 10 },   // Shelby (Memphis)
    // North Carolina
    "37119": { lat: 35.2271, lng: -80.8431, zoom: 10 },   // Mecklenburg (Charlotte)
    "37183": { lat: 35.7796, lng: -78.6382, zoom: 10 },   // Wake (Raleigh)
    // Nevada
    "32003": { lat: 36.1699, lng: -115.1398, zoom: 10 },  // Clark (Las Vegas)
    // Ohio
    "39049": { lat: 39.9612, lng: -82.9988, zoom: 10 },   // Franklin (Columbus)
    "39035": { lat: 41.4993, lng: -81.6944, zoom: 10 },   // Cuyahoga (Cleveland)
    // Indiana
    "18097": { lat: 39.7684, lng: -86.1581, zoom: 10 },   // Marion (Indianapolis)
    // Louisiana
    "22071": { lat: 29.9511, lng: -90.0715, zoom: 10 },   // Orleans (New Orleans)
    // Wisconsin
    "55079": { lat: 43.0389, lng: -87.9065, zoom: 10 },   // Milwaukee
    // Maryland
    "24510": { lat: 39.2904, lng: -76.6122, zoom: 10 },   // Baltimore City
    // Missouri
    "29510": { lat: 38.6270, lng: -90.1994, zoom: 10 },   // St. Louis City
    // Alabama
    "01073": { lat: 33.5186, lng: -86.8104, zoom: 9 },   // Jefferson (Birmingham)
}

/**
 * Resolve a city slug → GeoCenter.
 * Matches against COUNTY_CENTROIDS if we have a county FIPS for the city,
 * otherwise falls back to the state centroid.
 */
export function getCenterForCity(
    countyFips: string | undefined,
    stateSlug: string
): GeoCenter {
    if (countyFips && COUNTY_CENTROIDS[countyFips]) {
        return COUNTY_CENTROIDS[countyFips]
    }
    return STATE_CENTROIDS[stateSlug] ?? { lat: 39.8283, lng: -98.5795, zoom: 3 }
}

export function getCenterForState(stateSlug: string): GeoCenter {
    return STATE_CENTROIDS[stateSlug] ?? { lat: 39.8283, lng: -98.5795, zoom: 3 }
}
