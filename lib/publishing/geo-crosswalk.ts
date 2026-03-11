import { getSupabaseAdmin } from "@/lib/supabase/admin"
import { withRedisCache } from "@/lib/redis"
import { COUNTY_NAMES } from "./county-fips"
import zipCityRaw from "./zip-city-names.json"
const ZIP_CITY_NATIONAL: Record<string, string> = zipCityRaw as any

// ---------------------------------------------------------------------------
// Census FIPS → Human-readable geography names
// ---------------------------------------------------------------------------

// US State FIPS → { name, abbr }
const STATE_FIPS: Record<string, { name: string; abbr: string }> = {
    "01": { name: "Alabama", abbr: "AL" },
    "02": { name: "Alaska", abbr: "AK" },
    "04": { name: "Arizona", abbr: "AZ" },
    "05": { name: "Arkansas", abbr: "AR" },
    "06": { name: "California", abbr: "CA" },
    "08": { name: "Colorado", abbr: "CO" },
    "09": { name: "Connecticut", abbr: "CT" },
    "10": { name: "Delaware", abbr: "DE" },
    "11": { name: "District of Columbia", abbr: "DC" },
    "12": { name: "Florida", abbr: "FL" },
    "13": { name: "Georgia", abbr: "GA" },
    "15": { name: "Hawaii", abbr: "HI" },
    "16": { name: "Idaho", abbr: "ID" },
    "17": { name: "Illinois", abbr: "IL" },
    "18": { name: "Indiana", abbr: "IN" },
    "19": { name: "Iowa", abbr: "IA" },
    "20": { name: "Kansas", abbr: "KS" },
    "21": { name: "Kentucky", abbr: "KY" },
    "22": { name: "Louisiana", abbr: "LA" },
    "23": { name: "Maine", abbr: "ME" },
    "24": { name: "Maryland", abbr: "MD" },
    "25": { name: "Massachusetts", abbr: "MA" },
    "26": { name: "Michigan", abbr: "MI" },
    "27": { name: "Minnesota", abbr: "MN" },
    "28": { name: "Mississippi", abbr: "MS" },
    "29": { name: "Missouri", abbr: "MO" },
    "30": { name: "Montana", abbr: "MT" },
    "31": { name: "Nebraska", abbr: "NE" },
    "32": { name: "Nevada", abbr: "NV" },
    "33": { name: "New Hampshire", abbr: "NH" },
    "34": { name: "New Jersey", abbr: "NJ" },
    "35": { name: "New Mexico", abbr: "NM" },
    "36": { name: "New York", abbr: "NY" },
    "37": { name: "North Carolina", abbr: "NC" },
    "38": { name: "North Dakota", abbr: "ND" },
    "39": { name: "Ohio", abbr: "OH" },
    "40": { name: "Oklahoma", abbr: "OK" },
    "41": { name: "Oregon", abbr: "OR" },
    "42": { name: "Pennsylvania", abbr: "PA" },
    "44": { name: "Rhode Island", abbr: "RI" },
    "45": { name: "South Carolina", abbr: "SC" },
    "46": { name: "South Dakota", abbr: "SD" },
    "47": { name: "Tennessee", abbr: "TN" },
    "48": { name: "Texas", abbr: "TX" },
    "49": { name: "Utah", abbr: "UT" },
    "50": { name: "Vermont", abbr: "VT" },
    "51": { name: "Virginia", abbr: "VA" },
    "53": { name: "Washington", abbr: "WA" },
    "54": { name: "West Virginia", abbr: "WV" },
    "55": { name: "Wisconsin", abbr: "WI" },
    "56": { name: "Wyoming", abbr: "WY" },
    "72": { name: "Puerto Rico", abbr: "PR" },
}

// Major county FIPS → city name  (top metros first; extend as needed)
const COUNTY_CITY: Record<string, string> = {
    // Texas
    "48201": "Houston", "48113": "Dallas", "48029": "San Antonio",
    "48141": "El Paso", "48439": "Tarrant", "48085": "Collin",
    "48453": "Austin", "48157": "Fort Bend", "48339": "Montgomery",
    // New York
    "36061": "Manhattan", "36047": "Brooklyn", "36081": "Queens",
    "36005": "Bronx", "36085": "Staten Island",
    "36059": "Nassau", "36103": "Suffolk", "36119": "Westchester",
    // California
    "06037": "Los Angeles", "06073": "San Diego", "06065": "Riverside",
    "06071": "San Bernardino", "06059": "Orange County",
    "06075": "San Francisco", "06001": "Alameda", "06085": "San Jose",
    "06081": "San Mateo", "06013": "Contra Costa",
    // Illinois
    "17031": "Chicago", "17043": "DuPage", "17089": "Kane",
    "17097": "Lake", "17197": "Will",
    // Georgia
    "13121": "Atlanta", "13089": "DeKalb", "13067": "Cobb",
    "13135": "Gwinnett",
    // Florida
    "12086": "Miami", "12011": "Broward", "12099": "Palm Beach",
    "12095": "Orange", "12057": "Hillsborough",
    // Washington DC area
    "11001": "Washington",
    // Pennsylvania
    "42101": "Philadelphia", "42003": "Pittsburgh",
    // Massachusetts
    "25025": "Boston", "25017": "Middlesex",
    // Colorado
    "08031": "Denver", "08005": "Arapahoe", "08059": "Jefferson",
    // Washington state
    "53033": "Seattle", "53053": "Pierce", "53061": "Snohomish",
    // Arizona
    "04013": "Phoenix", "04019": "Tucson",
    // Michigan
    "26163": "Detroit", "26125": "Oakland",
    // Minnesota
    "27053": "Minneapolis", "27123": "Saint Paul",
    // Oregon
    "41051": "Portland", "41005": "Clackamas",
    // Tennessee
    "47037": "Nashville", "47157": "Memphis",
    // Missouri
    "29510": "St Louis", "29095": "Kansas City",
    // North Carolina
    "37119": "Charlotte", "37183": "Raleigh",
    // Maryland
    "24510": "Baltimore", "24031": "Montgomery",
    // Nevada
    "32003": "Las Vegas",
    // Ohio
    "39035": "Cleveland", "39049": "Columbus", "39061": "Cincinnati",
    // Indiana
    "18097": "Indianapolis",
    // Louisiana
    "22071": "New Orleans",
    // Wisconsin
    "55079": "Milwaukee",
}

// Well-known ZIP-to-neighborhood mapping overrides (curated, more specific than national data)
const ZIP_OVERRIDES: Record<string, string> = {
    // Houston
    "77002": "Downtown", "77003": "East End", "77004": "Third Ward", "77005": "West University",
    "77006": "Montrose", "77007": "Heights", "77008": "Shady Acres", "77009": "Northside Heights",
    "77010": "Downtown", "77011": "East End", "77012": "Gulfgate", "77013": "East Houston",
    "77014": "Greenspoint", "77015": "Channelview", "77016": "Kashmere", "77017": "South Houston",
    "77018": "Oak Forest", "77019": "River Oaks", "77020": "Denver Harbor",
    "77021": "MacGregor", "77022": "Near Northside", "77023": "Eastwood",
    "77024": "Memorial", "77025": "Braeswood", "77026": "Fifth Ward", "77027": "Galleria",
    "77028": "Trinity Gardens", "77029": "East Houston", "77030": "Medical Center",
    "77031": "Sharpstown", "77032": "Greenspoint North", "77033": "Sunnyside",
    "77034": "Ellington", "77035": "Meyerland", "77036": "Gulfton",
    "77037": "Aldine", "77038": "Northwest Houston", "77039": "Acres Homes North", "77040": "Fairfield",
    "77041": "Jersey Village", "77042": "Westchase", "77043": "Spring Branch",
    "77044": "Lake Houston", "77045": "Hiram Clarke", "77046": "Upper Kirby",
    "77047": "South Houston", "77048": "South Houston", "77049": "Lake Houston East",
    "77050": "Greenspoint East", "77051": "South Park", "77053": "Fort Bend",
    "77054": "Medical Center South", "77055": "Spring Branch", "77056": "Galleria",
    "77057": "Galleria West", "77058": "Clear Lake", "77059": "Clear Lake",
    "77060": "Aldine", "77061": "Gulfgate South", "77062": "Clear Lake",
    "77063": "Westpark", "77064": "Cypress", "77065": "Cypress",
    "77066": "Champions", "77067": "Greenspoint", "77068": "Champions",
    "77069": "Champions", "77070": "Willowbrook", "77071": "Fondren Southwest",
    "77072": "Alief", "77073": "Imperial Valley", "77074": "Sharpstown",
    "77075": "South Belt", "77076": "Airline", "77077": "Briar Forest",
    "77078": "Settegast", "77079": "Energy Corridor", "77080": "Spring Branch West",
    "77081": "Southwest", "77082": "Westchase", "77083": "Alief West",
    "77084": "Bear Creek", "77085": "Almeda", "77086": "Aldine",
    "77087": "Hobby Area", "77088": "Acres Homes", "77089": "Southeast Houston",
    "77090": "Champions East", "77091": "Acres Homes", "77092": "Hollister",
    "77093": "Aldine East", "77094": "Memorial West", "77095": "Copperfield",
    "77096": "Fondren", "77098": "Upper Kirby", "77099": "Westwood",
    "77301": "Conroe", "77302": "Conroe East", "77303": "Conroe North", "77304": "Conroe West",
    "77316": "Montgomery", "77318": "Willis", "77338": "Humble",
    "77339": "Kingwood", "77345": "Kingwood", "77346": "Atascocita",
    "77354": "Magnolia", "77355": "Magnolia", "77357": "New Caney",
    "77365": "Porter", "77373": "Spring", "77375": "Tomball", "77377": "Tomball",
    "77379": "Klein", "77380": "The Woodlands", "77381": "The Woodlands",
    "77382": "The Woodlands", "77384": "The Woodlands", "77385": "The Woodlands",
    "77386": "Spring", "77388": "Spring", "77389": "The Woodlands",
    "77396": "Atascocita", "77401": "Bellaire", "77406": "Richmond",
    "77429": "Cypress", "77433": "Cypress West",
    "77449": "Katy", "77450": "Katy", "77478": "Sugar Land", "77479": "Sugar Land",
    "77489": "Missouri City", "77493": "Katy", "77494": "Cinco Ranch", "77498": "Sugar Land",
    "77504": "Pasadena", "77520": "Baytown", "77546": "Friendswood",
    "77573": "League City", "77586": "Seabrook", "77598": "Webster",
    "77336": "Huffman",
    "77502": "Pasadena",
    "77503": "Pasadena",
    "77505": "Pasadena",
    "77506": "Pasadena",
    "77521": "Baytown",
    "77523": "Crosby",
    "77530": "Channelview",
    "77532": "Crosby",
    "77536": "Deer Park",
    "77547": "Galena Park",
    "77562": "Highlands",
    "77571": "La Porte",
    "77581": "Pearland",
    "77584": "Pearland",
    "77587": "South Houston City",
    // Dallas
    "75201": "Downtown", "75204": "Knox-Henderson", "75205": "Highland Park",
    "75206": "Lower Greenville", "75208": "Bishop Arts", "75209": "Love Field",
    "75214": "Lakewood", "75219": "Oak Lawn", "75225": "University Park",
    "75226": "Deep Ellum", "75228": "Casa Linda",
    "75202": "Downtown", "75203": "Cedars", "75207": "Design District",
    "75210": "South Dallas", "75211": "Oak Cliff", "75212": "West Dallas",
    "75215": "South Dallas", "75216": "South Oak Cliff", "75217": "Pleasant Grove",
    "75218": "White Rock", "75220": "Brookhaven", "75223": "East Dallas",
    "75224": "Kessler", "75227": "Pleasant Grove", "75229": "North Dallas",
    "75230": "Preston Hollow", "75231": "Lake Highlands", "75232": "Mountain Creek",
    "75233": "Red Bird", "75234": "Farmers Branch", "75235": "Love Field",
    "75236": "Duncanville", "75237": "DeSoto", "75238": "Lake Highlands",
    "75240": "Far North Dallas", "75241": "South Dallas", "75243": "Lake Highlands",
    "75244": "Addison", "75246": "Uptown", "75247": "Stemmons Corridor",
    "75248": "Far North Dallas", "75249": "Cedar Hill", "75251": "Galleria",
    "75252": "Far North Dallas", "75253": "Seagoville", "75254": "North Dallas",
    // San Antonio
    "78201": "Deco District", "78202": "East Side", "78203": "Denver Heights",
    "78204": "Southtown", "78205": "Downtown", "78206": "Government Hill",
    "78207": "Westside", "78208": "Dignowity Hill", "78209": "Alamo Heights",
    "78210": "Southside", "78211": "Harlandale", "78212": "Monte Vista",
    "78213": "Dellview", "78214": "Palm Heights", "78215": "Dignowity Hill",
    "78216": "Airport Area", "78217": "Northeast", "78218": "Terrell Hills",
    "78219": "East Terrell Hills", "78220": "Kirby", "78221": "South San Antonio",
    "78222": "Southeast", "78223": "Southeast", "78224": "Von Ormy",
    "78225": "Avenida Guadalupe", "78226": "Lackland", "78227": "Westwood Village",
    "78228": "Thunderbird Hills", "78229": "Medical Center", "78230": "Oak Hills",
    "78231": "Leon Valley", "78232": "Stone Oak South", "78233": "Windcrest",
    "78234": "Fort Sam Houston", "78237": "Edgewood", "78238": "Westover Hills",
    "78239": "Northeast Crossing", "78240": "Leon Creek", "78242": "Kelly",
    "78244": "East Side", "78245": "West San Antonio", "78247": "Stone Oak East",
    "78248": "Stone Oak", "78249": "Northwest Crossing", "78250": "Culebra",
    "78251": "Sea World", "78252": "West San Antonio", "78253": "Helotes Area",
    "78254": "Alamo Ranch", "78255": "The Dominion", "78256": "The Dominion",
    "78257": "TPC Parkway", "78258": "Stone Oak North", "78259": "Stone Oak",
    "78260": "Stone Oak Far North", "78261": "Johnson Ranch",
    // Austin
    "78701": "Downtown", "78702": "East Austin", "78703": "Tarrytown",
    "78704": "Travis Heights", "78705": "UT Campus", "78717": "Brushy Creek",
    "78719": "Del Valle", "78721": "Govalle", "78722": "Cherrywood",
    "78723": "Mueller", "78724": "East Austin", "78725": "Montopolis",
    "78726": "Canyon Creek", "78727": "Scofield", "78728": "Wells Branch",
    "78729": "Milwood", "78730": "River Place", "78731": "Northwest Hills",
    "78732": "Steiner Ranch", "78733": "Barton Creek", "78734": "Lakeway",
    "78735": "Circle C", "78736": "Oak Hill", "78737": "Shady Hollow",
    "78738": "Bee Cave", "78739": "South MoPac", "78741": "Riverside",
    "78744": "South Austin", "78745": "South Manchaca", "78746": "Westlake",
    "78747": "South Austin", "78748": "Slaughter Creek", "78749": "Southwest Austin",
    "78750": "Anderson Mill", "78751": "Hyde Park", "78752": "Windsor Park",
    "78753": "North Austin", "78754": "North Austin", "78756": "Brentwood",
    "78757": "Allandale", "78758": "North Lamar", "78759": "Great Hills",
    // Birmingham AL
    "35203": "Downtown", "35204": "West End", "35205": "Southside",
    "35206": "East Lake", "35207": "North Birmingham",
    "35208": "Ensley", "35209": "Homewood", "35210": "Crestwood",
    "35211": "West End", "35212": "Woodlawn",
    "35213": "Mountain Brook", "35214": "Center Point",
    "35215": "Roebuck", "35216": "Vestavia Hills",
    "35217": "Tarrant", "35218": "Ensley",
    "35221": "Midfield", "35222": "Avondale",
    "35223": "Mountain Brook", "35224": "Wylam",
    "35226": "Bluff Park", "35228": "Midfield",
    "35233": "UAB", "35234": "Norwood",
    "35235": "Huffman", "35242": "Greystone",
    "35243": "Brook Highland", "35244": "Hoover",
    // Atlanta GA
    "30301": "Downtown", "30303": "Downtown", "30305": "Buckhead",
    "30306": "Virginia Highland", "30307": "Inman Park",
    "30308": "Midtown", "30309": "Ansley Park",
    "30310": "West End", "30311": "Cascade",
    "30312": "Grant Park", "30313": "Castleberry Hill",
    "30314": "Vine City", "30315": "Capitol Gateway",
    "30316": "East Atlanta", "30317": "Kirkwood",
    "30318": "Collier Hills", "30319": "North Buckhead",
    "30322": "Druid Hills", "30324": "Lindbergh",
    "30326": "Buckhead South", "30327": "Tuxedo Park",
    "30329": "North Druid Hills", "30331": "Campbellton",
    "30332": "Georgia Tech", "30334": "Capitol",
    "30336": "Ben Hill", "30337": "East Point",
    "30339": "Vinings", "30340": "Chamblee",
    "30341": "Chamblee South", "30342": "Sandy Springs South",
    "30344": "College Park", "30345": "Northlake",
    "30349": "South Fulton", "30354": "Hapeville",
    // Chicago IL
    "60601": "The Loop", "60602": "The Loop", "60603": "The Loop",
    "60604": "The Loop", "60605": "South Loop", "60606": "West Loop",
    "60607": "West Loop", "60608": "Pilsen", "60609": "Back of the Yards",
    "60610": "Old Town", "60611": "Streeterville", "60612": "Near West Side",
    "60613": "Lakeview", "60614": "Lincoln Park", "60615": "Hyde Park",
    "60616": "Chinatown", "60617": "South Chicago", "60618": "North Center",
    "60619": "Chatham", "60620": "Auburn Gresham", "60621": "Englewood",
    "60622": "Wicker Park", "60623": "Little Village", "60624": "Garfield Park",
    "60625": "Lincoln Square", "60626": "Rogers Park",
    "60628": "Roseland", "60629": "Chicago Lawn",
    "60630": "Jefferson Park", "60631": "Edison Park",
    "60632": "Brighton Park", "60634": "Portage Park",
    "60636": "West Englewood", "60637": "Woodlawn",
    "60638": "Clearing", "60639": "Belmont Cragin",
    "60640": "Uptown", "60641": "Old Irving Park",
    "60642": "Bucktown", "60643": "Beverly",
    "60644": "Austin", "60645": "West Ridge",
    "60646": "Sauganash", "60647": "Logan Square",
    "60649": "South Shore", "60651": "Humboldt Park",
    "60652": "West Beverly", "60653": "Bronzeville",
    "60654": "River North", "60655": "Mount Greenwood",
    "60656": "Norwood Park", "60657": "Lakeview East",
    "60659": "Peterson Park", "60660": "Edgewater",
    "60661": "Fulton Market",
    // New York City
    "10001": "Chelsea", "10002": "Lower East Side", "10003": "East Village",
    "10004": "Financial District", "10005": "Financial District",
    "10006": "Financial District", "10007": "Tribeca",
    "10009": "East Village", "10010": "Gramercy",
    "10011": "West Village", "10012": "SoHo", "10013": "Tribeca",
    "10014": "West Village", "10016": "Murray Hill", "10017": "Midtown East",
    "10018": "Garment District", "10019": "Hells Kitchen",
    "10020": "Rockefeller Center", "10021": "Upper East Side",
    "10022": "Midtown East", "10023": "Upper West Side",
    "10024": "Upper West Side", "10025": "Morningside Heights",
    "10026": "Harlem", "10027": "Harlem", "10028": "Upper East Side",
    "10029": "East Harlem", "10030": "Central Harlem",
    "10031": "Hamilton Heights", "10032": "Washington Heights",
    "10033": "Washington Heights", "10034": "Inwood",
    "10035": "East Harlem", "10036": "Times Square",
    "10037": "Central Harlem", "10038": "Seaport",
    "10039": "Sugar Hill", "10040": "Fort George",
    "10044": "Roosevelt Island", "10065": "Lenox Hill",
    "10069": "Lincoln Center", "10075": "Upper East Side",
    "10128": "Yorkville", "10280": "Battery Park City",
    "10282": "Battery Park City",
    // Brooklyn
    "11201": "Brooklyn Heights", "11203": "East Flatbush",
    "11204": "Bensonhurst", "11205": "Fort Greene",
    "11206": "Williamsburg", "11207": "East New York",
    "11208": "Cypress Hills", "11209": "Bay Ridge",
    "11210": "Flatbush", "11211": "Williamsburg",
    "11212": "Brownsville", "11213": "Crown Heights",
    "11214": "Bath Beach", "11215": "Park Slope",
    "11216": "Bedford-Stuyvesant", "11217": "Boerum Hill",
    "11218": "Kensington", "11219": "Borough Park",
    "11220": "Sunset Park", "11221": "Bushwick",
    "11222": "Greenpoint", "11223": "Gravesend",
    "11224": "Coney Island", "11225": "Prospect Lefferts",
    "11226": "Flatbush", "11228": "Dyker Heights",
    "11229": "Sheepshead Bay", "11230": "Midwood",
    "11231": "Carroll Gardens", "11232": "Industry City",
    "11233": "Ocean Hill", "11234": "Canarsie",
    "11235": "Brighton Beach", "11236": "Canarsie",
    "11237": "Bushwick", "11238": "Prospect Heights",
    "11239": "Starrett City",
    // Queens
    "11101": "Long Island City", "11102": "Astoria",
    "11103": "Astoria", "11104": "Sunnyside",
    "11105": "Ditmars", "11106": "Astoria",
    "11354": "Downtown Flushing", "11355": "Flushing",
    "11356": "College Point", "11357": "Whitestone",
    "11358": "Auburndale", "11360": "Bayside",
    "11361": "Bayside", "11362": "Little Neck",
    "11363": "Douglaston", "11364": "Oakland Gardens",
    "11365": "Fresh Meadows", "11366": "Fresh Meadows",
    "11367": "Kew Gardens Hills", "11368": "Corona",
    "11369": "East Elmhurst", "11370": "Jackson Heights",
    "11372": "Jackson Heights", "11373": "Elmhurst",
    "11374": "Rego Park", "11375": "Forest Hills",
    "11377": "Woodside", "11378": "Maspeth",
    "11379": "Middle Village", "11385": "Ridgewood",
    "11411": "Cambria Heights", "11412": "St. Albans",
    "11413": "Springfield Gardens", "11414": "Howard Beach",
    "11415": "Kew Gardens", "11416": "Ozone Park",
    "11417": "Ozone Park", "11418": "Richmond Hill",
    "11419": "South Richmond Hill", "11420": "South Ozone Park",
    "11421": "Woodhaven", "11422": "Rosedale",
    "11423": "Hollis", "11427": "Queens Village",
    "11429": "Queens Village", "11432": "Jamaica",
    "11433": "Jamaica", "11434": "JFK Area",
    "11435": "Jamaica", "11436": "South Jamaica",
    // Bronx
    "10451": "South Bronx", "10452": "Highbridge",
    "10453": "University Heights", "10454": "Mott Haven",
    "10455": "Longwood", "10456": "Morrisania",
    "10457": "Tremont", "10458": "Belmont",
    "10459": "Hunts Point", "10460": "West Farms",
    "10461": "Morris Park", "10462": "Parkchester",
    "10463": "Kingsbridge", "10464": "City Island",
    "10465": "Throgs Neck", "10466": "Williamsbridge",
    "10467": "Norwood", "10468": "Fordham",
    "10469": "Eastchester", "10470": "Wakefield",
    "10471": "Riverdale", "10472": "Soundview",
    "10473": "Castle Hill", "10474": "Hunts Point",
    "10475": "Co-Op City",
    // Staten Island
    "10301": "St. George", "10302": "Port Richmond",
    "10303": "Mariners Harbor", "10304": "Stapleton",
    "10305": "Rosebank", "10306": "New Dorp",
    "10307": "Tottenville", "10308": "Great Kills",
    "10309": "Charleston", "10310": "West Brighton",
    "10312": "Annadale", "10314": "Bulls Head",
    // Miami / South Florida
    "33101": "Downtown Miami", "33125": "Little Havana",
    "33126": "Flagami", "33127": "Wynwood",
    "33128": "Downtown Miami", "33129": "Brickell",
    "33130": "Brickell", "33131": "Brickell",
    "33132": "Edgewater", "33133": "Coconut Grove",
    "33134": "Coral Gables", "33135": "Little Havana",
    "33136": "Overtown", "33137": "Design District",
    "33138": "Upper East Side", "33139": "South Beach",
    "33140": "Mid Beach", "33141": "North Beach",
    "33142": "Allapattah", "33144": "Westchester",
    "33145": "Shenandoah", "33146": "Coral Gables",
    "33147": "Liberty City", "33149": "Key Biscayne",
    "33150": "Little Haiti", "33154": "Bal Harbour",
    "33155": "Westchester South", "33156": "Pinecrest",
    "33157": "Palmetto Bay", "33158": "Palmetto Bay",
    "33160": "Sunny Isles", "33161": "North Miami",
    "33162": "North Miami Beach", "33165": "Westchester West",
    "33166": "Medley", "33167": "North Miami",
    "33168": "Miami Gardens", "33169": "Miami Gardens",
    "33170": "Homestead", "33172": "Doral",
    "33173": "Kendall", "33174": "Tamiami",
    "33175": "Kendall West", "33176": "Kendall South",
    "33177": "South Dade", "33178": "Doral West",
    "33179": "Ojus", "33180": "Aventura",
    "33181": "North Miami", "33182": "Sweetwater",
    "33183": "Kendall", "33184": "Sweetwater",
    "33185": "Kendall West", "33186": "Kendall South",
    "33189": "Cutler Bay", "33190": "Cutler Bay",
    "33193": "Kendale Lakes", "33196": "The Hammocks",
    // Philadelphia PA
    "19102": "Center City", "19103": "Rittenhouse",
    "19104": "University City", "19106": "Old City",
    "19107": "Washington Square", "19109": "Jewelers Row",
    "19111": "Fox Chase", "19114": "Torresdale",
    "19115": "Far Northeast", "19116": "Somerton",
    "19118": "Chestnut Hill", "19119": "Mount Airy",
    "19120": "Olney", "19121": "North Philadelphia",
    "19122": "Northern Liberties", "19123": "Spring Garden",
    "19124": "Frankford", "19125": "Fishtown",
    "19126": "Germantown", "19127": "Manayunk",
    "19128": "Roxborough", "19129": "East Falls",
    "19130": "Fairmount", "19131": "Overbrook",
    "19132": "Strawberry Mansion", "19133": "Fairhill",
    "19134": "Port Richmond", "19135": "Mayfair",
    "19136": "Holmesburg", "19137": "Bridesburg",
    "19138": "Germantown", "19139": "West Philadelphia",
    "19140": "Logan", "19141": "Fern Rock",
    "19142": "Southwest", "19143": "Kingsessing",
    "19144": "West Germantown", "19145": "South Philadelphia",
    "19146": "Graduate Hospital", "19147": "South Street",
    "19148": "South Philadelphia", "19149": "Rhawnhurst",
    "19150": "Cedarbrook", "19151": "Overbrook Park",
    "19152": "Bustleton", "19153": "Eastwick",
    "19154": "Northeast",
    // Phoenix AZ
    "85003": "Downtown", "85004": "Downtown", "85006": "Encanto",
    "85007": "South Central", "85008": "Papago",
    "85009": "Maryvale East", "85012": "Camelback East",
    "85013": "Alhambra", "85014": "Camelback East",
    "85015": "Alhambra", "85016": "Arcadia",
    "85017": "Maryvale", "85018": "Arcadia",
    "85019": "Maryvale", "85020": "North Mountain",
    "85021": "Sunnyslope", "85022": "Deer Valley South",
    "85023": "Moon Valley", "85024": "Desert Ridge",
    "85027": "Deer Valley", "85028": "Paradise Valley",
    "85029": "Deer Valley", "85031": "Maryvale",
    "85032": "Paradise Valley East", "85033": "Maryvale West",
    "85034": "Sky Harbor", "85035": "Estrella",
    "85037": "Estrella West", "85040": "Ahwatukee East",
    "85041": "South Mountain", "85042": "South Mountain East",
    "85043": "Laveen", "85044": "Ahwatukee",
    "85045": "Ahwatukee South", "85048": "Ahwatukee",
    "85050": "Desert Ridge", "85051": "Sunnyslope West",
    "85053": "Deer Valley", "85054": "Scottsdale South",
    // Denver CO
    "80202": "LoDo", "80203": "Capitol Hill",
    "80204": "Sun Valley", "80205": "Five Points",
    "80206": "Cherry Creek", "80207": "Stapleton",
    "80209": "Washington Park", "80210": "University Park",
    "80211": "Highlands", "80212": "Berkeley",
    "80214": "Edgewater", "80216": "Elyria-Swansea",
    "80218": "Cheesman Park", "80219": "Westwood",
    "80220": "Lowry", "80221": "Federal Heights",
    "80222": "Glendale", "80223": "Ruby Hill",
    "80224": "Hampden", "80227": "Bear Valley",
    "80230": "Lowry Field", "80231": "Hampden South",
    "80234": "Northglenn", "80236": "Fort Logan",
    "80237": "Goldsmith", "80238": "Stapleton North",
    "80239": "Montbello", "80246": "Cherry Creek South",
    "80247": "Hampden", "80249": "Green Valley Ranch",
    // Seattle WA
    "98101": "Downtown", "98102": "Capitol Hill",
    "98103": "Fremont", "98104": "Pioneer Square",
    "98105": "University District", "98106": "White Center",
    "98107": "Ballard", "98108": "Georgetown",
    "98109": "South Lake Union", "98112": "Madison Park",
    "98115": "Wedgwood", "98116": "West Seattle",
    "98117": "Crown Hill", "98118": "Columbia City",
    "98119": "Queen Anne", "98121": "Belltown",
    "98122": "Central District", "98125": "Lake City",
    "98126": "Admiral District", "98133": "Shoreline",
    "98136": "Fauntleroy", "98144": "Beacon Hill",
    "98146": "Boulevard Park", "98155": "Lake Forest Park",
    "98168": "Tukwila", "98177": "Richmond Beach",
    "98178": "Bryn Mawr", "98188": "SeaTac",
    "98198": "Des Moines", "98199": "Magnolia",
    // Portland OR
    "97201": "Downtown", "97202": "Sellwood",
    "97203": "St. Johns", "97204": "Downtown",
    "97205": "Nob Hill", "97206": "Foster-Powell",
    "97209": "Pearl District", "97210": "Forest Park",
    "97211": "Alberta", "97212": "Beaumont",
    "97213": "Rose City Park", "97214": "Hawthorne",
    "97215": "Mount Tabor", "97216": "Gateway",
    "97217": "Kenton", "97218": "Cully",
    "97219": "Multnomah Village", "97220": "Parkrose",
    "97221": "Hillsdale", "97222": "Milwaukie",
    "97227": "Eliot", "97230": "Wilkes",
    "97231": "Linnton", "97232": "Lloyd District",
    "97233": "Rockwood", "97236": "Lents",
    // Nashville TN
    "37201": "Downtown", "37203": "Midtown",
    "37204": "Berry Hill", "37205": "Belle Meade",
    "37206": "East Nashville", "37207": "North Nashville",
    "37208": "Germantown", "37209": "The Nations",
    "37210": "South Nashville", "37211": "Antioch",
    "37212": "Music Row", "37213": "Five Points",
    "37214": "Donelson", "37215": "Green Hills",
    "37216": "Inglewood", "37217": "Hermitage",
    "37218": "Bordeaux", "37219": "Downtown",
    "37220": "Oak Hill", "37221": "Bellevue",
    // Other notable metros -- Florida
    "32801": "Downtown Orlando", "32803": "Colonialtown",
    "32804": "College Park", "32806": "Delaney Park",
    "32807": "Union Park", "32808": "Pine Hills",
    "32809": "South Orlando", "32810": "Eatonville",
    "32811": "Metro West", "32812": "Conway",
    "32814": "Baldwin Park", "32819": "Dr. Phillips",
    "32821": "International Drive", "32822": "Azalea Park",
    "32824": "Meadow Woods", "32825": "Waterford Lakes",
    "32826": "UCF", "32827": "Lake Nona",
    "32828": "Avalon Park", "32829": "East Orlando",
    "32832": "Lake Nona South", "32835": "Metro West",
    "33602": "Downtown Tampa", "33603": "Seminole Heights",
    "33604": "Seminole Heights", "33605": "Ybor City",
    "33606": "Hyde Park", "33607": "West Tampa",
    "33609": "Palma Ceia", "33610": "East Tampa",
    "33611": "Bayshore", "33612": "USF Area",
    "33613": "USF North", "33614": "Town N Country",
    "33615": "Town N Country", "33616": "Ballast Point",
    "33617": "Temple Terrace", "33618": "Carrollwood",
    "33619": "Progress Village", "33624": "Citrus Park",
    "33625": "Citrus Park West", "33626": "Westchase",
    "33629": "Palma Ceia", "33634": "Egypt Lake",
    "33647": "New Tampa",
}

// Merge: use curated Houston names where available, otherwise national
export const ZIP_NAMES: Record<string, string> = { ...ZIP_CITY_NATIONAL, ...ZIP_OVERRIDES }

export interface GeoInfo {
    tractGeoid: string
    stateFips: string
    stateName: string
    stateAbbr: string
    countyFips: string     // 5-digit state+county
    city: string
    neighborhoodName: string  // Named area or "Tract XXXXXX"
    zcta5: string | null
    stateSlug: string      // "tx"
    citySlug: string       // "houston"
    neighborhoodSlug: string // "heights" or "tract-240100"
}

/**
 * Parse a Census Tract GeoID into geographic components.
 * GeoID anatomy: SS (state) + CCC (county) + TTTTTT (tract)
 */
export function parseTractGeoid(geoid: string): GeoInfo {
    const stateFips = geoid.substring(0, 2)
    const countyFips = geoid.substring(0, 5)
    const tractSuffix = geoid.substring(5)

    const stateInfo = STATE_FIPS[stateFips] || { name: "Unknown", abbr: "XX" }
    const city = COUNTY_CITY[countyFips] || COUNTY_NAMES[countyFips] || `County ${countyFips}`

    // Default: use tract number as neighborhood name (will be enriched later)
    const neighborhoodName = `Tract ${tractSuffix}`

    return {
        tractGeoid: geoid,
        stateFips,
        stateName: stateInfo.name,
        stateAbbr: stateInfo.abbr,
        countyFips,
        city,
        neighborhoodName,
        zcta5: null,
        stateSlug: stateInfo.abbr.toLowerCase(),
        citySlug: slugify(city),
        neighborhoodSlug: `tract-${tractSuffix}`,
    }
}
// Census tract → ZCTA crosswalk (85K+ mappings from Census Bureau)
import tractZctaRaw from "./tract-zcta-crosswalk.json"
const TRACT_ZCTA: Record<string, string> = tractZctaRaw as any

// Persistent tract name cache (populated by build_tract_name_cache.py for
// tracts that have no ZCTA crosswalk match)
import tractNameCacheRaw from "./tract-name-cache.json"
const TRACT_NAME_CACHE: Record<string, string> = tractNameCacheRaw as any

// Spatial tract labels (populated by build_tract_labels.py — TIGER Places, Cousubs, GNIS)
// Compact format: s=label_short, t=anchor_type, c=confidence first char (h/m/l)
import tractLabelsRaw from "./tract_labels.json"
const TRACT_LABELS: Record<string, {
    s: string; t: string; c: string;
}> = tractLabelsRaw as any

/**
 * Batch-enrich tract GeoIDs with human-readable names.
 * Phase 0: Spatial tract labels (TIGER Places / Cousubs / GNIS — highest quality).
 * Phase 1: Census tract-to-ZCTA crosswalk (static, instant).
 * Phase 2: parcel_ladder_v1 DB fallback.
 * Phase 3: Static tract-name-cache.json (populated by build script).
 * Phase 4: Supabase tract_name_cache table (persistent geocoder results).
 */
export async function batchEnrichTracts(
    tractGeoids: string[]
): Promise<Map<string, { name: string; slug: string; zcta5: string }>> {
    const result = new Map<string, { name: string; slug: string; zcta5: string }>()
    if (tractGeoids.length === 0) return result

    // Phase 0: Spatial tract labels (place/cousub/GNIS inference)
    // Cousub labels (e.g. "Northwest Harris") are very broad — if a curated
    // ZIP_OVERRIDES name exists for the tract's ZCTA, prefer that instead.
    for (const tractId of tractGeoids) {
        const label = TRACT_LABELS[tractId]
        if (label && label.s) {
            const zcta = TRACT_ZCTA[tractId] || ""
            // Check if a more specific curated ZIP name should override a cousub label
            if (label.t === "cousub" && zcta && ZIP_OVERRIDES[zcta]) {
                const curatedName = ZIP_OVERRIDES[zcta]
                result.set(tractId, {
                    name: curatedName,
                    slug: slugify(curatedName),
                    zcta5: zcta,
                })
            } else {
                result.set(tractId, {
                    name: label.s,
                    slug: slugify(label.s),
                    zcta5: zcta,
                })
            }
        }
    }

    // Phase 0.5: parcel_ladder_v1 neighborhood_name (HCAD jurisdictions, etc.)
    // This mirrors enrichWithNeighborhood() for single tracts — batch version
    const phase05Missing = tractGeoids.filter(id => !result.has(id))
    if (phase05Missing.length > 0) {
        try {
            const supabase = getSupabaseAdmin()
            const CHUNK = 50
            for (let i = 0; i < phase05Missing.length; i += CHUNK) {
                const chunk = phase05Missing.slice(i, i + CHUNK)
                const { data } = await supabase
                    .from("parcel_ladder_v1")
                    .select("tract_geoid20, neighborhood_name, zcta5")
                    .in("tract_geoid20", chunk)
                    .not("neighborhood_name", "is", null)

                if (data) {
                    for (const row of data as any[]) {
                        if (!result.has(row.tract_geoid20) && row.neighborhood_name) {
                            result.set(row.tract_geoid20, {
                                name: row.neighborhood_name,
                                slug: slugify(row.neighborhood_name),
                                zcta5: row.zcta5 || "",
                            })
                        }
                    }
                }
            }
        } catch { /* non-fatal */ }
    }

    // Phase 1: Census crosswalk (instant, covers ~85K tracts) — fills gaps
    const missing: string[] = []
    for (const tractId of tractGeoids) {
        if (result.has(tractId)) continue
        const zcta = TRACT_ZCTA[tractId]
        if (zcta) {
            const placeName = ZIP_NAMES[zcta]
            const name = placeName || `ZIP ${zcta}`
            result.set(tractId, { name, slug: slugify(name), zcta5: zcta })
        } else {
            missing.push(tractId)
        }
    }

    // Phase 2: parcel_ladder_v1 fallback for any remaining tracts
    if (missing.length > 0) {
        try {
            const supabase = getSupabaseAdmin()
            const CHUNK = 10
            for (let i = 0; i < missing.length; i += CHUNK) {
                const chunk = missing.slice(i, i + CHUNK)
                const { data } = await supabase
                    .from("parcel_ladder_v1")
                    .select("tract_geoid20, zcta5")
                    .in("tract_geoid20", chunk)
                    .not("zcta5", "is", null)
                    .limit(2000)

                if (data) {
                    for (const row of data as any[]) {
                        if (!result.has(row.tract_geoid20) && row.zcta5) {
                            const placeName = ZIP_NAMES[row.zcta5]
                            const name = placeName || `ZIP ${row.zcta5}`
                            result.set(row.tract_geoid20, { name, slug: slugify(name), zcta5: row.zcta5 })
                        }
                    }
                }
            }
        } catch {
            // Non-fatal
        }
    }

    // Phase 3: Static tract-name-cache.json (populated by build_tract_name_cache.py)
    const stillMissing: string[] = []
    for (const tractId of missing) {
        if (result.has(tractId)) continue
        const cached = TRACT_NAME_CACHE[tractId]
        if (cached) {
            result.set(tractId, { name: cached, slug: slugify(cached), zcta5: "" })
        } else {
            stillMissing.push(tractId)
        }
    }

    // Phase 4: Supabase tract_name_cache table (persistent geocoder results)
    if (stillMissing.length > 0) {
        try {
            const supabase = getSupabaseAdmin()
            const CHUNK = 50
            for (let i = 0; i < stillMissing.length; i += CHUNK) {
                const chunk = stillMissing.slice(i, i + CHUNK)
                const { data } = await supabase
                    .from("tract_name_cache")
                    .select("tract_geoid20, display_name")
                    .in("tract_geoid20", chunk)

                if (data) {
                    for (const row of data as any[]) {
                        if (!result.has(row.tract_geoid20) && row.display_name) {
                            result.set(row.tract_geoid20, {
                                name: row.display_name,
                                slug: slugify(row.display_name),
                                zcta5: "",
                            })
                        }
                    }
                }
            }
        } catch {
            // Non-fatal — table may not exist yet
        }
    }

    // Phase 5: County name fallback for non-standard tracts
    // (revised tracts with R/S suffixes, institutional tracts, etc.)
    for (const tractId of tractGeoids) {
        if (result.has(tractId)) continue
        const countyFips = tractId.substring(0, 5) // e.g. "01073"
        const countyName = COUNTY_NAMES[countyFips] || COUNTY_CITY[countyFips]
        if (countyName) {
            result.set(tractId, { name: countyName, slug: slugify(countyName), zcta5: "" })
        }
    }

    console.log(`[batchEnrich] enriched ${result.size}/${tractGeoids.length} tracts (${missing.length} missed ZCTA, ${stillMissing.length} missed all caches)`)
    return result
}

/**
 * Enrich single GeoInfo with neighborhood name.
 * Uses the same priority chain as batchEnrichTracts for consistency:
 *   1. TRACT_LABELS (with cousub downgrade to curated ZIP names)
 *   2. parcel_ladder_v1 neighborhood_name
 *   3. ZCTA5 → ZIP_NAMES lookup
 */
export async function enrichWithNeighborhood(geo: GeoInfo): Promise<GeoInfo> {
    // Try 0: Spatial tract labels (same logic as batchEnrichTracts Phase 0)
    const label = TRACT_LABELS[geo.tractGeoid]
    if (label && label.s) {
        const zcta = TRACT_ZCTA[geo.tractGeoid] || geo.zcta5 || null
        // Cousub labels are too broad — prefer curated ZIP name if available
        if (label.t === "cousub" && zcta && ZIP_OVERRIDES[zcta]) {
            const curatedName = ZIP_OVERRIDES[zcta]
            return {
                ...geo,
                neighborhoodName: curatedName,
                neighborhoodSlug: slugify(curatedName),
                zcta5: zcta,
            }
        }
        return {
            ...geo,
            neighborhoodName: label.s,
            neighborhoodSlug: slugify(label.s),
            zcta5: zcta,
        }
    }

    // Try 1: parcel_ladder_v1 neighborhood_name (HCAD jurisdictions)
    try {
        const supabase = getSupabaseAdmin()
        const { data } = await supabase
            .from("parcel_ladder_v1")
            .select("neighborhood_name, zcta5")
            .eq("tract_geoid20", geo.tractGeoid)
            .not("neighborhood_name", "is", null)
            .limit(1)
            .maybeSingle()

        if (data?.neighborhood_name) {
            return {
                ...geo,
                neighborhoodName: data.neighborhood_name,
                neighborhoodSlug: slugify(data.neighborhood_name),
                zcta5: data.zcta5 || geo.zcta5,
            }
        }
    } catch { /* non-fatal */ }

    // Try 2: ZCTA5 → ZIP_NAMES lookup
    try {
        const supabase = getSupabaseAdmin()
        const { data } = await supabase
            .from("parcel_ladder_v1")
            .select("zcta5")
            .eq("tract_geoid20", geo.tractGeoid)
            .not("zcta5", "is", null)
            .limit(1)
            .maybeSingle()

        if (data?.zcta5) {
            const placeName = ZIP_NAMES[data.zcta5]
            if (placeName) {
                return {
                    ...geo,
                    neighborhoodName: placeName,
                    neighborhoodSlug: slugify(placeName),
                    zcta5: data.zcta5,
                }
            } else {
                return {
                    ...geo,
                    neighborhoodName: `ZIP ${data.zcta5}`,
                    neighborhoodSlug: `zip-${data.zcta5}`,
                    zcta5: data.zcta5,
                }
            }
        }
    } catch { /* non-fatal */ }

    // Try 2.5: Static TRACT_ZCTA crosswalk (instant, no DB hit)
    const staticZcta = TRACT_ZCTA[geo.tractGeoid]
    if (staticZcta) {
        const placeName = ZIP_NAMES[staticZcta]
        if (placeName) {
            return {
                ...geo,
                neighborhoodName: placeName,
                neighborhoodSlug: slugify(placeName),
                zcta5: staticZcta,
            }
        }
    }

    // Try 3: County name fallback (matches batchEnrichTracts Phase 5)
    const countyFips = geo.tractGeoid.substring(0, 5)
    const countyName = COUNTY_NAMES[countyFips] || COUNTY_CITY[countyFips]
    if (countyName) {
        return {
            ...geo,
            neighborhoodName: countyName,
            neighborhoodSlug: slugify(countyName),
        }
    }

    return geo
}

/**
 * Look up a tract GeoID from URL slug components.
 * For "tract-*" slugs, does a direct lookup.
 * For human-readable slugs, rebuilds the same slug→tract mapping the city hub generates.
 */
export async function resolveSlugToTract(
    stateSlug: string,
    citySlug: string,
    neighborhoodSlug: string,
    schema = "forecast_queue"
): Promise<string | null> {
    // Find state FIPS from slug
    const stateEntry = Object.entries(STATE_FIPS).find(
        ([, v]) => v.abbr.toLowerCase() === stateSlug.toLowerCase()
    )
    if (!stateEntry) return null
    const stateFips = stateEntry[0]

    // Find county FIPS from city slug — check both sparse COUNTY_CITY and full COUNTY_NAMES
    const countyEntry = Object.entries(COUNTY_CITY).find(
        ([k, v]) => k.startsWith(stateFips) && slugify(v) === citySlug
    ) || Object.entries(COUNTY_NAMES).find(
        ([k, v]) => k.startsWith(stateFips) && slugify(v) === citySlug
    )

    // If neighborhoodSlug starts with "tract-", extract the tract suffix directly
    if (neighborhoodSlug.startsWith("tract-")) {
        const tractSuffix = neighborhoodSlug.replace("tract-", "")
        if (countyEntry) {
            // Try exact match first, then case-insensitive search for GEOIDs with letter suffixes (e.g. 70000R)
            const exactGeoid = `${countyEntry[0]}${tractSuffix}`
            const supabase = getSupabaseAdmin()
            const { data: exactMatch } = await supabase
                .schema(schema as any)
                .from("metrics_tract_forecast")
                .select("tract_geoid20")
                .eq("tract_geoid20", exactGeoid)
                .limit(1)
                .maybeSingle()
            if (exactMatch) return exactMatch.tract_geoid20

            // Case-insensitive: search for tracts matching this suffix pattern
            const { data: iMatch } = await supabase
                .schema(schema as any)
                .from("metrics_tract_forecast")
                .select("tract_geoid20")
                .ilike("tract_geoid20", `${countyEntry[0]}${tractSuffix}`)
                .limit(1)
                .maybeSingle()
            if (iMatch) return iMatch.tract_geoid20

            return exactGeoid  // fallback to constructed GEOID
        }
        // Fallback: search all tracts in state
        const supabase = getSupabaseAdmin()
        const { data } = await supabase
            .schema(schema as any)
            .from("metrics_tract_forecast")
            .select("tract_geoid20")
            .like("tract_geoid20", `${stateFips}%${tractSuffix}`)
            .limit(1)
            .single()
        return data?.tract_geoid20 || null
    }

    // Fast path: if neighborhoodSlug contains "-tr-", extract the tract suffix directly
    if (neighborhoodSlug.includes("-tr-")) {
        const parts = neighborhoodSlug.split("-tr-")
        const tractSuffix = parts[parts.length - 1]
        if (countyEntry) {
            const exactGeoid = `${countyEntry[0]}${tractSuffix}`
            const supabase = getSupabaseAdmin()
            const { data: exactMatch } = await supabase
                .schema(schema as any)
                .from("metrics_tract_forecast")
                .select("tract_geoid20")
                .eq("tract_geoid20", exactGeoid)
                .limit(1)
                .maybeSingle()
            if (exactMatch) return exactMatch.tract_geoid20
        }
    }

    // Human-readable slug: rebuild the slug→tract mapping from enrichment
    // This mirrors the exact logic in the city hub page
    if (countyEntry) {
        const mapping = await withRedisCache(`resolve_slug:${countyEntry[0]}:${schema}`, async () => {
            const supabase = getSupabaseAdmin()
            const { data: allTracts } = await supabase
                .schema(schema as any)
                .from("metrics_tract_forecast")
                .select("tract_geoid20")
                .gte("tract_geoid20", countyEntry[0])
                .lt("tract_geoid20", countyEntry[0] + "z")
                .eq("horizon_m", 12)
                .eq("series_kind", "forecast")
                .not("p50", "is", null)

            const slugToTract: [string, string][] = []
            if (!allTracts) return slugToTract

            const uniqueIds = [...new Set(allTracts.map((d: any) => d.tract_geoid20))]
            const enriched = await batchEnrichTracts(uniqueIds)

            // Build slug map with ZIP-based disambiguation (mirrors city hub page logic)
            const nameFreq = new Map<string, number>()
            const nameGroups = new Map<string, { tractId: string; name: string; zcta5: string }[]>()
            for (const tractId of uniqueIds) {
                const e = enriched.get(tractId)
                const name = e?.name || `Tract ${tractId.substring(5)}`
                const zcta5 = e?.zcta5 || ''
                nameFreq.set(name, (nameFreq.get(name) || 0) + 1)
                if (!nameGroups.has(name)) nameGroups.set(name, [])
                nameGroups.get(name)!.push({ tractId, name, zcta5 })
            }

            for (const tractId of uniqueIds) {
                const e = enriched.get(tractId)
                const name = e?.name || `Tract ${tractId.substring(5)}`
                const baseSlug = slugify(name)
                const total = nameFreq.get(name) || 1
                const zip = e?.zcta5 || ''

                if (total === 1) {
                    slugToTract.push([baseSlug, tractId])
                } else {
                    const group = nameGroups.get(name) || []
                    const zipFreq = group.filter(g => g.zcta5 === zip).length

                    if (zip && zipFreq === 1) {
                        slugToTract.push([`${baseSlug}-${zip}`, tractId])
                    } else {
                        const tractSuffix = tractId.substring(5)
                        slugToTract.push([`${baseSlug}-tr-${tractSuffix}`, tractId])
                    }
                }
            }

            return slugToTract
        })

        const slugMap = new Map<string, string>(mapping as [string, string][])
        const fromSlugMap = slugMap.get(neighborhoodSlug)
        if (fromSlugMap) return fromSlugMap

        // Fallback: if slug ends with a 5-digit ZIP (e.g. "houston-77067"),
        // try resolving via ZCTA→tract crosswalk. This handles stale slugs
        // that were generated before neighborhood names were updated.
        const zipMatch = neighborhoodSlug.match(/-(\d{5})$/)
        if (zipMatch) {
            const zip = zipMatch[1]
            // Find all tracts in this county that map to this ZCTA
            for (const [tractId, zcta] of Object.entries(TRACT_ZCTA)) {
                if (zcta === zip && tractId.startsWith(countyEntry[0])) {
                    // Verify it exists in the forecast database
                    const supabase = getSupabaseAdmin()
                    const { data: exists } = await supabase
                        .schema(schema as any)
                        .from("metrics_tract_forecast")
                        .select("tract_geoid20")
                        .eq("tract_geoid20", tractId)
                        .limit(1)
                        .maybeSingle()
                    if (exists) return exists.tract_geoid20
                }
            }
        }

        return null
    }

    return null
}

/**
 * Get all publishable tracts for a given state + city, with their geo info.
 * Returns tracts that have forecast data.
 */
export async function getTractsForCity(
    stateSlug: string,
    citySlug: string,
    schema = "forecast_queue"
): Promise<GeoInfo[]> {
    return withRedisCache(`tracts:${stateSlug}:${citySlug}:${schema}`, async () => {
        const stateEntry = Object.entries(STATE_FIPS).find(
            ([, v]) => v.abbr.toLowerCase() === stateSlug.toLowerCase()
        )
        if (!stateEntry) return []
        const stateFips = stateEntry[0]

        const countyEntry = Object.entries(COUNTY_CITY).find(
            ([k, v]) => k.startsWith(stateFips) && slugify(v) === citySlug
        ) || Object.entries(COUNTY_NAMES).find(
            ([k, v]) => k.startsWith(stateFips) && slugify(v) === citySlug
        )

        // Fallback for "county-XXXXX" slugs (non-standard FIPS that aren't in any county mapping)
        let countyFipsPrefix: string
        if (countyEntry) {
            countyFipsPrefix = countyEntry[0]
        } else {
            const match = citySlug.match(/^county-(\d{5})$/)
            if (match && match[1].startsWith(stateFips)) {
                countyFipsPrefix = match[1]
            } else {
                return []
            }
        }

        // Paginated fetch to bypass Supabase default 1000-row limit
        const supabase = getSupabaseAdmin()
        const PAGE = 1000
        const data: any[] = []
        let offset = 0
        while (true) {
            const { data: page } = await supabase
                .schema(schema as any)
                .from("metrics_tract_forecast")
                .select("tract_geoid20")
                .gte("tract_geoid20", countyFipsPrefix)
                .lt("tract_geoid20", countyFipsPrefix + "z")
                .eq("horizon_m", 12)
                .eq("series_kind", "forecast")
                .not("p50", "is", null)
                .order("tract_geoid20")
                .range(offset, offset + PAGE - 1)

            if (!page || page.length === 0) break
            data.push(...page)
            if (page.length < PAGE) break
            offset += PAGE
        }

        if (data.length === 0) return []

        // Deduplicate
        const uniqueTracts = [...new Set(data.map((d: any) => d.tract_geoid20))]
        return uniqueTracts.map(t => parseTractGeoid(t))
    })
}

/**
 * Get all cities with forecast data for a given state.
 */
export async function getCitiesForState(
    stateSlug: string,
    schema = "forecast_queue"
): Promise<{ city: string; citySlug: string; countyFips: string; tractCount: number }[]> {
    return withRedisCache(`cities:${stateSlug}:${schema}`, async () => {
        const stateEntry = Object.entries(STATE_FIPS).find(
            ([, v]) => v.abbr.toLowerCase() === stateSlug.toLowerCase()
        )
        if (!stateEntry) return []
        const stateFips = stateEntry[0]

        // Paginated fetch to bypass Supabase default 1000-row limit
        const supabase = getSupabaseAdmin()
        const PAGE = 1000
        const data: any[] = []
        let offset = 0
        while (true) {
            const { data: page } = await supabase
                .schema(schema as any)
                .from("metrics_tract_forecast")
                .select("tract_geoid20")
                .gte("tract_geoid20", stateFips)
                .lt("tract_geoid20", stateFips + "z")
                .eq("horizon_m", 12)
                .eq("series_kind", "forecast")
                .not("p50", "is", null)
                .order("tract_geoid20")
                .range(offset, offset + PAGE - 1)

            if (!page || page.length === 0) break
            data.push(...page)
            if (page.length < PAGE) break
            offset += PAGE
        }

        if (data.length === 0) return []

        // Group by county FIPS (first 5 digits), tracking unique tract GEOIDs per county
        const countyMap = new Map<string, { tracts: Set<string>; sampleTracts: string[] }>()
        for (const row of data as any[]) {
            const county = row.tract_geoid20.substring(0, 5)
            const entry = countyMap.get(county) || { tracts: new Set(), sampleTracts: [] }
            entry.tracts.add(row.tract_geoid20)
            if (entry.sampleTracts.length < 5 && !entry.sampleTracts.includes(row.tract_geoid20)) entry.sampleTracts.push(row.tract_geoid20)
            countyMap.set(county, entry)
        }

        const cities: { city: string; citySlug: string; countyFips: string; tractCount: number }[] = []

        // Collect unresolved counties for batch DB lookup
        const unresolved = new Map<string, string[]>() // countyFips -> sampleTracts
        for (const [countyFips, { tracts, sampleTracts }] of countyMap) {
            let city = COUNTY_CITY[countyFips] || COUNTY_NAMES[countyFips]

            // Try ZCTA crosswalk first
            if (!city) {
                for (const tractId of sampleTracts) {
                    const zcta = TRACT_ZCTA[tractId]
                    if (zcta) {
                        const placeName = ZIP_NAMES[zcta]
                        if (placeName) { city = placeName; break }
                    }
                }
            }

            if (city) {
                cities.push({ city, citySlug: slugify(city), countyFips, tractCount: tracts.size })
            } else {
                unresolved.set(countyFips, sampleTracts)
            }
        }

        // Batch DB fallback for still-unresolved counties
        if (unresolved.size > 0) {
            try {
                const allSampleTracts = [...unresolved.values()].flat()
                const CHUNK = 50
                const dbResults = new Map<string, string>() // tractId -> name
                for (let i = 0; i < allSampleTracts.length; i += CHUNK) {
                    const chunk = allSampleTracts.slice(i, i + CHUNK)
                    const { data: ladderRows } = await supabase
                        .from("parcel_ladder_v1")
                        .select("tract_geoid20, neighborhood_name, zcta5, city, county_name")
                        .in("tract_geoid20", chunk)
                        .limit(500)

                    if (ladderRows) {
                        for (const row of ladderRows as any[]) {
                            if (!dbResults.has(row.tract_geoid20)) {
                                const name = row.county_name
                                    || row.city
                                    || row.neighborhood_name
                                    || (row.zcta5 && ZIP_NAMES[row.zcta5])
                                    || (row.zcta5 ? `ZIP ${row.zcta5}` : null)
                                if (name) dbResults.set(row.tract_geoid20, name)
                            }
                        }
                    }
                }

                for (const [countyFips, sampleTracts] of unresolved) {
                    let city: string | undefined
                    for (const tractId of sampleTracts) {
                        const name = dbResults.get(tractId)
                        if (name) { city = name; break }
                    }

                    // If we STILL don't have a name after DB lookup, it's a synthetic FIPS (like 48900)
                    // Filter it out entirely rather than showing 'County 48900'
                    if (city) {
                        const { tracts } = countyMap.get(countyFips)!
                        cities.push({ city, citySlug: slugify(city), countyFips, tractCount: tracts.size })
                    }
                }
            } catch (err) {
                console.error("Error querying parcel_ladder_v1 for county names:", err)
                // If DB completely fails, we just don't load the unresolved counties
            }
        }

        return cities.sort((a, b) => b.tractCount - a.tractCount)
    })
}

/**
 * Get all states with forecast data.
 * Uses a large limit to capture all states from potentially millions of forecast rows.
 */
export async function getStatesWithData(
    schema = "forecast_queue"
): Promise<{ stateName: string; stateAbbr: string; stateSlug: string }[]> {
    return withRedisCache(`states:${schema}`, async () => {
        const supabase = getSupabaseAdmin()

        // Query each known state prefix to check for data (avoids huge limit)
        const states: { stateName: string; stateAbbr: string; stateSlug: string }[] = []

        for (const [fips, info] of Object.entries(STATE_FIPS)) {
            const nextFips = String(Number(fips) + 1).padStart(2, "0")
            const { count } = await supabase
                .schema(schema as any)
                .from("metrics_tract_forecast")
                .select("tract_geoid20", { count: "exact", head: true })
                .gte("tract_geoid20", fips)
                .lt("tract_geoid20", nextFips)
                .eq("horizon_m", 12)
                .eq("series_kind", "forecast")
                .not("p50", "is", null)
                .limit(1)

            if (count && count > 0) {
                states.push({
                    stateName: info.name,
                    stateAbbr: info.abbr,
                    stateSlug: info.abbr.toLowerCase(),
                })
            }
        }

        return states.sort((a, b) => a.stateName.localeCompare(b.stateName))
    })
}

// ---------------------------------------------------------------------------
// Origin-year resolution
// ---------------------------------------------------------------------------

/**
 * Returns the latest origin_year available in metrics_tract_forecast for a
 * given geoid prefix (e.g. a state FIPS "23" or county FIPS "23005").
 * Falls back to 2025 if no data is found.
 */
export async function getLatestOriginYear(
    geoidPrefix: string,
    schema = "forecast_queue"
): Promise<number> {
    return withRedisCache(`latest_origin_year:${geoidPrefix}:${schema}`, async () => {
        const supabase = getSupabaseAdmin()
        const { data } = await supabase
            .schema(schema as any)
            .from("metrics_tract_forecast")
            .select("origin_year")
            .gte("tract_geoid20", geoidPrefix)
            .lt("tract_geoid20", geoidPrefix + "z")
            .eq("horizon_m", 12)
            .eq("series_kind", "forecast")
            .not("p50", "is", null)
            .order("origin_year", { ascending: false })
            .limit(1)
            .maybeSingle()

        return (data as any)?.origin_year ?? 2025
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function slugify(str: string): string {
    return str
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/^-+|-+$/g, "")
}
