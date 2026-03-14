// Check for parcels table existence and schema
const fs = require('fs')
const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL
const supabaseKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY

async function checkParcels() {
    let output = "=== Parcels Table Check ===\n\n"

    // Try to select one row from parcels
    // Note: The table might be called 'parcels', 'parcel_boundaries', 'h3_aoi_parcels' etc.
    const candidates = ['parcels', 'parcel_boundaries', 'property_boundaries', 'h3_aoi_parcels']

    for (const table of candidates) {
        try {
            const url = `${supabaseUrl}/rest/v1/${table}?select=*&limit=1`
            const res = await fetch(url, {
                headers: {
                    apikey: supabaseKey,
                    Authorization: `Bearer ${supabaseKey}`
                }
            })

            if (res.ok) {
                const data = await res.json()
                if (data.length > 0) {
                    output += `✓ Found table '${table}' with columns: ${Object.keys(data[0]).join(', ')}\n`
                    // Check if it has geometry
                    if (data[0].geom || data[0].wkb_geometry || data[0].geometry) {
                        output += "  ✓ Has geometry column\n"
                    } else {
                        output += "  ⚠ No obvious geometry column found\n"
                    }
                } else {
                    output += `✓ Found table '${table}' but it is empty\n`
                }
            } else {
                output += `✗ Table '${table}' check failed: ${res.status} ${res.statusText}\n`
            }
        } catch (e) {
            output += `✗ Error checking '${table}': ${e.message}\n`
        }
    }

    console.log(output)
    fs.writeFileSync('scripts/check-parcels.txt', output)
}

checkParcels().catch(console.error)
