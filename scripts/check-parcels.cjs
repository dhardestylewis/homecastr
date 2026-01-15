// Check for parcels table existence and schema
const fs = require('fs')
const supabaseUrl = "https://earrhbknfjnhbudsucch.supabase.co"
const supabaseKey = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVhcnJoYmtuZmpuaGJ1ZHN1Y2NoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjY3MDQ3NTEsImV4cCI6MjA4MjI4MDc1MX0.qlmpskXzZImPJ3l9-Ze4CkaXnQXSdyVW7oWaoUn8Np0"

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
