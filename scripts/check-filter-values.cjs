// Quick DB check for reliability and med_years values
const fs = require('fs')
const supabaseUrl = "https://earrhbknfjnhbudsucch.supabase.co"
const supabaseKey = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVhcnJoYmtuZmpuaGJ1ZHN1Y2NoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjY3MDQ3NTEsImV4cCI6MjA4MjI4MDc1MX0.qlmpskXzZImPJ3l9-Ze4CkaXnQXSdyVW7oWaoUn8Np0"

async function query(sql) {
    const url = `${supabaseUrl}/rest/v1/rpc/sql?select=*`
    // Can't run raw SQL via REST, use select with filters instead
}

async function checkValues() {
    let output = "=== Checking reliability and med_years values ===\n\n"

    // Check for non-null reliability values
    const reliabilityUrl = `${supabaseUrl}/rest/v1/h3_precomputed_hex_details?select=reliability&reliability=neq.null&reliability=gt.0&limit=50`
    const reliabilityRes = await fetch(reliabilityUrl, {
        headers: { apikey: supabaseKey, Authorization: `Bearer ${supabaseKey}` }
    })
    const reliabilityData = await reliabilityRes.json()

    output += `Reliability (non-null, >0): ${reliabilityData.length} rows found\n`
    if (reliabilityData.length > 0) {
        const values = reliabilityData.map(r => r.reliability)
        output += `  Values: ${values.slice(0, 20).join(', ')}\n`
        output += `  Min: ${Math.min(...values)}, Max: ${Math.max(...values)}\n`
    }

    // Check for non-null med_years values
    const medYearsUrl = `${supabaseUrl}/rest/v1/h3_precomputed_hex_details?select=med_years&med_years=neq.null&med_years=gt.0&limit=50`
    const medYearsRes = await fetch(medYearsUrl, {
        headers: { apikey: supabaseKey, Authorization: `Bearer ${supabaseKey}` }
    })
    const medYearsData = await medYearsRes.json()

    output += `\nMed_years (non-null, >0): ${medYearsData.length} rows found\n`
    if (medYearsData.length > 0) {
        const values = medYearsData.map(r => r.med_years)
        output += `  Values: ${values.slice(0, 20).join(', ')}\n`
        output += `  Min: ${Math.min(...values)}, Max: ${Math.max(...values)}\n`
    }

    // Check for sample_accuracy values  
    const sampleAccUrl = `${supabaseUrl}/rest/v1/h3_precomputed_hex_details?select=sample_accuracy&sample_accuracy=neq.null&sample_accuracy=gt.0&limit=50`
    const sampleAccRes = await fetch(sampleAccUrl, {
        headers: { apikey: supabaseKey, Authorization: `Bearer ${supabaseKey}` }
    })
    const sampleAccData = await sampleAccRes.json()

    output += `\nSample_accuracy (non-null, >0): ${sampleAccData.length} rows found\n`
    if (sampleAccData.length > 0) {
        const values = sampleAccData.map(r => r.sample_accuracy)
        output += `  Values: ${values.slice(0, 20).join(', ')}\n`
    }

    console.log(output)
    fs.writeFileSync('scripts/filter-values-check.txt', output)
    console.log("\nWritten to scripts/filter-values-check.txt")
}

checkValues().catch(console.error)
