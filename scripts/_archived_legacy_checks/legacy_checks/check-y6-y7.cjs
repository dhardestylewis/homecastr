const fs = require('fs')
const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL
const supabaseKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY

async function checkY6Y7() {
    // Try to select y6 and y7 columns. If they don't exist, Supabase will return an error.
    const url = `${supabaseUrl}/rest/v1/h3_precomputed_hex_details?select=fan_p50_y6,fan_p50_y7&limit=1`

    console.log("Checking for fan_p50_y6 and fan_p50_y7...")

    const res = await fetch(url, {
        headers: { apikey: supabaseKey, Authorization: `Bearer ${supabaseKey}` }
    })

    if (res.status === 200) {
        const data = await res.json()
        console.log("Success! Columns exist.")
        console.log("Data:", data)
    } else {
        const err = await res.json()
        console.log("Failed. Columns likely missing.")
        console.log("Error:", err)
    }
}

checkY6Y7().catch(console.error)
