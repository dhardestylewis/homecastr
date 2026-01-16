const fs = require('fs')
const supabaseUrl = "https://earrhbknfjnhbudsucch.supabase.co"
const supabaseKey = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVhcnJoYmtuZmpuaGJ1ZHN1Y2NoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjY3MDQ3NTEsImV4cCI6MjA4MjI4MDc1MX0.qlmpskXzZImPJ3l9-Ze4CkaXnQXSdyVW7oWaoUn8Np0"

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
