
import { createClient } from "@supabase/supabase-js"

async function checkColumns() {
    const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
    const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY!
    const supabase = createClient(supabaseUrl, supabaseKey)

    const { data, error } = await supabase
        .from("h3_precomputed_hex_details")
        .select("*")
        .limit(1)
        .single()

    if (error) {
        console.error("Error:", error)
        return
    }

    console.log("Columns:", Object.keys(data))
    console.log("Sample Data:", JSON.stringify(data, null, 2))
}

checkColumns()
