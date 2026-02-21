
import { createClient } from "@supabase/supabase-js"

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
const supabaseKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!

async function main() {
    const supabase = createClient(supabaseUrl, supabaseKey)

    console.log("Checking schema for table: h3_precomputed_hex_details")

    // method 1: try to select just the column we care about to see if it errors
    const { data, error } = await supabase
        .from('h3_precomputed_hex_details')
        .select('cap_rate')
        .limit(1)

    if (error) {
        console.log("❌ Error selecting cap_rate:", error.message)
    } else {
        console.log("✅ Successfully selected 'cap_rate'. Column exists.")
    }
}

main()
