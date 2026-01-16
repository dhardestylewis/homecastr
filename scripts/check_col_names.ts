
import { createClient } from '@supabase/supabase-js'

const supabaseUrl = "https://earrhbknfjnhbudsucch.supabase.co"
const supabaseKey = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVhcnJoYmtuZmpuaGJ1ZHN1Y2NoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjY3MDQ3NTEsImV4cCI6MjA4MjI4MDc1MX0.qlmpskXzZImPJ3l9-Ze4CkaXnQXSdyVW7oWaoUn8Np0"

const supabase = createClient(supabaseUrl, supabaseKey)

async function checkColumns() {
    console.log("Checking columns for 2017 row...")
    const { data: sample, error } = await supabase
        .from('h3_precomputed_hex_details')
        .select('*')
        .eq('forecast_year', 2017)
        .limit(1)

    if (error) {
        console.error("Error:", error)
        return
    }

    if (!sample || sample.length === 0) {
        console.log("No data found for 2017")
        return
    }

    const row = sample[0]
    console.log("Keys found:", Object.keys(row).sort())
    console.log("predicted_value:", row.predicted_value)
    console.log("med_predicted_value:", row.med_predicted_value)
}

checkColumns()
