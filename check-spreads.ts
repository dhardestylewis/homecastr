import { createClient } from "@supabase/supabase-js"
import * as dotenv from "dotenv"
dotenv.config({ path: ".env.local" })

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY!
if (!supabaseUrl || !supabaseKey) {
  console.error("Missing supabase credentials")
  process.exit(1)
}

const supabase = createClient(supabaseUrl, supabaseKey)

async function main() {
  const { data, error } = await supabase
    .schema("forecast_queue")
    .from("metrics_tract_forecast")
    .select("horizon_m, p10, p50, p90")
    .in("horizon_m", [12, 60])
    .eq("origin_year", 2025)
    .gt("p50", 0)
    .limit(20000)

  if (error) {
    console.error(error)
    process.exit(1)
  }

  const spreads12 = data.filter((r: any) => r.horizon_m === 12).map((r: any) => ((r.p90 - r.p10) / r.p50) * 100).sort((a, b) => a - b)
  const spreads60 = data.filter((r: any) => r.horizon_m === 60).map((r: any) => ((r.p90 - r.p10) / r.p50) * 100).sort((a, b) => a - b)

  const getStats = (arr: number[]) => ({
    count: arr.length,
    p10: arr[Math.floor(arr.length * 0.1)],
    p25: arr[Math.floor(arr.length * 0.25)],
    p50: arr[Math.floor(arr.length * 0.50)],
    p75: arr[Math.floor(arr.length * 0.75)],
    p90: arr[Math.floor(arr.length * 0.90)]
  })

  require('fs').writeFileSync('results.json', JSON.stringify({ m12: getStats(spreads12), m60: getStats(spreads60) }, null, 2))
}

main().catch(console.error)
