import { getSupabaseServerClient } from "@/lib/supabase/server"
import { NextResponse } from "next/server"

export async function GET(request: Request) {
  const supabase = await getSupabaseServerClient()
  
  // 1. Check Austin geocode tract logic
  console.log("Looking up Austin lat/lng: 30.2711286, -97.7436995 (Austin coordinates)");
  const { data: tractData } = await supabase
    .rpc("find_tract_at_point", {
      p_lat: 30.2711286,  // Fixed Austin coordinates
      p_lng: -97.7436995,
    });
  
  // 2. See what we have in tract_slug_lookup for TX
  const { data: txLookup } = await supabase
    .from("tract_slug_lookup")
    .select("geoid:tract_geoid", { count: 'exact' })
    .eq("state_slug", "tx")
    .limit(1);

  // 3. Let's see an example of a TX slug and NY slug
  const { data: txExample } = await supabase
    .from("tract_slug_lookup")
    .select("*")
    .eq("state_slug", "tx")
    .limit(1);

  const { data: nyExample } = await supabase
    .from("tract_slug_lookup")
    .select("*")
    .eq("state_slug", "ny")
    .limit(1);

  return NextResponse.json({
    tractData,
    txLookupCount: txLookup,
    txExample,
    nyExample
  });
}
