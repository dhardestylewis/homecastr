"""
Diagnostic: What origin_year values exist for Maine (FIPS 23) tracts
in metrics_tract_forecast? Also checks if tract counts are inflated
due to multiple rows per tract.
"""
import os, sys
from dotenv import load_dotenv

load_dotenv(".env.local")

from supabase import create_client

url = os.environ["NEXT_PUBLIC_SUPABASE_URL"]
key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
schema = os.environ.get("FORECAST_SCHEMA", "forecast_queue")

sb = create_client(url, key)

print("=== Origin years available for Maine (FIPS 23) ===")
resp = (
    sb.schema(schema)
    .from_("metrics_tract_forecast")
    .select("origin_year, horizon_m, series_kind")
    .gte("tract_geoid20", "23")
    .lt("tract_geoid20", "23z")
    .not_.is_("p50", "null")
    .order("origin_year")
    .limit(5000)
    .execute()
)
rows = resp.data or []
combos = {}
for r in rows:
    key_ = (r["origin_year"], r["horizon_m"], r["series_kind"])
    combos[key_] = combos.get(key_, 0) + 1

print(f"{'origin_year':>12} {'horizon_m':>10} {'series_kind':>14} {'row_count':>10}")
print("-" * 52)
for (oy, hm, sk), cnt in sorted(combos.items()):
    print(f"{str(oy):>12} {str(hm):>10} {str(sk):>14} {cnt:>10}")

print()
print("=== Cumberland County (FIPS 23005) tract count check ===")
# Count rows for Cumberland with horizon_m=12 (what state page uses)
resp2 = (
    sb.schema(schema)
    .from_("metrics_tract_forecast")
    .select("tract_geoid20, origin_year")
    .gte("tract_geoid20", "23005")
    .lt("tract_geoid20", "23005z")
    .eq("horizon_m", 12)
    .eq("series_kind", "forecast")
    .not_.is_("p50", "null")
    .limit(5000)
    .execute()
)
cumb_rows = resp2.data or []
print(f"Total rows (h12, forecast, no origin_year filter): {len(cumb_rows)}")
unique_tracts = len(set(r["tract_geoid20"] for r in cumb_rows))
print(f"Unique tract GEOIDs: {unique_tracts}")
origin_years = sorted(set(r["origin_year"] for r in cumb_rows))
print(f"Origin years present: {origin_years}")

# Now check what the city page gets: origin_year=2025 only
resp3 = (
    sb.schema(schema)
    .from_("metrics_tract_forecast")
    .select("tract_geoid20")
    .gte("tract_geoid20", "23005")
    .lt("tract_geoid20", "23005z")
    .eq("horizon_m", 12)
    .eq("origin_year", 2025)
    .eq("series_kind", "forecast")
    .not_.is_("p50", "null")
    .limit(5000)
    .execute()
)
cumb_2025 = resp3.data or []
print(f"\nWith origin_year=2025 filter (what city page uses): {len(cumb_2025)} rows, {len(set(r['tract_geoid20'] for r in cumb_2025))} unique tracts")
