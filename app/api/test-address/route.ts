import { addressToForecast } from "@/app/actions/address-to-forecast";
import { geocodeAddress } from "@/app/actions/geocode";
import { NextResponse } from "next/server";

export async function GET(request: Request) {
  const url = new URL(request.url);
  const q = url.searchParams.get("q") || "Austin, TX";
  
  try {
    const geo = await geocodeAddress(q);
    if (!geo) {
      return NextResponse.json({ error: "Geocode failed" });
    }
    
    const forecast = await addressToForecast(geo.lat, geo.lng);
    return NextResponse.json({ 
      geo, 
      forecast 
    });
  } catch (error: any) {
    return NextResponse.json({ error: error.message || "Unknown error" });
  }
}
