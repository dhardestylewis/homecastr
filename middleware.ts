import { NextResponse } from 'next/server'
import type { NextRequest } from 'next/server'

export function middleware(request: NextRequest) {
  // We only want to auto-locate on the main /app route when no coordinates are specified
  if (request.nextUrl.pathname === '/app') {
    const { searchParams } = request.nextUrl
    
    // Only redirect if coordinates are missing
    if (!searchParams.has('lat') || !searchParams.has('lng')) {
      const latitude = request.headers.get('x-vercel-ip-latitude')
      const longitude = request.headers.get('x-vercel-ip-longitude')
      const country = request.headers.get('x-vercel-ip-country')

      // Default to Manhattan, NY (Premium default)
      let newLat = '40.7484'
      let newLng = '-73.9857'
      let newZoom = '10'

      // If in the US and Vercel provides coordinates, use their coarse location
      if (country === 'US' && latitude && longitude) {
        newLat = latitude
        newLng = longitude
      }

      const redirectUrl = request.nextUrl.clone()
      redirectUrl.searchParams.set('lat', newLat)
      redirectUrl.searchParams.set('lng', newLng)
      if (!searchParams.has('zoom')) {
        redirectUrl.searchParams.set('zoom', newZoom)
      }

      return NextResponse.redirect(redirectUrl)
    }
  }
  
  return NextResponse.next()
}

export const config = {
  matcher: ['/app'],
}
