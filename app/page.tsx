import Link from "next/link"
import { ArrowRight } from "lucide-react"
import { HomecastrLogo } from "@/components/homecastr-logo"
import { HeroForecastBar, MockForecastCard } from "@/components/home/hero-product-preview"
import { ProofStrip } from "@/components/home/proof-strip"
import { FeatureGrid } from "@/components/home/feature-grid"
import { TrustSection } from "@/components/home/trust-section"
import { EnterpriseSection } from "@/components/home/enterprise-section"
import { FooterSection } from "@/components/home/footer-section"
import { fetchFeaturedForecast, type FeaturedForecastData } from "@/lib/publishing/featured-forecast"

// Fallback data in case DB fetch fails
const FALLBACK_DATA: FeaturedForecastData = {
  tract: {
    geoid: "48201312300",
    neighborhoodSlug: "third-ward",
    citySlug: "houston",
    stateSlug: "texas",
  },
  location: {
    neighborhood: "Third Ward",
    city: "Houston",
    state: "TX",
    zip: "77003",
  },
  currentValue: 455000,
  horizons: [
    { year: 2026, p10: 432250, p50: 480886, p90: 529574 },
    { year: 2027, p10: 431999, p50: 507999, p90: 583999 },
    { year: 2028, p10: 429566, p50: 536708, p90: 643850 },
    { year: 2029, p10: 425325, p50: 567100, p90: 708875 },
    { year: 2030, p10: 419388, p50: 599268, p90: 779148 },
    { year: 2031, p10: 411503, p50: 633313, p90: 855073 },
  ],
}

export default async function HomePage() {
  // Fetch real forecast data from Supabase - use fallback if fetch fails
  let forecastData: FeaturedForecastData
  try {
    const data = await fetchFeaturedForecast()
    forecastData = data || FALLBACK_DATA
  } catch {
    forecastData = FALLBACK_DATA
  }

  return (
    <div className="min-h-screen bg-background text-foreground overflow-auto">
      {/* Navigation - minimal, sticky */}
      <header className="sticky top-0 z-50 border-b border-border/40 bg-background/80 backdrop-blur-md">
        <div className="max-w-6xl mx-auto px-6 h-14 flex items-center justify-between">
          <Link href="/" className="flex items-center gap-2">
            <HomecastrLogo size={24} variant="horizontal" />
          </Link>
          
          <nav className="hidden md:flex items-center gap-8">
            <Link href="/methodology" className="text-sm text-muted-foreground hover:text-foreground transition-colors">
              Methodology
            </Link>
            <Link href="/forecasts" className="text-sm text-muted-foreground hover:text-foreground transition-colors">
              Markets
            </Link>
            <Link href="/api-docs" className="text-sm text-muted-foreground hover:text-foreground transition-colors">
              API
            </Link>
            <Link href="/faq" className="text-sm text-muted-foreground hover:text-foreground transition-colors">
              FAQ
            </Link>
          </nav>

          <Link
            href="/app"
            className="inline-flex items-center gap-2 px-4 py-2 text-sm font-medium bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
          >
            Open Map
            <ArrowRight className="w-3.5 h-3.5" />
          </Link>
        </div>
      </header>

      <main>
        {/* Hero Section - Query-first with guided forecast bar */}
        <section className="relative">
          <div className="hero-gradient absolute inset-0 pointer-events-none" />
          
          <div className="max-w-6xl mx-auto px-6 pt-20 pb-12 md:pt-28 md:pb-16">
            <div className="text-center max-w-3xl mx-auto mb-10">
              {/* Lead with the promise */}
              <h1 className="text-4xl md:text-6xl lg:text-7xl font-bold tracking-tight leading-[1.1] text-balance mb-6">
                See where your home&apos;s value is headed.
              </h1>
              
              <p className="text-lg md:text-xl text-muted-foreground leading-relaxed mb-10">
                Get a property-level forecast with downside, base-case, and upside scenarios over the next five years.
              </p>

              {/* Guided forecast bar */}
              <HeroForecastBar />
            </div>

            {/* Secondary CTA */}
            <div className="flex justify-center mt-8">
              <Link
                href="/forecasts"
                className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground transition-colors"
              >
                Browse markets by state and city
                <span aria-hidden="true">→</span>
              </Link>
            </div>
          </div>

          {/* Mock forecast card - shows what the user will get, powered by real data */}
          <div className="mt-4">
            <div className="text-center mb-6">
              <span className="text-sm font-medium text-muted-foreground">A property forecast, not just a number</span>
            </div>
            <MockForecastCard data={forecastData} />
          </div>
        </section>

        {/* Proof Strip - Trust before sell */}
        <ProofStrip />

        {/* Core Value Props - What makes this different */}
        <FeatureGrid />

        {/* Trust & Methodology Section */}
        <TrustSection />

        {/* Enterprise/API Section - Extension of core product */}
        <EnterpriseSection />

        {/* Footer */}
        <FooterSection />
      </main>
    </div>
  )
}
