import Link from "next/link"
import { ArrowRight } from "lucide-react"
import { HomecastrLogo } from "@/components/homecastr-logo"
import { HeroForecastBar, MockForecastCard } from "@/components/home/hero-product-preview"
import { ProofStrip } from "@/components/home/proof-strip"
import { FeatureGrid } from "@/components/home/feature-grid"
import { TrustSection } from "@/components/home/trust-section"
import { EnterpriseSection } from "@/components/home/enterprise-section"
import { FooterSection } from "@/components/home/footer-section"

export default function HomePage() {
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
                className="text-sm text-muted-foreground hover:text-foreground transition-colors underline underline-offset-4"
              >
                Or browse markets by state and city
              </Link>
            </div>
          </div>

          {/* Mock forecast card - shows what the user will get */}
          <div className="mt-4">
            <div className="text-center mb-6">
              <span className="text-xs uppercase tracking-wider text-muted-foreground">What you&apos;ll see</span>
            </div>
            <MockForecastCard />
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
