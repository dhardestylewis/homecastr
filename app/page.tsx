import Link from "next/link"
import { ArrowRight, ChevronRight, Terminal, BarChart3, Map, Shield, Zap, Building2, Users, TrendingUp } from "lucide-react"
import { HomecastrLogo } from "@/components/homecastr-logo"
import { HeroProductPreview } from "@/components/home/hero-product-preview"
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
        {/* Hero Section - Single dominant promise */}
        <section className="relative">
          <div className="hero-gradient absolute inset-0 pointer-events-none" />
          
          <div className="max-w-6xl mx-auto px-6 pt-20 pb-16 md:pt-28 md:pb-24">
            <div className="max-w-3xl">
              {/* No badge - lead with the promise directly */}
              <h1 className="text-4xl md:text-6xl lg:text-7xl font-bold tracking-tight leading-[1.1] text-balance mb-6">
                See where a home&apos;s value is headed.
              </h1>
              
              <p className="text-lg md:text-xl text-muted-foreground leading-relaxed max-w-2xl mb-10">
                Property-level forecasts with probability bands. Not just what a home is worth today, but where it&apos;s going over the next five years.
              </p>

              {/* Single primary CTA */}
              <div className="flex flex-col sm:flex-row gap-4">
                <Link
                  href="/app"
                  className="inline-flex items-center justify-center gap-2 px-6 py-3 text-base font-medium bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
                >
                  Explore the Forecast Map
                  <ArrowRight className="w-4 h-4" />
                </Link>
                <Link
                  href="/api-docs"
                  className="inline-flex items-center justify-center gap-2 px-6 py-3 text-base font-medium text-foreground border border-border rounded-md hover:bg-muted/50 transition-colors"
                >
                  View API Docs
                  <ChevronRight className="w-4 h-4" />
                </Link>
              </div>
            </div>
          </div>

          {/* Product Preview - Immediate visual surface */}
          <HeroProductPreview />
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
