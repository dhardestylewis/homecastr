import Link from "next/link"
import { ArrowRight, ChevronRight, Mail, Linkedin, ExternalLink } from "lucide-react"
import { HomecastrLogo } from "@/components/homecastr-logo"
import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "About Homecastr | Company",
  description: "Building the forecast layer for residential real estate. Learn about our mission, methodology, and the team behind Homecastr.",
}

export default function AboutPage() {
  return (
    <div className="min-h-screen bg-background text-foreground">
      {/* Navigation */}
      <header className="sticky top-0 z-50 border-b border-border/40 bg-background/80 backdrop-blur-md">
        <div className="max-w-4xl mx-auto px-6 h-14 flex items-center justify-between">
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
            <Link href="/about" className="text-sm text-foreground font-medium">
              Company
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

      <main className="max-w-4xl mx-auto px-6 py-16 md:py-24">
        {/* Hero */}
        <header className="mb-16">
          <h1 className="text-3xl md:text-4xl font-bold tracking-tight mb-4 text-balance">
            Building the forecast layer for residential real estate.
          </h1>
          <p className="text-lg text-muted-foreground leading-relaxed max-w-2xl">
            Homecastr helps people understand where home values may be headed, not just what a home is worth today.
          </p>
        </header>

        {/* Section 1: Why Homecastr exists */}
        <section className="mb-16">
          <h2 className="text-xl font-semibold mb-4">Why Homecastr exists</h2>
          <div className="prose prose-neutral dark:prose-invert max-w-none">
            <p className="text-muted-foreground leading-relaxed">
              Most home valuation tools give you a single number for what a property is worth today. That number is useful, but it does not help you think about risk, timing, or the range of outcomes that could unfold over the next few years.
            </p>
            <p className="text-muted-foreground leading-relaxed mt-4">
              Homecastr was built to fill that gap. We produce property-level and neighborhood-level forecasts with calibrated probability bands—downside, base case, and upside scenarios—so homeowners, investors, and professionals can make more informed decisions. We believe that honest uncertainty quantification is more valuable than false precision.
            </p>
          </div>
        </section>

        {/* Section 2: What makes it credible */}
        <section className="mb-16">
          <h2 className="text-xl font-semibold mb-6">What makes it credible</h2>
          <div className="grid gap-6 md:grid-cols-3">
            <div className="p-5 rounded-xl border border-border bg-card">
              <h3 className="font-semibold mb-2">Probabilistic forecasts</h3>
              <p className="text-sm text-muted-foreground leading-relaxed">
                P10/P50/P90 percentile bands express uncertainty explicitly rather than hiding it behind a single estimate.
              </p>
            </div>
            <div className="p-5 rounded-xl border border-border bg-card">
              <h3 className="font-semibold mb-2">Property-level coverage</h3>
              <p className="text-sm text-muted-foreground leading-relaxed">
                Forecasts at the neighborhood and tract level, not just metro-wide averages. The house next door can have a different outlook.
              </p>
            </div>
            <div className="p-5 rounded-xl border border-border bg-card">
              <h3 className="font-semibold mb-2">Backtested reporting</h3>
              <p className="text-sm text-muted-foreground leading-relaxed">
                Published accuracy metrics by geography and horizon. We report how well the model has performed, not just what it predicts.
              </p>
            </div>
          </div>
          <div className="mt-6">
            <Link
              href="/methodology"
              className="inline-flex items-center gap-1 text-sm font-medium text-primary hover:underline underline-offset-4"
            >
              Read the methodology
              <ChevronRight className="w-4 h-4" />
            </Link>
          </div>
        </section>

        {/* Section 3: Founder */}
        <section className="mb-16">
          <h2 className="text-xl font-semibold mb-6">Founder</h2>
          <div className="p-6 rounded-xl border border-border bg-card">
            <div className="flex flex-col md:flex-row gap-6">
              {/* Founder info */}
              <div className="flex-1">
                <h3 className="text-lg font-semibold mb-1">Daniel Hardesty Lewis</h3>
                <p className="text-sm text-muted-foreground mb-4">Founder, Homecastr</p>
                <p className="text-sm text-muted-foreground leading-relaxed">
                  Background in large-scale scientific computing and geospatial machine learning. Previously at the Texas Advanced Computing Center (TACC) and Columbia University, working on computational modeling and applied research. Started Homecastr to bring probability-aware forecasting to residential real estate decisions.
                </p>
                <div className="flex items-center gap-4 mt-4">
                  <a
                    href="https://linkedin.com/in/danielhardestylewis"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-flex items-center gap-1.5 text-sm text-muted-foreground hover:text-foreground transition-colors"
                  >
                    <Linkedin className="w-4 h-4" />
                    LinkedIn
                  </a>
                  <Link
                    href="/methodology"
                    className="inline-flex items-center gap-1.5 text-sm text-muted-foreground hover:text-foreground transition-colors"
                  >
                    <ExternalLink className="w-4 h-4" />
                    Publications
                  </Link>
                </div>
              </div>
            </div>
          </div>
        </section>

        {/* Section 4: Research foundation */}
        <section className="mb-16">
          <h2 className="text-xl font-semibold mb-6">Research foundation</h2>
          <div className="p-6 rounded-xl border border-border bg-card">
            <p className="text-sm text-muted-foreground leading-relaxed mb-6">
              Homecastr's methodology draws on applied research in large-scale probabilistic modeling, geospatial data science, and computational forecasting. The model architecture emphasizes interpretability and honest uncertainty quantification over black-box point estimates.
            </p>
            <div className="grid gap-4 md:grid-cols-2">
              <div className="flex items-start gap-3">
                <div className="w-1.5 h-1.5 rounded-full bg-primary mt-2 shrink-0" />
                <span className="text-sm text-muted-foreground">Large-scale probabilistic simulation</span>
              </div>
              <div className="flex items-start gap-3">
                <div className="w-1.5 h-1.5 rounded-full bg-primary mt-2 shrink-0" />
                <span className="text-sm text-muted-foreground">Geospatial machine learning</span>
              </div>
              <div className="flex items-start gap-3">
                <div className="w-1.5 h-1.5 rounded-full bg-primary mt-2 shrink-0" />
                <span className="text-sm text-muted-foreground">Calibrated uncertainty bands</span>
              </div>
              <div className="flex items-start gap-3">
                <div className="w-1.5 h-1.5 rounded-full bg-primary mt-2 shrink-0" />
                <span className="text-sm text-muted-foreground">Applied research orientation</span>
              </div>
            </div>
          </div>
        </section>

        {/* Section 5: Work with us */}
        <section className="mb-16">
          <h2 className="text-xl font-semibold mb-6">Work with us</h2>
          <div className="grid gap-4 md:grid-cols-2">
            <div className="p-5 rounded-xl border border-border bg-card">
              <h3 className="font-semibold mb-2">API & Institutional Access</h3>
              <p className="text-sm text-muted-foreground leading-relaxed mb-4">
                Access property and neighborhood forecasts through API for portfolio analysis, underwriting, or product integration.
              </p>
              <Link
                href="/api-docs"
                className="inline-flex items-center gap-1 text-sm font-medium text-primary hover:underline underline-offset-4"
              >
                View API docs
                <ChevronRight className="w-4 h-4" />
              </Link>
            </div>
            <div className="p-5 rounded-xl border border-border bg-card">
              <h3 className="font-semibold mb-2">Research Collaborations</h3>
              <p className="text-sm text-muted-foreground leading-relaxed mb-4">
                Interested in collaborating on housing market research, model validation, or applied forecasting projects.
              </p>
              <a
                href="mailto:hello@homecastr.com"
                className="inline-flex items-center gap-1.5 text-sm font-medium text-primary hover:underline underline-offset-4"
              >
                <Mail className="w-4 h-4" />
                hello@homecastr.com
              </a>
            </div>
          </div>
        </section>

        {/* Back to product CTA */}
        <section className="pt-8 border-t border-border">
          <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4">
            <p className="text-sm text-muted-foreground">
              Ready to explore forecasts?
            </p>
            <div className="flex gap-3">
              <Link
                href="/app"
                className="inline-flex items-center gap-2 px-4 py-2 text-sm font-medium bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
              >
                Open Map
                <ArrowRight className="w-3.5 h-3.5" />
              </Link>
              <Link
                href="/forecasts"
                className="inline-flex items-center gap-2 px-4 py-2 text-sm font-medium text-foreground border border-border rounded-md hover:bg-muted/50 transition-colors"
              >
                Browse Markets
              </Link>
            </div>
          </div>
        </section>
      </main>

      {/* Minimal footer */}
      <footer className="py-8 border-t border-border">
        <div className="max-w-4xl mx-auto px-6 flex flex-col md:flex-row justify-between items-center gap-4">
          <div className="text-sm text-muted-foreground">
            © 2026 Homecastr. All rights reserved.
          </div>
          <div className="flex items-center gap-6 text-sm text-muted-foreground">
            <Link href="/privacy" className="hover:text-foreground transition-colors">Privacy</Link>
            <Link href="/terms" className="hover:text-foreground transition-colors">Terms</Link>
          </div>
        </div>
      </footer>
    </div>
  )
}
