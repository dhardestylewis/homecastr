import Link from "next/link"
import { ArrowRight, Map, Linkedin, Github } from "lucide-react"
import { HomecastrLogo } from "@/components/homecastr-logo"

export function FooterSection() {
  return (
    <>
      {/* Final CTA */}
      <section className="py-16 md:py-20 border-t border-border bg-muted/30">
        <div className="max-w-6xl mx-auto px-6">
          <div className="flex flex-col md:flex-row items-center justify-between gap-8">
            <div>
              <h2 className="text-xl md:text-2xl font-bold tracking-tight mb-2">
                Ready to see the forecast?
              </h2>
              <p className="text-muted-foreground text-sm max-w-lg">
                Look up any home and see where its value is headed. Property-level forecasts, probability bands, and more.
              </p>
            </div>
            <div className="flex gap-3 shrink-0">
              <Link
                href="/app"
                className="inline-flex items-center gap-2 px-5 py-2.5 text-sm font-medium bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors whitespace-nowrap"
              >
                Explore the Forecast Map
                <ArrowRight className="w-4 h-4" />
              </Link>
              <Link
                href="/forecasts"
                className="inline-flex items-center gap-2 px-5 py-2.5 text-sm font-medium text-foreground border border-border rounded-md hover:bg-muted/50 transition-colors whitespace-nowrap"
              >
                <Map className="w-4 h-4" />
                Browse Markets
              </Link>
            </div>
          </div>
        </div>
      </section>

      {/* Footer */}
      <footer className="py-12 border-t border-border">
        <div className="max-w-6xl mx-auto px-6">
          <div className="grid md:grid-cols-4 gap-8 mb-10">
            {/* Brand */}
            <div className="md:col-span-1">
              <HomecastrLogo size={20} variant="horizontal" />
              <p className="text-sm text-muted-foreground mt-3 leading-relaxed">
                Forecast where a home is headed, not just what it is worth today.
              </p>
            </div>

            {/* Links */}
            <div>
              <h4 className="text-sm font-semibold mb-4">Product</h4>
              <ul className="space-y-2.5">
                <li>
                  <Link href="/app" className="text-sm text-muted-foreground hover:text-foreground transition-colors">
                    Forecast Map
                  </Link>
                </li>
                <li>
                  <Link href="/forecasts" className="text-sm text-muted-foreground hover:text-foreground transition-colors">
                    Browse Markets
                  </Link>
                </li>
                <li>
                  <Link href="/api-docs" className="text-sm text-muted-foreground hover:text-foreground transition-colors">
                    API
                  </Link>
                </li>
              </ul>
            </div>

            <div>
              <h4 className="text-sm font-semibold mb-4">Company</h4>
              <ul className="space-y-2.5">
                <li>
                  <Link href="/methodology" className="text-sm text-muted-foreground hover:text-foreground transition-colors">
                    Methodology
                  </Link>
                </li>
                <li>
                  <Link href="/faq" className="text-sm text-muted-foreground hover:text-foreground transition-colors">
                    FAQ
                  </Link>
                </li>
                <li>
                  <Link href="/support" className="text-sm text-muted-foreground hover:text-foreground transition-colors">
                    Support
                  </Link>
                </li>
              </ul>
            </div>

            <div>
              <h4 className="text-sm font-semibold mb-4">Legal</h4>
              <ul className="space-y-2.5">
                <li>
                  <Link href="/privacy" className="text-sm text-muted-foreground hover:text-foreground transition-colors">
                    Privacy
                  </Link>
                </li>
                <li>
                  <Link href="/terms" className="text-sm text-muted-foreground hover:text-foreground transition-colors">
                    Terms
                  </Link>
                </li>
              </ul>
            </div>
          </div>

          {/* Bottom row */}
          <div className="flex flex-col md:flex-row justify-between items-center gap-4 pt-8 border-t border-border">
            <div className="text-sm text-muted-foreground">
              © 2026 Homecastr. All rights reserved.
            </div>
            <div className="flex items-center gap-4">
              <a
                href="https://linkedin.com/company/homecastr"
                target="_blank"
                rel="noopener noreferrer"
                className="text-muted-foreground hover:text-foreground transition-colors"
                aria-label="LinkedIn"
              >
                <Linkedin className="w-4.5 h-4.5" />
              </a>
              <a
                href="https://github.com/dhardestylewis"
                target="_blank"
                rel="noopener noreferrer"
                className="text-muted-foreground hover:text-foreground transition-colors"
                aria-label="GitHub"
              >
                <Github className="w-4.5 h-4.5" />
              </a>
            </div>
          </div>
        </div>
      </footer>
    </>
  )
}
