import Link from "next/link"
import { ArrowRight, Terminal, Building2, Users, Briefcase } from "lucide-react"

export function EnterpriseSection() {
  return (
    <section className="py-20 md:py-28">
      <div className="max-w-6xl mx-auto px-6">
        <div className="grid md:grid-cols-2 gap-12 md:gap-16 items-start">
          {/* Left: API messaging */}
          <div>
            <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-muted text-xs font-medium uppercase tracking-wider mb-6">
              <Terminal className="w-3 h-3" />
              API Access
            </div>
            
            <h2 className="text-2xl md:text-3xl font-bold tracking-tight mb-4">
              The same forecasts, via API.
            </h2>
            <p className="text-muted-foreground leading-relaxed mb-6">
              Access lot-level and neighborhood-level forecasts programmatically. 
              JSON responses, API key auth, sub-second latency.
            </p>

            {/* Code sample */}
            <div className="terminal-block mb-8">
              <div className="px-4 py-2 border-b border-border text-xs text-muted-foreground">
                GET /api/v1/forecast
              </div>
              <pre className="p-4 text-xs leading-relaxed overflow-x-auto">
                <code className="text-foreground">{`{
  "address": "123 Main St, Houston, TX",
  "current_value": 295000,
  "forecasts": {
    "p10": 268000,
    "p50": 345000,
    "p90": 425000
  },
  "horizon_years": 5
}`}</code>
              </pre>
            </div>

            <div className="flex flex-col sm:flex-row gap-3">
              <Link
                href="/api-docs#get-key"
                className="inline-flex items-center justify-center gap-2 px-5 py-2.5 text-sm font-medium bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
              >
                Get Free API Key
                <ArrowRight className="w-4 h-4" />
              </Link>
              <Link
                href="/api-docs"
                className="inline-flex items-center justify-center gap-2 px-5 py-2.5 text-sm font-medium text-foreground border border-border rounded-md hover:bg-muted/50 transition-colors"
              >
                API Documentation
              </Link>
            </div>
          </div>

          {/* Right: Use cases */}
          <div>
            <h3 className="text-sm font-medium text-muted-foreground uppercase tracking-wider mb-6">
              Built for
            </h3>
            
            <div className="space-y-4">
              <div className="flex items-start gap-4 p-4 rounded-lg border border-border bg-card">
                <div className="w-9 h-9 rounded-md bg-muted flex items-center justify-center shrink-0">
                  <Building2 className="w-4.5 h-4.5 text-muted-foreground" />
                </div>
                <div>
                  <h4 className="font-semibold text-sm mb-1">SFR Acquisitions</h4>
                  <p className="text-sm text-muted-foreground">
                    Score buy/hold/sell across 50 to 5,000+ doors with forward-looking data.
                  </p>
                </div>
              </div>

              <div className="flex items-start gap-4 p-4 rounded-lg border border-border bg-card">
                <div className="w-9 h-9 rounded-md bg-muted flex items-center justify-center shrink-0">
                  <Briefcase className="w-4.5 h-4.5 text-muted-foreground" />
                </div>
                <div>
                  <h4 className="font-semibold text-sm mb-1">Investment Committees</h4>
                  <p className="text-sm text-muted-foreground">
                    Underwrite new deals with probabilistic market outlooks, not just current comps.
                  </p>
                </div>
              </div>

              <div className="flex items-start gap-4 p-4 rounded-lg border border-border bg-card">
                <div className="w-9 h-9 rounded-md bg-muted flex items-center justify-center shrink-0">
                  <Users className="w-4.5 h-4.5 text-muted-foreground" />
                </div>
                <div>
                  <h4 className="font-semibold text-sm mb-1">Mortgage Risk Desks</h4>
                  <p className="text-sm text-muted-foreground">
                    Stress-test collateral under rate scenarios with property-level forecasts.
                  </p>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
  )
}
