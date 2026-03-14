import Link from "next/link"
import { ArrowRight, Terminal, Building2, Users, Home, TrendingUp } from "lucide-react"

export function EnterpriseSection() {
  return (
    <section className="py-20 md:py-28">
      <div className="max-w-6xl mx-auto px-6">
        <div className="grid md:grid-cols-2 gap-12 md:gap-16 items-start">
          {/* Left: Audience - broader consumer focus first */}
          <div>
            <h3 className="text-sm font-medium text-muted-foreground uppercase tracking-wider mb-6">
              Built for people making housing decisions
            </h3>
            
            <div className="space-y-4">
              <div className="flex items-start gap-4 p-4 rounded-lg border border-border bg-card">
                <div className="w-9 h-9 rounded-md bg-muted flex items-center justify-center shrink-0">
                  <Home className="w-4.5 h-4.5 text-muted-foreground" />
                </div>
                <div>
                  <h4 className="font-semibold text-sm mb-1">Homeowners</h4>
                  <p className="text-sm text-muted-foreground">
                    Understand the range of outcomes for your largest asset.
                  </p>
                </div>
              </div>

              <div className="flex items-start gap-4 p-4 rounded-lg border border-border bg-card">
                <div className="w-9 h-9 rounded-md bg-muted flex items-center justify-center shrink-0">
                  <TrendingUp className="w-4.5 h-4.5 text-muted-foreground" />
                </div>
                <div>
                  <h4 className="font-semibold text-sm mb-1">Investors</h4>
                  <p className="text-sm text-muted-foreground">
                    Compare upside and downside across properties and neighborhoods.
                  </p>
                </div>
              </div>

              <div className="flex items-start gap-4 p-4 rounded-lg border border-border bg-card">
                <div className="w-9 h-9 rounded-md bg-muted flex items-center justify-center shrink-0">
                  <Users className="w-4.5 h-4.5 text-muted-foreground" />
                </div>
                <div>
                  <h4 className="font-semibold text-sm mb-1">Agents</h4>
                  <p className="text-sm text-muted-foreground">
                    Bring forward-looking market intelligence into client conversations.
                  </p>
                </div>
              </div>

              <div className="flex items-start gap-4 p-4 rounded-lg border border-border bg-card">
                <div className="w-9 h-9 rounded-md bg-muted flex items-center justify-center shrink-0">
                  <Building2 className="w-4.5 h-4.5 text-muted-foreground" />
                </div>
                <div>
                  <h4 className="font-semibold text-sm mb-1">Institutions</h4>
                  <p className="text-sm text-muted-foreground">
                    Access property and neighborhood forecasts through API and bulk workflows.
                  </p>
                </div>
              </div>
            </div>
          </div>

          {/* Right: API messaging - monetization expansion */}
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
        </div>
      </div>
    </section>
  )
}
