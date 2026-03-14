import Link from "next/link"
import { Shield, Award, FileText, ChevronRight } from "lucide-react"

export function TrustSection() {
  return (
    <section className="py-20 md:py-28 border-y border-border bg-muted/20">
      <div className="max-w-6xl mx-auto px-6">
        <div className="grid md:grid-cols-2 gap-12 md:gap-16 items-center">
          {/* Left: Trust messaging */}
          <div>
            <h2 className="text-2xl md:text-3xl font-bold tracking-tight mb-4">
              Built to be audited.
            </h2>
            <p className="text-muted-foreground leading-relaxed mb-6">
              Forecast performance is reported using industry-standard MdAPE, with metrics available by geography 
              and forecast horizon. Results are as strong as 8% annual compounding MdAPE in select evaluations.
            </p>
            <p className="text-muted-foreground leading-relaxed mb-8">
              Every forecast includes interpretable percentile bands and regime-aware attributions. 
              No black-box point estimates.
            </p>
            
            <Link
              href="/methodology"
              className="inline-flex items-center gap-2 text-sm font-medium hover:underline underline-offset-4"
            >
              Read the Methodology
              <ChevronRight className="w-4 h-4" />
            </Link>
          </div>

          {/* Right: Trust signals - reordered for skeptical users */}
          <div className="space-y-4">
            <div className="p-5 rounded-lg border border-border bg-card">
              <div className="flex items-start gap-4">
                <div className="w-10 h-10 rounded-md bg-muted flex items-center justify-center shrink-0">
                  <Shield className="w-5 h-5 text-muted-foreground" />
                </div>
                <div>
                  <h3 className="font-semibold mb-1">Backtested Forecasts</h3>
                  <p className="text-sm text-muted-foreground">
                    Historical validation across multiple market cycles.
                  </p>
                </div>
              </div>
            </div>

            <div className="p-5 rounded-lg border border-border bg-card">
              <div className="flex items-start gap-4">
                <div className="w-10 h-10 rounded-md bg-muted flex items-center justify-center shrink-0">
                  <FileText className="w-5 h-5 text-muted-foreground" />
                </div>
                <div>
                  <h3 className="font-semibold mb-1">Accuracy Reporting</h3>
                  <p className="text-sm text-muted-foreground">
                    Error metrics published by geography and horizon.
                  </p>
                </div>
              </div>
            </div>

            <div className="p-5 rounded-lg border border-border bg-card">
              <div className="flex items-start gap-4">
                <div className="w-10 h-10 rounded-md bg-muted flex items-center justify-center shrink-0">
                  <FileText className="w-5 h-5 text-muted-foreground" />
                </div>
                <div>
                  <h3 className="font-semibold mb-1">Probabilistic Output</h3>
                  <p className="text-sm text-muted-foreground">
                    Honest uncertainty quantification, not false precision.
                  </p>
                </div>
              </div>
            </div>

            <div className="p-5 rounded-lg border border-border bg-card">
              <div className="flex items-start gap-4">
                <div className="w-10 h-10 rounded-md bg-muted flex items-center justify-center shrink-0">
                  <Award className="w-5 h-5 text-muted-foreground" />
                </div>
                <div>
                  <h3 className="font-semibold mb-1">Research Foundation</h3>
                  <p className="text-sm text-muted-foreground">
                    Built by a team with backgrounds in large-scale modeling, geospatial ML, and applied research.
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
