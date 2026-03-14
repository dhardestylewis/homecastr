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
              Accuracy you can audit.
            </h2>
            <p className="text-muted-foreground leading-relaxed mb-6">
              Forecast accuracy is measured using industry-standard MdAPE (Median Absolute Percentage Error), 
              with results as strong as 8% annual compounding error. All metrics are available by geography 
              and forecast horizon.
            </p>
            <p className="text-muted-foreground leading-relaxed mb-8">
              Every forecast includes interpretable percentile bands and regime-aware attributions. 
              No black-box point estimates.
            </p>
            
            <Link
              href="/methodology"
              className="inline-flex items-center gap-2 text-sm font-medium hover:underline underline-offset-4"
            >
              Read our methodology
              <ChevronRight className="w-4 h-4" />
            </Link>
          </div>

          {/* Right: Trust signals */}
          <div className="space-y-4">
            <div className="p-5 rounded-lg border border-border bg-card">
              <div className="flex items-start gap-4">
                <div className="w-10 h-10 rounded-md bg-muted flex items-center justify-center shrink-0">
                  <Shield className="w-5 h-5 text-muted-foreground" />
                </div>
                <div>
                  <h3 className="font-semibold mb-1">Backtested Forecasts</h3>
                  <p className="text-sm text-muted-foreground">
                    Historical validation across multiple market cycles. Published error metrics for every geography.
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
                    Built by researchers from TACC and Columbia. Published in peer-reviewed journals.
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
                    Honest uncertainty quantification. We tell you what we don&apos;t know.
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
