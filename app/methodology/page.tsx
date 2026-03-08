import type { Metadata } from "next"
import Link from "next/link"
import { ArrowLeft } from "lucide-react"

export const metadata: Metadata = {
    title: "Methodology | Homecastr",
    description: "How Homecastr calculates probabilistic home price forecasts, uncertainty bands, and opportunity scores.",
}

export default function MethodologyPage() {
    return (
        <div className="min-h-screen bg-background text-foreground font-sans">
            <header className="py-4 px-6 md:px-12 border-b border-border/40">
                <Link href="/" className="inline-flex items-center gap-2 text-sm font-medium text-muted-foreground hover:text-foreground transition-colors">
                    <ArrowLeft className="w-4 h-4" /> Back to Homecastr
                </Link>
            </header>
            <main className="max-w-3xl mx-auto py-12 px-6">
                <h1 className="text-4xl font-extrabold tracking-tight mb-8">Methodology & Limitations</h1>

                <div className="prose prose-neutral dark:prose-invert max-w-none space-y-8">
                    <section>
                        <h2 className="text-2xl font-bold mb-4">How We Forecast at the Parcel Level</h2>
                        <p className="text-lg text-muted-foreground">Homecastr operates on the premise that single-point estimates (e.g., "this home will be worth exactly $450,000 next year") are fundamentally misleading. Real estate markets are subject to macroeconomic shifts, hyper-local zoning changes, and statistical noise. Instead, we provide <strong>probabilistic forecasts</strong> for every parcel.</p>
                    </section>

                    <section>
                        <h2 className="text-2xl font-bold mb-4">Understanding the Uncertainty Bands (P10, P50, P90)</h2>
                        <p className="text-muted-foreground">For every forecast horizon, we output three critical numbers:</p>
                        <ul className="list-disc pl-5 mt-4 space-y-4 text-muted-foreground">
                            <li><strong className="text-foreground">P50 (Median Expected Value):</strong> The most likely future value. There is a 50% chance the actual value will be higher, and a 50% chance it will be lower.</li>
                            <li><strong className="text-foreground">P10 (Downside Scenario):</strong> A conservative estimate. There is only a 10% chance the property will fall below this value. Strong for risk analysis.</li>
                            <li><strong className="text-foreground">P90 (Upside Scenario):</strong> An optimistic estimate. There is only a 10% chance the property will exceed this value.</li>
                        </ul>
                        <p className="mt-6 text-muted-foreground">The spread between P10 and P90 is the <em>Uncertainty Band</em>. A narrow band means our model is highly confident (usually due to highly uniform comparable sales). A wide band signifies high volatility or unique property characteristics.</p>
                    </section>

                    <section>
                        <h2 className="text-2xl font-bold mb-4">Data Sources and Refresh Cadence</h2>
                        <p className="text-muted-foreground">Our foundation is built upon public and proprietary datasets, primarily:</p>
                        <ul className="list-disc pl-5 mt-4 space-y-4 text-muted-foreground">
                            <li><strong className="text-foreground">County Appraisal Districts (e.g., HCAD):</strong> Providing the base truth for parcel geometries, historical appraised values, and structural characteristics (sqft, year built, condition).</li>
                            <li><strong className="text-foreground">Market Transaction Data:</strong> Aggregated sales records to calibrate appraisals to actual market clearing prices.</li>
                            <li><strong className="text-foreground">Macro-Demographics:</strong> American Community Survey (ACS) and localized census data to measure structural demand shifts.</li>
                        </ul>
                        <p className="mt-6 text-muted-foreground"><strong className="text-foreground">Model Version:</strong> v11 (Current). <strong className="text-foreground">Refresh Cadence:</strong> Models are recalibrated quarterly to capture seasonal variance and immediate macroeconomic shifts.</p>
                    </section>

                    <section>
                        <h2 className="text-2xl font-bold mb-4">Limitations</h2>
                        <p className="text-muted-foreground">While our models are rigorously backtested, they are not financial advice. Known limitations include:</p>
                        <ul className="list-disc pl-5 mt-4 space-y-4 text-muted-foreground">
                            <li><strong className="text-foreground">Off-market renovations:</strong> If a home is gutted and remodeled without public permits, our model cannot instantly see the value increase, leading to an under-forecast.</li>
                            <li><strong className="text-foreground">Unprecedented Macro Shocks:</strong> Like all statistical models, unprecedented black-swan events (e.g., sudden hyper-inflation or localized natural disasters) will push actuals outside the P10-P90 bounds.</li>
                        </ul>
                    </section>
                </div>
            </main>
        </div>
    )
}
