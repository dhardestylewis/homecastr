import type { Metadata } from "next"
import Link from "next/link"
import { ArrowLeft } from "lucide-react"

export const metadata: Metadata = {
    title: "FAQ & Glossary | Homecastr",
    description: "Frequently asked questions and technical glossary for Homecastr's forecasting platform.",
}

export default function FAQPage() {
    return (
        <div className="h-screen overflow-auto bg-background text-foreground font-sans">
            <header className="py-4 px-6 md:px-12 border-b border-border/40">
                <Link href="/" className="inline-flex items-center gap-2 text-sm font-medium text-muted-foreground hover:text-foreground transition-colors">
                    <ArrowLeft className="w-4 h-4" /> Back to Homecastr
                </Link>
            </header>
            <main className="max-w-3xl mx-auto py-12 px-6">
                <h1 className="text-4xl font-extrabold tracking-tight mb-4">FAQ & Glossary</h1>
                <p className="text-lg text-muted-foreground mb-12">Definitions and answers to help you navigate our probabilistic forecasts.</p>

                <div className="space-y-16">
                    <section>
                        <h2 className="text-2xl font-bold mb-8 border-b border-border/50 pb-4">Glossary of Terms</h2>
                        <div className="grid gap-8 sm:grid-cols-2">
                            <div className="bg-muted/20 p-6 rounded-lg border border-border/50">
                                <h3 className="text-lg font-bold text-primary mb-2">Uncertainty Band</h3>
                                <p className="text-sm text-muted-foreground">The statistical range between our P10 (conservative) and P90 (optimistic) forecasts. A wide band indicates the model sees multiple possible outcomes, usually due to high neighborhood volatility or unique property features.</p>
                            </div>
                            <div className="bg-muted/20 p-6 rounded-lg border border-border/50">
                                <h3 className="text-lg font-bold text-primary mb-2">P50 (Median Forecast)</h3>
                                <p className="text-sm text-muted-foreground">The middle-of-the-road expectation. Statistically, there is an equal chance the eventual value will land above or below this number.</p>
                            </div>
                            <div className="bg-muted/20 p-6 rounded-lg border border-border/50">
                                <h3 className="text-lg font-bold text-primary mb-2">Parcel vs. Neighborhood</h3>
                                <p className="text-sm text-muted-foreground">A <strong>parcel</strong> is an individual lot/property. A <strong>neighborhood</strong> (often represented by a Census Tract or ZCTA) is an aggregation. Homecastr calculates at the parcel level and aggregates up.</p>
                            </div>
                            <div className="bg-muted/20 p-6 rounded-lg border border-border/50">
                                <h3 className="text-lg font-bold text-primary mb-2">H3 Hex</h3>
                                <p className="text-sm text-muted-foreground">A spatial indexing system originally developed by Uber. We use H3 resolution 9 hexagons to visualize localized density and growth trends without exposing individualized data too early.</p>
                            </div>
                        </div>
                    </section>

                    <section>
                        <h2 className="text-2xl font-bold mb-8 border-b border-border/50 pb-4">Frequently Asked Questions</h2>
                        <div className="space-y-8">
                            <div>
                                <h3 className="text-xl font-bold">Why not just give me one number?</h3>
                                <p className="text-muted-foreground mt-2 text-balance leading-relaxed">
                                    Because the future is not a single number. If you are an investor buying a property, knowing that the "average" outcome is a $20k profit is less useful than knowing there is a 10% chance you lose $50k. Probability bands let you underwrite risk.
                                </p>
                            </div>
                            <div>
                                <h3 className="text-xl font-bold">How often is the data updated?</h3>
                                <p className="text-muted-foreground mt-2 leading-relaxed">
                                    We process new transaction data continuously, but our core structural models undergo a full recalibration every quarter. Look for the "Model Version" in the app for exact calibration dates.
                                </p>
                            </div>
                            <div>
                                <h3 className="text-xl font-bold">What areas do you cover?</h3>
                                <p className="text-muted-foreground mt-2 leading-relaxed">
                                    Our V11 model covers residential properties across the United States, including Texas, New York, Florida, and more. Browse the <a href="/forecasts" className="text-primary hover:underline">forecast directory</a> to find your state, county, or neighborhood.
                                </p>
                            </div>
                            <div>
                                <h3 className="text-xl font-bold">How can I access the raw data?</h3>
                                <p className="text-muted-foreground mt-2 leading-relaxed">
                                    Raw data exports and API access are available for enterprise clients. Please reach out to our team at sales@homecastr.com.
                                </p>
                            </div>
                        </div>
                    </section>
                </div>
            </main>
        </div>
    )
}
