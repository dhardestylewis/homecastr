import type { Metadata } from "next"
import Link from "next/link"
import { ArrowLeft, CheckCircle2 } from "lucide-react"

export const metadata: Metadata = {
    title: "Houston Coverage | Homecastr",
    description: "Explore our parcel-level residential forecasting coverage across Houston, TX and Harris County.",
}

export default function HoustonCoveragePage() {
    return (
        <div className="min-h-screen bg-background text-foreground font-sans">
            <header className="py-4 px-6 md:px-12 border-b border-border/40">
                <Link href="/" className="inline-flex items-center gap-2 text-sm font-medium text-muted-foreground hover:text-foreground transition-colors">
                    <ArrowLeft className="w-4 h-4" /> Back to Homecastr
                </Link>
            </header>
            <main className="max-w-5xl mx-auto py-12 px-6">
                <div className="flex flex-col md:flex-row gap-12 items-start">
                    <div className="flex-1">
                        <h1 className="text-4xl font-extrabold tracking-tight mb-6">Houston, TX Coverage</h1>
                        <p className="text-lg text-muted-foreground mb-8 text-balance">
                            Houston serves as the flagship market for Homecastr's v11 forecasting models. We provide comprehensive, parcel-level coverage across Harris County and surrounding municipalities.
                        </p>

                        <div className="bg-muted/30 border border-border rounded-xl p-6 mb-8">
                            <h2 className="text-xl font-bold mb-4">Coverage Stats</h2>
                            <ul className="space-y-4">
                                <li className="flex items-center gap-3">
                                    <CheckCircle2 className="w-6 h-6 text-lime-500" />
                                    <span className="text-lg"><strong>1.2 Million+</strong> Residential Parcels</span>
                                </li>
                                <li className="flex items-center gap-3">
                                    <CheckCircle2 className="w-6 h-6 text-lime-500" />
                                    <span className="text-lg"><strong>140+</strong> Zip Codes</span>
                                </li>
                                <li className="flex items-center gap-3">
                                    <CheckCircle2 className="w-6 h-6 text-lime-500" />
                                    <span className="text-lg"><strong>20+ Years</strong> of Historical Training Data</span>
                                </li>
                            </ul>
                        </div>

                        <div className="prose prose-neutral dark:prose-invert max-w-none">
                            <h2 className="text-2xl font-bold mb-4 mt-8">How to Use the Data</h2>
                            <p className="text-muted-foreground mb-4">
                                Our Houston data can be accessed via the <Link href="/app" className="underline font-medium hover:text-primary">Interactive Map</Link>. Currently, we track variations across inside-the-loop density (Montrose, Heights), suburban sprawl (Katy, Cypress), and master-planned communities (The Woodlands).
                            </p>
                        </div>

                        <div className="mt-8">
                            <Link href="/houston-home-price-forecast" className="inline-flex items-center justify-center bg-primary text-primary-foreground px-6 py-3 rounded-md font-bold text-sm shadow-sm hover:bg-primary/90 transition-colors">
                                View Houston Neighborhoods
                            </Link>
                        </div>
                    </div>

                    <div className="w-full md:w-[350px] shrink-0 bg-background border border-border rounded-xl p-8 shadow-sm top-8 sticky">
                        <h3 className="font-bold text-xl mb-6">Supported Counties</h3>
                        <ul className="space-y-6">
                            <li className="flex gap-4">
                                <div className="w-2 h-2 rounded-full bg-lime-500 mt-2 shrink-0"></div>
                                <div>
                                    <div className="font-bold">Harris County</div>
                                    <div className="text-sm text-muted-foreground mt-1">Full parcel and H3 hex coverage. Active v11 models.</div>
                                </div>
                            </li>
                            <li className="flex gap-4">
                                <div className="w-2 h-2 rounded-full bg-muted-foreground mt-2 shrink-0"></div>
                                <div>
                                    <div className="font-bold text-muted-foreground line-through">Fort Bend County</div>
                                    <div className="text-sm text-muted-foreground mt-1">Coming Q3 2026.</div>
                                </div>
                            </li>
                            <li className="flex gap-4">
                                <div className="w-2 h-2 rounded-full bg-muted-foreground mt-2 shrink-0"></div>
                                <div>
                                    <div className="font-bold text-muted-foreground line-through">Montgomery County</div>
                                    <div className="text-sm text-muted-foreground mt-1">Coming Q3 2026.</div>
                                </div>
                            </li>
                        </ul>
                    </div>
                </div>
            </main>
        </div>
    )
}
