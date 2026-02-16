import Link from 'next/link'
import { ArrowRight, BarChart3, Building2, Globe, Layers, Sparkles, TrendingUp, Terminal } from 'lucide-react'

export default function AboutPage() {
    return (
        <div className="overflow-auto h-screen">
            <div className="min-h-screen bg-background text-foreground font-sans">

                {/* Nav */}
                <header className="border-b border-border/40 bg-background/80 backdrop-blur-md sticky top-0 z-50">
                    <div className="max-w-6xl mx-auto px-6 h-16 flex items-center justify-between">
                        <span className="font-bold tracking-tight text-xl">Homecastr</span>
                        <nav className="flex items-center gap-6">
                            <a href="#institutional" className="text-sm font-medium text-muted-foreground hover:text-foreground transition-colors">For Institutions</a>
                            <Link href="/support" className="text-sm font-medium text-muted-foreground hover:text-foreground transition-colors">Support</Link>
                            <Link href="/" className="text-sm font-medium px-4 py-1.5 rounded-full bg-primary text-primary-foreground hover:bg-primary/90 transition-colors">
                                Open Dashboard
                            </Link>
                        </nav>
                    </div>
                </header>

                {/* ============================================================ */}
                {/* SECTION 1: HOMEOWNERS & BROKERS                              */}
                {/* ============================================================ */}

                {/* Hero — consumer */}
                <section className="max-w-6xl mx-auto px-6 py-24 md:py-32">
                    <div className="max-w-3xl">
                        <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-primary/10 text-primary text-[10px] font-bold uppercase tracking-widest mb-8">
                            <Sparkles className="w-3 h-3" />
                            AI-Powered Home Forecasts
                        </div>
                        <h1 className="text-5xl md:text-7xl font-bold tracking-tight leading-[1.1] mb-6">
                            Know what your home
                            <span className="text-primary"> will be worth</span>
                        </h1>
                        <p className="text-xl text-muted-foreground leading-relaxed mb-10 max-w-2xl">
                            Homecastr forecasts your home&apos;s future value. Not just one number, but a range
                            showing the likely low, expected, and high outcomes. Whether you&apos;re buying, selling,
                            or just curious, see where your home is headed.
                        </p>
                        <Link
                            href="/"
                            className="inline-flex items-center gap-2 px-6 py-3 rounded-xl bg-primary text-primary-foreground font-medium hover:bg-primary/90 transition-colors shadow-lg shadow-primary/20"
                        >
                            Look Up My Home
                            <ArrowRight className="w-4 h-4" />
                        </Link>
                    </div>
                </section>

                {/* How it works — plain language */}
                <section className="bg-muted/10 border-y border-border/40 py-24">
                    <div className="max-w-6xl mx-auto px-6">
                        <h2 className="text-3xl font-bold tracking-tight mb-16 text-center">How it works</h2>
                        <div className="grid md:grid-cols-3 gap-10">
                            <div className="space-y-4">
                                <div className="w-12 h-12 rounded-2xl bg-primary/10 flex items-center justify-center">
                                    <Layers className="w-6 h-6 text-primary" />
                                </div>
                                <h3 className="text-lg font-bold">We run thousands of &ldquo;what ifs&rdquo;</h3>
                                <p className="text-sm text-muted-foreground leading-relaxed">
                                    Our model simulates many possible futures for your neighborhood,
                                    accounting for interest rates, market trends, and local demand.
                                </p>
                            </div>
                            <div className="space-y-4">
                                <div className="w-12 h-12 rounded-2xl bg-primary/10 flex items-center justify-center">
                                    <BarChart3 className="w-6 h-6 text-primary" />
                                </div>
                                <h3 className="text-lg font-bold">You get a range, not a point</h3>
                                <p className="text-sm text-muted-foreground leading-relaxed">
                                    Instead of one number, we give you the conservative, most likely,
                                    and upside estimate, so you can plan with confidence.
                                </p>
                            </div>
                            <div className="space-y-4">
                                <div className="w-12 h-12 rounded-2xl bg-primary/10 flex items-center justify-center">
                                    <TrendingUp className="w-6 h-6 text-primary" />
                                </div>
                                <h3 className="text-lg font-bold">Down to your specific property</h3>
                                <p className="text-sm text-muted-foreground leading-relaxed">
                                    Most tools only forecast at the zip-code level. We forecast for your
                                    individual property, because the house next door can have a very different outlook.
                                </p>
                            </div>
                        </div>
                    </div>
                </section>

                {/* Coverage + consumer use cases */}
                <section className="max-w-6xl mx-auto px-6 py-24">
                    <div className="grid md:grid-cols-2 gap-16 items-center">
                        <div>
                            <h2 className="text-3xl font-bold tracking-tight mb-6">Starting with Houston</h2>
                            <p className="text-muted-foreground leading-relaxed mb-6">
                                We&apos;re live across the greater Houston metro, including the Heights, Katy, Sugar Land,
                                The Woodlands, and everywhere in between. More cities coming soon.
                            </p>
                            <div className="flex gap-8">
                                <div>
                                    <div className="text-3xl font-bold text-primary">1M+</div>
                                    <div className="text-sm text-muted-foreground">Homes Covered</div>
                                </div>
                                <div>
                                    <div className="text-3xl font-bold text-primary">5yr</div>
                                    <div className="text-sm text-muted-foreground">Forecast Window</div>
                                </div>
                            </div>
                        </div>
                        <div className="p-8 rounded-2xl glass-panel">
                            <div className="flex items-center gap-3 mb-6">
                                <Globe className="w-5 h-5 text-primary" />
                                <span className="font-bold">Who is this for?</span>
                            </div>
                            <ul className="space-y-3 text-sm text-muted-foreground">
                                <li className="flex items-start gap-2"><span className="text-primary mt-0.5">→</span> <strong>Home buyers</strong> wondering if now is the right time</li>
                                <li className="flex items-start gap-2"><span className="text-primary mt-0.5">→</span> <strong>Home sellers</strong> deciding when to list</li>
                                <li className="flex items-start gap-2"><span className="text-primary mt-0.5">→</span> <strong>Brokers &amp; agents</strong> advising clients with data</li>
                                <li className="flex items-start gap-2"><span className="text-primary mt-0.5">→</span> <strong>Homeowners</strong> tracking their biggest asset</li>
                                <li className="flex items-start gap-2"><span className="text-primary mt-0.5">→</span> <strong>Investors</strong> comparing neighborhoods at a glance</li>
                            </ul>
                        </div>
                    </div>
                </section>

                {/* Consumer CTA */}
                <section className="bg-muted/10 border-t border-border/40 py-20">
                    <div className="max-w-6xl mx-auto px-6 text-center">
                        <h2 className="text-3xl font-bold tracking-tight mb-4">Ready to see your forecast?</h2>
                        <p className="text-muted-foreground mb-8 max-w-lg mx-auto">
                            Look up any home in Houston and see where its value is headed.
                        </p>
                        <Link
                            href="/"
                            className="inline-flex items-center gap-2 px-6 py-3 rounded-xl bg-primary text-primary-foreground font-medium hover:bg-primary/90 transition-colors shadow-lg shadow-primary/20"
                        >
                            Open Dashboard
                            <ArrowRight className="w-4 h-4" />
                        </Link>
                    </div>
                </section>

                {/* ============================================================ */}
                {/* SECTION 2: INSTITUTIONAL / BUYSIDE / REIT / QUANT / RESEARCH */}
                {/* ============================================================ */}

                <div id="institutional" className="border-t-4 border-primary/20" />

                <section className="max-w-6xl mx-auto px-6 py-24 md:py-32">
                    <div className="max-w-3xl">
                        <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-primary/10 text-primary text-[10px] font-bold uppercase tracking-widest mb-8">
                            <Building2 className="w-3 h-3" />
                            For Real Estate Investors &amp; Operators
                        </div>
                        <h2 className="text-4xl md:text-5xl font-bold tracking-tight leading-[1.1] mb-6">
                            Portfolio-grade forecasts,
                            <span className="text-primary"> delivered via API</span>
                        </h2>
                        <p className="text-lg text-muted-foreground leading-relaxed max-w-2xl">
                            Homecastr&apos;s foundation model generates probabilistic price bands for every
                            residential property in the market. Access lot-level and neighborhood-level
                            forecasts via REST API, with accuracy as strong as 8% annual compounding error (MdAPE).
                        </p>
                    </div>
                </section>

                <section className="bg-muted/10 border-y border-border/40 py-24">
                    <div className="max-w-6xl mx-auto px-6">
                        <div className="grid md:grid-cols-3 gap-10">
                            <div className="space-y-4">
                                <div className="w-12 h-12 rounded-2xl bg-primary/10 flex items-center justify-center">
                                    <Terminal className="w-6 h-6 text-primary" />
                                </div>
                                <h3 className="text-lg font-bold">REST API</h3>
                                <p className="text-sm text-muted-foreground leading-relaxed">
                                    Programmatic access to lot-level and hex-level forecasts.
                                    JSON responses, API key auth, sub-second latency.
                                </p>
                            </div>
                            <div className="space-y-4">
                                <div className="w-12 h-12 rounded-2xl bg-primary/10 flex items-center justify-center">
                                    <BarChart3 className="w-6 h-6 text-primary" />
                                </div>
                                <h3 className="text-lg font-bold">Percentile Bands</h3>
                                <p className="text-sm text-muted-foreground leading-relaxed">
                                    P10/P50/P90 distributions across 1 to 5 year horizons.
                                    Calibrated from scenario ensembles, not point estimates.
                                </p>
                            </div>
                            <div className="space-y-4">
                                <div className="w-12 h-12 rounded-2xl bg-primary/10 flex items-center justify-center">
                                    <Globe className="w-6 h-6 text-primary" />
                                </div>
                                <h3 className="text-lg font-bold">H3 Spatial Index</h3>
                                <p className="text-sm text-muted-foreground leading-relaxed">
                                    Neighborhood metrics indexed by Uber H3.
                                    Opportunity scoring and aggregated growth signals at the block level.
                                </p>
                            </div>
                        </div>
                    </div>
                </section>

                {/* Institutional use cases */}
                <section className="max-w-6xl mx-auto px-6 py-24">
                    <div className="grid md:grid-cols-2 gap-16 items-center">
                        <div className="p-8 rounded-2xl glass-panel">
                            <div className="flex items-center gap-3 mb-6">
                                <Building2 className="w-5 h-5 text-primary" />
                                <span className="font-bold">Built for</span>
                            </div>
                            <ul className="space-y-3 text-sm text-muted-foreground">
                                <li className="flex items-start gap-2"><span className="text-primary mt-0.5">→</span> <strong>SFR operators</strong> managing portfolios of 50 to 5,000+ doors</li>
                                <li className="flex items-start gap-2"><span className="text-primary mt-0.5">→</span> <strong>Property investors</strong> screening acquisition and disposition targets</li>
                                <li className="flex items-start gap-2"><span className="text-primary mt-0.5">→</span> <strong>Lenders</strong> assessing collateral risk at origination</li>
                                <li className="flex items-start gap-2"><span className="text-primary mt-0.5">→</span> <strong>Institutional desks</strong> integrating residential price signals into risk pipelines</li>
                            </ul>
                        </div>
                        <div>
                            <h2 className="text-3xl font-bold tracking-tight mb-6">Accuracy you can audit</h2>
                            <p className="text-muted-foreground leading-relaxed mb-4">
                                Forecast accuracy is measured using industry-standard MdAPE
                                (Median Absolute Percentage Error), with results as strong as 8% annual
                                compounding error. Metrics are available by geography and forecast horizon.
                            </p>
                            <p className="text-muted-foreground leading-relaxed">
                                All forecasts include interpretable percentile bands and
                                regime-aware attributions. No black-box point estimates.
                            </p>
                        </div>
                    </div>
                </section>

                {/* Institutional CTA */}
                <section className="bg-muted/10 border-y border-border/40 py-20">
                    <div className="max-w-6xl mx-auto px-6 text-center">
                        <h2 className="text-3xl font-bold tracking-tight mb-4">Get API access</h2>
                        <p className="text-muted-foreground mb-8 max-w-lg mx-auto">
                            Generate a free API key instantly. No sales call required.
                        </p>
                        <div className="flex gap-4 justify-center">
                            <Link
                                href="/api-docs#get-key"
                                className="inline-flex items-center gap-2 px-6 py-3 rounded-xl bg-primary text-primary-foreground font-medium hover:bg-primary/90 transition-colors shadow-lg shadow-primary/20"
                            >
                                Get Free API Key
                                <ArrowRight className="w-4 h-4" />
                            </Link>
                            <Link
                                href="/api-docs"
                                className="inline-flex items-center gap-2 px-6 py-3 rounded-xl bg-muted/30 border border-border/50 font-medium hover:bg-muted/50 transition-colors"
                            >
                                API Documentation
                            </Link>
                        </div>
                    </div>
                </section>

                {/* Footer */}
                <footer className="py-12">
                    <div className="max-w-6xl mx-auto px-6 flex flex-col md:flex-row justify-between items-center gap-6">
                        <div className="text-sm text-muted-foreground">© 2026 Homecastr. All rights reserved.</div>
                        <div className="flex gap-8 text-sm font-medium text-muted-foreground">
                            <Link href="/privacy" className="hover:text-foreground">Privacy</Link>
                            <Link href="/terms" className="hover:text-foreground">Terms</Link>
                            <Link href="/support" className="hover:text-foreground">Support</Link>
                            <Link href="/api-docs" className="hover:text-foreground">API</Link>
                        </div>
                    </div>
                </footer>
            </div>
        </div>
    )
}
