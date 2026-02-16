import Link from 'next/link'
import { ArrowRight, BarChart3, Globe, Layers, Sparkles, TrendingUp } from 'lucide-react'

export default function AboutPage() {
    return (
        <div className="overflow-auto h-screen">
            <div className="min-h-screen bg-background text-foreground font-sans">

                {/* Nav */}
                <header className="border-b border-border/40 bg-background/80 backdrop-blur-md sticky top-0 z-50">
                    <div className="max-w-6xl mx-auto px-6 h-16 flex items-center justify-between">
                        <span className="font-bold tracking-tight text-xl">Homecastr</span>
                        <nav className="flex items-center gap-6">
                            <Link href="/api-docs" className="text-sm font-medium text-muted-foreground hover:text-foreground transition-colors">API</Link>
                            <Link href="/support" className="text-sm font-medium text-muted-foreground hover:text-foreground transition-colors">Support</Link>
                            <Link href="/" className="text-sm font-medium px-4 py-1.5 rounded-full bg-primary text-primary-foreground hover:bg-primary/90 transition-colors">
                                Open Dashboard
                            </Link>
                        </nav>
                    </div>
                </header>

                {/* Hero */}
                <section className="max-w-6xl mx-auto px-6 py-24 md:py-32">
                    <div className="max-w-3xl">
                        <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-primary/10 text-primary text-[10px] font-bold uppercase tracking-widest mb-8">
                            <Sparkles className="w-3 h-3" />
                            Foundation Model for Real Estate
                        </div>
                        <h1 className="text-5xl md:text-7xl font-bold tracking-tight leading-[1.1] mb-6">
                            See what your home
                            <span className="text-primary"> will be worth</span>
                        </h1>
                        <p className="text-xl text-muted-foreground leading-relaxed mb-10 max-w-2xl">
                            Homecastr is a foundation model that forecasts value at the home level.
                            We provide probable price bands to help you with your home buying and selling decisions,
                            generated from many scenarios of the future.
                        </p>
                        <div className="flex gap-4">
                            <Link
                                href="/"
                                className="inline-flex items-center gap-2 px-6 py-3 rounded-xl bg-primary text-primary-foreground font-medium hover:bg-primary/90 transition-colors shadow-lg shadow-primary/20"
                            >
                                Explore the Map
                                <ArrowRight className="w-4 h-4" />
                            </Link>
                            <Link
                                href="/api-docs"
                                className="inline-flex items-center gap-2 px-6 py-3 rounded-xl bg-muted/30 border border-border/50 font-medium hover:bg-muted/50 transition-colors"
                            >
                                API Docs
                            </Link>
                        </div>
                    </div>
                </section>

                {/* Features */}
                <section className="bg-muted/10 border-y border-border/40 py-24">
                    <div className="max-w-6xl mx-auto px-6">
                        <h2 className="text-3xl font-bold tracking-tight mb-16 text-center">How it works</h2>
                        <div className="grid md:grid-cols-3 gap-10">
                            <div className="space-y-4">
                                <div className="w-12 h-12 rounded-2xl bg-primary/10 flex items-center justify-center">
                                    <Layers className="w-6 h-6 text-primary" />
                                </div>
                                <h3 className="text-lg font-bold">Many Scenarios</h3>
                                <p className="text-sm text-muted-foreground leading-relaxed">
                                    We simulate many possible futures for each property — capturing
                                    market shifts, economic conditions, and neighborhood dynamics.
                                </p>
                            </div>
                            <div className="space-y-4">
                                <div className="w-12 h-12 rounded-2xl bg-primary/10 flex items-center justify-center">
                                    <BarChart3 className="w-6 h-6 text-primary" />
                                </div>
                                <h3 className="text-lg font-bold">Price Bands</h3>
                                <p className="text-sm text-muted-foreground leading-relaxed">
                                    Instead of a single number, we give you P10/P50/P90 price bands —
                                    so you know the downside, the expected, and the upside.
                                </p>
                            </div>
                            <div className="space-y-4">
                                <div className="w-12 h-12 rounded-2xl bg-primary/10 flex items-center justify-center">
                                    <TrendingUp className="w-6 h-6 text-primary" />
                                </div>
                                <h3 className="text-lg font-bold">Lot-Level Accuracy</h3>
                                <p className="text-sm text-muted-foreground leading-relaxed">
                                    Forecasts resolve to individual properties, not just zip codes.
                                    Accuracy is measured using industry-standard MdAPE.
                                </p>
                            </div>
                        </div>
                    </div>
                </section>

                {/* Coverage */}
                <section className="max-w-6xl mx-auto px-6 py-24">
                    <div className="grid md:grid-cols-2 gap-16 items-center">
                        <div>
                            <h2 className="text-3xl font-bold tracking-tight mb-6">Starting with Houston</h2>
                            <p className="text-muted-foreground leading-relaxed mb-6">
                                We&apos;re launching with comprehensive coverage of the greater Houston metro area —
                                one of the largest and most dynamic housing markets in the country. More markets coming soon.
                            </p>
                            <div className="flex gap-8">
                                <div>
                                    <div className="text-3xl font-bold text-primary">1M+</div>
                                    <div className="text-sm text-muted-foreground">Properties</div>
                                </div>
                                <div>
                                    <div className="text-3xl font-bold text-primary">5yr</div>
                                    <div className="text-sm text-muted-foreground">Forecast Horizon</div>
                                </div>
                                <div>
                                    <div className="text-3xl font-bold text-primary">P10–P90</div>
                                    <div className="text-sm text-muted-foreground">Price Bands</div>
                                </div>
                            </div>
                        </div>
                        <div className="p-8 rounded-2xl glass-panel">
                            <div className="flex items-center gap-3 mb-6">
                                <Globe className="w-5 h-5 text-primary" />
                                <span className="font-bold">Use Cases</span>
                            </div>
                            <ul className="space-y-3 text-sm text-muted-foreground">
                                <li className="flex items-start gap-2"><span className="text-primary mt-0.5">→</span> Home buyers evaluating long-term value</li>
                                <li className="flex items-start gap-2"><span className="text-primary mt-0.5">→</span> Sellers timing their listing</li>
                                <li className="flex items-start gap-2"><span className="text-primary mt-0.5">→</span> Investors comparing neighborhoods</li>
                                <li className="flex items-start gap-2"><span className="text-primary mt-0.5">→</span> Lenders assessing collateral risk</li>
                                <li className="flex items-start gap-2"><span className="text-primary mt-0.5">→</span> Developers identifying growth corridors</li>
                            </ul>
                        </div>
                    </div>
                </section>

                {/* CTA */}
                <section className="bg-muted/10 border-y border-border/40 py-20">
                    <div className="max-w-6xl mx-auto px-6 text-center">
                        <h2 className="text-3xl font-bold tracking-tight mb-4">Ready to explore?</h2>
                        <p className="text-muted-foreground mb-8 max-w-lg mx-auto">
                            Open the interactive dashboard to browse forecasts, or integrate directly via our REST API.
                        </p>
                        <div className="flex gap-4 justify-center">
                            <Link
                                href="/"
                                className="inline-flex items-center gap-2 px-6 py-3 rounded-xl bg-primary text-primary-foreground font-medium hover:bg-primary/90 transition-colors shadow-lg shadow-primary/20"
                            >
                                Open Dashboard
                                <ArrowRight className="w-4 h-4" />
                            </Link>
                            <Link
                                href="/api-docs#get-key"
                                className="inline-flex items-center gap-2 px-6 py-3 rounded-xl bg-muted/30 border border-border/50 font-medium hover:bg-muted/50 transition-colors"
                            >
                                Get Free API Key
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
