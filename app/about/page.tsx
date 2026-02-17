import Link from 'next/link'
import { ArrowRight, BarChart3, Building2, Globe, Layers, Sparkles, TrendingUp, Terminal } from 'lucide-react'
import { FanChart } from '@/components/fan-chart'

// Static demo data for landing page FanChart — realistic Houston ~$300K property
const DEMO_FAN_DATA = {
    years: [2026, 2027, 2028, 2029, 2030],
    p10: [285000, 278000, 274000, 271000, 268000],
    p50: [295000, 305000, 318000, 330000, 345000],
    p90: [310000, 338000, 365000, 395000, 425000],
    y_med: [293000, 302000, 314000, 326000, 340000],
}
const DEMO_HISTORICAL = [220000, 235000, 265000, 290000, 305000, 298000, 292000]

// Inline SVG: Monte Carlo spaghetti lines
function MonteCarloSVG() {
    return (
        <svg viewBox="0 0 120 60" className="w-full h-16 mt-3" preserveAspectRatio="xMidYMid meet">
            <path d="M10 45 Q30 40 50 35 Q70 25 90 18 L110 12" fill="none" stroke="currentColor" strokeOpacity={0.12} strokeWidth={1.5} />
            <path d="M10 45 Q30 42 50 40 Q70 38 90 42 L110 48" fill="none" stroke="currentColor" strokeOpacity={0.12} strokeWidth={1.5} />
            <path d="M10 45 Q30 38 50 30 Q70 28 90 25 L110 20" fill="none" stroke="currentColor" strokeOpacity={0.12} strokeWidth={1.5} />
            <path d="M10 45 Q30 43 50 42 Q70 35 90 30 L110 28" fill="none" stroke="currentColor" strokeOpacity={0.12} strokeWidth={1.5} />
            <path d="M10 45 Q30 36 50 28 Q70 22 90 15 L110 8" fill="none" stroke="currentColor" strokeOpacity={0.12} strokeWidth={1.5} />
            <path d="M10 45 Q30 44 50 46 Q70 48 90 50 L110 52" fill="none" stroke="currentColor" strokeOpacity={0.12} strokeWidth={1.5} />
            {/* P50 median line */}
            <path d="M10 45 Q30 40 50 34 Q70 30 90 26 L110 22" fill="none" stroke="hsl(var(--primary))" strokeWidth={2} strokeOpacity={0.6} />
            {/* Fan area hint */}
            <path d="M10 45 Q30 38 50 30 Q70 25 90 18 L110 12 L110 48 Q90 46 70 42 Q50 42 30 43 L10 45 Z" fill="hsl(var(--primary))" fillOpacity={0.08} />
        </svg>
    )
}

// Inline SVG: P10/P50/P90 range bars
function RangeBarsSVG() {
    return (
        <svg viewBox="0 0 120 60" className="w-full h-16 mt-3" preserveAspectRatio="xMidYMid meet">
            {/* Range bar background */}
            <rect x="15" y="18" width="90" height="24" rx="4" fill="hsl(var(--primary))" fillOpacity={0.08} />
            {/* P10 marker */}
            <line x1="25" y1="14" x2="25" y2="46" stroke="currentColor" strokeOpacity={0.3} strokeWidth={1.5} />
            <text x="25" y="55" textAnchor="middle" className="text-[7px] fill-muted-foreground font-mono">$268K</text>
            <text x="25" y="12" textAnchor="middle" className="text-[6px] fill-muted-foreground">P10</text>
            {/* P50 marker */}
            <line x1="60" y1="14" x2="60" y2="46" stroke="hsl(var(--primary))" strokeWidth={2.5} />
            <text x="60" y="55" textAnchor="middle" className="text-[7px] fill-primary font-mono font-bold">$345K</text>
            <text x="60" y="12" textAnchor="middle" className="text-[6px] fill-primary font-bold">P50</text>
            {/* P90 marker */}
            <line x1="95" y1="14" x2="95" y2="46" stroke="currentColor" strokeOpacity={0.3} strokeWidth={1.5} />
            <text x="95" y="55" textAnchor="middle" className="text-[7px] fill-muted-foreground font-mono">$425K</text>
            <text x="95" y="12" textAnchor="middle" className="text-[6px] fill-muted-foreground">P90</text>
        </svg>
    )
}

// Inline SVG: Lot grid (property parcels with varying forecast colors)
function LotGridSVG() {
    const lots = [
        { x: 5, y: 5, w: 22, h: 22, opacity: 0.5 },
        { x: 30, y: 5, w: 28, h: 22, opacity: 0.7 },
        { x: 61, y: 5, w: 22, h: 22, opacity: 0.2 },
        { x: 86, y: 5, w: 28, h: 22, opacity: 0.6 },
        { x: 5, y: 30, w: 28, h: 25, opacity: 0.3 },
        { x: 36, y: 30, w: 22, h: 25, opacity: 0.8 },
        { x: 61, y: 30, w: 28, h: 25, opacity: 0.15 },
        { x: 92, y: 30, w: 22, h: 25, opacity: 0.45 },
    ]
    return (
        <svg viewBox="0 0 120 60" className="w-full h-16 mt-3" preserveAspectRatio="xMidYMid meet">
            {lots.map((l, i) => (
                <rect key={i} x={l.x} y={l.y} width={l.w} height={l.h} rx={2}
                    fill="hsl(var(--primary))" fillOpacity={l.opacity}
                    stroke="hsl(var(--primary))" strokeWidth={0.5} strokeOpacity={0.25} />
            ))}
        </svg>
    )
}

// Inline SVG: SHAP-style attribution bars (explainability visual)
function AttributionBarSVG() {
    return (
        <svg viewBox="0 0 120 60" className="w-full h-16 mt-3" preserveAspectRatio="xMidYMid meet">
            {/* Baseline */}
            <line x1="60" y1="4" x2="60" y2="56" stroke="currentColor" strokeOpacity={0.15} strokeWidth={0.5} strokeDasharray="2 2" />
            {/* Positive drivers (right of baseline) */}
            <rect x="60" y="6" width="35" height="8" rx="2" fill="hsl(142 70% 45%)" fillOpacity={0.6} />
            <text x="58" y="13" textAnchor="end" className="text-[6px] fill-muted-foreground">Rate cuts</text>
            <rect x="60" y="18" width="22" height="8" rx="2" fill="hsl(142 70% 45%)" fillOpacity={0.45} />
            <text x="58" y="25" textAnchor="end" className="text-[6px] fill-muted-foreground">Demand</text>
            {/* Negative driver (left of baseline) */}
            <rect x="38" y="30" width="22" height="8" rx="2" fill="hsl(0 70% 55%)" fillOpacity={0.5} />
            <text x="58" y="37" textAnchor="end" className="text-[6px] fill-muted-foreground">Supply</text>
            {/* Net result */}
            <rect x="60" y="44" width="28" height="8" rx="2" fill="hsl(var(--primary))" fillOpacity={0.7} />
            <text x="58" y="51" textAnchor="end" className="text-[6px] fill-primary font-bold">Net ↑</text>
        </svg>
    )
}

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

                {/* Hero — consumer (2-column: copy left, demo chart right) */}
                <section className="max-w-6xl mx-auto px-6 py-24 md:py-32">
                    <div className="grid md:grid-cols-2 gap-12 items-center">
                        <div>
                            <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-primary/10 text-primary text-[10px] font-bold uppercase tracking-widest mb-8">
                                <Sparkles className="w-3 h-3" />
                                AI-Powered Home Forecasts
                            </div>
                            <h1 className="text-5xl md:text-7xl font-bold tracking-tight leading-[1.1] mb-6">
                                See where your home&apos;s
                                <span className="text-primary"> value is headed</span>
                            </h1>
                            <p className="text-xl text-muted-foreground leading-relaxed mb-10 max-w-2xl">
                                Zillow tells you what a home is worth today. Homecastr shows you where it&apos;s
                                going. Down to your specific property, not just a zip code. With conservative,
                                expected, and upside scenarios so you can plan with confidence.
                            </p>
                            <Link
                                href="/"
                                className="inline-flex items-center gap-2 px-6 py-3 rounded-xl bg-primary text-primary-foreground font-medium hover:bg-primary/90 transition-colors shadow-lg shadow-primary/20"
                            >
                                Look Up My Home
                                <ArrowRight className="w-4 h-4" />
                            </Link>
                        </div>
                        {/* Demo FanChart — static data, no API calls */}
                        <div className="hidden md:block">
                            <div className="rounded-2xl glass-panel p-4">
                                <div className="flex items-center gap-2 mb-2">
                                    <div className="w-2 h-2 rounded-full bg-primary" />
                                    <span className="text-xs font-medium text-muted-foreground">Sample Forecast, Houston Heights</span>
                                </div>
                                <FanChart
                                    data={DEMO_FAN_DATA}
                                    height={200}
                                    currentYear={2026}
                                    historicalValues={DEMO_HISTORICAL}
                                />
                                <div className="flex justify-between mt-2 text-[10px] text-muted-foreground">
                                    <span>Conservative: $268K</span>
                                    <span className="text-primary font-medium">Expected: $345K</span>
                                    <span>Upside: $425K</span>
                                </div>
                            </div>
                        </div>
                    </div>
                </section>

                {/* How it works — with inline SVG illustrations */}
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
                                <MonteCarloSVG />
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
                                <RangeBarsSVG />
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
                                <LotGridSVG />
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
                            <LotGridSVG />
                            <div className="flex items-center gap-3 mb-6 mt-4">
                                <Globe className="w-5 h-5 text-primary" />
                                <span className="font-bold">Who is this for?</span>
                            </div>
                            <ul className="space-y-3 text-sm text-muted-foreground">
                                <li className="flex items-start gap-2"><span className="text-primary mt-0.5">→</span> <strong>Investors</strong> find the neighborhoods that outperform</li>
                                <li className="flex items-start gap-2"><span className="text-primary mt-0.5">→</span> <strong>Homeowners</strong> see where your biggest asset is headed</li>
                                <li className="flex items-start gap-2"><span className="text-primary mt-0.5">→</span> <strong>Agents</strong> advise clients with data no one else has</li>
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
                            <span className="text-primary"> via API &amp; PDF</span>
                        </h2>
                        <p className="text-lg text-muted-foreground leading-relaxed max-w-2xl">
                            Homecastr&apos;s foundation model generates probabilistic price bands for every
                            residential property in the market. Access lot-level and neighborhood-level
                            forecasts via REST API or downloadable PDF reports, with accuracy as strong as 14% annual compounding error (MdAPE).
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
                                {/* Sample API response */}
                                <pre className="text-[10px] font-mono bg-black/5 dark:bg-white/5 rounded-lg p-3 overflow-x-auto border border-border/30 leading-relaxed">
                                    {`{
  "h3_id": "862a100c7ffffff",
  "horizon": 3,
  "p10": 268000,
  "p50": 345000,
  "p90": 425000
}`}
                                </pre>
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
                                <RangeBarsSVG />
                            </div>
                            <div className="space-y-4">
                                <div className="w-12 h-12 rounded-2xl bg-primary/10 flex items-center justify-center">
                                    <Sparkles className="w-6 h-6 text-primary" />
                                </div>
                                <h3 className="text-lg font-bold">Explainable Forecasts</h3>
                                <p className="text-sm text-muted-foreground leading-relaxed">
                                    Every forecast includes interpretable attributions: see
                                    which drivers move prices, not just the numbers.
                                </p>
                                <AttributionBarSVG />
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
                                <li className="flex items-start gap-2"><span className="text-primary mt-0.5">→</span> <strong>SFR acquisitions teams</strong> scoring buy/hold/sell across 50 to 5,000+ doors</li>
                                <li className="flex items-start gap-2"><span className="text-primary mt-0.5">→</span> <strong>Investment committees</strong> underwriting new deals with forward-looking data</li>
                                <li className="flex items-start gap-2"><span className="text-primary mt-0.5">→</span> <strong>Mortgage risk desks</strong> stress-testing collateral under rate scenarios</li>
                                <li className="flex items-start gap-2"><span className="text-primary mt-0.5">→</span> <strong>RE research analysts</strong> building market outlook reports and investment memos</li>
                            </ul>
                        </div>
                        <div>
                            <h2 className="text-3xl font-bold tracking-tight mb-6">Accuracy you can audit</h2>
                            <p className="text-muted-foreground leading-relaxed mb-4">
                                Forecast accuracy is measured using industry-standard MdAPE
                                (Median Absolute Percentage Error), with results as strong as 14% annual
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

                {/* Team */}
                <section className="max-w-6xl mx-auto px-6 py-24">
                    <div className="max-w-2xl mx-auto text-center">
                        <h2 className="text-3xl font-bold tracking-tight mb-6">Built by</h2>
                        <div className="inline-flex flex-col items-center gap-4 mb-8">
                            <img
                                src="/dhl.jpg"
                                alt="Daniel Hardesty Lewis"
                                className="w-32 h-32 rounded-full object-cover border-4 border-primary/10 shadow-xl"
                            />
                            <div className="space-y-1">
                                <div className="text-2xl font-bold">Daniel Hardesty Lewis</div>
                                <div className="text-base text-primary font-medium">Founder & CEO</div>
                                <div className="text-xs text-muted-foreground uppercase tracking-widest mt-1">
                                    TOP500 Supercomputing • Bagnold Medal Contributor • NSF 10 Big Ideas
                                </div>
                            </div>
                        </div>

                        <div className="grid md:grid-cols-2 gap-8 text-left max-w-4xl mx-auto">
                            <div className="space-y-4">
                                <h3 className="font-bold text-lg border-b border-border/40 pb-2">Experience</h3>
                                <ul className="space-y-4 text-sm text-foreground/80 leading-relaxed">
                                    <li>
                                        <div className="font-bold">Founder, Summit Geospatial</div>
                                        <div className="text-muted-foreground mb-1">2023 – Present</div>
                                        Building the highest quality seamless elevation data for Texas.
                                    </li>
                                    <li>
                                        <div className="font-bold">Senior Data Scientist, TACC</div>
                                        <div className="text-muted-foreground mb-1">2018 – 2023 · Austin, TX</div>
                                        Led $40M disaster resiliency initiative (TDIS). Scaled climate models on world's most powerful supercomputers. Contributed to Paola Passalacqua's 2022 Bagnold Medal research.
                                    </li>
                                    <li>
                                        <div className="font-bold">Co-Instructor, UT Austin</div>
                                        <div className="text-muted-foreground mb-1">2018 – 2020</div>
                                        Taught Machine Learning for Geosciences and Scientific Computation.
                                    </li>
                                </ul>
                            </div>

                            <div className="space-y-4">
                                <h3 className="font-bold text-lg border-b border-border/40 pb-2">Education</h3>
                                <ul className="space-y-4 text-sm text-foreground/80 leading-relaxed">
                                    <li>
                                        <div className="font-bold">Columbia University</div>
                                        <div className="text-muted-foreground">M.S. Urban Planning (Current)</div>
                                        <div className="text-xs text-muted-foreground mt-1">Cross-registered in Engineering (Machine Learning)</div>
                                    </li>
                                    <li>
                                        <div className="font-bold">UT Austin</div>
                                        <div className="text-muted-foreground">B.S. Mathematics</div>
                                    </li>
                                </ul>

                                <h3 className="font-bold text-lg border-b border-border/40 pb-2 pt-4">Research & Awards</h3>
                                <ul className="space-y-2 text-sm text-foreground/80 leading-relaxed">
                                    <li>• Published in <em>ACM Transactions on Interactive Intelligent Systems</em></li>
                                    <li>• DARPA World Modelers: Disaster resiliency in East Africa</li>
                                    <li>• Fellow, Texas Institute for Discovery Education in Science</li>
                                </ul>
                            </div>
                        </div>

                        <div className="mt-12 flex justify-center gap-4">
                            <a href="https://linkedin.com/in/dhardestylewis" target="_blank" rel="noopener noreferrer" className="inline-flex items-center gap-2 px-4 py-2 rounded-full bg-[#0077b5]/10 text-[#0077b5] hover:bg-[#0077b5]/20 transition-colors text-sm font-medium">
                                View LinkedIn Profile
                            </a>
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
            </div >
        </div >
    )
}
