"use client"

import React, { useState } from 'react'
import { Bot, Code2, Database, Globe, Lock, Terminal, Check, Copy, Key } from 'lucide-react'

export default function ApiDocsPage() {
    const [email, setEmail] = useState("")
    const [apiKey, setApiKey] = useState<string | null>(null)
    const [loading, setLoading] = useState(false)
    const [error, setError] = useState<string | null>(null)
    const [copied, setCopied] = useState(false)

    const generateKey = async () => {
        if (!email || !email.includes("@")) {
            setError("Please enter a valid email address.")
            return
        }
        setLoading(true)
        setError(null)
        try {
            const res = await fetch("/api/v1/keys", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ email }),
            })
            const data = await res.json()
            if (data.error) {
                setError(data.error)
            } else {
                setApiKey(data.key)
            }
        } catch {
            setError("Something went wrong. Please try again.")
        } finally {
            setLoading(false)
        }
    }

    const copyKey = () => {
        if (apiKey) {
            navigator.clipboard.writeText(apiKey)
            setCopied(true)
            setTimeout(() => setCopied(false), 2000)
        }
    }

    return (
        <div className="min-h-screen bg-background text-foreground font-sans selection:bg-primary/20">
            {/* Header / Nav */}
            <header className="border-b border-border/40 bg-background/80 backdrop-blur-md sticky top-0 z-50">
                <div className="max-w-7xl mx-auto px-6 h-16 flex items-center justify-between">
                    <div className="flex items-center gap-2">
                        <div className="w-8 h-8 rounded-lg bg-primary/10 flex items-center justify-center">
                            <Bot className="w-5 h-5 text-primary" />
                        </div>
                        <span className="font-bold tracking-tight text-xl">Homecastr API</span>
                    </div>
                    <nav className="flex items-center gap-6">
                        <a href="/" className="text-sm font-medium text-muted-foreground hover:text-foreground transition-colors">Dashboard</a>
                        <a href="#endpoints" className="text-sm font-medium text-muted-foreground hover:text-foreground transition-colors">Endpoints</a>
                        <div className="h-4 w-px bg-border/40" />
                        <a href="#get-key" className="text-sm font-medium px-4 py-1.5 rounded-full bg-primary text-primary-foreground hover:bg-primary/90 transition-colors">
                            Get Free Key
                        </a>
                    </nav>
                </div>
            </header>

            <main className="max-w-7xl mx-auto px-6 py-12 md:py-20">
                {/* Hero */}
                <section className="mb-20 text-center md:text-left">
                    <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-primary/10 text-primary text-[10px] font-bold uppercase tracking-widest mb-6">
                        <Terminal className="w-3 h-3" />
                        Developer Portal
                    </div>
                    <h1 className="text-4xl md:text-6xl font-bold tracking-tight mb-6 bg-clip-text text-foreground">
                        Smarter Data. <br />
                        <span className="text-primary">Programmatic Scale.</span>
                    </h1>
                    <p className="text-lg text-muted-foreground max-w-2xl leading-relaxed">
                        Access our proprietary real estate forecasting engine via simple REST endpoints.
                        From lot-level predictions to granular neighborhood H3 metrics, build the future of property analysis.
                    </p>
                </section>

                {/* Core Values / Features */}
                <div className="grid md:grid-cols-3 gap-8 mb-32">
                    <div className="p-6 rounded-2xl glass-panel relative overflow-hidden group">
                        <div className="w-10 h-10 rounded-xl bg-primary/10 flex items-center justify-center mb-4 group-hover:scale-110 transition-transform">
                            <Globe className="w-5 h-5 text-primary" />
                        </div>
                        <h3 className="text-lg font-bold mb-2">Lot-Level Accuracy</h3>
                        <p className="text-sm text-muted-foreground leading-relaxed">
                            Specific property forecasts identified by unique account IDs, providing historical context and future valuations.
                        </p>
                    </div>
                    <div className="p-6 rounded-2xl glass-panel relative overflow-hidden group">
                        <div className="w-10 h-10 rounded-xl bg-primary/10 flex items-center justify-center mb-4 group-hover:scale-110 transition-transform">
                            <Database className="w-5 h-5 text-primary" />
                        </div>
                        <h3 className="text-lg font-bold mb-2">H3 Grid Metrics</h3>
                        <p className="text-sm text-muted-foreground leading-relaxed">
                            Granular hexagonal spatial data including Opportunity, Reliability, and localized growth metrics.
                        </p>
                    </div>
                    <div className="p-6 rounded-2xl glass-panel relative overflow-hidden group">
                        <div className="w-10 h-10 rounded-xl bg-primary/10 flex items-center justify-center mb-4 group-hover:scale-110 transition-transform">
                            <Lock className="w-5 h-5 text-primary" />
                        </div>
                        <h3 className="text-lg font-bold mb-2">Scenario Bands</h3>
                        <p className="text-sm text-muted-foreground leading-relaxed">
                            Statistical percentile bands (P10, P50, P90) across multiple forecast horizons for risk-adjusted planning.
                        </p>
                    </div>
                </div>

                {/* Get API Key Section */}
                <section id="get-key" className="mb-32">
                    <div className="max-w-xl mx-auto text-center">
                        <div className="w-14 h-14 rounded-2xl bg-primary/10 flex items-center justify-center mx-auto mb-6">
                            <Key className="w-7 h-7 text-primary" />
                        </div>
                        <h2 className="text-3xl font-bold tracking-tight mb-3">Get Your API Key</h2>
                        <p className="text-muted-foreground mb-8">Instant access. No credit card required.</p>

                        {apiKey ? (
                            <div className="space-y-4">
                                <div className="p-4 rounded-xl bg-[#1e1e24] border border-white/10 font-mono text-sm text-slate-300 flex items-center justify-between gap-3">
                                    <span className="truncate">{apiKey}</span>
                                    <button
                                        onClick={copyKey}
                                        className="flex-shrink-0 p-2 rounded-lg hover:bg-white/10 transition-colors"
                                    >
                                        {copied ? <Check className="w-4 h-4 text-green-400" /> : <Copy className="w-4 h-4 text-slate-400" />}
                                    </button>
                                </div>
                                <p className="text-xs text-muted-foreground">
                                    Save this key. It won&apos;t be shown again. Use it as a <code className="text-primary">x-api-key</code> header.
                                </p>
                            </div>
                        ) : (
                            <div className="flex gap-3 max-w-md mx-auto">
                                <input
                                    type="email"
                                    value={email}
                                    onChange={e => setEmail(e.target.value)}
                                    onKeyDown={e => e.key === "Enter" && generateKey()}
                                    placeholder="you@company.com"
                                    className="flex-1 px-4 py-2.5 rounded-xl bg-muted/30 border border-border/50 text-sm text-foreground placeholder:text-muted-foreground/50 focus:outline-none focus:ring-2 focus:ring-primary/30"
                                />
                                <button
                                    onClick={generateKey}
                                    disabled={loading}
                                    className="px-6 py-2.5 rounded-xl bg-primary text-primary-foreground font-medium text-sm hover:bg-primary/90 transition-colors disabled:opacity-50"
                                >
                                    {loading ? "..." : "Generate"}
                                </button>
                            </div>
                        )}
                        {error && <p className="text-red-500 text-sm mt-3">{error}</p>}
                    </div>
                </section>

                <hr className="border-border/40 my-20" id="endpoints" />

                {/* Endpoints Section */}
                <section className="space-y-24">
                    {/* Lot Level */}
                    <div>
                        <div className="flex items-center gap-3 mb-8">
                            <div className="w-1.5 h-8 bg-primary rounded-full" />
                            <h2 className="text-3xl font-bold tracking-tight">Lot-Level Forecasts</h2>
                        </div>

                        <div className="grid lg:grid-cols-2 gap-12">
                            <div className="space-y-6">
                                <p className="text-muted-foreground leading-relaxed">
                                    Retrieve 4-year predictive forecasts for any accounted residential property in Harris County.
                                    Includes year-over-year valuations.
                                </p>

                                <div className="space-y-4">
                                    <div className="flex items-center gap-3 text-sm font-mono p-4 rounded-xl bg-muted/30 border border-border/50">
                                        <span className="text-primary font-bold">GET</span>
                                        <span className="text-foreground">/api/v1/forecast/lot</span>
                                    </div>

                                    <div className="space-y-2">
                                        <h4 className="text-xs font-bold uppercase tracking-wider text-muted-foreground">Parameters</h4>
                                        <div className="flex items-center justify-between py-2 border-b border-border/40 text-sm">
                                            <code className="text-primary">acct</code>
                                            <span className="text-muted-foreground">Unique account ID string (e.g. &quot;123...&quot;)</span>
                                        </div>
                                    </div>
                                </div>
                            </div>

                            {/* Code Example */}
                            <div className="rounded-2xl bg-[#1e1e24] p-6 shadow-2xl border border-white/5 relative overflow-hidden group">
                                <div className="absolute top-0 right-0 p-4 opacity-10 group-hover:opacity-20 transition-opacity">
                                    <Code2 className="w-12 h-12 text-white" />
                                </div>
                                <pre className="text-[12px] leading-relaxed font-mono text-slate-300 overflow-x-auto">
                                    {`{
  "acct": "1036810000007",
  "forecasts": [
    { "year": 2026, "valuation": 412500 },
    { "year": 2027, "valuation": 431000 },
    { "year": 2028, "valuation": 452000 },
    { "year": 2029, "valuation": 475000 }
  ]
}`}
                                </pre>
                            </div>
                        </div>
                    </div>

                    {/* H3 Level */}
                    <div>
                        <div className="flex items-center gap-3 mb-8">
                            <div className="w-1.5 h-8 bg-primary rounded-full" />
                            <h2 className="text-3xl font-bold tracking-tight">H3 Neighborhood Forecasts</h2>
                        </div>

                        <div className="grid lg:grid-cols-2 gap-12">
                            <div className="space-y-6">
                                <p className="text-muted-foreground leading-relaxed">
                                    Access aggregated neighborhood metrics via Uber&apos;s H3 indexing system. Includes opportunity scores,
                                    reliability metrics, and statistical confidence bands (P10/P50/P90).
                                </p>

                                <div className="space-y-4">
                                    <div className="flex items-center gap-3 text-sm font-mono p-4 rounded-xl bg-muted/30 border border-border/50">
                                        <span className="text-primary font-bold">GET</span>
                                        <span className="text-foreground">/api/v1/forecast/hex</span>
                                    </div>

                                    <div className="space-y-3">
                                        <h4 className="text-xs font-bold uppercase tracking-wider text-muted-foreground">Parameters</h4>
                                        <div className="flex items-center justify-between py-2 border-b border-border/40 text-sm">
                                            <code className="text-primary">h3_id</code>
                                            <span className="text-muted-foreground">Valid H3 index string (Resolution 7-10)</span>
                                        </div>
                                        <div className="flex items-center justify-between py-2 border-b border-border/40 text-sm">
                                            <code className="text-primary">year</code>
                                            <span className="text-muted-foreground">Forecast target year (default: 2026)</span>
                                        </div>
                                    </div>
                                </div>
                            </div>

                            {/* Code Example */}
                            <div className="rounded-2xl bg-[#1e1e24] p-6 shadow-2xl border border-white/5 relative overflow-hidden group">
                                <pre className="text-[11px] leading-relaxed font-mono text-slate-300 overflow-x-auto">
                                    {`{
  "h3_id": "8926a100d9fffff",
  "bands": {
    "p10": [280000, 285000, 292000, 301000, 310000],
    "p50": [310000, 325000, 340000, 358000, 375000],
    "p90": [345000, 365000, 390000, 420000, 455000],
    "years_horizon": [1, 2, 3, 4, 5]
  },
  "metrics": {
    "opportunity": 12.4,
    "reliability": 0.89
  }
}`}
                                </pre>
                            </div>
                        </div>
                    </div>
                </section>
            </main>

            {/* Footer */}
            <footer className="border-t border-border/40 py-12 bg-muted/10">
                <div className="max-w-7xl mx-auto px-6 flex flex-col md:flex-row justify-between items-center gap-6">
                    <div className="text-sm text-muted-foreground">
                        © 2026 Homecastr. All rights reserved.
                    </div>
                    <div className="flex gap-8 text-sm font-medium text-muted-foreground">
                        <a href="/privacy" className="hover:text-foreground">Privacy</a>
                        <a href="/terms" className="hover:text-foreground">Terms</a>
                        <a href="/support" className="hover:text-foreground">Support</a>
                    </div>
                </div>
            </footer>
        </div>
    )
}
