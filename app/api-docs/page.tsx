"use client"

import { useState } from "react"
import Link from "next/link"
import { HomecastrLogo } from "@/components/homecastr-logo"
import { ArrowRight, Copy, Check, Key, Zap, Code2, Terminal } from "lucide-react"

// ── Demo key (same as server) ──
const DEMO_KEY = "hc_demo_public_readonly"

function CopyButton({ text }: { text: string }) {
    const [copied, setCopied] = useState(false)
    return (
        <button
            onClick={() => {
                navigator.clipboard.writeText(text)
                setCopied(true)
                setTimeout(() => setCopied(false), 2000)
            }}
            className="inline-flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
            title="Copy to clipboard"
        >
            {copied ? <Check className="w-3.5 h-3.5 text-lime-500" /> : <Copy className="w-3.5 h-3.5" />}
            {copied ? "Copied" : "Copy"}
        </button>
    )
}

function TryItWidget() {
    const [address, setAddress] = useState("123 Main St Houston TX")
    const [response, setResponse] = useState<string | null>(null)
    const [loading, setLoading] = useState(false)
    const [error, setError] = useState<string | null>(null)

    async function tryIt() {
        setLoading(true)
        setError(null)
        setResponse(null)
        try {
            const res = await fetch(
                `/api/v1/forecast?address=${encodeURIComponent(address)}`,
                { headers: { "x-api-key": DEMO_KEY } }
            )
            const data = await res.json()
            setResponse(JSON.stringify(data, null, 2))
            if (!res.ok) setError(`HTTP ${res.status}`)
        } catch (e: any) {
            setError(e.message)
        } finally {
            setLoading(false)
        }
    }

    return (
        <div className="rounded-2xl border border-border/50 bg-muted/10 overflow-hidden">
            <div className="px-6 py-4 border-b border-border/40 flex items-center gap-3">
                <Zap className="w-5 h-5 text-primary" />
                <span className="font-bold">Try It Now</span>
                <span className="text-xs text-muted-foreground ml-auto">Uses demo key &mdash; no signup required</span>
            </div>
            <div className="p-6 space-y-4">
                <div className="flex gap-3">
                    <div className="flex-1">
                        <label className="block text-xs font-medium text-muted-foreground mb-1.5">Address</label>
                        <input
                            type="text"
                            value={address}
                            onChange={(e) => setAddress(e.target.value)}
                            onKeyDown={(e) => e.key === "Enter" && tryIt()}
                            className="w-full px-4 py-2.5 rounded-lg border border-border/50 bg-background text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary/30"
                            placeholder="123 Main St Houston TX"
                        />
                    </div>
                    <div className="flex items-end">
                        <button
                            onClick={tryIt}
                            disabled={loading || !address.trim()}
                            className="px-6 py-2.5 rounded-lg bg-primary text-primary-foreground font-medium text-sm hover:bg-primary/90 transition-colors disabled:opacity-50 shadow-sm"
                        >
                            {loading ? "Loading\u2026" : "Send Request"}
                        </button>
                    </div>
                </div>

                {/* curl equivalent */}
                <div className="relative">
                    <div className="flex items-center justify-between mb-1.5">
                        <span className="text-xs font-medium text-muted-foreground">curl equivalent</span>
                        <CopyButton text={`curl -s -H "x-api-key: ${DEMO_KEY}" "${typeof window !== "undefined" ? window.location.origin : ""}/api/v1/forecast?address=${encodeURIComponent(address)}"`} />
                    </div>
                    <pre className="text-[11px] font-mono bg-zinc-950 text-zinc-300 rounded-lg p-4 overflow-x-auto leading-relaxed">
                        {`curl -s -H "x-api-key: ${DEMO_KEY}" \\
  "${typeof window !== "undefined" ? window.location.origin : "https://homecastr.ai"}/api/v1/forecast?address=${encodeURIComponent(address)}"`}
                    </pre>
                </div>

                {/* Response */}
                {(response || error) && (
                    <div>
                        <div className="flex items-center justify-between mb-1.5">
                            <span className={`text-xs font-medium ${error ? "text-red-400" : "text-lime-500"}`}>
                                {error ? `Error: ${error}` : "Response"}
                            </span>
                            {response && <CopyButton text={response} />}
                        </div>
                        <pre className={`text-[11px] font-mono rounded-lg p-4 overflow-x-auto max-h-96 overflow-y-auto leading-relaxed ${error ? "bg-red-950/30 text-red-300 border border-red-900/30" : "bg-zinc-950 text-zinc-300"}`}>
                            {response}
                        </pre>
                    </div>
                )}
            </div>
        </div>
    )
}

function GetKeyForm() {
    const [email, setEmail] = useState("")
    const [apiKey, setApiKey] = useState<string | null>(null)
    const [loading, setLoading] = useState(false)
    const [error, setError] = useState<string | null>(null)

    async function generateKey() {
        setLoading(true)
        setError(null)
        try {
            const res = await fetch("/api/v1/keys", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ email }),
            })
            const data = await res.json()
            if (!res.ok) throw new Error(data.error || "Failed")
            setApiKey(data.key)
        } catch (e: any) {
            setError(e.message)
        } finally {
            setLoading(false)
        }
    }

    return (
        <div id="get-key" className="rounded-2xl border border-primary/30 bg-primary/5 overflow-hidden">
            <div className="px-6 py-4 border-b border-primary/20 flex items-center gap-3">
                <Key className="w-5 h-5 text-primary" />
                <span className="font-bold">Get Your API Key</span>
                <span className="text-xs text-muted-foreground ml-auto">Free &mdash; instant &mdash; no sales call</span>
            </div>
            <div className="p-6 space-y-4">
                {apiKey ? (
                    <div className="space-y-3">
                        <div className="text-sm text-lime-600 dark:text-lime-400 font-medium">&check; API key generated!</div>
                        <div className="flex items-center gap-3">
                            <code className="flex-1 px-4 py-2.5 rounded-lg bg-background border border-border/50 font-mono text-sm select-all">
                                {apiKey}
                            </code>
                            <CopyButton text={apiKey} />
                        </div>
                        <p className="text-xs text-muted-foreground">
                            Save this key &mdash; it will not be shown again. Use it via the <code className="px-1 py-0.5 bg-muted rounded text-[10px]">x-api-key</code> header.
                        </p>
                    </div>
                ) : (
                    <div className="flex gap-3">
                        <div className="flex-1">
                            <input
                                type="email"
                                value={email}
                                onChange={(e) => setEmail(e.target.value)}
                                onKeyDown={(e) => e.key === "Enter" && generateKey()}
                                className="w-full px-4 py-2.5 rounded-lg border border-border/50 bg-background text-sm focus:outline-none focus:ring-2 focus:ring-primary/30"
                                placeholder="you@company.com"
                            />
                        </div>
                        <button
                            onClick={generateKey}
                            disabled={loading || !email.includes("@")}
                            className="px-6 py-2.5 rounded-lg bg-primary text-primary-foreground font-medium text-sm hover:bg-primary/90 transition-colors disabled:opacity-50 shadow-sm"
                        >
                            {loading ? "Generating\u2026" : "Generate Key"}
                        </button>
                    </div>
                )}
                {error && <p className="text-xs text-red-500">{error}</p>}
            </div>
        </div>
    )
}

function EndpointCard({
    method,
    path,
    description,
    params,
    curlExample,
}: {
    method: string
    path: string
    description: string
    params: { name: string; type: string; required: boolean; description: string }[]
    curlExample: string
}) {
    return (
        <div className="rounded-2xl border border-border/50 overflow-hidden">
            <div className="px-6 py-4 border-b border-border/40 flex items-center gap-3">
                <span className="px-2 py-0.5 rounded text-[10px] font-bold uppercase tracking-wider bg-lime-500/15 text-lime-600 dark:text-lime-400">
                    {method}
                </span>
                <code className="font-mono text-sm font-bold">{path}</code>
            </div>
            <div className="p-6 space-y-4">
                <p className="text-sm text-muted-foreground">{description}</p>

                <div>
                    <h4 className="text-xs font-bold uppercase tracking-wider text-muted-foreground mb-2">Parameters</h4>
                    <div className="rounded-lg border border-border/30 overflow-hidden">
                        <table className="w-full text-sm">
                            <thead className="bg-muted/20">
                                <tr>
                                    <th className="text-left px-4 py-2 text-xs font-medium text-muted-foreground">Name</th>
                                    <th className="text-left px-4 py-2 text-xs font-medium text-muted-foreground">Type</th>
                                    <th className="text-left px-4 py-2 text-xs font-medium text-muted-foreground">Required</th>
                                    <th className="text-left px-4 py-2 text-xs font-medium text-muted-foreground">Description</th>
                                </tr>
                            </thead>
                            <tbody className="divide-y divide-border/20">
                                {params.map((p) => (
                                    <tr key={p.name}>
                                        <td className="px-4 py-2 font-mono text-xs">{p.name}</td>
                                        <td className="px-4 py-2 text-xs text-muted-foreground">{p.type}</td>
                                        <td className="px-4 py-2 text-xs">
                                            {p.required ? (
                                                <span className="text-primary font-medium">Yes</span>
                                            ) : (
                                                <span className="text-muted-foreground">No</span>
                                            )}
                                        </td>
                                        <td className="px-4 py-2 text-xs text-muted-foreground">{p.description}</td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                </div>

                <div>
                    <div className="flex items-center justify-between mb-1.5">
                        <h4 className="text-xs font-bold uppercase tracking-wider text-muted-foreground">Example</h4>
                        <CopyButton text={curlExample} />
                    </div>
                    <pre className="text-[11px] font-mono bg-zinc-950 text-zinc-300 rounded-lg p-4 overflow-x-auto leading-relaxed">
                        {curlExample}
                    </pre>
                </div>
            </div>
        </div>
    )
}

export default function ApiDocsPage() {
    return (
        <div className="overflow-auto h-screen">
            <div className="min-h-screen bg-background text-foreground font-sans">
                {/* Nav */}
                <header className="border-b border-border/40 bg-background/80 backdrop-blur-md sticky top-0 z-50">
                    <div className="max-w-5xl mx-auto px-6 h-16 flex items-center justify-between">
                        <Link href="/" className="flex items-center gap-2">
                            <HomecastrLogo size={28} variant="horizontal" />
                        </Link>
                        <nav className="hidden md:flex items-center gap-6">
                            <Link href="/" className="text-sm font-medium text-muted-foreground hover:text-foreground transition-colors">Home</Link>
                            <Link href="/forecasts" className="text-sm font-medium text-muted-foreground hover:text-foreground transition-colors">Markets</Link>
                            <Link href="/methodology" className="text-sm font-medium text-muted-foreground hover:text-foreground transition-colors">Methodology</Link>
                        </nav>
                        <Link href="/app" className="text-sm font-bold px-5 py-2 rounded-lg bg-primary text-primary-foreground hover:bg-primary/90 transition-colors shadow-sm">
                            Open App
                        </Link>
                    </div>
                </header>

                <div className="max-w-5xl mx-auto px-6 py-16 space-y-16">
                    {/* Hero */}
                    <div>
                        <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-primary/10 text-primary text-[10px] font-bold uppercase tracking-widest mb-6">
                            <Code2 className="w-3 h-3" />
                            Developer API
                        </div>
                        <h1 className="text-4xl md:text-5xl font-bold tracking-tight leading-[1.1] mb-4">
                            Forecast any U.S. home value
                            <br className="hidden md:block" />
                            <span className="text-primary"> with one API call</span>
                        </h1>
                        <p className="text-lg text-muted-foreground max-w-2xl leading-relaxed">
                            Parcel-level and neighbourhood-level probabilistic price forecasts.
                            P10/P50/P90 distributions across 1&ndash;5 year horizons.
                            One endpoint, one address, instant results.
                        </p>
                    </div>

                    {/* Quick start */}
                    <div className="space-y-4">
                        <h2 className="text-2xl font-bold tracking-tight flex items-center gap-3">
                            <Terminal className="w-6 h-6 text-primary" />
                            Quick Start
                        </h2>
                        <p className="text-sm text-muted-foreground">
                            Try the API right now with the built-in demo key. No signup required.
                        </p>
                        <div className="relative">
                            <div className="absolute top-2 right-3 z-10">
                                <CopyButton text={`curl -s -H "x-api-key: ${DEMO_KEY}" "https://homecastr.ai/api/v1/forecast?address=123+Main+St+Houston+TX"`} />
                            </div>
                            <pre className="text-[12px] font-mono bg-zinc-950 text-zinc-300 rounded-lg p-5 overflow-x-auto leading-relaxed">
                                {`curl -s -H "x-api-key: ${DEMO_KEY}" \\
  "https://homecastr.ai/api/v1/forecast?address=123+Main+St+Houston+TX"`}
                            </pre>
                        </div>
                    </div>

                    {/* Try it widget */}
                    <TryItWidget />

                    {/* Get key */}
                    <GetKeyForm />

                    {/* Auth */}
                    <div className="space-y-4">
                        <h2 className="text-2xl font-bold tracking-tight">Authentication</h2>
                        <p className="text-sm text-muted-foreground leading-relaxed">
                            All API requests require an API key passed via the <code className="px-1.5 py-0.5 bg-muted rounded text-xs font-mono">x-api-key</code> header.
                        </p>
                        <div className="rounded-lg border border-border/30 bg-muted/10 p-4">
                            <table className="w-full text-sm">
                                <thead>
                                    <tr>
                                        <th className="text-left pb-2 text-xs font-medium text-muted-foreground">Key Type</th>
                                        <th className="text-left pb-2 text-xs font-medium text-muted-foreground">Rate Limit</th>
                                        <th className="text-left pb-2 text-xs font-medium text-muted-foreground">Use Case</th>
                                    </tr>
                                </thead>
                                <tbody className="divide-y divide-border/20">
                                    <tr>
                                        <td className="py-2 font-mono text-xs">hc_demo_public_readonly</td>
                                        <td className="py-2 text-xs text-muted-foreground">50 req/hour</td>
                                        <td className="py-2 text-xs text-muted-foreground">Testing &amp; evaluation</td>
                                    </tr>
                                    <tr>
                                        <td className="py-2 font-mono text-xs">hc_*</td>
                                        <td className="py-2 text-xs text-muted-foreground">Unlimited (fair use)</td>
                                        <td className="py-2 text-xs text-muted-foreground">Production</td>
                                    </tr>
                                </tbody>
                            </table>
                        </div>
                    </div>

                    {/* Endpoints */}
                    <div id="endpoints" className="space-y-8">
                        <h2 className="text-2xl font-bold tracking-tight">Endpoints</h2>

                        <EndpointCard
                            method="GET"
                            path="/api/v1/forecast"
                            description="Forecast home values by address. Geocodes the address, maps to an H3 neighborhood cell, and returns probabilistic forecasts with P10/P50/P90 bands."
                            params={[
                                { name: "address", type: "string", required: true, description: "US street address (e.g. '123 Main St Houston TX')" },
                                { name: "year", type: "integer", required: false, description: "Target forecast year (default: 2030). Range: 2026\u20132030." },
                            ]}
                            curlExample={`curl -s -H "x-api-key: YOUR_API_KEY" \\
  "https://homecastr.ai/api/v1/forecast?address=123+Main+St+Houston+TX&year=2028"`}
                        />

                        <EndpointCard
                            method="GET"
                            path="/api/v1/forecast/hex"
                            description="Forecast by H3 cell ID. For developers who already know the H3 index of their target neighborhood."
                            params={[
                                { name: "h3_id", type: "string", required: true, description: "H3 cell ID at resolution 8 (e.g. '882a100c65fffff')" },
                                { name: "year", type: "integer", required: false, description: "Target forecast year (default: 2026)" },
                            ]}
                            curlExample={`curl -s -H "x-api-key: YOUR_API_KEY" \\
  "https://homecastr.ai/api/v1/forecast/hex?h3_id=882a100c65fffff&year=2028"`}
                        />

                        <EndpointCard
                            method="GET"
                            path="/api/v1/forecast/lot"
                            description="Forecast by tax parcel account ID. For integrations with county appraisal districts."
                            params={[
                                { name: "acct", type: "string", required: true, description: "County tax account / parcel ID" },
                            ]}
                            curlExample={`curl -s -H "x-api-key: YOUR_API_KEY" \\
  "https://homecastr.ai/api/v1/forecast/lot?acct=1234567890123"`}
                        />

                        <EndpointCard
                            method="POST"
                            path="/api/v1/keys"
                            description="Generate a new API key. Keys are free and issued instantly."
                            params={[
                                { name: "email", type: "string (body)", required: true, description: "Your email address" },
                            ]}
                            curlExample={`curl -s -X POST "https://homecastr.ai/api/v1/keys" \\
  -H "Content-Type: application/json" \\
  -d '{"email": "you@company.com"}'`}
                        />
                    </div>

                    {/* Code snippets */}
                    <div className="space-y-6">
                        <h2 className="text-2xl font-bold tracking-tight">Code Examples</h2>

                        <div className="grid md:grid-cols-2 gap-6">
                            <div>
                                <div className="flex items-center justify-between mb-2">
                                    <span className="text-xs font-bold uppercase tracking-wider text-muted-foreground">JavaScript / Node.js</span>
                                </div>
                                <pre className="text-[11px] font-mono bg-zinc-950 text-zinc-300 rounded-lg p-4 overflow-x-auto leading-relaxed h-full">
                                    {`const res = await fetch(
  "https://homecastr.ai/api/v1/forecast" +
  "?address=123+Main+St+Houston+TX",
  {
    headers: {
      "x-api-key": "YOUR_API_KEY"
    }
  }
);
const data = await res.json();
console.log(data.forecasts);
// { p10: 268000, p50: 345000, p90: 425000 }`}
                                </pre>
                            </div>

                            <div>
                                <div className="flex items-center justify-between mb-2">
                                    <span className="text-xs font-bold uppercase tracking-wider text-muted-foreground">Python</span>
                                </div>
                                <pre className="text-[11px] font-mono bg-zinc-950 text-zinc-300 rounded-lg p-4 overflow-x-auto leading-relaxed h-full">
                                    {`import requests

r = requests.get(
    "https://homecastr.ai/api/v1/forecast",
    params={"address": "123 Main St Houston TX"},
    headers={"x-api-key": "YOUR_API_KEY"},
)
data = r.json()
print(data["forecasts"])
# {'p10': 268000, 'p50': 345000, 'p90': 425000}`}
                                </pre>
                            </div>
                        </div>
                    </div>

                    {/* Response schema */}
                    <div className="space-y-4">
                        <h2 className="text-2xl font-bold tracking-tight">Response Schema</h2>
                        <pre className="text-[11px] font-mono bg-zinc-950 text-zinc-300 rounded-lg p-5 overflow-x-auto leading-relaxed">
                            {`{
  "address": "string \u2014 Geocoded address",
  "coordinates": { "lat": "number", "lng": "number" },
  "h3_cell": "string \u2014 H3 cell ID at resolution 8",
  "forecast_year": "number \u2014 Target year",
  "current_value": "number | null \u2014 Current median value ($)",
  "appreciation_pct": "number | null \u2014 Expected appreciation (%)",
  "forecasts": {
    "p10": "number \u2014 Conservative (10th percentile) value ($)",
    "p50": "number \u2014 Expected (median) value ($)",
    "p90": "number \u2014 Upside (90th percentile) value ($)"
  },
  "horizon_years": "number \u2014 Years from origin to forecast",
  "reliability": "number | null \u2014 Model confidence (0\u20131)",
  "fan_chart": {
    "years": "[2026, 2027, 2028, 2029, 2030]",
    "p10": "[number, ...]",
    "p50": "[number, ...]",
    "p90": "[number, ...]"
  },
  "property_count": "number | null \u2014 Properties in this cell",
  "_links": {
    "self": "string \u2014 This request URL",
    "hex": "string \u2014 Direct hex endpoint for this cell",
    "docs": "string \u2014 API docs URL"
  }
}`}
                        </pre>
                    </div>

                    {/* Footer CTA */}
                    <div className="border-t border-border/40 pt-12 text-center">
                        <h2 className="text-2xl font-bold tracking-tight mb-3">Ready to build?</h2>
                        <p className="text-muted-foreground mb-6 text-sm">
                            Get your API key and start forecasting in under 30 seconds.
                        </p>
                        <div className="flex gap-4 justify-center">
                            <a
                                href="#get-key"
                                className="inline-flex items-center gap-2 px-6 py-3 rounded-xl bg-primary text-primary-foreground font-medium hover:bg-primary/90 transition-colors shadow-lg shadow-primary/20"
                            >
                                Get API Key
                                <ArrowRight className="w-4 h-4" />
                            </a>
                            <Link
                                href="/"
                                className="inline-flex items-center gap-2 px-6 py-3 rounded-xl bg-muted/30 border border-border/50 font-medium hover:bg-muted/50 transition-colors"
                            >
                                Back to Home
                            </Link>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    )
}
