"use client"

import { Mail, MessageSquare, FileText, Send, Loader2 } from 'lucide-react'
import { useState } from 'react'

export default function SupportPage() {
    const [sending, setSending] = useState(false)
    const [sent, setSent] = useState(false)

    const handleSubmit = (e: React.FormEvent) => {
        e.preventDefault()
        setSending(true)
        // Simulate API call
        setTimeout(() => {
            setSending(false)
            setSent(true)
        }, 1500)
    }

    return (
        <div className="overflow-auto h-screen">
            <div className="min-h-screen bg-background text-foreground font-sans">
                <div className="max-w-3xl mx-auto px-6 py-16">
                    <a href="/about" className="text-sm text-primary hover:underline mb-8 inline-block">← Back to Home</a>
                    <h1 className="text-4xl font-bold tracking-tight mb-2">Support</h1>
                    <p className="text-muted-foreground text-sm mb-10">We&apos;re here to help you get the most out of Homecastr.</p>

                    <div className="grid md:grid-cols-3 gap-6 mb-16">
                        <div className="p-6 rounded-2xl glass-panel space-y-3">
                            <div className="w-10 h-10 rounded-xl bg-primary/10 flex items-center justify-center">
                                <Mail className="w-5 h-5 text-primary" />
                            </div>
                            <h3 className="font-bold">Email</h3>
                            <p className="text-sm text-muted-foreground">
                                Reach us at{" "}
                                <a href="mailto:support@homecastr.com" className="text-primary hover:underline">support@homecastr.com</a>
                                . We respond within 24 hours on business days.
                            </p>
                        </div>

                        <div className="p-6 rounded-2xl glass-panel space-y-3">
                            <div className="w-10 h-10 rounded-xl bg-primary/10 flex items-center justify-center">
                                <MessageSquare className="w-5 h-5 text-primary" />
                            </div>
                            <h3 className="font-bold">API Issues</h3>
                            <p className="text-sm text-muted-foreground">
                                For API-specific issues (rate limits, authentication, data questions), include
                                your API key prefix and error response in your message.
                            </p>
                        </div>

                        <div className="p-6 rounded-2xl glass-panel space-y-3">
                            <div className="w-10 h-10 rounded-xl bg-primary/10 flex items-center justify-center">
                                <FileText className="w-5 h-5 text-primary" />
                            </div>
                            <h3 className="font-bold">Documentation</h3>
                            <p className="text-sm text-muted-foreground">
                                Check our{" "}
                                <a href="/api-docs" className="text-primary hover:underline">API documentation</a>
                                {" "}for endpoint references, parameters, and example responses.
                            </p>
                        </div>
                    </div>

                    <div className="mb-16">
                        <h2 className="text-2xl font-bold mb-6">Send us a message</h2>
                        <div className="p-8 rounded-2xl glass-panel border border-border/40">
                            {sent ? (
                                <div className="flex flex-col items-center justify-center py-12 text-center">
                                    <div className="w-16 h-16 rounded-full bg-green-500/10 flex items-center justify-center mb-4">
                                        <Send className="w-8 h-8 text-green-500" />
                                    </div>
                                    <h3 className="text-xl font-bold mb-2">Message Sent!</h3>
                                    <p className="text-muted-foreground">Thanks for reaching out. We&apos;ll get back to you shortly.</p>
                                    <button
                                        onClick={() => setSent(false)}
                                        className="mt-6 text-sm text-primary hover:underline"
                                    >
                                        Send another message
                                    </button>
                                </div>
                            ) : (
                                <form onSubmit={handleSubmit} className="space-y-6">
                                    <div className="grid md:grid-cols-2 gap-6">
                                        <div className="space-y-2">
                                            <label htmlFor="name" className="text-sm font-medium">Name</label>
                                            <input
                                                id="name"
                                                required
                                                className="w-full px-4 py-2 rounded-lg bg-muted/30 border border-border/50 focus:outline-none focus:ring-2 focus:ring-primary/20"
                                                placeholder="Your name"
                                            />
                                        </div>
                                        <div className="space-y-2">
                                            <label htmlFor="email" className="text-sm font-medium">Email</label>
                                            <input
                                                id="email"
                                                type="email"
                                                required
                                                className="w-full px-4 py-2 rounded-lg bg-muted/30 border border-border/50 focus:outline-none focus:ring-2 focus:ring-primary/20"
                                                placeholder="you@company.com"
                                            />
                                        </div>
                                    </div>

                                    <div className="space-y-2">
                                        <label htmlFor="topic" className="text-sm font-medium">Topic</label>
                                        <select
                                            id="topic"
                                            className="w-full px-4 py-2 rounded-lg bg-muted/30 border border-border/50 focus:outline-none focus:ring-2 focus:ring-primary/20"
                                        >
                                            <option>General Support</option>
                                            <option>API Access</option>
                                            <option>Bug Report</option>
                                            <option>Feature Request</option>
                                            <option>Billing</option>
                                        </select>
                                    </div>

                                    <div className="space-y-2">
                                        <label htmlFor="message" className="text-sm font-medium">Message</label>
                                        <textarea
                                            id="message"
                                            required
                                            rows={5}
                                            className="w-full px-4 py-2 rounded-lg bg-muted/30 border border-border/50 focus:outline-none focus:ring-2 focus:ring-primary/20 resize-none"
                                            placeholder="How can we help?"
                                        />
                                    </div>

                                    <button
                                        type="submit"
                                        disabled={sending}
                                        className="inline-flex items-center gap-2 px-6 py-3 rounded-xl bg-primary text-primary-foreground font-medium hover:bg-primary/90 transition-colors disabled:opacity-50"
                                    >
                                        {sending ? (
                                            <>
                                                <Loader2 className="w-4 h-4 animate-spin" />
                                                Sending...
                                            </>
                                        ) : (
                                            <>
                                                Send Message
                                                <Send className="w-4 h-4" />
                                            </>
                                        )}
                                    </button>
                                </form>
                            )}
                        </div>
                    </div>

                    <div className="space-y-6">
                        <h2 className="text-2xl font-bold">Frequently Asked Questions</h2>

                        <div className="space-y-4">
                            <div className="p-5 rounded-xl bg-muted/20 border border-border/40">
                                <h3 className="font-semibold text-sm mb-2">How do I get an API key?</h3>
                                <p className="text-sm text-muted-foreground">
                                    Click &quot;Get Started Free&quot; on the API docs page to generate a key instantly.
                                    We&apos;ll provision access within one business day.
                                </p>
                            </div>

                            <div className="p-5 rounded-xl bg-muted/20 border border-border/40">
                                <h3 className="font-semibold text-sm mb-2">What areas do you cover?</h3>
                                <p className="text-sm text-muted-foreground">
                                    We currently cover residential properties across the greater Houston metro area.
                                    Additional markets are planned for future releases.
                                </p>
                            </div>

                            <div className="p-5 rounded-xl bg-muted/20 border border-border/40">
                                <h3 className="font-semibold text-sm mb-2">How accurate are the forecasts?</h3>
                                <p className="text-sm text-muted-foreground">
                                    Our foundation model generates probabilistic price bands (P10/P50/P90) across many
                                    scenarios. Accuracy is measured using industry-standard MdAPE (Median Absolute Percentage Error),
                                    expressed as annual compounding error, and varies by area and forecast horizon.
                                </p>
                            </div>

                            <div className="p-5 rounded-xl bg-muted/20 border border-border/40">
                                <h3 className="font-semibold text-sm mb-2">What are the rate limits?</h3>
                                <p className="text-sm text-muted-foreground">
                                    Default tier: 100 requests/minute, 10,000 requests/day.
                                    Contact us for higher limits or commercial access.
                                </p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    )
}
