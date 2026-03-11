"use client"

import { useState, useCallback } from "react"
import { X, CalendarDays, Mail, Copy, Check, Send, Loader2 } from "lucide-react"

declare global {
    interface Window {
        Calendly?: {
            initPopupWidget: (opts: { url: string }) => void
        }
    }
}

interface ContactModalProps {
    isOpen: boolean
    onClose: () => void
    embedded?: boolean
}

export function ContactModal({ isOpen, onClose, embedded = false }: ContactModalProps) {
    const [copied, setCopied] = useState(false)
    const [formName, setFormName] = useState("")
    const [formEmail, setFormEmail] = useState("")
    const [formMessage, setFormMessage] = useState("")
    const [sending, setSending] = useState(false)
    const [sent, setSent] = useState(false)
    const [error, setError] = useState<string | null>(null)

    const handleCopyEmail = useCallback(() => {
        navigator.clipboard.writeText("daniel@homecastr.com")
        setCopied(true)
        setTimeout(() => setCopied(false), 2000)
    }, [])

    const handleScheduleCall = useCallback(() => {
        if (window.Calendly) {
            window.Calendly.initPopupWidget({
                url: "https://calendly.com/daniel-homecastr/30min",
            })
        } else {
            window.open("https://calendly.com/daniel-homecastr/30min", "_blank")
        }
    }, [])

    const handleSendMessage = useCallback(async () => {
        if (sending) return
        setSending(true)
        setError(null)

        try {
            const res = await fetch("/api/contact", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    name: formName.trim(),
                    email: formEmail.trim(),
                    message: formMessage.trim(),
                }),
            })

            if (!res.ok) {
                const data = await res.json().catch(() => ({}))
                throw new Error(data.error || "Failed to send")
            }

            setSent(true)
            setFormName("")
            setFormEmail("")
            setFormMessage("")
            // Reset sent state after a few seconds
            setTimeout(() => setSent(false), 5000)
        } catch (err) {
            setError(err instanceof Error ? err.message : "Something went wrong. Please try again.")
        } finally {
            setSending(false)
        }
    }, [sending, formName, formEmail, formMessage])

    if (!isOpen) return null

    return (
        <div className={embedded ? "h-full flex flex-col overflow-y-auto" : "fixed inset-0 z-[10001] flex items-center justify-center"}>
            {/* Backdrop — hidden in embedded mode */}
            {!embedded && (
            <div
                className="absolute inset-0 bg-black/50 backdrop-blur-sm"
                onClick={onClose}
            />
            )}

            {/* Modal */}
            <div className={embedded ? "flex-1 flex flex-col" : "relative z-10 w-full max-w-md mx-4 glass-panel rounded-2xl shadow-2xl border border-border/50 overflow-hidden"}>
                {/* Header — hidden in embedded mode */}
                {!embedded && (
                <div className="flex items-center justify-between px-6 pt-5 pb-3">
                    <div>
                        <h2 className="text-lg font-bold text-foreground">Get in touch</h2>
                        <p className="text-xs text-muted-foreground mt-0.5">
                            Custom forecasts, bulk analysis, API access &amp; pilot programs
                        </p>
                    </div>
                    <button
                        onClick={onClose}
                        className="p-1.5 rounded-lg hover:bg-accent/50 text-muted-foreground hover:text-foreground transition-colors"
                    >
                        <X size={18} />
                    </button>
                </div>
                )}

                <div className="px-6 pb-6 space-y-4">
                    {/* Option 1: Schedule a call */}
                    <button
                        onClick={handleScheduleCall}
                        className="w-full flex items-center gap-3 px-4 py-3.5 rounded-xl bg-[hsl(45,80%,45%)]/10 hover:bg-[hsl(45,80%,45%)]/20 border border-[hsl(45,80%,45%)]/30 transition-all duration-200 group"
                    >
                        <div className="w-10 h-10 rounded-lg bg-[hsl(45,80%,45%)]/20 flex items-center justify-center shrink-0">
                            <CalendarDays size={20} className="text-[hsl(45,80%,45%)]" />
                        </div>
                        <div className="text-left">
                            <span className="text-sm font-semibold text-foreground block">
                                Schedule a 30-minute call
                            </span>
                            <span className="text-xs text-muted-foreground">
                                Wed–Fri, 10am–12pm ET • Zoom
                            </span>
                        </div>
                    </button>

                    {/* Divider */}
                    <div className="flex items-center gap-3">
                        <div className="flex-1 h-px bg-border" />
                        <span className="text-xs text-muted-foreground font-medium">or</span>
                        <div className="flex-1 h-px bg-border" />
                    </div>

                    {/* Option 2: Quick message form */}
                    {sent ? (
                        <div className="flex flex-col items-center gap-2 py-4">
                            <div className="w-10 h-10 rounded-full bg-green-500/10 flex items-center justify-center">
                                <Check size={20} className="text-green-500" />
                            </div>
                            <p className="text-sm font-semibold text-foreground">Message sent!</p>
                            <p className="text-xs text-muted-foreground text-center">
                                We&apos;ll get back to you within 24 hours.
                                {formEmail && " A confirmation has been sent to your email."}
                            </p>
                        </div>
                    ) : (
                        <div className="space-y-2.5">
                            <div className="grid grid-cols-2 gap-2">
                                <input
                                    type="text"
                                    placeholder="Name"
                                    value={formName}
                                    onChange={(e) => setFormName(e.target.value)}
                                    className="w-full px-3 py-2 rounded-lg bg-background border border-border text-sm text-foreground placeholder:text-muted-foreground/60 focus:outline-none focus:ring-1 focus:ring-[hsl(45,80%,45%)]/50"
                                />
                                <input
                                    type="email"
                                    placeholder="Email"
                                    value={formEmail}
                                    onChange={(e) => setFormEmail(e.target.value)}
                                    className="w-full px-3 py-2 rounded-lg bg-background border border-border text-sm text-foreground placeholder:text-muted-foreground/60 focus:outline-none focus:ring-1 focus:ring-[hsl(45,80%,45%)]/50"
                                />
                            </div>
                            <textarea
                                placeholder="Tell us about your use case (optional)..."
                                value={formMessage}
                                onChange={(e) => setFormMessage(e.target.value)}
                                rows={3}
                                className="w-full px-3 py-2 rounded-lg bg-background border border-border text-sm text-foreground placeholder:text-muted-foreground/60 focus:outline-none focus:ring-1 focus:ring-[hsl(45,80%,45%)]/50 resize-none"
                            />
                            {error && (
                                <p className="text-xs text-red-500">{error}</p>
                            )}
                            <button
                                onClick={handleSendMessage}
                                disabled={sending || (!formName.trim() && !formEmail.trim())}
                                className="w-full flex items-center justify-center gap-2 px-4 py-2.5 rounded-xl bg-foreground text-background text-sm font-semibold hover:opacity-90 transition-opacity disabled:opacity-40 disabled:cursor-not-allowed"
                            >
                                {sending ? (
                                    <>
                                        <Loader2 size={14} className="animate-spin" />
                                        Sending...
                                    </>
                                ) : (
                                    <>
                                        <Send size={14} />
                                        Send message
                                    </>
                                )}
                            </button>
                        </div>
                    )}

                    {/* Divider */}
                    <div className="flex items-center gap-3">
                        <div className="flex-1 h-px bg-border" />
                        <span className="text-xs text-muted-foreground font-medium">or email directly</span>
                        <div className="flex-1 h-px bg-border" />
                    </div>

                    {/* Option 3: Email address */}
                    <div className="flex items-center gap-2">
                        <a
                            href="mailto:daniel@homecastr.com"
                            className="flex-1 flex items-center gap-2 px-4 py-2.5 rounded-xl bg-background border border-border hover:border-[hsl(45,80%,45%)]/40 transition-colors no-underline"
                        >
                            <Mail size={16} className="text-muted-foreground shrink-0" />
                            <span className="text-sm font-medium text-foreground">daniel@homecastr.com</span>
                        </a>
                        <button
                            onClick={handleCopyEmail}
                            className="px-3 py-2.5 rounded-xl bg-background border border-border hover:border-[hsl(45,80%,45%)]/40 transition-colors"
                            title="Copy email"
                        >
                            {copied ? (
                                <Check size={16} className="text-green-500" />
                            ) : (
                                <Copy size={16} className="text-muted-foreground" />
                            )}
                        </button>
                    </div>
                </div>
            </div>
        </div>
    )
}
