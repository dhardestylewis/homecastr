"use client"

import { useState, useCallback } from "react"
import { CalendarDays, Mail, Copy, Check, Send, Loader2, Lock } from "lucide-react"

declare global {
    interface Window {
        Calendly?: {
            initPopupWidget: (opts: { url: string }) => void
        }
    }
}

interface RequestAnalysisModalProps {
    neighborhoodName: string
}

export function RequestAnalysisModal({ neighborhoodName }: RequestAnalysisModalProps) {
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
                    message: `[Data Request] ${neighborhoodName}\n\n${formMessage.trim()}`,
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
            setTimeout(() => setSent(false), 5000)
        } catch (err) {
            setError(err instanceof Error ? err.message : "Something went wrong. Please try again.")
        } finally {
            setSending(false)
        }
    }, [sending, formName, formEmail, formMessage, neighborhoodName])

    return (
        <div className="fixed inset-0 z-[10001] flex items-center justify-center">
            {/* Backdrop */}
            <div className="absolute inset-0 bg-background/20 backdrop-blur-md" />

            {/* Modal */}
            <div className="relative z-10 w-full max-w-md mx-4 glass-panel rounded-2xl shadow-2xl border border-border/50 overflow-hidden">
                {/* Header */}
                <div className="flex items-center gap-3 px-6 pt-6 pb-4 border-b border-border/40">
                    <div className="w-10 h-10 rounded-full bg-primary/10 flex items-center justify-center shrink-0">
                        <Lock size={18} className="text-primary" />
                    </div>
                    <div>
                        <h2 className="text-lg font-bold text-foreground">Forecast Locked</h2>
                        <p className="text-sm text-foreground mt-1">
                            This forecast is available for purchase.
                        </p>
                    </div>
                </div>

                <div className="px-6 py-6 space-y-4">
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
                                Discuss this market directly with our data team
                            </span>
                        </div>
                    </button>

                    {/* Divider */}
                    <div className="flex items-center gap-3">
                        <div className="flex-1 h-px bg-border" />
                        <span className="text-xs text-muted-foreground font-medium">or request via email</span>
                        <div className="flex-1 h-px bg-border" />
                    </div>

                    {/* Option 2: Quick message form */}
                    {sent ? (
                        <div className="flex flex-col items-center gap-2 py-4">
                            <div className="w-10 h-10 rounded-full bg-green-500/10 flex items-center justify-center">
                                <Check size={20} className="text-green-500" />
                            </div>
                            <p className="text-sm font-semibold text-foreground">Request sent!</p>
                            <p className="text-xs text-muted-foreground text-center">
                                We&apos;ll get back to you with the custom report.
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
                                placeholder="Any specific questions about this area?"
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
                                        Request Data
                                    </>
                                )}
                            </button>
                        </div>
                    )}
                </div>
            </div>
        </div>
    )
}
