"use client"

import { useState, useCallback } from "react"
import { X, MessageSquare, Check, Send, Loader2 } from "lucide-react"

interface FeedbackModalProps {
    isOpen: boolean
    onClose: () => void
}

export function FeedbackModal({ isOpen, onClose }: FeedbackModalProps) {
    const [formEmail, setFormEmail] = useState("")
    const [formMessage, setFormMessage] = useState("")
    const [sending, setSending] = useState(false)
    const [sent, setSent] = useState(false)
    const [error, setError] = useState<string | null>(null)

    const handleSendMessage = useCallback(async () => {
        if (sending || !formMessage.trim()) return
        setSending(true)
        setError(null)

        try {
            const res = await fetch("/api/contact", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    name: "App Feedback",
                    email: formEmail.trim(),
                    message: `[App Feedback]\n\n${formMessage.trim()}`,
                }),
            })

            if (!res.ok) {
                const data = await res.json().catch(() => ({}))
                throw new Error(data.error || "Failed to send")
            }

            setSent(true)
            setFormEmail("")
            setFormMessage("")
            // Reset sent state after a few seconds and close modal
            setTimeout(() => {
                setSent(false)
                onClose()
            }, 3000)
        } catch (err) {
            setError(err instanceof Error ? err.message : "Something went wrong. Please try again.")
        } finally {
            setSending(false)
        }
    }, [sending, formEmail, formMessage, onClose])

    if (!isOpen) return null

    return (
        <div className="fixed inset-0 z-[10001] flex items-center justify-center">
            {/* Backdrop */}
            <div
                className="absolute inset-0 bg-black/50 backdrop-blur-sm"
                onClick={onClose}
            />

            {/* Modal */}
            <div className="relative z-10 w-full max-w-md mx-4 glass-panel rounded-2xl shadow-2xl border border-border/50 overflow-hidden">
                {/* Header */}
                <div className="flex items-center justify-between px-6 pt-5 pb-3">
                    <div className="flex items-center gap-2">
                        <MessageSquare size={18} className="text-primary" />
                        <div>
                            <h2 className="text-lg font-bold text-foreground">Share Feedback</h2>
                            <p className="text-xs text-muted-foreground mt-0.5">
                                Found a bug? Have a feature request? Let us know.
                            </p>
                        </div>
                    </div>
                    <button
                        onClick={onClose}
                        className="p-1.5 rounded-lg hover:bg-accent/50 text-muted-foreground hover:text-foreground transition-colors"
                    >
                        <X size={18} />
                    </button>
                </div>

                <div className="px-6 pb-6 space-y-4">
                    {sent ? (
                        <div className="flex flex-col items-center gap-2 py-4">
                            <div className="w-10 h-10 rounded-full bg-green-500/10 flex items-center justify-center">
                                <Check size={20} className="text-green-500" />
                            </div>
                            <p className="text-sm font-semibold text-foreground">Feedback sent!</p>
                            <p className="text-xs text-muted-foreground text-center">
                                Thank you for helping improve Homecastr!
                            </p>
                        </div>
                    ) : (
                        <div className="space-y-3">
                            <textarea
                                placeholder="What's on your mind? Found a bug? A feature you'd love to see?"
                                value={formMessage}
                                onChange={(e) => setFormMessage(e.target.value)}
                                rows={4}
                                className="w-full px-3 py-2 rounded-lg bg-background border border-border text-sm text-foreground placeholder:text-muted-foreground/60 focus:outline-none focus:ring-1 focus:ring-[hsl(45,80%,45%)]/50 resize-none"
                            />
                            <input
                                type="email"
                                placeholder="Email (optional, if you want a reply)"
                                value={formEmail}
                                onChange={(e) => setFormEmail(e.target.value)}
                                className="w-full px-3 py-2 rounded-lg bg-background border border-border text-sm text-foreground placeholder:text-muted-foreground/60 focus:outline-none focus:ring-1 focus:ring-[hsl(45,80%,45%)]/50"
                            />
                            {error && (
                                <p className="text-xs text-red-500">{error}</p>
                            )}
                            <button
                                onClick={handleSendMessage}
                                disabled={sending || !formMessage.trim()}
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
                                        Submit Feedback
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
