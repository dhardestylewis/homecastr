import type React from "react"
import type { Metadata } from "next"
import Link from "next/link"
import { Suspense } from "react"
import { HomecastrLogo } from "@/components/homecastr-logo"
import { AssistantProvider } from "@/components/assistant/AssistantProvider"
import { ForecastAssistant } from "@/components/assistant/ForecastAssistant"

export const metadata: Metadata = {
    title: {
        template: "%s | Homecastr",
        default: "Homecastr Forecasts",
    },
    description: "Homecastr explains where home values are likely headed, why, and which nearby areas may offer stronger appreciation potential.",
}

export default function ForecastsLayout({
    children,
}: {
    children: React.ReactNode
}) {
    return (
        <Suspense fallback={null}>
        <AssistantProvider>
        <div className="h-screen overflow-auto bg-background text-foreground">
            {/* Header */}
            <header className="sticky top-0 z-50 border-b border-border bg-background/95 backdrop-blur-xl">
                <div className="mx-auto flex h-16 max-w-6xl items-center justify-between px-4 sm:px-6">
                    <Link href="/" className="flex items-center group">
                        <HomecastrLogo size={28} variant="horizontal" />
                    </Link>
                    <nav className="flex items-center gap-6">
                        <Link href="/forecasts" className="text-sm text-muted-foreground hover:text-foreground transition-colors">
                            Forecasts
                        </Link>
                        <Link
                            href="/"
                            className="text-sm px-4 py-2 rounded-lg bg-primary text-primary-foreground hover:opacity-90 transition-all"
                        >
                            Explore Map
                        </Link>
                    </nav>
                </div>
            </header>

            {/* Main content */}
            <main className="mx-auto max-w-6xl px-4 py-8 sm:px-6 sm:py-12">
                {children}
            </main>

            {/* Footer */}
            <footer className="border-t border-border bg-card">
                <div className="mx-auto max-w-6xl px-4 py-10 sm:px-6">
                    <div className="flex flex-col gap-6 sm:flex-row sm:items-center sm:justify-between">
                        <div className="space-y-2">
                            <HomecastrLogo size={24} variant="horizontal" className="opacity-60" />
                            <p className="text-xs text-muted-foreground max-w-md">
                                Homecastr explains where home values are likely headed, why, and which nearby areas may offer stronger appreciation potential. Forecasts are model outputs, not financial advice.
                            </p>
                        </div>
                        <div className="flex gap-6 text-xs text-muted-foreground">
                            <Link href="/about" className="hover:text-foreground transition-colors">About</Link>
                            <Link href="/privacy" className="hover:text-foreground transition-colors">Privacy</Link>
                            <Link href="/terms" className="hover:text-foreground transition-colors">Terms</Link>
                            <Link href="/support" className="hover:text-foreground transition-colors">Support</Link>
                        </div>
                    </div>
                    <p className="mt-6 text-xs text-muted-foreground/50">
                        © {new Date().getFullYear()} Homecastr. All rights reserved.
                    </p>
                </div>
            </footer>
            
            {/* Contextual Assistant - floats on forecast pages */}
            <ForecastAssistant />
        </div>
        </AssistantProvider>
        </Suspense>
    )
}
