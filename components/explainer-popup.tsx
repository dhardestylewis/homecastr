"use client"

import { useState, useEffect } from "react"
import { X, MousePointer2, GitCompare, MousePointerClick, Lock } from "lucide-react"

export function ExplainerPopup() {
    const [isOpen, setIsOpen] = useState(false)
    const [isMinimized, setIsMinimized] = useState(false)
    const [shouldRender, setShouldRender] = useState(false)

    useEffect(() => {
        // Check if user has seen the popup
        const hasSeen = localStorage.getItem("properlytic_explainer_seen")
        if (!hasSeen) {
            // Small delay to allow map to load first
            const timer = setTimeout(() => {
                setIsOpen(true)
                setShouldRender(true)
            }, 1000)
            return () => clearTimeout(timer)
        } else {
            // If seen, we start in minimized state (optional, or just strictly hidden?)
            // The user said "original popup instructions... should minimize... rather than disappear"
            // This implies the Transition acts that way.
            // If I reload, does it reappear minimized? Or show nothing?
            // Usually it shouldn't pester. But maybe a help button is nice.
            // For now, I'll respect the "seen" flag to NOT show the modal.
            // But I'll set shouldRender to true and isMinimized to true so the help button is there?
            // "minimize to a corner visibly minimizing rather than disappear forever"

            // Let's decided: "seen" -> Start Minimized.
            setIsOpen(true)
            setIsMinimized(true)
            setShouldRender(true)
        }
    }, [])

    const handleMinimize = () => {
        setIsMinimized(true)
        localStorage.setItem("properlytic_explainer_seen", "true")
    }

    const handleMaximize = () => {
        setIsMinimized(false)
    }

    const handleClose = (e: React.MouseEvent) => {
        e.stopPropagation()
        setIsOpen(false)
        setShouldRender(false)
        localStorage.setItem("properlytic_explainer_seen", "true")
    }

    if (!shouldRender) return null

    return (
        <>
            {/* Full Modal */}
            <div
                className={`fixed inset-0 z-[10000] flex items-center justify-center transition-colors duration-1000 ease-in-out ${isMinimized ? "pointer-events-none bg-transparent" : "bg-black/60 backdrop-blur-sm p-4"
                    } ${!isOpen ? "hidden" : ""}`}
            >
                <div
                    className={`
                        bg-background border border-border/50 text-foreground w-full rounded-2xl shadow-2xl overflow-hidden glass-panel 
                        transition-all duration-1000 ease-[cubic-bezier(0.4,0,0.2,1)]
                        ${isMinimized
                            ? "opacity-0 scale-[0.05]" // Shrink and fade
                            : "max-w-2xl opacity-100 scale-100 translate-x-0 translate-y-0"
                        }
                    `}
                    style={{
                        transform: isMinimized ? "translate(calc(50vw - 3rem), calc(50vh - 3rem)) scale(0.05)" : undefined
                    }}
                >
                    {/* Header */}
                    <div className="p-6 border-b border-white/10 bg-white/5 flex justify-between items-start">
                        <div>
                            <h2 className="text-2xl font-bold text-foreground">
                                Welcome to Homecastr
                            </h2>
                            <p className="text-muted-foreground mt-1 text-sm font-medium">
                                Smarter models. Clearer forecasts.
                            </p>
                        </div>
                    </div>

                    {/* Content */}
                    <div className="p-6 space-y-6">
                        <div className="grid md:grid-cols-2 gap-6">
                            {/* Project Overview */}
                            <div className="space-y-3">
                                <h3 className="font-semibold text-lg text-primary">About the Map</h3>
                                <p className="text-sm text-foreground leading-relaxed">
                                    Homecastr is a foundation model that forecasts value at the home level.
                                    We provide probable price bands to help you with your home buying and selling decisions, generated from many scenarios of the future.
                                </p>
                                <p className="text-sm text-foreground leading-relaxed">
                                    Use this tool to identify high-growth, compare neighborhoods, and see what the future might be.
                                </p>
                            </div>

                            {/* Interaction Guide */}
                            <div className="space-y-4">
                                <h3 className="font-semibold text-lg text-primary">How to use</h3>

                                <div className="flex gap-3 items-start">
                                    <div className="p-2 bg-primary/10 rounded-lg">
                                        <MousePointerClick className="w-5 h-5 text-primary" />
                                    </div>
                                    <div>
                                        <div className="font-medium text-sm">Click to Lock</div>
                                        <div className="text-xs text-muted-foreground">Select a tile to lock the tooltip and view detailed charts.</div>
                                    </div>
                                </div>

                                <div className="flex gap-3 items-start">
                                    <div className="p-2 bg-primary/10 rounded-lg">
                                        <GitCompare className="w-5 h-5 text-primary" />
                                    </div>
                                    <div>
                                        <div className="font-medium text-sm">Hover to Compare</div>
                                        <div className="text-xs text-muted-foreground">While locked, hover other tiles to overlay comparison data.</div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>

                    {/* Footer */}
                    <div className="p-6 pt-0 flex justify-end">
                        <button
                            onClick={handleMinimize}
                            className="px-6 py-2.5 bg-primary text-primary-foreground font-medium rounded-lg hover:bg-primary/90 transition-colors shadow-lg shadow-primary/20"
                        >
                            Get Started
                        </button>
                    </div>
                </div>
            </div>

            {/* Minimized Button */}
            <button
                onClick={handleMaximize}
                className={`
                    fixed bottom-6 right-6 z-[10000] w-12 h-12 rounded-full bg-primary text-primary-foreground shadow-lg shadow-primary/25 
                    flex items-center justify-center hover:scale-110 active:scale-95 transition-all duration-500 ease-out
                    ${isMinimized ? "translate-y-0 opacity-100 rotate-0" : "translate-y-20 opacity-0 rotate-90 pointer-events-none"}
                `}
                aria-label="Open Help"
            >
                <span className="sr-only">Help</span>
                <span className="text-xl font-bold">?</span>
            </button>
        </>
    )
}
