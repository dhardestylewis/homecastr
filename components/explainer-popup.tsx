"use client"

import { useState, useEffect } from "react"
import { X, MousePointer2, GitCompare, MousePointerClick, Lock } from "lucide-react"

export function ExplainerPopup() {
    const [isOpen, setIsOpen] = useState(false)

    useEffect(() => {
        // Check if user has seen the popup
        const hasSeen = localStorage.getItem("properlytic_explainer_seen")
        if (!hasSeen) {
            // Small delay to allow map to load first
            const timer = setTimeout(() => setIsOpen(true), 1000)
            return () => clearTimeout(timer)
        }
    }, [])

    const handleDismiss = () => {
        setIsOpen(false)
        localStorage.setItem("properlytic_explainer_seen", "true")
    }

    if (!isOpen) return null

    return (
        <div className="fixed inset-0 z-[10000] flex items-center justify-center bg-black/60 backdrop-blur-sm p-4 animate-in fade-in duration-300">
            <div className="bg-[#0f172a] border border-border/50 text-foreground max-w-2xl w-full rounded-2xl shadow-2xl overflow-hidden glass-panel animate-in zoom-in-95 duration-300">
                {/* Header */}
                <div className="p-6 border-b border-white/10 bg-white/5 flex justify-between items-start">
                    <div>
                        <h2 className="text-2xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-teal-400 to-amber-400">
                            Welcome to InvestMap
                        </h2>
                        <p className="text-muted-foreground mt-1">
                            A powerful platform for visualizing real estate value trends, forecasts, and confidence metrics.
                        </p>
                    </div>
                    <button
                        onClick={handleDismiss}
                        className="p-1 hover:bg-white/10 rounded-full transition-colors"
                    >
                        <X className="w-5 h-5 text-muted-foreground" />
                    </button>
                </div>

                {/* Content */}
                <div className="p-6 space-y-6">
                    <div className="grid md:grid-cols-2 gap-6">
                        {/* Project Overview */}
                        <div className="space-y-3">
                            <h3 className="font-semibold text-lg text-teal-400">What is this?</h3>
                            <p className="text-sm text-slate-300 leading-relaxed">
                                InvestMap aggregates property data into hexagonal grids to reveal granular market dynamics.
                                We define "Value" not just by price, but by predictive confidence and stability over time.
                            </p>
                            <p className="text-sm text-slate-300 leading-relaxed">
                                Use this tool to identify high-growth areas, compare neighborhoods, and visualize AI-driven future value projections.
                            </p>
                        </div>

                        {/* Interaction Guide */}
                        <div className="space-y-4">
                            <h3 className="font-semibold text-lg text-amber-400">How to use</h3>

                            <div className="flex gap-3 items-start">
                                <div className="p-2 bg-teal-500/10 rounded-lg">
                                    <MousePointerClick className="w-5 h-5 text-teal-400" />
                                </div>
                                <div>
                                    <div className="font-medium text-sm">Click to Lock</div>
                                    <div className="text-xs text-muted-foreground">Select a tile to lock the tooltip and view detailed charts.</div>
                                </div>
                            </div>

                            <div className="flex gap-3 items-start">
                                <div className="p-2 bg-amber-500/10 rounded-lg">
                                    <GitCompare className="w-5 h-5 text-amber-400" />
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
                        onClick={handleDismiss}
                        className="px-6 py-2.5 bg-primary text-primary-foreground font-medium rounded-lg hover:bg-primary/90 transition-colors shadow-lg shadow-primary/20"
                    >
                        Get Started
                    </button>
                </div>
            </div>
        </div>
    )
}
