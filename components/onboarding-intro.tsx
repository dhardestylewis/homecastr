"use client"

import { useState, useEffect, useCallback, useRef } from "react"

/**
 * Cinematic animated onboarding intro — replaces the old ExplainerPopup.
 *
 * Features:
 * 1. IP geolocation → flyTo user's approximate neighborhood
 * 2. Animated cursor dispatches real mousemove/click on MapLibre canvas
 * 3. After click, reads the ACTUAL selected feature name from the map
 * 4. Pulsing highlight on tooltip when "forecast" caption shows
 * 5. Selection LEFT ACTIVE on exit for immediate interactivity
 * 6. Device-aware CTA (click/hover vs tap/hold)
 *
 * Debug mode: add ?onboarding_debug=1 to URL to force-show and see detected info.
 *             add ?onboarding_lat=29.77&onboarding_lng=-95.36 to simulate IP coords.
 *
 * pointer-events: none — users can interact with the map underneath.
 * Uses localStorage to only show on first visit.
 */

const LOCALSTORAGE_KEY = "properlytic_onboarding_seen"
const TOTAL_DURATION_MS = 12000

// Cursor path: ends near center since we fly to user's neighborhood
const CURSOR_KEYFRAMES: [number, number, number][] = [
    [0, 30, 20],
    [10, 30, 20],
    [35, 38, 38],
    [55, 45, 50],
    [70, 45, 50],
    [85, 45, 50],
    [100, 45, 50],
]

function getCursorPos(t: number): { top: number; left: number } {
    const pct = t * 100
    let i = 0
    for (; i < CURSOR_KEYFRAMES.length - 1; i++) {
        if (pct <= CURSOR_KEYFRAMES[i + 1][0]) break
    }
    const [t0, top0, left0] = CURSOR_KEYFRAMES[i]
    const [t1, top1, left1] = CURSOR_KEYFRAMES[Math.min(i + 1, CURSOR_KEYFRAMES.length - 1)]
    const range = t1 - t0 || 1
    const local = (pct - t0) / range
    return {
        top: top0 + (top1 - top0) * local,
        left: left0 + (left1 - left0) * local,
    }
}

/** Access MapLibre map instance from the DOM */
function getMapInstance(): any | null {
    const container = document.querySelector(".maplibregl-map") as HTMLElement | null
    if (!container) return null
    // @ts-expect-error internal MapLibre property
    return container._map || container.__map || null
}

/** Read the feature name at a screen point from the map */
function readFeatureNameAtPoint(x: number, y: number): string | null {
    const map = getMapInstance()
    if (!map) return null
    try {
        // Query ALL rendered features at the point
        const features = map.queryRenderedFeatures([x, y])
        for (const f of features) {
            // Look for the forecast-map related layers which have neighborhood names
            const props = f.properties
            if (props?.name) return props.name
            if (props?.neighborhood) return props.neighborhood
            if (props?.NAME) return props.NAME
            if (props?.NAMELSAD) return props.NAMELSAD
        }
    } catch {
        // queryRenderedFeatures might fail if map isn't ready
    }
    return null
}

export function OnboardingIntro() {
    const [shouldShow, setShouldShow] = useState(false)
    const [phase, setPhase] = useState<"idle" | "playing" | "fading" | "done">("idle")
    const [activeCaption, setActiveCaption] = useState<number | null>(null)
    const [showRipple, setShowRipple] = useState(false)
    const [featureName, setFeatureName] = useState<string | null>(null)
    const [isMobile, setIsMobile] = useState(false)
    const [debugInfo, setDebugInfo] = useState<string>("")
    const hoverIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null)
    const isDebugRef = useRef(false)

    // Detect mobile on mount
    useEffect(() => {
        setIsMobile(window.innerWidth < 768)
    }, [])

    // --- 1. Check localStorage + IP geolocation → flyTo ---
    useEffect(() => {
        const params = new URLSearchParams(window.location.search)
        const isDebug = params.get("onboarding_debug") === "1"
        isDebugRef.current = isDebug

        // Check localStorage (skip in debug mode)
        if (!isDebug) {
            const hasSeen = localStorage.getItem(LOCALSTORAGE_KEY)
            if (hasSeen) return
        }

        let cancelled = false

        async function detectAndFly() {
            // Check for debug coordinate overrides
            const overrideLat = params.get("onboarding_lat")
            const overrideLng = params.get("onboarding_lng")

            let lat: number | null = null
            let lng: number | null = null

            if (overrideLat && overrideLng) {
                // Debug mode: use provided coordinates
                lat = parseFloat(overrideLat)
                lng = parseFloat(overrideLng)
                if (isDebug) setDebugInfo(`Debug coords: ${lat.toFixed(4)}, ${lng.toFixed(4)}`)
            } else {
                // Production: IP geolocation
                try {
                    const res = await fetch("https://ipapi.co/json/", {
                        signal: AbortSignal.timeout(3000),
                    })
                    const data = await res.json()
                    lat = data.latitude
                    lng = data.longitude

                    if (isDebug) {
                        setDebugInfo(`IP: ${data.ip}, City: ${data.city}, Coords: ${lat?.toFixed(4)}, ${lng?.toFixed(4)}`)
                    }
                } catch {
                    if (isDebug) setDebugInfo("IP geolocation failed — using default center")
                }
            }

            if (lat && lng && !cancelled) {
                // FlyTo user's detected neighborhood
                const map = getMapInstance()
                if (map && !cancelled) {
                    map.flyTo({
                        center: [lng, lat],
                        zoom: 13,
                        duration: 2000,
                        essential: true,
                    })
                    if (isDebug) {
                        setDebugInfo(prev => prev + ` | Flying to [${lng.toFixed(4)}, ${lat.toFixed(4)}]`)
                    }
                }
            }
        }

        detectAndFly()

        // Start animation after delay (let flyTo complete)
        const timer = setTimeout(() => {
            if (!cancelled) {
                // Mark as seen immediately — don't wait for full 12s playback.
                // This prevents re-showing if the user navigates away mid-demo.
                if (!isDebug) {
                    localStorage.setItem(LOCALSTORAGE_KEY, "true")
                }
                setShouldShow(true)
                setPhase("playing")
            }
        }, 2500)

        return () => {
            cancelled = true
            clearTimeout(timer)
        }
    }, [])

    // --- 2. Synthetic mouse events ---
    const dispatchHover = useCallback((topPct: number, leftPct: number) => {
        const canvas = document.querySelector(".maplibregl-canvas") as HTMLCanvasElement | null
        if (!canvas) return
        const clientX = window.innerWidth * (leftPct / 100)
        const clientY = window.innerHeight * (topPct / 100)
        canvas.dispatchEvent(new MouseEvent("mousemove", {
            clientX,
            clientY,
            bubbles: true,
            cancelable: true,
        }))
    }, [])

    const dispatchClick = useCallback((topPct: number, leftPct: number) => {
        const canvas = document.querySelector(".maplibregl-canvas") as HTMLCanvasElement | null
        if (!canvas) return
        const clientX = window.innerWidth * (leftPct / 100)
        const clientY = window.innerHeight * (topPct / 100)
        canvas.dispatchEvent(new MouseEvent("click", {
            clientX,
            clientY,
            bubbles: true,
            cancelable: true,
        }))

        // Read the actual feature name at the click point
        setTimeout(() => {
            const name = readFeatureNameAtPoint(clientX, clientY)
            if (name) {
                setFeatureName(name)
                if (isDebugRef.current) {
                    setDebugInfo(prev => prev + ` | Feature: "${name}"`)
                }
            }
        }, 300) // small delay for map to process the click
    }, [])

    // --- 3. Highlight the real tooltip ---
    const highlightTooltip = useCallback((show: boolean) => {
        const tooltips = document.querySelectorAll(".glass-panel")
        if (tooltips.length > 0) {
            const tooltip = tooltips[tooltips.length - 1]
            if (show) {
                tooltip.classList.add("onboarding-tooltip-highlight")
            } else {
                tooltip.classList.remove("onboarding-tooltip-highlight")
            }
        }
    }, [])

    // --- 4. Animation sequencing ---
    useEffect(() => {
        if (phase !== "playing") return
        const timers: ReturnType<typeof setTimeout>[] = []

        // Caption 0: generic city-level intro (before click — always accurate)
        timers.push(setTimeout(() => setActiveCaption(0), 500))
        timers.push(setTimeout(() => setActiveCaption(null), 3500))

        // Start hover events along cursor path
        const CURSOR_CSS_DURATION = 7000
        const HOVER_START_MS = 3000
        const HOVER_END_MS = 9000
        timers.push(setTimeout(() => {
            const hoverStartTime = Date.now() - HOVER_START_MS
            hoverIntervalRef.current = setInterval(() => {
                const elapsed = Date.now() - hoverStartTime
                const t = Math.min(elapsed / CURSOR_CSS_DURATION, 1)
                const pos = getCursorPos(t)
                dispatchHover(pos.top, pos.left)
            }, 80)
        }, HOVER_START_MS))

        // Ripple at cursor destination
        timers.push(setTimeout(() => setShowRipple(true), 4000))

        // Click to select feature + show tooltip (~5s)
        timers.push(setTimeout(() => {
            dispatchClick(45, 50)
        }, 5000))

        // Caption 1: "This is [actual feature name]" — appears AFTER click, reading real name
        // Shows at 5.8s (after 300ms feature read delay from click at 5s)
        timers.push(setTimeout(() => setActiveCaption(1), 5800))
        timers.push(setTimeout(() => {
            highlightTooltip(true)
        }, 6000))
        timers.push(setTimeout(() => {
            setActiveCaption(null)
            highlightTooltip(false)
        }, 8000))

        // Caption 2: device-aware CTA
        timers.push(setTimeout(() => setActiveCaption(2), 8400))
        timers.push(setTimeout(() => setActiveCaption(null), 10200))

        // Stop hover + hide ripple
        timers.push(setTimeout(() => {
            if (hoverIntervalRef.current) {
                clearInterval(hoverIntervalRef.current)
                hoverIntervalRef.current = null
            }
            setShowRipple(false)
        }, HOVER_END_MS))

        // Fade out — LEAVE selection active
        timers.push(setTimeout(() => setPhase("fading"), TOTAL_DURATION_MS - 1500))

        // Done
        timers.push(setTimeout(() => {
            setPhase("done")
            localStorage.setItem(LOCALSTORAGE_KEY, "true")
        }, TOTAL_DURATION_MS))

        return () => {
            timers.forEach(clearTimeout)
            if (hoverIntervalRef.current) {
                clearInterval(hoverIntervalRef.current)
                hoverIntervalRef.current = null
            }
        }
    }, [phase, dispatchHover, dispatchClick, highlightTooltip])

    const handleSkip = useCallback(() => {
        if (hoverIntervalRef.current) {
            clearInterval(hoverIntervalRef.current)
            hoverIntervalRef.current = null
        }
        highlightTooltip(false)
        setPhase("done")
        localStorage.setItem(LOCALSTORAGE_KEY, "true")
    }, [highlightTooltip])

    if (!shouldShow || phase === "done") return null

    const captions = [
        "Explore Your Neighborhood",
        featureName
            ? `This is ${featureName} — every color is a forecast`
            : "Every color is a property value forecast",
        isMobile
            ? "Tap to select · Hold to compare"
            : "Click to select · Hover to compare",
    ]

    return (
        <div
            className={`onboarding-overlay ${phase === "fading" ? "onboarding-fade-out" : "onboarding-fade-in"}`}
            style={{ pointerEvents: "none" }}
        >
            {/* Subtle vignette */}
            <div className="onboarding-vignette" />

            {/* Debug overlay */}
            {isDebugRef.current && debugInfo && (
                <div style={{
                    position: "absolute",
                    top: 8,
                    left: 8,
                    zIndex: 100,
                    padding: "6px 12px",
                    background: "rgba(0,0,0,0.8)",
                    color: "#4ade80",
                    fontFamily: "monospace",
                    fontSize: "11px",
                    borderRadius: 8,
                    maxWidth: "90vw",
                    wordBreak: "break-all",
                    pointerEvents: "none",
                }}>
                    {debugInfo}
                </div>
            )}

            {/* Animated cursor */}
            <div className="onboarding-cursor-container">
                <svg width="32" height="32" viewBox="0 0 24 24" fill="none" className="onboarding-cursor">
                    <defs>
                        <filter id="cursor-glow" x="-50%" y="-50%" width="200%" height="200%">
                            <feGaussianBlur stdDeviation="2" result="blur" />
                            <feMerge>
                                <feMergeNode in="blur" />
                                <feMergeNode in="SourceGraphic" />
                            </feMerge>
                        </filter>
                        <linearGradient id="cursor-gradient" x1="0" y1="0" x2="1" y2="1">
                            <stop offset="0%" stopColor="#fbbf24" />
                            <stop offset="100%" stopColor="#f59e0b" />
                        </linearGradient>
                    </defs>
                    <path
                        d="M5 3l14 8-6.5 1.5L11 19z"
                        fill="url(#cursor-gradient)"
                        stroke="#fff"
                        strokeWidth="1"
                        strokeLinejoin="round"
                        filter="url(#cursor-glow)"
                    />
                </svg>
                <div className="onboarding-cursor-trail" />
            </div>

            {/* Ripple at cursor destination */}
            {showRipple && (
                <div className="onboarding-ripple-container onboarding-ripple-center">
                    <div className="onboarding-ripple onboarding-ripple-1" />
                    <div className="onboarding-ripple onboarding-ripple-2" />
                    <div className="onboarding-ripple onboarding-ripple-3" />
                </div>
            )}

            {/* Caption sequence — bottom-left, above Chat/Talk buttons */}
            <div className="onboarding-captions">
                {captions.map((text, i) => (
                    <div
                        key={i}
                        className={`onboarding-caption ${activeCaption === i ? "onboarding-caption-visible" : "onboarding-caption-hidden"}`}
                    >
                        <span>{text}</span>
                    </div>
                ))}
            </div>

            {/* Skip button */}
            <button
                className="onboarding-skip"
                onClick={handleSkip}
                style={{ pointerEvents: "auto" }}
            >
                Skip ›
            </button>
        </div>
    )
}
