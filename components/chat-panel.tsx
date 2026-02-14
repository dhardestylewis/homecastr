"use client"

import { useState, useRef, useEffect, useCallback } from "react"
import { Send, X, Loader2, MessageSquare, MapPin, Sparkles } from "lucide-react"

export interface MapAction {
    lat: number
    lng: number
    zoom: number
    select_hex_id?: string
}

interface ChatMessage {
    role: "user" | "assistant"
    content: string
    mapActions?: MapAction[]
    toolsUsed?: string[]
}

interface ChatPanelProps {
    isOpen: boolean
    onClose: () => void
    onMapAction: (action: MapAction) => void
}

export function ChatPanel({ isOpen, onClose, onMapAction }: ChatPanelProps) {
    const [messages, setMessages] = useState<ChatMessage[]>([])
    const [input, setInput] = useState("")
    const [isLoading, setIsLoading] = useState(false)
    const messagesEndRef = useRef<HTMLDivElement>(null)
    const inputRef = useRef<HTMLInputElement>(null)

    // Auto-scroll to bottom
    useEffect(() => {
        messagesEndRef.current?.scrollIntoView({ behavior: "smooth" })
    }, [messages])

    // Focus input when panel opens
    useEffect(() => {
        if (isOpen) {
            setTimeout(() => inputRef.current?.focus(), 300)
        }
    }, [isOpen])

    const sendMessage = useCallback(async () => {
        if (!input.trim() || isLoading) return

        const userMessage: ChatMessage = { role: "user", content: input.trim() }
        const newMessages = [...messages, userMessage]
        setMessages(newMessages)
        setInput("")
        setIsLoading(true)

        try {
            const response = await fetch("/api/chat", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    messages: newMessages.map((m) => ({
                        role: m.role,
                        content: m.content,
                    })),
                }),
            })

            if (!response.ok) {
                const err = await response.json()
                throw new Error(err.error || "Chat request failed")
            }

            const data = await response.json()

            const assistantMessage: ChatMessage = {
                role: "assistant",
                content: data.message.content,
                mapActions: data.mapActions,
                toolsUsed: data.toolCallsUsed,
            }

            setMessages([...newMessages, assistantMessage])

            // Execute map actions (fly-to, select hex)
            if (data.mapActions && data.mapActions.length > 0) {
                // Use the last map action (most specific)
                const action = data.mapActions[data.mapActions.length - 1]
                onMapAction(action)
            }
        } catch (error: any) {
            setMessages([
                ...newMessages,
                {
                    role: "assistant",
                    content: `⚠️ ${error.message || "Something went wrong. Check your API key in .env.local"}`,
                },
            ])
        } finally {
            setIsLoading(false)
        }
    }, [input, messages, isLoading, onMapAction])

    const handleKeyDown = (e: React.KeyboardEvent) => {
        if (e.key === "Enter" && !e.shiftKey) {
            e.preventDefault()
            sendMessage()
        }
        if (e.key === "Escape") {
            onClose()
        }
    }

    return (
        <div
            className={`
        absolute top-0 left-0 z-[70] h-full
        transition-all duration-300 ease-in-out
        ${isOpen ? "w-full md:w-[400px] opacity-100" : "w-0 opacity-0 pointer-events-none"}
      `}
        >
            <div className="h-full flex flex-col bg-background/95 backdrop-blur-xl border-r border-border shadow-2xl">
                {/* Header */}
                <div className="flex items-center justify-between px-4 py-3 border-b border-border bg-background/80">
                    <div className="flex items-center gap-2">
                        <Sparkles className="w-4 h-4 text-primary" />
                        <span className="font-semibold text-sm">Homecastr AI</span>
                        <span className="text-[10px] px-1.5 py-0.5 bg-primary/10 text-primary rounded-full font-medium">
                            Beta
                        </span>
                    </div>
                    <button
                        onClick={onClose}
                        className="w-7 h-7 rounded-lg flex items-center justify-center hover:bg-muted transition-colors"
                        aria-label="Close chat"
                    >
                        <X className="w-4 h-4" />
                    </button>
                </div>

                {/* Messages */}
                <div className="flex-1 overflow-y-auto px-4 py-3 space-y-4">
                    {messages.length === 0 && (
                        <div className="flex flex-col items-center justify-center h-full text-center px-6 gap-4 opacity-60">
                            <MessageSquare className="w-10 h-10 text-muted-foreground/50" />
                            <div>
                                <p className="text-sm font-medium text-muted-foreground">Ask me anything</p>
                                <p className="text-xs text-muted-foreground/70 mt-1">
                                    I can look up neighborhoods, compare areas, explain metrics, and navigate the map for you.
                                </p>
                            </div>
                            <div className="flex flex-col gap-2 w-full max-w-[260px]">
                                {[
                                    "Show me Montrose",
                                    "Where has the most growth potential?",
                                    "Compare Heights vs Montrose",
                                    "What does reliability mean?",
                                ].map((suggestion) => (
                                    <button
                                        key={suggestion}
                                        onClick={() => {
                                            setInput(suggestion)
                                            setTimeout(() => inputRef.current?.focus(), 0)
                                        }}
                                        className="text-xs text-left px-3 py-2 rounded-lg border border-border/50 hover:bg-muted/50 hover:border-border transition-all text-muted-foreground hover:text-foreground"
                                    >
                                        {suggestion}
                                    </button>
                                ))}
                            </div>
                        </div>
                    )}

                    {messages.map((msg, i) => (
                        <div
                            key={i}
                            className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}
                        >
                            <div
                                className={`max-w-[85%] rounded-2xl px-3.5 py-2.5 text-sm leading-relaxed ${msg.role === "user"
                                    ? "bg-primary text-primary-foreground rounded-br-md"
                                    : "bg-muted/60 text-foreground rounded-bl-md"
                                    }`}
                            >
                                <p className="whitespace-pre-wrap">{msg.content}</p>

                                {/* Show map action indicator */}
                                {msg.mapActions && msg.mapActions.length > 0 && (
                                    <div className="flex items-center gap-1 mt-2 pt-2 border-t border-border/30">
                                        <MapPin className="w-3 h-3 text-primary" />
                                        <span className="text-[10px] text-muted-foreground">
                                            Map updated
                                        </span>
                                    </div>
                                )}

                                {/* Show tools used */}
                                {msg.toolsUsed && msg.toolsUsed.length > 0 && (
                                    <div className="flex flex-wrap gap-1 mt-1.5">
                                        {msg.toolsUsed.map((tool, idx) => (
                                            <span
                                                key={`${tool}-${idx}`}
                                                className="text-[9px] px-1.5 py-0.5 bg-background/50 rounded text-muted-foreground font-mono"
                                            >
                                                {tool}
                                            </span>
                                        ))}
                                    </div>
                                )}
                            </div>
                        </div>
                    ))}

                    {isLoading && (
                        <div className="flex justify-start">
                            <div className="bg-muted/60 rounded-2xl rounded-bl-md px-4 py-3">
                                <div className="flex items-center gap-2">
                                    <Loader2 className="w-3.5 h-3.5 animate-spin text-muted-foreground" />
                                    <span className="text-xs text-muted-foreground">Thinking...</span>
                                </div>
                            </div>
                        </div>
                    )}

                    <div ref={messagesEndRef} />
                </div>

                {/* Input */}
                <div className="px-3 pb-3 pt-2 border-t border-border">
                    <div className="flex items-center gap-2 bg-muted/30 rounded-xl px-3 py-1.5 border border-border/50 focus-within:border-primary/50 transition-colors">
                        <input
                            ref={inputRef}
                            value={input}
                            onChange={(e) => setInput(e.target.value)}
                            onKeyDown={handleKeyDown}
                            placeholder="Ask about a neighborhood, address, or metric..."
                            className="flex-1 bg-transparent text-sm outline-none placeholder:text-muted-foreground/50 py-1.5"
                            disabled={isLoading}
                        />
                        <button
                            onClick={sendMessage}
                            disabled={!input.trim() || isLoading}
                            className="w-8 h-8 rounded-lg flex items-center justify-center bg-primary text-primary-foreground disabled:opacity-30 disabled:cursor-not-allowed hover:bg-primary/90 transition-colors shrink-0"
                            aria-label="Send message"
                        >
                            <Send className="w-3.5 h-3.5" />
                        </button>
                    </div>
                    <p className="text-[9px] text-muted-foreground/50 text-center mt-1.5">
                        AI responses are for informational purposes only
                    </p>
                </div>
            </div>
        </div>
    )
}
