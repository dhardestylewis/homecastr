"use client"

import { useState, useRef, useEffect } from "react"
import { useAssistant } from "./AssistantProvider"
import { MessageSquare, X, Send, Loader2, Sparkles, ArrowRight } from "lucide-react"
import { cn } from "@/lib/utils"
import ReactMarkdown from "react-markdown"

const QUICK_PROMPTS = [
  { label: "Downside vs upside", prompt: "Show me the downside scenario vs upside scenario for this area" },
  { label: "Worth in 2030?", prompt: "What could my house be worth in 2030?" },
  { label: "Compare areas", prompt: "What are some similar neighborhoods I could compare this to?" },
  { label: "Why this range?", prompt: "Why is the forecast range this wide?" },
]

export function ForecastAssistant() {
  const { messages, isOpen, isLoading, sendMessage, setOpen, forecastContext } = useAssistant()
  const [input, setInput] = useState("")
  const messagesEndRef = useRef<HTMLDivElement>(null)
  const inputRef = useRef<HTMLInputElement>(null)

  // Auto-scroll to bottom on new messages
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" })
  }, [messages])

  // Focus input when opening
  useEffect(() => {
    if (isOpen) {
      setTimeout(() => inputRef.current?.focus(), 100)
    }
  }, [isOpen])

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!input.trim() || isLoading) return
    
    const message = input.trim()
    setInput("")
    await sendMessage(message)
  }

  const handleQuickPrompt = async (prompt: string) => {
    if (isLoading) return
    await sendMessage(prompt)
  }

  if (!isOpen) {
    return (
      <button
        onClick={() => setOpen(true)}
        className="fixed bottom-6 right-6 z-50 flex items-center gap-2 px-4 py-3 rounded-full bg-primary text-primary-foreground shadow-lg hover:shadow-xl hover:scale-105 transition-all"
      >
        <Sparkles className="w-5 h-5" />
        <span className="font-medium">Ask about this forecast</span>
      </button>
    )
  }

  return (
    <div className="fixed bottom-6 right-6 z-50 w-[400px] max-w-[calc(100vw-48px)] h-[600px] max-h-[calc(100vh-120px)] flex flex-col bg-background border border-border rounded-2xl shadow-2xl overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-border bg-card">
        <div className="flex items-center gap-2">
          <div className="w-8 h-8 rounded-full bg-primary/10 flex items-center justify-center">
            <Sparkles className="w-4 h-4 text-primary" />
          </div>
          <div>
            <p className="text-sm font-medium">Forecast Assistant</p>
            {forecastContext?.neighborhoodName && (
              <p className="text-xs text-muted-foreground truncate max-w-[200px]">
                {forecastContext.neighborhoodName}
              </p>
            )}
          </div>
        </div>
        <button
          onClick={() => setOpen(false)}
          className="p-2 rounded-lg hover:bg-accent transition-colors"
        >
          <X className="w-4 h-4" />
        </button>
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {messages.length === 0 ? (
          <div className="space-y-4">
            <div className="text-center py-6">
              <div className="w-12 h-12 rounded-full bg-primary/10 flex items-center justify-center mx-auto mb-3">
                <MessageSquare className="w-6 h-6 text-primary" />
              </div>
              <p className="text-sm text-muted-foreground">
                Ask me about this forecast, compare areas, or explore other neighborhoods.
              </p>
            </div>
            
            {/* Quick prompts */}
            <div className="space-y-2">
              <p className="text-xs text-muted-foreground uppercase tracking-wider">Try asking:</p>
              <div className="grid gap-2">
                {QUICK_PROMPTS.map((item) => (
                  <button
                    key={item.label}
                    onClick={() => handleQuickPrompt(item.prompt)}
                    disabled={isLoading}
                    className="flex items-center justify-between px-3 py-2.5 text-sm text-left rounded-lg border border-border bg-card hover:bg-accent hover:border-primary/20 transition-all group"
                  >
                    <span>{item.label}</span>
                    <ArrowRight className="w-4 h-4 text-muted-foreground group-hover:text-primary transition-colors" />
                  </button>
                ))}
              </div>
            </div>
          </div>
        ) : (
          messages.map((message) => (
            <div
              key={message.id}
              className={cn(
                "flex",
                message.role === "user" ? "justify-end" : "justify-start"
              )}
            >
              <div
                className={cn(
                  "max-w-[85%] rounded-2xl px-4 py-2.5 text-sm",
                  message.role === "user"
                    ? "bg-primary text-primary-foreground rounded-br-md"
                    : "bg-card border border-border rounded-bl-md"
                )}
              >
                {message.role === "assistant" ? (
                  <div className="prose prose-sm dark:prose-invert max-w-none">
                    <ReactMarkdown
                      components={{
                        p: ({ children }) => <p className="mb-2 last:mb-0">{children}</p>,
                        ul: ({ children }) => <ul className="mb-2 pl-4 list-disc">{children}</ul>,
                        li: ({ children }) => <li className="mb-1">{children}</li>,
                        strong: ({ children }) => <strong className="font-semibold">{children}</strong>,
                      }}
                    >
                      {message.content}
                    </ReactMarkdown>
                  </div>
                ) : (
                  <p>{message.content}</p>
                )}
                
                {/* Show navigation indicator */}
                {message.toolCalls?.some(tc => tc.name === "navigateToForecast") && (
                  <div className="mt-2 pt-2 border-t border-border/50 text-xs text-muted-foreground flex items-center gap-1">
                    <ArrowRight className="w-3 h-3" />
                    Navigating...
                  </div>
                )}
              </div>
            </div>
          ))
        )}
        
        {isLoading && (
          <div className="flex justify-start">
            <div className="bg-card border border-border rounded-2xl rounded-bl-md px-4 py-3">
              <Loader2 className="w-4 h-4 animate-spin text-muted-foreground" />
            </div>
          </div>
        )}
        
        <div ref={messagesEndRef} />
      </div>

      {/* Input */}
      <form onSubmit={handleSubmit} className="p-4 border-t border-border bg-card">
        <div className="flex items-center gap-2">
          <input
            ref={inputRef}
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="Ask about this forecast..."
            disabled={isLoading}
            className="flex-1 px-4 py-2.5 text-sm rounded-xl border border-border bg-background focus:outline-none focus:ring-2 focus:ring-primary/20 focus:border-primary transition-all"
          />
          <button
            type="submit"
            disabled={!input.trim() || isLoading}
            className="p-2.5 rounded-xl bg-primary text-primary-foreground disabled:opacity-50 disabled:cursor-not-allowed hover:opacity-90 transition-all"
          >
            <Send className="w-4 h-4" />
          </button>
        </div>
      </form>
    </div>
  )
}
