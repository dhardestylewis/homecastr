"use client"

import { createContext, useContext, useState, useCallback, useEffect, type ReactNode } from "react"
import { useRouter, useSearchParams } from "next/navigation"

export interface ForecastContext {
  tractGeoid?: string
  neighborhoodName?: string
  city?: string
  state?: string
  currentUrl?: string
}

interface Message {
  id: string
  role: "user" | "assistant"
  content: string
  toolCalls?: Array<{
    name: string
    args: Record<string, unknown>
    result?: unknown
  }>
}

interface AssistantContextValue {
  // Chat state
  messages: Message[]
  isOpen: boolean
  isLoading: boolean
  threadId: string | null
  
  // Page context
  forecastContext: ForecastContext | null
  
  // Actions
  setOpen: (open: boolean) => void
  sendMessage: (content: string) => Promise<void>
  setForecastContext: (ctx: ForecastContext | null) => void
  clearChat: () => void
  
  // Navigation (for tool-triggered navigation)
  navigateTo: (url: string) => void
}

const AssistantContext = createContext<AssistantContextValue | null>(null)

export function useAssistant() {
  const ctx = useContext(AssistantContext)
  if (!ctx) throw new Error("useAssistant must be used within AssistantProvider")
  return ctx
}

export function AssistantProvider({ children }: { children: ReactNode }) {
  const router = useRouter()
  const searchParams = useSearchParams()
  
  const [messages, setMessages] = useState<Message[]>([])
  const [isOpen, setIsOpen] = useState(false)
  const [isLoading, setIsLoading] = useState(false)
  const [forecastContext, setForecastContext] = useState<ForecastContext | null>(null)
  const [threadId, setThreadId] = useState<string | null>(null)
  const [hasInitialized, setHasInitialized] = useState(false)

  // Load thread from URL param on mount
  useEffect(() => {
    if (hasInitialized) return
    
    const threadParam = searchParams.get("thread")
    const queryParam = searchParams.get("q")
    
    if (threadParam) {
      // Load existing thread
      loadThread(threadParam)
      setIsOpen(true)
    } else if (queryParam) {
      // Auto-send query and create new thread
      setIsOpen(true)
      // Wait for context to be set, then send
      const timeout = setTimeout(() => {
        sendMessageInternal(queryParam, true)
      }, 500)
      return () => clearTimeout(timeout)
    }
    
    setHasInitialized(true)
  }, [searchParams, hasInitialized])

  // Save thread to database when messages change
  useEffect(() => {
    if (messages.length === 0) return
    if (messages.length === 1 && messages[0].id === "greeting") return
    
    saveThread()
  }, [messages])

  const loadThread = async (id: string) => {
    try {
      const response = await fetch(`/api/threads/${id}`)
      if (!response.ok) return
      
      const data = await response.json()
      setThreadId(id)
      setMessages(data.messages || [])
      if (data.forecastContext) {
        setForecastContext(data.forecastContext)
      }
    } catch (error) {
      console.error("Failed to load thread:", error)
    }
  }

  const saveThread = async () => {
    try {
      const body = {
        threadId,
        messages,
        forecastContext,
      }
      
      const response = await fetch("/api/threads", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      })
      
      if (!response.ok) return
      
      const data = await response.json()
      
      // Update thread ID if this was a new thread
      if (data.id && data.id !== threadId) {
        setThreadId(data.id)
        // Update URL with thread ID (without full page reload)
        const url = new URL(window.location.href)
        url.searchParams.set("thread", data.id)
        url.searchParams.delete("q") // Remove query param after processing
        window.history.replaceState({}, "", url.toString())
      }
    } catch (error) {
      console.error("Failed to save thread:", error)
    }
  }

  const navigateTo = useCallback((url: string) => {
    // Preserve thread ID when navigating
    if (threadId) {
      const urlObj = new URL(url, window.location.origin)
      urlObj.searchParams.set("thread", threadId)
      router.push(urlObj.toString())
    } else {
      router.push(url)
    }
  }, [router, threadId])

  const sendMessageInternal = async (content: string, isInitialQuery = false) => {
    const userMessage: Message = {
      id: `user-${Date.now()}`,
      role: "user",
      content,
    }
    
    setMessages(prev => [...prev, userMessage])
    setIsLoading(true)

    try {
      // Build context-aware system message for the forecast page
      const contextMessage = forecastContext?.neighborhoodName 
        ? `The user is viewing the forecast for ${forecastContext.neighborhoodName} in ${forecastContext.city}, ${forecastContext.state} (Census Tract ${forecastContext.tractGeoid}). Answer questions about this specific forecast. Be concise and data-focused.`
        : ""
      
      // Build messages array with context
      const currentMessages = isInitialQuery ? [] : messages
      const chatMessages = [
        ...(contextMessage ? [{ role: "system" as const, content: contextMessage }] : []),
        ...currentMessages.filter(m => m.id !== "greeting").map(m => ({
          role: m.role as "user" | "assistant",
          content: m.content,
        })),
        { role: "user" as const, content },
      ]

      const response = await fetch("/api/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          messages: chatMessages,
          mode: "forecast",
        }),
      })

      if (!response.ok) throw new Error("Chat request failed")

      const data = await response.json()
      
      const assistantMessage: Message = {
        id: `assistant-${Date.now()}`,
        role: "assistant",
        content: data.message?.content || data.error || "No response",
      }

      // Handle map actions that might trigger navigation
      if (data.mapActions && Array.isArray(data.mapActions)) {
        const navAction = data.mapActions.find((a: { action?: string; area_id?: string; level?: string }) => 
          a.area_id && a.level === "tract"
        )
        if (navAction) {
          assistantMessage.toolCalls = [{
            name: "fly_to_location",
            args: navAction,
          }]
        }
      }

      setMessages(prev => [...prev, assistantMessage])
    } catch (error) {
      console.error("Chat error:", error)
      setMessages(prev => [...prev, {
        id: `error-${Date.now()}`,
        role: "assistant",
        content: "Sorry, I encountered an error. Please try again.",
      }])
    } finally {
      setIsLoading(false)
    }
  }

  const sendMessage = useCallback(async (content: string) => {
    await sendMessageInternal(content, false)
  }, [messages, forecastContext])

  const clearChat = useCallback(() => {
    setMessages([])
    setThreadId(null)
    // Remove thread param from URL
    const url = new URL(window.location.href)
    url.searchParams.delete("thread")
    url.searchParams.delete("q")
    window.history.replaceState({}, "", url.toString())
  }, [])

  const setOpen = useCallback((open: boolean) => {
    setIsOpen(open)
    // Auto-greet when opening on a forecast page
    if (open && messages.length === 0 && forecastContext?.neighborhoodName) {
      setMessages([{
        id: "greeting",
        role: "assistant",
        content: `I can help you understand this forecast for ${forecastContext.neighborhoodName}. Ask me about the outlook, compare with other areas, or explore different neighborhoods.`,
      }])
    }
  }, [messages.length, forecastContext?.neighborhoodName])

  return (
    <AssistantContext.Provider
      value={{
        messages,
        isOpen,
        isLoading,
        threadId,
        forecastContext,
        setOpen,
        sendMessage,
        setForecastContext,
        clearChat,
        navigateTo,
      }}
    >
      {children}
    </AssistantContext.Provider>
  )
}
