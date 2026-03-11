"use client"

import { useState, useEffect, useRef, useCallback } from "react"
import { X, Minimize2, Maximize2, Mic, MicOff, Video, VideoOff } from "lucide-react"
import { cn } from "@/lib/utils"
import { executeTopLevelTool } from "@/app/actions/tools";
import { executeTopLevelForecastTool } from "@/app/actions/tools-forecast";

interface TavusMiniWindowProps {
  conversationUrl: string
  onClose: () => void
  chatOpen?: boolean
  forecastMode?: boolean
  embedded?: boolean
}

export function TavusMiniWindow({ conversationUrl, onClose, chatOpen = false, forecastMode = false, embedded = false }: TavusMiniWindowProps) {
  const [isMinimized, setIsMinimized] = useState(false)
  const [isMicOn, setIsMicOn] = useState(true)
  const [isCamOn, setIsCamOn] = useState(true)
  const [isJoined, setIsJoined] = useState(false)
  const [hasError, setHasError] = useState<string | null>(null)
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const [remoteParticipant, setRemoteParticipant] = useState<any>(null)

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const callRef = useRef<any>(null)
  const localVideoRef = useRef<HTMLVideoElement>(null)
  const remoteVideoRef = useRef<HTMLVideoElement>(null)
  const remoteAudioRef = useRef<HTMLAudioElement>(null)

  // Create call object and auto-join immediately
  useEffect(() => {
    if (!conversationUrl) return

    let cancelled = false

    async function initCall() {
      try {
        // Dynamic import — daily-js accesses window/navigator at module level
        const DailyIframe = (await import("@daily-co/daily-js")).default

        if (cancelled) return

        const call = DailyIframe.createCallObject({
          videoSource: true,
          audioSource: true,
        })
        callRef.current = call

        const updateRemote = () => {
          if (!callRef.current) return
          const participants = callRef.current.participants()
          const remote = Object.entries(participants).find(([id]: [string, unknown]) => id !== "local")
          setRemoteParticipant(remote ? remote[1] : null)
        }

        call.on("joined-meeting", () => {
          setIsJoined(true)
          updateRemote()
        })
        call.on("participant-joined", updateRemote)
        call.on("participant-updated", updateRemote)
        call.on("participant-left", () => setRemoteParticipant(null))
        call.on("left-meeting", () => {
          setIsJoined(false)
          onClose()
        })
        call.on("error", (e: unknown) => {
          console.error("[DAILY] Error:", e)
          setHasError("Connection failed. Please try again.")
        })

        // Listen for Tavus tool calls via Daily's "app-message"
        call.on("app-message", async (e: any) => {
          const raw = e.data || e;
          if (raw.event_type === "conversation.tool_call") {
            const props = raw.properties || {}
            const conversationId = raw.conversation_id

            console.log("[TAVUS] Raw Tool Call Event:", JSON.stringify(raw))

            const toolName = raw.tool_name || raw.name || props.name || props.tool_name
            const activeToolCallId = raw.tool_call_id || raw.call_id || raw.inference_id || props.tool_call_id || props.inference_id
            let params = raw.parameters || raw.arguments || props.arguments || props.parameters

            console.log("[TAVUS] Extracted:", { toolName, activeToolCallId, params })

            let parsedParams = params;
            if (typeof params === "string") {
              try { parsedParams = JSON.parse(params); } catch (err) { console.error("[TAVUS] Param parse error:", err); }
            }

            // 0. Immediate Echo (Progress Line)
            if (parsedParams.response_to_user && conversationId) {
              call.sendAppMessage({
                message_type: "conversation",
                event_type: "conversation.echo",
                conversation_id: conversationId,
                properties: { text: parsedParams.response_to_user }
              }, "*")
            }

            // 1. Execute the tool
            try {
              let resultJson: string;

              if (toolName === "fly_to_location") {
                // UI Tool: Handle client-side immediately
                console.log("[TAVUS] Handling fly_to_location in client", parsedParams)
                window.dispatchEvent(new CustomEvent("tavus-map-action", {
                  detail: { action: "fly_to_location", params: parsedParams }
                }))
                resultJson = JSON.stringify({ success: true, action: "panning and zooming map" })
              } else if (toolName === "set_forecast_year") {
                // UI Tool: Change the forecast year
                console.log("[TAVUS] Handling set_forecast_year in client", parsedParams)
                window.dispatchEvent(new CustomEvent("tavus-map-action", {
                  detail: { action: "set_forecast_year", params: parsedParams }
                }))
                resultJson = JSON.stringify({ success: true, year: parsedParams.year })
              } else if (toolName === "set_color_mode") {
                // UI Tool: Switch between value/growth view
                console.log("[TAVUS] Handling set_color_mode in client", parsedParams)
                window.dispatchEvent(new CustomEvent("tavus-map-action", {
                  detail: { action: "set_color_mode", params: parsedParams }
                }))
                resultJson = JSON.stringify({ success: true, mode: parsedParams.mode })
              } else if (toolName === "clear_comparison") {
                // UI Tool: Clear comparison overlay only
                console.log("[TAVUS] Handling clear_comparison in client")
                window.dispatchEvent(new CustomEvent("tavus-map-action", {
                  detail: { action: "clear_comparison", params: {} }
                }))
                resultJson = JSON.stringify({ success: true, action: "comparison cleared" })
              } else if (toolName === "end_session") {
                // UI Tool: Close the video call
                console.log("[TAVUS] Handling end_session in client")
                resultJson = JSON.stringify({ success: true, action: "ending session" })
                // Delay leave slightly so the agent can say goodbye first
                setTimeout(() => {
                  if (callRef.current) {
                    callRef.current.leave().catch(() => { })
                  }
                  onClose()
                }, 3000)
              } else {
                // Server Tool: Proxy via Server Action
                console.log(`[TAVUS] Proxying server tool: ${toolName} (forecastMode=${forecastMode})`)
                resultJson = forecastMode
                  ? await executeTopLevelForecastTool(toolName, parsedParams)
                  : await executeTopLevelTool(toolName, parsedParams)
              }

              console.log(`[TAVUS] Tool ${toolName} result:`, resultJson)

              // 2. Dispatch data result to map for secondary visualization (if not fly_to)
              if (toolName !== "fly_to_location") {
                const result = JSON.parse(resultJson)
                window.dispatchEvent(new CustomEvent("tavus-map-action", {
                  detail: { action: toolName, params: parsedParams, result: result }
                }))
              }

              // 3. Send tool result back to the Tavus LLM — only for server-proxied tools
              // Client-side tools (fly_to, set_year, etc.) don't return data the agent needs
              // Use conversation.echo instead of conversation.respond to avoid polluting transcript as user speech
              const clientOnlyTools = ["fly_to_location", "set_forecast_year", "set_color_mode", "clear_comparison", "end_session"]
              if (conversationId && !clientOnlyTools.includes(toolName)) {
                // Summarize the result compactly for the LLM
                const summaryText = resultJson.length > 1500
                  ? resultJson.substring(0, 1500) + "...(truncated)"
                  : resultJson

                call.sendAppMessage({
                  message_type: "conversation",
                  event_type: "conversation.echo",
                  conversation_id: conversationId,
                  properties: {
                    text: `Summarize the result for ${toolName} like a human. Avoid all technical IDs.`,
                    modality: "text",
                    context: summaryText
                  }
                }, "*")
              }
            } catch (err) {
              console.error(`[TAVUS] Tool ${toolName} failed:`, err)
              if (conversationId) {
                call.sendAppMessage({
                  message_type: "conversation",
                  event_type: "conversation.echo",
                  conversation_id: conversationId,
                  properties: { text: `The tool ${toolName} encountered an error. Let the user know briefly.` }
                }, "*")
              }
            }
          }
        })

        // Auto-join immediately — no lobby, no pre-join screen
        await call.join({ url: conversationUrl })
      } catch (err) {
        console.error("[DAILY] Init error:", err)
        if (!cancelled) {
          setHasError("Failed to start video call. Please try again.")
        }
      }
    }

    initCall()

    return () => {
      cancelled = true
      if (callRef.current) {
        callRef.current.leave().catch(() => { })
        callRef.current.destroy()
        callRef.current = null
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [conversationUrl])

  // Attach local video track
  useEffect(() => {
    if (!callRef.current || !isJoined) return
    const local = callRef.current.participants()?.local
    if (local?.tracks?.video?.persistentTrack && localVideoRef.current) {
      localVideoRef.current.srcObject = new MediaStream([local.tracks.video.persistentTrack])
    }
  }, [isJoined, isCamOn])

  // Attach remote video + audio tracks
  useEffect(() => {
    if (!remoteParticipant) return

    if (
      remoteParticipant.tracks?.video?.state === "playable" &&
      remoteParticipant.tracks.video.persistentTrack &&
      remoteVideoRef.current
    ) {
      remoteVideoRef.current.srcObject = new MediaStream([
        remoteParticipant.tracks.video.persistentTrack,
      ])
    }

    if (
      remoteParticipant.tracks?.audio?.state === "playable" &&
      remoteParticipant.tracks.audio.persistentTrack &&
      remoteAudioRef.current
    ) {
      remoteAudioRef.current.srcObject = new MediaStream([
        remoteParticipant.tracks.audio.persistentTrack,
      ])
    }
  }, [remoteParticipant])

  const toggleMic = useCallback(() => {
    if (!callRef.current) return
    callRef.current.setLocalAudio(!isMicOn)
    setIsMicOn(!isMicOn)
  }, [isMicOn])

  const toggleCam = useCallback(() => {
    if (!callRef.current) return
    callRef.current.setLocalVideo(!isCamOn)
    setIsCamOn(!isCamOn)
  }, [isCamOn])

  const handleLeave = useCallback(() => {
    if (callRef.current) {
      callRef.current.leave().catch(() => { })
    }
    onClose()
  }, [onClose])

  return (
    <div
      className={embedded
        ? "h-full w-full overflow-hidden"
        : cn(
          "fixed z-[10001] transition-all duration-300 ease-in-out",
          "bottom-0 left-0 right-0 w-full h-[25vh] rounded-t-xl overflow-hidden",
          "md:bottom-5 md:w-auto md:h-auto md:rounded-2xl",
          chatOpen ? "md:left-[420px]" : "md:left-5",
          isMinimized
            ? "md:w-[280px] md:h-[56px]"
            : "md:w-[340px] md:h-[520px]"
        )}
    >
      <div className="relative w-full h-full rounded-2xl overflow-hidden shadow-2xl border border-white/10 bg-[#0f0f14] flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between px-4 py-2.5 bg-[#16161e] border-b border-white/10 shrink-0">
          <div className="flex items-center gap-2">
            <div className={cn("w-2 h-2 rounded-full", isJoined ? "bg-green-500 animate-pulse" : hasError ? "bg-red-500" : "bg-yellow-500 animate-pulse")} />
            <span className="text-xs font-semibold text-white/90 tracking-wide uppercase">
              {hasError ? "Error" : isJoined ? "Homecastr Live" : "Connecting..."}
            </span>
          </div>
          <div className="flex items-center gap-1">
            <button
              onClick={() => setIsMinimized(!isMinimized)}
              className="p-1.5 rounded-md hover:bg-white/10 transition-colors"
              aria-label={isMinimized ? "Maximize" : "Minimize"}
            >
              {isMinimized ? (
                <Maximize2 className="w-3.5 h-3.5 text-white/60" />
              ) : (
                <Minimize2 className="w-3.5 h-3.5 text-white/60" />
              )}
            </button>
            <button
              onClick={handleLeave}
              className="p-1.5 rounded-md hover:bg-red-500/20 transition-colors"
              aria-label="Leave call"
            >
              <X className="w-3.5 h-3.5 text-white/60 hover:text-red-400" />
            </button>
          </div>
        </div>

        {/* Error state */}
        {!isMinimized && hasError && (
          <div className="flex-1 flex items-center justify-center p-6">
            <div className="text-center space-y-3">
              <div className="text-sm text-red-400 font-medium">{hasError}</div>
              <button
                onClick={handleLeave}
                className="px-4 py-1.5 rounded-lg bg-white/10 hover:bg-white/20 text-xs text-white/70 transition-colors"
              >
                Close
              </button>
            </div>
          </div>
        )}

        {/* Video area — top/bottom split layout */}
        {!isMinimized && !hasError && (
          <div className="flex-1 flex flex-row md:flex-col bg-black relative overflow-hidden">
            {/* Top: Remote participant (Homecastr agent) */}
            <div className="flex-1 relative bg-[#0a0a0f] overflow-hidden">
              {remoteParticipant ? (
                <>
                  <video
                    ref={remoteVideoRef}
                    autoPlay
                    playsInline
                    className="w-full h-full object-cover"
                  />
                  <audio ref={remoteAudioRef} autoPlay playsInline />
                  <div className="absolute bottom-1 left-1 px-1.5 py-0.5 rounded bg-black/60 text-[8px] text-white/70 font-medium">
                    Homecastr Agent
                  </div>
                </>
              ) : (
                <div className="w-full h-full flex items-center justify-center">
                  <div className="flex flex-col items-center gap-2">
                    <div className="w-6 h-6 border-2 border-primary/30 border-t-primary rounded-full animate-spin" />
                    <span className="text-[9px] text-white/40">
                      {isJoined ? "Agent joining..." : "Connecting..."}
                    </span>
                  </div>
                </div>
              )}
            </div>

            {/* Bottom: Local participant (You) */}
            <div className="w-1/3 md:w-full md:h-[120px] relative bg-[#111118] overflow-hidden border-l md:border-l-0 md:border-t border-white/10">
              <video
                ref={localVideoRef}
                autoPlay
                playsInline
                muted
                className={cn(
                  "w-full h-full object-cover",
                  !isCamOn && "hidden"
                )}
              />
              {!isCamOn && (
                <div className="w-full h-full flex items-center justify-center">
                  <div className="w-10 h-10 rounded-full bg-white/10 flex items-center justify-center">
                    <VideoOff className="w-4 h-4 text-white/40" />
                  </div>
                </div>
              )}
              <div className="absolute bottom-1 left-1 px-1.5 py-0.5 rounded bg-black/60 text-[8px] text-white/70 font-medium">
                You
              </div>
            </div>

            {/* Floating controls */}
            <div className="absolute bottom-2 md:bottom-[130px] left-1/2 -translate-x-1/2 flex items-center gap-2 px-3 py-1.5 rounded-full bg-black/70 backdrop-blur-sm border border-white/10">
              <button
                onClick={toggleMic}
                className={cn(
                  "p-2 rounded-full transition-colors",
                  isMicOn ? "bg-white/10 hover:bg-white/20" : "bg-red-500/80 hover:bg-red-500"
                )}
                aria-label={isMicOn ? "Mute" : "Unmute"}
              >
                {isMicOn ? (
                  <Mic className="w-3.5 h-3.5 text-white" />
                ) : (
                  <MicOff className="w-3.5 h-3.5 text-white" />
                )}
              </button>
              <button
                onClick={toggleCam}
                className={cn(
                  "p-2 rounded-full transition-colors",
                  isCamOn ? "bg-white/10 hover:bg-white/20" : "bg-red-500/80 hover:bg-red-500"
                )}
                aria-label={isCamOn ? "Camera off" : "Camera on"}
              >
                {isCamOn ? (
                  <Video className="w-3.5 h-3.5 text-white" />
                ) : (
                  <VideoOff className="w-3.5 h-3.5 text-white" />
                )}
              </button>
              <button
                onClick={handleLeave}
                className="p-2 rounded-full bg-red-600 hover:bg-red-500 transition-colors"
                aria-label="End call"
              >
                <X className="w-3.5 h-3.5 text-white" />
              </button>
            </div>
          </div>
        )}

        {/* Minimized state */}
        {isMinimized && (
          <div
            className="flex-1 flex items-center px-4 cursor-pointer"
            onClick={() => setIsMinimized(false)}
          >
            <div className="flex items-center gap-2">
              <div className={cn("w-2 h-2 rounded-full", isJoined ? "bg-green-500 animate-pulse" : "bg-yellow-500 animate-pulse")} />
              <span className="text-xs text-white/50">
                Call in progress — click to expand
              </span>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
