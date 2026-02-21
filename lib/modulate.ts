/**
 * Modulate.ai Voice Intelligence client.
 *
 * Wraps Modulate's API for:
 *  - Conversation analysis (emotion, stress, off-script, toxicity)
 *  - Deepfake / synthetic voice detection
 *  - Post-call quality scoring
 *
 * All methods are server-only (use in API routes / server actions).
 */

const MODULATE_BASE_URL = "https://api.modulate.ai/v1"

export interface ModulateAnalysisRequest {
  conversation_id: string
  transcript: { role: string; content: string }[]
  audio_url?: string
  metadata?: Record<string, unknown>
}

export interface EmotionScore {
  timestamp?: string
  emotion: string
  confidence: number
}

export interface SafetyFlag {
  category: string
  severity: "low" | "medium" | "high" | "critical"
  description: string
  timestamp?: string
}

export interface ModulateAnalysisResult {
  conversation_id: string
  overall_safety_score: number
  compliance_score: number
  emotion_timeline: EmotionScore[]
  safety_flags: SafetyFlag[]
  deepfake_detection: {
    is_synthetic: boolean
    confidence: number
  }
  off_script_events: {
    detected: boolean
    instances: { timestamp?: string; description: string }[]
  }
  summary: string
  raw_response?: unknown
}

function getApiKey(): string {
  const key = process.env.MODULATE_API_KEY
  if (!key) throw new Error("MODULATE_API_KEY is not configured")
  return key
}

function headers(): Record<string, string> {
  return {
    "Content-Type": "application/json",
    Authorization: `Bearer ${getApiKey()}`,
    "X-Api-Key": getApiKey(),
  }
}

/**
 * Analyze a completed conversation transcript via Modulate Voice Intelligence.
 */
export async function analyzeConversation(
  req: ModulateAnalysisRequest
): Promise<ModulateAnalysisResult> {
  const payload = {
    conversation_id: req.conversation_id,
    transcript: req.transcript,
    audio_url: req.audio_url,
    analysis_types: [
      "emotion_detection",
      "toxicity_detection",
      "off_script_detection",
      "deepfake_detection",
      "compliance_check",
    ],
    metadata: req.metadata ?? {},
  }

  try {
    const res = await fetch(`${MODULATE_BASE_URL}/analyze`, {
      method: "POST",
      headers: headers(),
      body: JSON.stringify(payload),
    })

    if (res.ok) {
      const data = await res.json()
      return mapApiResponse(req.conversation_id, data)
    }

    // If the main endpoint isn't available (early-access / enterprise-only),
    // fall back to local analysis using the transcript directly.
    console.warn(
      `[Modulate] API returned ${res.status}, falling back to local transcript analysis`
    )
    return analyzeTranscriptLocally(req)
  } catch (err) {
    console.warn("[Modulate] API unreachable, falling back to local analysis:", err)
    return analyzeTranscriptLocally(req)
  }
}

/**
 * Check a single audio segment for deepfake / synthetic voice.
 */
export async function detectDeepfake(audioUrl: string): Promise<{
  is_synthetic: boolean
  confidence: number
}> {
  try {
    const res = await fetch(`${MODULATE_BASE_URL}/deepfake-detect`, {
      method: "POST",
      headers: headers(),
      body: JSON.stringify({ audio_url: audioUrl }),
    })

    if (res.ok) {
      const data = await res.json()
      return {
        is_synthetic: data.is_synthetic ?? false,
        confidence: data.confidence ?? 0,
      }
    }
  } catch (err) {
    console.warn("[Modulate] Deepfake detection unavailable:", err)
  }

  return { is_synthetic: false, confidence: 0 }
}

/**
 * Submit real-time audio stream URL for live monitoring.
 * Returns a session ID that can be polled for live alerts.
 */
export async function startLiveMonitoring(params: {
  conversation_id: string
  stream_url: string
  webhook_url?: string
}): Promise<{ session_id: string } | null> {
  try {
    const res = await fetch(`${MODULATE_BASE_URL}/monitor/start`, {
      method: "POST",
      headers: headers(),
      body: JSON.stringify({
        conversation_id: params.conversation_id,
        stream_url: params.stream_url,
        webhook_url: params.webhook_url,
        detection_types: ["emotion", "toxicity", "off_script", "deepfake"],
      }),
    })

    if (res.ok) {
      return await res.json()
    }
    console.warn(`[Modulate] Live monitoring returned ${res.status}`)
  } catch (err) {
    console.warn("[Modulate] Live monitoring unavailable:", err)
  }
  return null
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

function mapApiResponse(
  conversationId: string,
  data: any
): ModulateAnalysisResult {
  return {
    conversation_id: conversationId,
    overall_safety_score: data.safety_score ?? data.overall_score ?? 1.0,
    compliance_score: data.compliance_score ?? 1.0,
    emotion_timeline: (data.emotions ?? []).map((e: any) => ({
      timestamp: e.timestamp,
      emotion: e.label ?? e.emotion,
      confidence: e.confidence ?? e.score ?? 0,
    })),
    safety_flags: (data.flags ?? data.safety_flags ?? []).map((f: any) => ({
      category: f.category ?? f.type,
      severity: f.severity ?? "low",
      description: f.description ?? f.message ?? "",
      timestamp: f.timestamp,
    })),
    deepfake_detection: {
      is_synthetic: data.deepfake?.is_synthetic ?? false,
      confidence: data.deepfake?.confidence ?? 0,
    },
    off_script_events: {
      detected: (data.off_script?.instances?.length ?? 0) > 0,
      instances: data.off_script?.instances ?? [],
    },
    summary: data.summary ?? "Analysis complete.",
    raw_response: data,
  }
}

/**
 * Lightweight local analysis when the Modulate API isn't reachable.
 * Extracts basic signals from the transcript text itself.
 */
function analyzeTranscriptLocally(
  req: ModulateAnalysisRequest
): ModulateAnalysisResult {
  const messages = req.transcript.filter(
    (m) => m.role === "user" || m.role === "assistant"
  )

  const userMessages = messages.filter((m) => m.role === "user")
  const assistantMessages = messages.filter((m) => m.role === "assistant")

  // Simple keyword-based emotion heuristic
  const emotions: EmotionScore[] = []
  const flags: SafetyFlag[] = []

  const negativePatterns = [
    /\b(angry|frustrated|annoyed|upset|terrible|horrible|worst|hate)\b/i,
    /\b(scam|fraud|lie|lying|cheat|fake)\b/i,
  ]
  const positivePatterns = [
    /\b(great|excellent|wonderful|amazing|love|perfect|fantastic)\b/i,
    /\b(thank|thanks|helpful|appreciate)\b/i,
  ]
  const offScriptPatterns = [
    /\b(ignore.*instructions|forget.*rules|pretend|act as|jailbreak)\b/i,
    /\b(guaranteed.*return|no.*risk|can't lose|100%)\b/i,
  ]

  const offScriptInstances: { description: string }[] = []

  for (const msg of messages) {
    const text = msg.content

    for (const pat of negativePatterns) {
      if (pat.test(text)) {
        emotions.push({ emotion: "negative", confidence: 0.7 })
        if (msg.role === "user") {
          flags.push({
            category: "user_frustration",
            severity: "medium",
            description: `User expressed negative sentiment: "${text.slice(0, 80)}..."`,
          })
        }
      }
    }
    for (const pat of positivePatterns) {
      if (pat.test(text)) {
        emotions.push({ emotion: "positive", confidence: 0.7 })
      }
    }
    for (const pat of offScriptPatterns) {
      if (pat.test(text)) {
        if (msg.role === "assistant") {
          offScriptInstances.push({
            description: `Agent may have gone off-script: "${text.slice(0, 100)}..."`,
          })
          flags.push({
            category: "off_script",
            severity: "high",
            description: `Potential compliance issue in agent response`,
          })
        } else {
          offScriptInstances.push({
            description: `User attempted prompt injection: "${text.slice(0, 100)}..."`,
          })
          flags.push({
            category: "prompt_injection_attempt",
            severity: "medium",
            description: `User may be trying to manipulate the agent`,
          })
        }
      }
    }
  }

  if (emotions.length === 0) {
    emotions.push({ emotion: "neutral", confidence: 0.8 })
  }

  const safetyScore = Math.max(0, 1 - flags.length * 0.15)

  return {
    conversation_id: req.conversation_id,
    overall_safety_score: Math.round(safetyScore * 100) / 100,
    compliance_score: offScriptInstances.length === 0 ? 1.0 : 0.6,
    emotion_timeline: emotions,
    safety_flags: flags,
    deepfake_detection: { is_synthetic: false, confidence: 0 },
    off_script_events: {
      detected: offScriptInstances.length > 0,
      instances: offScriptInstances,
    },
    summary: `Analyzed ${messages.length} messages (${userMessages.length} user, ${assistantMessages.length} agent). Found ${flags.length} flag(s). Overall safety: ${Math.round(safetyScore * 100)}%.`,
    raw_response: { source: "local_fallback" },
  }
}
